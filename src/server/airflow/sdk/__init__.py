"""airflow.sdk: DAG, @dag, @task, BaseOperator — the authoring API of Airflow 3.

A DAG file is imported by lambdaflow's worker; every DAG it creates is collected in LOADED. Operators
record what the planner needs (task id, upstream ids, trigger rule, retries) and keep their Python
callables for the worker that executes them. Unknown keyword arguments raise NotSupportedError.
"""
import datetime
import functools
import inspect

from airflow.exceptions import NotSupportedError

LOADED = []      # DAGs created while importing a DAG file (the worker reads and clears it)
_ACTIVE = []     # `with DAG(...)` / @dag stack

TRIGGER_RULES = {"all_success", "all_failed", "all_done", "all_skipped", "one_success", "one_failed", "one_done",
                 "none_failed", "none_failed_min_one_success", "none_skipped", "always"}


def _refuse(what, kwargs, allowed):
    extra = sorted(set(kwargs) - set(allowed))
    if extra:
        raise NotSupportedError(f"{what}: {', '.join(extra)} not supported by lambdaflow (yet) — "
                                "see tests/compat/REPORT.md; move to Airflow / MWAA for it")


class DAG:
    ALLOWED = {"description", "schedule", "start_date", "end_date", "catchup", "default_args", "params", "tags",
               "max_active_runs", "doc_md"}

    def __init__(self, dag_id, **kwargs):
        _refuse(f"DAG({dag_id})", kwargs, self.ALLOWED)
        self.dag_id = dag_id
        self.description = kwargs.get("description")
        self.schedule = kwargs.get("schedule")
        self.start_date = kwargs.get("start_date")
        self.end_date = kwargs.get("end_date")
        self.catchup = kwargs.get("catchup", False)
        self.default_args = dict(kwargs.get("default_args") or {})
        self.params = dict(kwargs.get("params") or {})
        self.tags = list(kwargs.get("tags") or [])
        self.max_active_runs = kwargs.get("max_active_runs", 16)
        self.tasks = {}
        LOADED.append(self)

    def __enter__(self):
        _ACTIVE.append(self)
        return self

    def __exit__(self, *exc):
        _ACTIVE.pop()

    def add_task(self, op):
        if op.task_id in self.tasks:
            raise ValueError(f"task id {op.task_id!r} is used twice in DAG {self.dag_id!r}")
        self.tasks[op.task_id] = op

    def unique_task_id(self, base):
        """@task called twice: the second is base__1, as in Airflow."""
        if base not in self.tasks:
            return base
        n = 1
        while f"{base}__{n}" in self.tasks:
            n += 1
        return f"{base}__{n}"


def dag(dag_id=None, **dag_kwargs):
    """@dag(...) on a function: calling the function builds the DAG (dag_id = the function name)."""
    def wrap(fn):
        @functools.wraps(fn)
        def build(*args, **kwargs):
            with DAG(dag_id or fn.__name__, **dag_kwargs) as d:
                fn(*args, **kwargs)
            return d
        return build
    if callable(dag_id):           # @dag without parentheses
        fn, dag_id = dag_id, None
        return wrap(fn)
    return wrap


def _deps(x):
    return x.operator if isinstance(x, XComArg) else x


class _Chain:
    """`a >> b`, `a >> [b, c]`, `[a, b] >> c`: upstream / downstream bookkeeping."""

    def _link(self, other, down):
        for o in other if isinstance(other, (list, tuple)) else [other]:
            o = _deps(o)
            if down:
                o.upstream_task_ids.add(_deps(self).task_id)
            else:
                _deps(self).upstream_task_ids.add(o.task_id)
        return other

    def __rshift__(self, other):
        return self._link(other, True)

    def __lshift__(self, other):
        return self._link(other, False)

    def __rrshift__(self, other):        # [a, b] >> self
        for o in other:
            _deps(self).upstream_task_ids.add(_deps(o).task_id)
        return self

    def __rlshift__(self, other):        # [a, b] << self
        for o in other:
            _deps(o).upstream_task_ids.add(_deps(self).task_id)
        return self


class BaseOperator(_Chain):
    ALLOWED = {"task_id", "dag", "trigger_rule", "retries", "retry_delay", "owner", "doc_md", "multiple_outputs",
               "do_xcom_push"}
    template_fields = ()
    pushes_return_value = True       # Branch / ShortCircuit operators keep their result out of XCom

    def __init__(self, task_id, **kwargs):
        d = kwargs.pop("dag", None) or (_ACTIVE[-1] if _ACTIVE else None)
        if d is None:
            raise ValueError(f"task {task_id!r} is not inside a DAG")
        args = dict(d.default_args, **kwargs)
        _refuse(f"{type(self).__name__}({task_id})", args, self.ALLOWED | set(self.EXTRA))
        self.task_id = task_id
        self.dag = d
        self.trigger_rule = args.get("trigger_rule", "all_success")
        if self.trigger_rule not in TRIGGER_RULES:
            raise ValueError(f"unknown trigger_rule {self.trigger_rule!r}")
        self.retries = int(args.get("retries", 0))
        delay = args.get("retry_delay", datetime.timedelta(seconds=300))
        self.retry_delay = delay.total_seconds() if isinstance(delay, datetime.timedelta) else float(delay)
        self.multiple_outputs = bool(args.get("multiple_outputs", False))
        self.do_xcom_push = bool(args.get("do_xcom_push", True))
        self.upstream_task_ids = set()
        self.args = args
        d.add_task(self)

    EXTRA = ()   # operator-specific keyword arguments

    @property
    def output(self):
        return XComArg(self)

    def execute(self, context):
        return None


class XComArg(_Chain):
    """The future result of a task (what a @task call returns); `x["key"]` is one key of multiple_outputs."""

    def __init__(self, operator, key="return_value"):
        self.operator, self.key = operator, key

    def __getitem__(self, key):
        return XComArg(self.operator, key)

    def resolve(self, ti):
        return ti.xcom_pull(task_ids=self.operator.task_id, key=self.key)


def resolve(value, ti):
    """Replace XComArgs (also inside lists / dicts) by their values."""
    if isinstance(value, XComArg):
        return value.resolve(ti)
    if isinstance(value, (list, tuple)):
        return type(value)(resolve(v, ti) for v in value)
    if isinstance(value, dict):
        return {k: resolve(v, ti) for k, v in value.items()}
    return value


def _xcom_args(value):
    if isinstance(value, XComArg):
        yield value
    elif isinstance(value, (list, tuple)):
        for v in value:
            yield from _xcom_args(v)
    elif isinstance(value, dict):
        for v in value.values():
            yield from _xcom_args(v)


def call_with_context(fn, args, kwargs, context):
    """Airflow's rule: a callable with **kwargs gets the whole context, otherwise the context keys it names."""
    sig = inspect.signature(fn)
    if any(p.kind == p.VAR_KEYWORD for p in sig.parameters.values()):
        return fn(*args, **dict(context, **kwargs))
    bound = sig.bind_partial(*args, **kwargs).arguments
    extra = {n: context[n] for n in sig.parameters if n not in bound and n in context}
    return fn(*args, **dict(kwargs, **extra))


class _TaskDecorator:
    """@task, @task(...), @task.branch, @task.short_circuit."""

    def __init__(self, kind="python", **opts):
        self.kind, self.opts = kind, opts

    def __call__(self, fn=None, **opts):
        if fn is None:                     # @task(multiple_outputs=True)
            return _TaskDecorator(self.kind, **dict(self.opts, **opts))

        @functools.wraps(fn)
        def make(*args, **kwargs):
            from airflow.providers.standard.operators.python import (BranchPythonOperator, PythonOperator,
                                                                    ShortCircuitOperator)
            cls = {"python": PythonOperator, "branch": BranchPythonOperator, "short_circuit": ShortCircuitOperator}[self.kind]
            d = _ACTIVE[-1] if _ACTIVE else None
            if d is None:
                raise ValueError(f"@task {fn.__name__} called outside a DAG")
            opts = dict(self.opts)
            task_id = d.unique_task_id(opts.pop("task_id", fn.__name__))
            op = cls(task_id=task_id, python_callable=fn, op_args=list(args), op_kwargs=kwargs, **opts)
            for x in _xcom_args([args, kwargs]):
                op.upstream_task_ids.add(x.operator.task_id)
            return XComArg(op)
        return make

    @property
    def branch(self):
        return _TaskDecorator("branch")

    @property
    def short_circuit(self):
        return _TaskDecorator("short_circuit")


task = _TaskDecorator()
