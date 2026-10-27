"""lambdaflow's engine: DAG files in S3, all state in one DynamoDB table, Airflow's scheduling rules.

    S3   dags/<file>.py                       the DAG files (same layout as MWAA)
    DynamoDB (pk, sk):
         dag#<dag_id>          meta           what the planner needs: tasks, upstream ids, trigger rules, retries
         dag#<dag_id>          run#<run_id>   one DAG run: state, logical date, conf
         run#<dag_id>#<run_id> ti#<task_id>   one task instance: state, try number, start / end, next try
         run#<dag_id>#<run_id> xcom#<task>#<key>   one XCom value (JSON)
         err                   <file>         the import error of a DAG file

One DAG run is a loop of three steps — the Step Functions state machine in AWS, run_local() on a laptop:
    step()      plan: apply trigger rules (skip / upstream_failed), return the tasks that can run now
    run_task()  execute one task (the worker Lambda), write its state and XCom
    wait        when only retries are pending, wait until the earliest one is due
"""
import copy
import datetime
import json
import os
import time
import traceback
import types
import uuid

import boto3
import jinja2
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

TERMINAL = {"success", "failed", "skipped", "upstream_failed"}


def _now():
    return time.time()


def _iso(t):
    return datetime.datetime.fromtimestamp(t, datetime.timezone.utc).isoformat().replace("+00:00", "Z")


def _date(s):
    return datetime.datetime.fromisoformat(s.replace("Z", "+00:00"))


def evaluate(rule, upstream_states):
    """Airflow's trigger-rule decision for a task that has not started: run | wait | skipped | upstream_failed."""
    n = len(upstream_states)
    if n == 0 or rule == "always":
        return "run"
    s, sk = upstream_states.count("success"), upstream_states.count("skipped")
    f, uf = upstream_states.count("failed"), upstream_states.count("upstream_failed")
    done = s + sk + f + uf == n
    if rule == "all_success":
        return "upstream_failed" if f or uf else "skipped" if sk else "run" if s == n else "wait"
    if rule == "all_failed":
        return "skipped" if s or sk else "run" if f + uf == n else "wait"
    if rule == "all_done":
        return "run" if done else "wait"
    if rule == "all_skipped":
        return "skipped" if s or f or uf else "run" if sk == n else "wait"
    if rule == "one_success":
        return "run" if s else ("skipped" if sk == n else "upstream_failed") if done else "wait"
    if rule == "one_failed":
        return "run" if f or uf else "skipped" if done else "wait"
    if rule == "one_done":
        return "run" if s or f else "skipped" if done else "wait"
    if rule == "none_failed":
        return "upstream_failed" if f or uf else "run" if done else "wait"
    if rule == "none_failed_min_one_success":
        return "upstream_failed" if f or uf else ("skipped" if sk == n else "run") if done else "wait"
    if rule == "none_skipped":
        return "skipped" if sk else "run" if done else "wait"
    raise ValueError(f"unknown trigger rule {rule!r}")


def _macros():
    def ds_add(ds, days):
        return (datetime.date.fromisoformat(ds) + datetime.timedelta(days=int(days))).isoformat()

    def ds_format(ds, input_format, output_format):
        return datetime.datetime.strptime(ds, input_format).strftime(output_format)
    return types.SimpleNamespace(ds_add=ds_add, ds_format=ds_format, datetime=datetime.datetime,
                                 timedelta=datetime.timedelta, json=json, time=time, uuid=uuid)


_JINJA = jinja2.Environment()


def render(value, context):
    """Render Jinja templates inside strings, lists, tuples and dicts (a template field's value)."""
    if isinstance(value, str):
        return _JINJA.from_string(value).render(**context) if "{{" in value or "{%" in value else value
    if isinstance(value, (list, tuple)):
        return type(value)(render(v, context) for v in value)
    if isinstance(value, dict):
        return {k: render(v, context) for k, v in value.items()}
    return value


class Engine:
    def __init__(self, table_name, bucket, db=None, s3=None):
        self.db = db or boto3.resource("dynamodb")
        self.table = self.db.Table(table_name)
        self.s3 = s3 or boto3.client("s3")
        self.bucket = bucket
        self._modules = {}   # S3 key -> (ETag, {dag_id: DAG}): a warm worker imports a DAG file once

    # ---------------- DAG files ----------------

    def put_dag_file(self, name, source):
        key = "dags/" + name
        self.s3.put_object(Bucket=self.bucket, Key=key, Body=source.encode())
        return self.parse(key)

    def _dags_in(self, key):
        """Import the DAG file (lambdaflow's `airflow` package is on the path) and return its DAGs."""
        obj = self.s3.get_object(Bucket=self.bucket, Key=key)
        etag = obj["ETag"]
        cached = self._modules.get(key)
        if cached and cached[0] == etag:
            return cached[1]
        import airflow.sdk as sdk
        path = os.path.join(os.environ.get("LAMBDAFLOW_CACHE", "/tmp/lambdaflow"), key)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        source = obj["Body"].read().decode()
        with open(path, "w", encoding="utf-8") as f:   # so tracebacks show the DAG file's lines
            f.write(source)
        module = types.ModuleType(os.path.splitext(os.path.basename(key))[0])
        module.__file__ = path
        del sdk.LOADED[:]
        try:
            exec(compile(source, path, "exec"), module.__dict__)
            dags = {d.dag_id: d for d in sdk.LOADED}
        finally:
            del sdk.LOADED[:]
        self._modules[key] = (etag, dags)
        return dags

    def parse(self, key):
        """Read a DAG file into the table (one meta item per DAG); an import error is stored, not raised."""
        try:
            dags = self._dags_in(key)
        except Exception as e:  # noqa: BLE001 - any import error is the user's, shown in the console
            self.table.put_item(Item={"pk": "err", "sk": key, "error": f"{type(e).__name__}: {e}",
                                      "trace": traceback.format_exc()[-4000:]})
            return []
        self.table.delete_item(Key={"pk": "err", "sk": key})
        for d in dags.values():
            self.table.put_item(Item={"pk": f"dag#{d.dag_id}", "sk": "meta", "file": key,
                                      "spec": json.dumps(self._spec(d), default=str)})
        return sorted(dags)

    @staticmethod
    def _spec(d):
        return {"dag_id": d.dag_id, "schedule": d.schedule, "params": d.params, "tags": d.tags,
                "description": d.description, "max_active_runs": d.max_active_runs,
                "tasks": {t.task_id: {"upstream": sorted(t.upstream_task_ids), "trigger_rule": t.trigger_rule,
                                      "retries": t.retries, "retry_delay": t.retry_delay,
                                      "operator": type(t).__name__} for t in d.tasks.values()}}

    def spec(self, dag_id):
        item = self.table.get_item(Key={"pk": f"dag#{dag_id}", "sk": "meta"}, ConsistentRead=True).get("Item")
        if not item:
            raise KeyError(f"no DAG {dag_id!r}")
        return json.loads(item["spec"]), item["file"]

    # ---------------- runs ----------------

    def trigger(self, dag_id, logical_date=None, conf=None, run_id=None):
        """Create a DAG run; a run id that exists already is not created twice (at-least-once triggers)."""
        self.spec(dag_id)
        logical_date = logical_date or _iso(_now())
        run_id = run_id or f"manual__{logical_date}"
        try:
            self.table.put_item(Item={"pk": f"dag#{dag_id}", "sk": f"run#{run_id}", "state": "queued",
                                      "logical_date": logical_date, "conf": json.dumps(conf or {}), "start": _iso(_now())},
                                ConditionExpression="attribute_not_exists(sk)")
        except ClientError as e:
            if e.response["Error"]["Code"] != "ConditionalCheckFailedException":
                raise
        return run_id

    def _run(self, dag_id, run_id):
        return self.table.get_item(Key={"pk": f"dag#{dag_id}", "sk": f"run#{run_id}"}, ConsistentRead=True)["Item"]

    def _tis(self, dag_id, run_id):
        items, kw = [], {"KeyConditionExpression": Key("pk").eq(f"run#{dag_id}#{run_id}") & Key("sk").begins_with("ti#"),
                         "ConsistentRead": True}
        while True:
            r = self.table.query(**kw)
            items += r["Items"]
            if "LastEvaluatedKey" not in r:
                return {i["sk"][3:]: i for i in items}
            kw["ExclusiveStartKey"] = r["LastEvaluatedKey"]

    def _set_ti(self, dag_id, run_id, task_id, only_if_new=False, **attrs):
        """Write a task instance's fields; only_if_new: only if nothing has decided its state yet."""
        names = {f"#a{i}": k for i, k in enumerate(attrs)}
        vals = {f":v{i}": v for i, v in enumerate(attrs.values())}
        kw = {"Key": {"pk": f"run#{dag_id}#{run_id}", "sk": f"ti#{task_id}"},
              "UpdateExpression": "SET " + ", ".join(f"#a{i} = :v{i}" for i in range(len(attrs))),
              "ExpressionAttributeNames": names, "ExpressionAttributeValues": vals}
        if only_if_new:
            kw["ConditionExpression"] = "attribute_not_exists(#s)"
            names["#s"] = "state"
        try:
            self.table.update_item(**kw)
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                return False
            raise

    def step(self, dag_id, run_id):
        """Plan one round: {"tasks": [...]} to run now, {"wait": seconds} until a retry is due, or {"done", "state"}."""
        spec, _ = self.spec(dag_id)
        tasks = spec["tasks"]
        run = self._run(dag_id, run_id)
        if run["state"] in ("success", "failed"):
            return {"done": True, "state": run["state"]}
        if run["state"] == "queued":
            self.table.update_item(Key={"pk": f"dag#{dag_id}", "sk": f"run#{run_id}"},
                                   UpdateExpression="SET #s = :r", ExpressionAttributeNames={"#s": "state"},
                                   ExpressionAttributeValues={":r": "running"})
        tis = self._tis(dag_id, run_id)
        state = lambda t: tis.get(t, {}).get("state")  # noqa: E731
        changed = True
        while changed:   # skipped / upstream_failed cascade through the graph
            changed = False
            for t, info in tasks.items():
                if state(t) is None:
                    decision = evaluate(info["trigger_rule"], [state(u) for u in info["upstream"]])
                    if decision in ("skipped", "upstream_failed"):
                        self._set_ti(dag_id, run_id, t, only_if_new=True, state=decision, try_number=0)
                        tis[t] = {"state": decision, "try_number": 0}
                        changed = True
        now = _now()
        ready = [t for t, info in tasks.items() if state(t) is None
                 and evaluate(info["trigger_rule"], [state(u) for u in info["upstream"]]) == "run"]
        ready += [t for t in tasks if state(t) == "up_for_retry" and float(tis[t].get("next_try", 0)) <= now]
        for t in ready:
            self._set_ti(dag_id, run_id, t, state="queued")
        if ready:
            return {"tasks": ready}
        if all(state(t) in TERMINAL for t in tasks):
            leaves = [t for t in tasks if not any(t in i["upstream"] for i in tasks.values())]
            final = "failed" if any(state(t) in ("failed", "upstream_failed") for t in leaves) else "success"
            self.table.update_item(Key={"pk": f"dag#{dag_id}", "sk": f"run#{run_id}"},
                                   UpdateExpression="SET #s = :s, #e = :e", ExpressionAttributeNames={"#s": "state", "#e": "end"},
                                   ExpressionAttributeValues={":s": final, ":e": _iso(_now())})
            return {"done": True, "state": final}
        retries = [float(tis[t]["next_try"]) for t in tasks if state(t) == "up_for_retry"]
        return {"wait": max(1, int(min(retries) - now + 0.999)) if retries else 1}

    # ---------------- one task ----------------

    def run_task(self, dag_id, run_id, task_id):
        spec, key = self.spec(dag_id)
        op = self._dags_in(key)[dag_id].tasks[task_id]
        run = self._run(dag_id, run_id)
        prev = self._tis(dag_id, run_id).get(task_id, {})
        try_number = int(prev.get("try_number", 0)) + 1
        start = _now()
        self._set_ti(dag_id, run_id, task_id, state="running", try_number=try_number, start=_iso(start))
        ti = TaskInstance(self, spec, dag_id, run_id, task_id, try_number)
        context = self._context(op, run, ti)
        from airflow.exceptions import AirflowFailException, AirflowSkipException
        try:
            task = copy.copy(op)   # render templates on a copy: the imported DAG is reused by the next task
            for field in op.template_fields:
                setattr(task, field, render(getattr(op, field), context))
            context["task"] = task
            result = task.execute(context)
            if result is not None and op.pushes_return_value and op.do_xcom_push:
                if op.multiple_outputs and isinstance(result, dict):
                    for k, v in result.items():
                        ti.xcom_push(k, v)
                ti.xcom_push("return_value", result)
            final = "success"
        except AirflowSkipException:
            final = "skipped"
        except AirflowFailException:
            traceback.print_exc()
            final = "failed"
        except Exception:  # noqa: BLE001 - a task failure, retried if retries are left
            traceback.print_exc()
            final = "up_for_retry" if try_number <= op.retries else "failed"
        attrs = {"state": final, "end": _iso(_now())}
        if final == "up_for_retry":
            attrs["next_try"] = str(_now() + op.retry_delay)
        self._set_ti(dag_id, run_id, task_id, **attrs)
        return final

    def _context(self, op, run, ti):
        ld = _date(run["logical_date"])
        conf = json.loads(run.get("conf") or "{}")
        params = dict(op.dag.params, **conf)   # [core] dag_run_conf_overrides_params = True
        dag_run = types.SimpleNamespace(dag_id=ti.dag_id, run_id=ti.run_id, conf=conf, logical_date=ld)
        return {"ti": ti, "task_instance": ti, "task": op, "dag": op.dag, "dag_run": dag_run, "run_id": ti.run_id,
                "logical_date": ld, "data_interval_start": ld, "data_interval_end": ld,
                "ds": ld.strftime("%Y-%m-%d"), "ds_nodash": ld.strftime("%Y%m%d"),
                "ts": ld.isoformat(), "ts_nodash": ld.strftime("%Y%m%dT%H%M%S"),
                "params": params, "macros": _macros(), "try_number": ti.try_number}

    # ---------------- reading back ----------------

    def xcom(self, dag_id, run_id, task_id, key="return_value"):
        item = self.table.get_item(Key={"pk": f"run#{dag_id}#{run_id}", "sk": f"xcom#{task_id}#{key}"},
                                   ConsistentRead=True).get("Item")
        return json.loads(item["value"]) if item else None

    def run_state(self, dag_id, run_id):
        """The run, its task instances and its XComs (what the console and the compatibility tests read)."""
        run = self._run(dag_id, run_id)
        items, kw = [], {"KeyConditionExpression": Key("pk").eq(f"run#{dag_id}#{run_id}"), "ConsistentRead": True}
        while True:
            r = self.table.query(**kw)
            items += r["Items"]
            if "LastEvaluatedKey" not in r:
                break
            kw["ExclusiveStartKey"] = r["LastEvaluatedKey"]
        tis = {i["sk"][3:]: i for i in items if i["sk"].startswith("ti#")}
        xcom = {}
        for i in items:
            if i["sk"].startswith("xcom#"):
                task, key = i["sk"][5:].split("#", 1)
                xcom[f"{task}.{key}"] = json.loads(i["value"])
        return run, tis, xcom


class TaskInstance:
    """The `ti` a task sees: xcom_pull / xcom_push, try_number, and skipping for the branch operators."""

    def __init__(self, engine, spec, dag_id, run_id, task_id, try_number):
        self.engine, self.spec = engine, spec
        self.dag_id, self.run_id, self.task_id, self.try_number = dag_id, run_id, task_id, try_number
        self.map_index = -1

    def xcom_push(self, key, value):
        self.engine.table.put_item(Item={"pk": f"run#{self.dag_id}#{self.run_id}", "sk": f"xcom#{self.task_id}#{key}",
                                         "value": json.dumps(value)})

    def xcom_pull(self, task_ids=None, key="return_value", default=None, **_kw):
        if task_ids is None:
            task_ids = self.task_id
        if isinstance(task_ids, str):
            v = self.engine.xcom(self.dag_id, self.run_id, task_ids, key)
            return default if v is None else v
        return [self.engine.xcom(self.dag_id, self.run_id, t, key) for t in task_ids]

    def skip_downstream(self, follow=None, all_descendants=False):
        tasks = self.spec["tasks"]
        down = lambda t: [d for d, i in tasks.items() if t in i["upstream"]]  # noqa: E731
        if all_descendants:
            skip, todo = set(), down(self.task_id)
            while todo:
                t = todo.pop()
                if t not in skip:
                    skip.add(t)
                    todo += down(t)
        else:
            skip = {t for t in down(self.task_id) if t not in (follow or set())}
        for t in sorted(skip):
            self.engine._set_ti(self.dag_id, self.run_id, t, only_if_new=True, state="skipped", try_number=0)


def run_local(engine, dag_id, logical_date=None, conf=None, sleep=time.sleep):
    """One DAG run on this machine: the state machine's loop, calling the same functions in-process."""
    run_id = engine.trigger(dag_id, logical_date, conf)
    while True:
        s = engine.step(dag_id, run_id)
        if s.get("done"):
            return run_id
        for t in s.get("tasks", []):
            engine.run_task(dag_id, run_id, t)
        if s.get("wait"):
            sleep(s["wait"])
