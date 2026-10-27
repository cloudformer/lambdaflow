"""PythonOperator, BranchPythonOperator, ShortCircuitOperator (airflow.providers.standard.operators.python)."""
from airflow.sdk import BaseOperator, call_with_context, resolve


class PythonOperator(BaseOperator):
    EXTRA = ("python_callable", "op_args", "op_kwargs", "templates_dict")
    template_fields = ("templates_dict", "op_args", "op_kwargs")

    def __init__(self, task_id, python_callable, op_args=None, op_kwargs=None, templates_dict=None, **kwargs):
        if not callable(python_callable):
            raise TypeError("python_callable must be callable")
        super().__init__(task_id, **kwargs)
        self.python_callable = python_callable
        self.op_args = list(op_args or [])
        self.op_kwargs = dict(op_kwargs or {})
        self.templates_dict = templates_dict

    def execute(self, context):
        ti = context["ti"]
        if self.templates_dict is not None:
            context = dict(context, templates_dict=self.templates_dict)
        return call_with_context(self.python_callable, resolve(self.op_args, ti), resolve(self.op_kwargs, ti), context)


class BranchPythonOperator(PythonOperator):
    """The callable returns the task id (or list of ids) to follow; the other direct downstream tasks are skipped."""
    pushes_return_value = False

    def execute(self, context):
        chosen = super().execute(context)
        chosen = {chosen} if isinstance(chosen, str) else set(chosen or [])
        context["ti"].skip_downstream(follow=chosen)
        return chosen


class ShortCircuitOperator(PythonOperator):
    """A falsy result skips every task downstream (ignore_downstream_trigger_rules=True, the default)."""
    pushes_return_value = False

    def execute(self, context):
        result = super().execute(context)
        if not result:
            context["ti"].skip_downstream(all_descendants=True)
        return result
