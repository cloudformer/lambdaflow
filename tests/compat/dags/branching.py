"""@task.branch picks one path; the other is skipped; the join needs the right trigger rule."""
import datetime

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import ShortCircuitOperator
from airflow.sdk import DAG, task

with DAG("branching", start_date=datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc), schedule=None, catchup=False):
    @task.branch
    def choose(**context):
        return "left" if context["params"].get("side", "left") == "left" else "right"

    left, right = EmptyOperator(task_id="left"), EmptyOperator(task_id="right")
    join = EmptyOperator(task_id="join", trigger_rule="none_failed_min_one_success")
    strict_join = EmptyOperator(task_id="strict_join")
    choose() >> [left, right]
    [left, right] >> join
    [left, right] >> strict_join

    stop = ShortCircuitOperator(task_id="stop", python_callable=lambda: False)
    stop >> EmptyOperator(task_id="never") >> EmptyOperator(task_id="never_either")
