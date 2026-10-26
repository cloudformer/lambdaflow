"""A failing task and what its downstream tasks do under each trigger rule."""
import datetime

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG


def fail():
    raise RuntimeError("boom")


with DAG("trigger_rules", start_date=datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc), schedule=None, catchup=False):
    ok = EmptyOperator(task_id="ok")
    bad = PythonOperator(task_id="bad", python_callable=fail)
    rules = ["all_success", "all_failed", "all_done", "one_success", "one_failed", "none_failed",
             "none_failed_min_one_success", "none_skipped", "always"]
    for rule in rules:
        [ok, bad] >> EmptyOperator(task_id=rule, trigger_rule=rule)
    EmptyOperator(task_id="after_upstream_failed") << EmptyOperator(task_id="child_of_bad") << bad
