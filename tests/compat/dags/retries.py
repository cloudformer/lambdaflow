"""A task that fails on its first two tries and succeeds on the third; another that always fails."""
import datetime

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG


def flaky(ti):
    if ti.try_number < 3:
        raise ValueError(f"try {ti.try_number}")
    return ti.try_number


def broken():
    raise RuntimeError("always")


with DAG("retries", start_date=datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc), schedule=None, catchup=False,
         default_args={"retry_delay": datetime.timedelta(seconds=1)}):
    PythonOperator(task_id="flaky", python_callable=flaky, retries=2)
    PythonOperator(task_id="broken", python_callable=broken, retries=1)
