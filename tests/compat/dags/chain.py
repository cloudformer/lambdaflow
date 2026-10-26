"""a >> b >> c; each task returns a value, the next one pulls it."""
import datetime

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

with DAG("chain", start_date=datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc), schedule=None, catchup=False):
    a = PythonOperator(task_id="a", python_callable=lambda: 1)
    b = PythonOperator(task_id="b", python_callable=lambda ti: ti.xcom_pull(task_ids="a") + 1)
    c = PythonOperator(task_id="c", python_callable=lambda ti: ti.xcom_pull(task_ids="b") * 10)
    a >> b >> c
