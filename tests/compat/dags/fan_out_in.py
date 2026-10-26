"""a >> [b, c] >> d: b and c run in parallel, d pulls both."""
import datetime

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

with DAG("fan_out_in", start_date=datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc), schedule=None, catchup=False):
    a = PythonOperator(task_id="a", python_callable=lambda: "x")
    b = PythonOperator(task_id="b", python_callable=lambda ti: ti.xcom_pull(task_ids="a") + "b")
    c = PythonOperator(task_id="c", python_callable=lambda ti: ti.xcom_pull(task_ids="a") + "c")
    d = PythonOperator(task_id="d", python_callable=lambda ti: sorted(ti.xcom_pull(task_ids=["b", "c"])))
    a >> [b, c] >> d
