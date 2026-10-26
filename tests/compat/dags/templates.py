"""Jinja templates in operator fields: the logical date, params, dag_run.conf, macros; Bash output as XCom."""
import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

with DAG("templates", start_date=datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc), schedule=None, catchup=False,
         params={"name": "world"}):
    BashOperator(task_id="bash", bash_command="echo '{{ ds }} {{ ds_nodash }} {{ params.name }} {{ dag_run.conf.get(\"x\", \"-\") }}'")
    PythonOperator(task_id="py", python_callable=lambda a, b: f"{a}|{b}",
                   op_args=["{{ macros.ds_add(ds, 7) }}", "{{ logical_date.strftime('%Y-%m') }}"])
