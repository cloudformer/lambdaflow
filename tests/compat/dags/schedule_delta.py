"""schedule=timedelta: Airflow counts intervals from start_date; EventBridge's rate() from when the schedule is created."""
import datetime

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG

with DAG("schedule_delta", start_date=datetime.datetime(2030, 1, 2, 13, 7, tzinfo=datetime.timezone.utc),
         schedule=datetime.timedelta(minutes=90), catchup=False):
    EmptyOperator(task_id="noop")
