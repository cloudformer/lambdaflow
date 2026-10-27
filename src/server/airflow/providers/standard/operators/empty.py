"""EmptyOperator (airflow.providers.standard.operators.empty): does nothing, pushes nothing."""
from airflow.sdk import BaseOperator


class EmptyOperator(BaseOperator):
    pushes_return_value = False
