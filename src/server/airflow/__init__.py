"""lambdaflow's `airflow`: the DAG-authoring API of Apache Airflow 3.3, so DAG files run unchanged.

Only what passed a comparison test against a real Airflow is here (tests/compat/); anything else a DAG
file uses fails at parse time with NotSupportedError, never silently different.
"""
