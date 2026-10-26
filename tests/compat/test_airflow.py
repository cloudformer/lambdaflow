"""lambdaflow vs a real Airflow 3.3: the compatibility matrix.

One test = one feature; docstring "Category | Feature | note". The list only grows. Each test runs one DAG
from tests/compat/dags/ on both sides (see conftest.py). Run with:

    docker compose -f tests/compat/docker-compose.yml up -d airflow
    AIRFLOW_URL=http://admin:admin@127.0.0.1:8088 python3 -m pytest tests -q
    docker compose -f tests/compat/docker-compose.yml down
"""


def test_chain(pair):
    """DAGs | a >> b >> c with PythonOperator, XCom pulled downstream"""
    pair.dag("chain", edges=[("a", "b"), ("b", "c")])


def test_fan_out_in(pair):
    """DAGs | fan-out / fan-in: a >> [b, c] >> d, xcom_pull of a task list"""
    pair.dag("fan_out_in", edges=[("a", "b"), ("a", "c"), ("b", "d"), ("c", "d")])


def test_taskflow(pair):
    """DAGs | TaskFlow @dag / @task, return values as arguments, multiple_outputs"""
    pair.dag("taskflow", edges=[("extract", "split"), ("split", "load")])


def test_retries(pair):
    """Execution | retries and retry_delay: succeeds on try 3; exhausts retries and fails"""
    pair.dag("retries")


def test_trigger_rules(pair):
    """Execution | every trigger_rule after a failed and a successful upstream; upstream_failed propagation"""
    pair.dag("trigger_rules")


def test_branching(pair):
    """Execution | @task.branch skips the other path; join rules; ShortCircuitOperator skips downstream"""
    pair.dag("branching", edges=[("choose", "left"), ("left", "join")])


def test_branching_conf(pair):
    """Execution | params overridden by the trigger's conf pick the other branch"""
    pair.dag("branching", conf={"side": "right"}, edges=[("choose", "right"), ("right", "join")])


def test_templates(pair):
    """Templates | {{ ds }}, {{ ds_nodash }}, params, dag_run.conf, macros.ds_add, logical_date; Bash stdout as XCom"""
    pair.dag("templates", conf={"x": "42"})
