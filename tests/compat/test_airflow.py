"""lambdaflow vs a real Airflow 3.3: the compatibility matrix.

One test = one feature; docstring "Category | Feature | note". The list only grows. Each test runs one DAG
from tests/compat/dags/ on both sides (see conftest.py). Run with:

    docker compose -f tests/compat/docker-compose.yml up -d airflow
    AIRFLOW_URL=http://admin:admin@127.0.0.1:8088 python3 -m pytest tests -q
    docker compose -f tests/compat/docker-compose.yml down
"""
import pytest


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


def test_schedules(pair):
    """Scheduling | cron, presets and @once → EventBridge Scheduler: the first run after start_date matches Airflow's, in UTC and named timezones (19 DAGs)"""
    pair.next_runs("schedules")


@pytest.mark.quirk
@pytest.mark.xfail(reason="EventBridge's rate() counts from when the schedule is created, Airflow from start_date; "
                          "use a cron schedule when the phase matters", strict=True)
def test_schedule_timedelta(pair):
    """Scheduling | schedule=timedelta(minutes=90) → rate(90 minutes)"""
    pair.next_runs("schedule_delta")


@pytest.mark.unsupported("refused at parse time with NotSupportedError, shown as the file's import error")
def test_schedule_refused(ours):
    """Unsupported | day-of-month and day-of-week both set, a step or L / # in a day field, @continuous, catchup=True"""
    head = ('import datetime\nfrom airflow.sdk import DAG\nfrom airflow.providers.standard.operators.empty import EmptyOperator\n'
            'with DAG("x", start_date=datetime.datetime(2030, 1, 1), {}):\n    EmptyOperator(task_id="a")\n')
    for args in ['schedule="0 0 1 * 1"', 'schedule="0 0 * * */2"', 'schedule="0 0 L * *"', 'schedule="0 0 * * 5#3"',
                 'schedule="@continuous"', 'schedule="@daily", catchup=True']:
        assert ours.engine.put_dag_file("x.py", head.format(args)) == [], args
        assert "NotSupportedError" in ours.engine.import_errors()[0]["error"], args
