"""The engine and the worker in-process against moto: what the comparison with Airflow cannot show."""
import json

import pytest

DAG_OK = '''
import datetime
from airflow.sdk import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
with DAG("ok", start_date=datetime.datetime(2026, 1, 1), schedule=None):
    EmptyOperator(task_id="a") >> EmptyOperator(task_id="b")
'''

DAG_LOGS = '''
import datetime, logging
from airflow.sdk import DAG, task
with DAG("logs", start_date=datetime.datetime(2026, 1, 1), schedule=None, default_args={"retries": 1, "retry_delay": 0}):
    @task
    def hello():
        print("hello from print")
        logging.getLogger(__name__).info("hello from logging")
        return 1

    @task
    def boom(x):
        raise RuntimeError("boom " + str(x))
    boom(hello())
'''

DAG_SCHEDULED = '''
import datetime
from airflow.sdk import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
with DAG("nightly", start_date=datetime.datetime(2030, 1, 1, tzinfo=datetime.timezone.utc), schedule="0 6 * * 1-5",
         max_active_runs=1):
    EmptyOperator(task_id="a")
'''

GROUP, TARGET, ROLE = "lambdaflow-test", "arn:aws:states:us-east-2:123456789012:stateMachine:lambdaflow-test", \
    "arn:aws:iam::123456789012:role/lambdaflow-test-scheduler"


@pytest.fixture
def aws(tmp_path, monkeypatch):
    import boto3
    from moto import mock_aws
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-2")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("LAMBDAFLOW_CACHE", str(tmp_path))
    with mock_aws():
        db, s3 = boto3.resource("dynamodb"), boto3.client("s3")
        db.create_table(TableName="t", BillingMode="PAY_PER_REQUEST",
                        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}, {"AttributeName": "sk", "KeyType": "RANGE"}],
                        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}, {"AttributeName": "sk", "AttributeType": "S"}])
        s3.create_bucket(Bucket="dags-test", CreateBucketConfiguration={"LocationConstraint": "us-east-2"})
        scheduler = boto3.client("scheduler")
        scheduler.create_schedule_group(Name=GROUP)
        yield db, s3, scheduler


@pytest.fixture
def engine(aws):
    from engine import Engine
    db, s3, _ = aws
    return Engine("t", "dags-test", db=db, s3=s3)


@pytest.fixture
def console_engine(aws):
    """The console's engine: keeps EventBridge Scheduler in sync."""
    from engine import Engine
    db, s3, scheduler = aws
    return Engine("t", "dags-test", db=db, s3=s3,
                  schedules={"client": scheduler, "group": GROUP, "target": TARGET, "role": ROLE})


def test_unknown_arguments_are_refused_at_parse_time(engine):
    src = DAG_OK.replace('schedule=None', 'schedule=None, dagrun_timeout=None')
    assert engine.put_dag_file("bad.py", src) == []
    err = engine.table.get_item(Key={"pk": "err", "sk": "dags/bad.py"})["Item"]
    assert "NotSupportedError" in err["error"] and "dagrun_timeout" in err["error"]
    src = DAG_OK.replace('EmptyOperator(task_id="a")', 'EmptyOperator(task_id="a", pool="p")')
    assert engine.put_dag_file("bad.py", src) == []
    assert "pool" in engine.table.get_item(Key={"pk": "err", "sk": "dags/bad.py"})["Item"]["error"]


def test_import_error_is_stored_and_cleared_when_fixed(engine):
    assert engine.put_dag_file("ok.py", "import nope_not_a_module") == []
    assert "ModuleNotFoundError" in engine.table.get_item(Key={"pk": "err", "sk": "dags/ok.py"})["Item"]["error"]
    assert engine.put_dag_file("ok.py", DAG_OK) == ["ok"]
    assert "Item" not in engine.table.get_item(Key={"pk": "err", "sk": "dags/ok.py"})


def test_an_unsupported_schedule_is_an_import_error(engine):
    assert engine.put_dag_file("ok.py", DAG_OK.replace('schedule=None', 'schedule="0 0 1 * 1"')) == []
    assert "day-of-week" in engine.import_errors()[0]["error"]


def test_a_dag_removed_from_its_file_disappears_and_pause_survives_a_reparse(engine):
    two = DAG_OK + DAG_OK.replace('"ok"', '"ok2"')
    assert engine.put_dag_file("ok.py", two) == ["ok", "ok2"]
    assert engine.meta("ok")["paused"] is True                       # Airflow's default: paused at creation
    engine.set_paused("ok", False)
    assert engine.put_dag_file("ok.py", DAG_OK) == ["ok"]
    assert [d["dag_id"] for d in engine.dags()] == ["ok"]
    assert engine.meta("ok")["paused"] is False
    assert engine.forget_file("dags/ok.py") == ["ok"] and engine.dags() == []


def test_a_run_is_created_once_and_runs_to_the_end(engine):
    from engine import run_local
    engine.put_dag_file("ok.py", DAG_OK)
    r1 = engine.trigger("ok", "2026-01-01T00:00:00Z", run_id="scheduled__2026-01-01")
    r2 = engine.trigger("ok", "2026-01-01T00:00:00Z", run_id="scheduled__2026-01-01")   # a duplicate delivery
    assert r1 == r2
    assert engine.step("ok", r1) == {"tasks": ["a"]}
    assert engine.step("ok", r1) == {"wait": 1}          # a is queued: nothing new to start
    engine.run_task("ok", r1, "a")
    assert engine.step("ok", r1) == {"tasks": ["b"]}
    engine.run_task("ok", r1, "b")
    assert engine.step("ok", r1) == {"done": True, "state": "success"}
    run_id = run_local(engine, "ok", "2026-01-02T00:00:00Z")
    assert run_id == "manual__2026-01-02T00:00:00+00:00"            # Airflow's run id format
    assert engine.run_state("ok", run_id)[0]["state"] == "success"


def test_a_task_is_claimed_once_when_two_loops_plan_the_same_run(engine, monkeypatch):
    engine.put_dag_file("ok.py", DAG_OK)
    run_id = engine.trigger("ok", "2026-01-01T00:00:00Z")
    assert engine.step("ok", run_id) == {"tasks": ["a"]}
    stale = engine._tis
    monkeypatch.setattr(engine, "_tis", lambda d, r: {})            # the second loop read before the claim
    assert engine.step("ok", run_id) == {"wait": 1}                 # its conditional claim fails: a runs once
    monkeypatch.setattr(engine, "_tis", stale)


def test_the_scheduled_start_is_deduplicated_by_the_worker(engine):
    import worker
    engine.put_dag_file("ok.py", DAG_OK)
    worker.ENGINE = engine
    first = worker.lambda_handler({"action": "start", "dag_id": "ok", "logical_date": "2026-03-01T06:00:00Z"})
    again = worker.lambda_handler({"action": "start", "dag_id": "ok", "logical_date": "2026-03-01T06:00:00Z"})
    assert first == {"dag_id": "ok", "run_id": "scheduled__2026-03-01T06:00:00+00:00", "go": True}
    assert again["go"] is False and again["run_id"] == first["run_id"]
    assert worker.lambda_handler({"action": "start", "dag_id": "gone", "logical_date": "2026-03-01T06:00:00Z"})["go"] is False
    assert worker.loop({"action": "resume", "dag_id": "ok", "run_id": first["run_id"]}, sleep=lambda s: None)["go"]
    assert engine.runs("ok")[0]["state"] == "success" and engine.runs("ok")[0]["run_type"] == "scheduled"


def test_max_active_runs_keeps_a_run_queued(engine):
    from engine import drive
    engine.put_dag_file("n.py", DAG_SCHEDULED)
    r1 = engine.trigger("nightly", "2030-01-01T06:00:00Z")
    r2 = engine.trigger("nightly", "2030-01-02T06:00:00Z")
    assert engine.step("nightly", r1) == {"tasks": ["a"]}
    assert engine.step("nightly", r2) == {"wait": 30}               # max_active_runs=1: r1 is running
    assert engine.runs("nightly")[0]["state"] == "queued"
    engine.run_task("nightly", r1, "a")                              # the task the first step claimed
    drive(engine, "nightly", r1, sleep=lambda s: None)
    assert engine.step("nightly", r2) == {"tasks": ["a"]}


def test_task_logs_retries_and_a_dead_worker(engine):
    from engine import run_local
    engine.put_dag_file("logs.py", DAG_LOGS)
    run_id = run_local(engine, "logs", "2026-01-01T00:00:00Z", sleep=lambda s: None)
    _, tis, _ = engine.run_state("logs", run_id)
    assert tis["boom"]["state"] == "failed" and int(tis["boom"]["try_number"]) == 2
    hello = engine.read_log("logs", run_id, "hello", 1)
    assert "hello from print" in hello and "hello from logging" in hello and "Marking task as SUCCESS" in hello
    first = engine.read_log("logs", run_id, "boom", 1)
    assert "RuntimeError: boom 1" in first and "Traceback" in first and "UP_FOR_RETRY" in first
    assert "Marking task as FAILED" in engine.read_log("logs", run_id, "boom", 2)
    assert engine.read_log("logs", run_id, "boom", 3) is None
    r2 = engine.trigger("logs", "2026-01-02T00:00:00Z")             # the worker dies while running hello
    engine.step("logs", r2)
    assert engine.fail_task("logs", r2, "hello", "Task timed out after 900 seconds") == "up_for_retry"
    assert "timed out" in engine.read_log("logs", r2, "hello", 1)


def test_clear_reruns_the_task_and_its_downstream_with_the_next_try(engine):
    from engine import drive, run_local
    engine.put_dag_file("logs.py", DAG_LOGS)
    run_id = run_local(engine, "logs", "2026-01-01T00:00:00Z", sleep=lambda s: None)
    assert engine.clear("logs", run_id, ["hello"]) == ["boom", "hello"]
    run, tis, xcom = engine.run_state("logs", run_id)
    assert run["state"] == "queued" and "state" not in tis["hello"] and xcom == {}
    assert drive(engine, "logs", run_id, sleep=lambda s: None) == "failed"
    _, tis, _ = engine.run_state("logs", run_id)
    assert int(tis["hello"]["try_number"]) == 2 and int(tis["boom"]["try_number"]) == 4
    assert "hello from print" in engine.read_log("logs", run_id, "hello", 2)


def test_mark_success_on_the_failed_leaf_makes_the_run_succeed(engine):
    from engine import drive, run_local
    engine.put_dag_file("logs.py", DAG_LOGS)
    run_id = run_local(engine, "logs", "2026-01-01T00:00:00Z", sleep=lambda s: None)
    engine.mark("logs", run_id, "boom", "success")
    assert drive(engine, "logs", run_id, sleep=lambda s: None) == "success"
    with pytest.raises(ValueError):
        engine.mark("logs", run_id, "boom", "skipped")


def test_delete_dag_removes_runs_logs_and_the_file(engine):
    from engine import run_local
    engine.put_dag_file("logs.py", DAG_LOGS)
    run_local(engine, "logs", "2026-01-01T00:00:00Z", sleep=lambda s: None)
    engine.delete_dag("logs")
    assert engine.table.scan()["Items"] == []
    assert engine.s3.list_objects_v2(Bucket="dags-test").get("KeyCount") == 0


def test_the_schedule_follows_the_dag(console_engine, aws):
    _, _, scheduler = aws
    e = console_engine
    e.put_dag_file("n.py", DAG_SCHEDULED)
    assert e.sync_schedule("nightly") == "DISABLED"                 # paused at creation
    s = scheduler.get_schedule(Name="nightly", GroupName=GROUP)
    assert s["ScheduleExpression"] == "cron(0 6 ? * MON-FRI *)" and s["ScheduleExpressionTimezone"] == "UTC"
    assert s["Target"]["Arn"] == TARGET and s["Target"]["RoleArn"] == ROLE
    assert json.loads(s["Target"]["Input"]) == {"action": "start", "dag_id": "nightly",
                                                "logical_date": "<aws.scheduler.scheduled-time>"}
    e.set_paused("nightly", False)
    assert scheduler.get_schedule(Name="nightly", GroupName=GROUP)["State"] == "ENABLED"
    assert e.next_run(e.spec("nightly")[0]) == "2030-01-01T06:00:00Z"
    e.put_dag_file("n.py", DAG_SCHEDULED.replace('"0 6 * * 1-5"', '"@daily"'))
    e.sync_schedule("nightly")
    assert scheduler.get_schedule(Name="nightly", GroupName=GROUP)["ScheduleExpression"] == "cron(0 0 * * ? *)"
    e.put_dag_file("n.py", DAG_SCHEDULED.replace('"0 6 * * 1-5"', 'None'))
    assert e.sync_schedule("nightly") is None
    assert [x["Name"] for x in scheduler.list_schedules(GroupName=GROUP)["Schedules"]] == []
    e.put_dag_file("n.py", DAG_SCHEDULED)
    e.sync_schedule("nightly")
    e.delete_dag("nightly")
    assert scheduler.list_schedules(GroupName=GROUP)["Schedules"] == []


@pytest.mark.parametrize("rule,ups,expect", [
    ("all_success", ["success", "running"], "wait"),
    ("one_success", ["success", None], "run"),            # starts before the other upstream is done
    ("one_failed", [None, "failed"], "run"),
    ("one_success", ["skipped", "skipped"], "skipped"),
    ("one_success", ["failed", "skipped"], "upstream_failed"),
    ("all_skipped", ["skipped", "skipped"], "run"),
    ("one_done", ["skipped", "upstream_failed"], "skipped"),
    ("none_failed_min_one_success", ["skipped", "skipped"], "skipped"),
    ("all_success", [], "run"),
])
def test_trigger_rules_beyond_the_compat_dag(rule, ups, expect):
    from engine import evaluate
    assert evaluate(rule, ups) == expect
