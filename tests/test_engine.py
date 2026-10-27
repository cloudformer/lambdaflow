"""The engine in-process against moto: what the comparison with Airflow cannot show."""
import pytest

DAG_OK = '''
import datetime
from airflow.sdk import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
with DAG("ok", start_date=datetime.datetime(2026, 1, 1), schedule=None):
    EmptyOperator(task_id="a") >> EmptyOperator(task_id="b")
'''


@pytest.fixture
def engine(tmp_path, monkeypatch):
    import boto3
    from moto import mock_aws

    from engine import Engine
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
        yield Engine("t", "dags-test", db=db, s3=s3)


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
    assert engine.run_state("ok", run_id)[0]["state"] == "success"


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
