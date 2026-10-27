"""The console Lambda in-process against moto: auth, every route, the airflow CLI box, S3 events.

The state machine is replaced by worker.loop() called synchronously, so a trigger returns a finished run.
"""
import base64
import json

import pytest

AUTH = "Basic " + base64.b64encode(b"admin:pw").decode()

DAG = '''
import datetime
from airflow.sdk import DAG, task
with DAG("hello", start_date=datetime.datetime(2026, 1, 1), schedule="@daily", tags=["demo"], params={"who": "world"}):
    @task
    def greet(params=None):
        print("hello", params["who"])
        return params["who"]

    @task
    def shout(who):
        if who == "fail":
            raise ValueError("no")
        return who.upper()
    shout(greet())
'''


@pytest.fixture
def lf(tmp_path, monkeypatch):
    import boto3
    from moto import mock_aws

    import lambda_function
    import worker
    for k, v in {"AWS_DEFAULT_REGION": "us-east-2", "AWS_ACCESS_KEY_ID": "testing", "AWS_SECRET_ACCESS_KEY": "testing",
                 "LAMBDAFLOW_CACHE": str(tmp_path), "TABLE": "lambdaflow", "BUCKET": "lambdaflow-test"}.items():
        monkeypatch.setenv(k, v)
    for k in ("STATE_MACHINE_ARN", "WORKER_FUNCTION", "SCHEDULE_GROUP"):
        monkeypatch.delenv(k, raising=False)
    with mock_aws():
        boto3.resource("dynamodb").create_table(
            TableName="lambdaflow", BillingMode="PAY_PER_REQUEST",
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}, {"AttributeName": "sk", "KeyType": "RANGE"}],
            AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}, {"AttributeName": "sk", "AttributeType": "S"}])
        boto3.client("s3").create_bucket(Bucket="lambdaflow-test", CreateBucketConfiguration={"LocationConstraint": "us-east-2"})
        monkeypatch.setattr(lambda_function, "ENGINE", None)
        monkeypatch.setattr(worker, "ENGINE", None)
        monkeypatch.setattr(lambda_function, "RUNNER", lambda p: worker.loop(p, sleep=lambda s: None))
        yield lambda_function


def call(lf, method, path, body=None, auth=AUTH, query=None):
    event = {"requestContext": {"http": {"method": method}}, "rawPath": path, "queryStringParameters": query,
             "headers": {"authorization": auth} if auth else {}, "body": json.dumps(body) if body is not None else None}
    resp = lf.lambda_handler(event, None)
    is_json = resp["headers"]["content-type"].startswith("application/json")
    return resp["statusCode"], json.loads(resp["body"]) if is_json else resp["body"]


def cli(lf, command):
    return call(lf, "POST", "/api/cli", {"command": command})


def test_console_is_public_and_api_needs_auth(lf):
    status, body = call(lf, "GET", "/", auth=None)
    assert status == 200 and "<html" in body and "lambdaflow" in body
    assert call(lf, "GET", "/api/ping", auth=None)[0] == 401
    assert call(lf, "GET", "/api/ping", auth="Basic " + base64.b64encode(b"admin:wrong").decode())[0] == 401
    status, data = call(lf, "GET", "/api/ping")
    assert status == 200 and data["ok"] is True and isinstance(data["ms"], float)
    assert call(lf, "GET", "/api/nope")[0] == 404


def test_upload_trigger_and_read_back_a_run(lf):
    status, r = call(lf, "PUT", "/api/files/hello.py", {"source": DAG})
    assert status == 200 and r["dags"] == ["hello"] and r["error"] is None
    dags = call(lf, "GET", "/api/dags")[1]["dags"]
    assert [(d["dag_id"], d["paused"], d["schedule"], d["expression"], d["tags"]) for d in dags] == \
        [("hello", True, "@daily", "cron(0 0 * * ? *)", ["demo"])]
    status, r = call(lf, "POST", "/api/dags/hello/runs", {"conf": {"who": "ada"}})
    assert status == 400 and "paused" in r["error"]
    status, r = call(lf, "POST", "/api/dags/hello/runs", {"conf": {"who": "ada"}, "logical_date": "2026-02-01T00:00:00Z",
                                                         "unpause": True})
    assert status == 201 and r["run_id"] == "manual__2026-02-01T00:00:00+00:00" and r["backend"] == "local runner"
    run_id = r["run_id"]
    status, r = call(lf, "POST", "/api/dags/hello/runs", {"logical_date": "2026-02-01T00:00:00Z"})
    assert status == 400 and "exists already" in r["error"]

    dag = call(lf, "GET", "/api/dags/hello")[1]
    assert dag["dag"]["paused"] is False and dag["dag"]["next_run"].endswith("T00:00:00Z")
    assert [(x["run_id"], x["state"]) for x in dag["runs"]] == [(run_id, "success")]
    assert {t: (i["state"], i["try_number"]) for t, i in dag["runs"][0]["tis"].items()} == \
        {"greet": ("success", 1), "shout": ("success", 1)}
    q = "/api/dags/hello/runs/" + run_id.replace("+", "%2B").replace(":", "%3A")   # as the console encodes it
    r = call(lf, "GET", q)[1]
    assert r["run"]["state"] == "success" and r["run"]["conf"] == {"who": "ada"}
    assert r["xcom"] == {"greet.return_value": "ada", "shout.return_value": "ADA"}
    log = call(lf, "GET", q + "/tasks/greet/log")[1]
    assert log["try_number"] == 1 and "hello ada" in log["log"]
    assert call(lf, "GET", q + "/tasks/greet/log", query={"try": "2"})[1]["log"] == ""
    assert "def greet" in call(lf, "GET", "/api/dags/hello/source")[1]["source"]


def test_clear_failed_and_mark(lf):
    call(lf, "PUT", "/api/files/hello.py", {"source": DAG})
    run_id = call(lf, "POST", "/api/dags/hello/runs", {"conf": {"who": "fail"}, "unpause": True})[1]["run_id"]
    q = "/api/dags/hello/runs/" + run_id
    assert call(lf, "GET", q)[1]["run"]["state"] == "failed"
    status, r = call(lf, "POST", q + "/clear", {"only_failed": True})
    assert status == 200 and r["cleared"] == ["shout"]
    tis = call(lf, "GET", q)[1]["tis"]
    assert tis["shout"]["try_number"] == 2 and tis["shout"]["state"] == "failed" and tis["greet"]["try_number"] == 1
    assert call(lf, "POST", q + "/tasks/shout/mark", {"state": "success"})[0] == 200
    assert call(lf, "GET", q)[1]["run"]["state"] == "success"
    assert call(lf, "POST", q + "/tasks/shout/mark", {"state": "running"})[0] == 400
    assert call(lf, "POST", q + "/clear", {})[1]["cleared"] == ["greet", "shout"]


def test_import_errors_unknown_dags_and_delete(lf):
    status, r = call(lf, "PUT", "/api/files/broken.py", {"source": "import nope"})
    assert status == 200 and r["dags"] == [] and "ModuleNotFoundError" in r["error"]["error"]
    assert call(lf, "GET", "/api/import-errors")[1]["import_errors"][0]["file"] == "dags/broken.py"
    assert call(lf, "GET", "/api/dags")[1]["import_errors"][0]["file"] == "dags/broken.py"
    status, r = call(lf, "GET", "/api/dags/nope")
    assert status == 404 and r["error"] == "no DAG 'nope'"
    assert call(lf, "PUT", "/api/files/x.txt", {"source": ""})[0] == 404           # only .py files
    assert call(lf, "PUT", "/api/files/x.py", {})[0] == 400
    call(lf, "PUT", "/api/files/hello.py", {"source": DAG})
    assert call(lf, "DELETE", "/api/dags/hello")[1]["deleted"] == "hello"
    assert call(lf, "GET", "/api/dags/hello")[0] == 404


def test_pause_unpause(lf):
    call(lf, "PUT", "/api/files/hello.py", {"source": DAG})
    assert call(lf, "POST", "/api/dags/hello/pause", {"paused": False})[1]["paused"] is False
    assert call(lf, "GET", "/api/dags")[1]["dags"][0]["paused"] is False
    assert call(lf, "POST", "/api/dags/hello/pause", {"paused": True})[1]["paused"] is True
    assert call(lf, "GET", "/api/dags")[1]["dags"][0]["next_run"] is None


def test_the_airflow_cli_box(lf):
    call(lf, "PUT", "/api/files/hello.py", {"source": DAG})
    assert "dags trigger" in cli(lf, "help")[1]["output"]
    out = cli(lf, "airflow dags list")[1]["output"]
    assert out.splitlines()[0].split(" | ")[0].strip() == "dag_id" and "hello" in out and "@daily" in out
    status, r = cli(lf, "dags trigger hello")
    assert status == 400 and "dags unpause hello" in r["error"]
    assert cli(lf, "dags unpause hello")[1]["output"] == "Dag: hello, paused: False"
    r = cli(lf, """dags trigger hello -c '{"who": "cli"}' -l 2026-03-01T00:00:00Z""")[1]
    assert r["run_id"] == "manual__2026-03-01T00:00:00+00:00" and r["output"].startswith("Created <DagRun hello")
    assert "success" in cli(lf, "dags list-runs hello")[1]["output"]
    assert cli(lf, f"dags state hello {r['run_id']}")[1]["output"] == "success"
    assert cli(lf, "dags state hello 2026-03-01T00:00:00Z")[1]["output"] == "success"
    states = cli(lf, f"tasks states-for-dag-run hello {r['run_id']}")[1]["output"]
    assert "greet" in states and "shout" in states and states.count("success") == 2
    assert cli(lf, "tasks list hello")[1]["output"] == "greet\nshout"
    assert cli(lf, f"tasks clear hello -t shout -r {r['run_id']}")[1]["output"] == "Cleared: shout"
    assert cli(lf, "dags next-execution hello")[1]["output"].endswith("T00:00:00Z")
    assert "No data found" == cli(lf, "dags list-import-errors")[1]["output"]
    assert cli(lf, "dags delete hello")[0] == 400                                 # needs -y
    assert cli(lf, "dags delete hello -y")[1]["output"] == "Removed DAG hello"
    assert cli(lf, "dags backfill hello")[0] == 400
    assert cli(lf, "dags trigger")[0] == 400
    assert cli(lf, "dags trigger 'unbalanced")[0] == 400


def test_s3_events_parse_and_forget(lf):
    import boto3
    boto3.client("s3").put_object(Bucket="lambdaflow-test", Key="dags/hello.py", Body=DAG.encode())
    rec = lambda name: {"Records": [{"eventName": name, "s3": {"object": {"key": "dags/hello.py"}}}]}  # noqa: E731
    assert lf.lambda_handler(rec("ObjectCreated:Put"), None)["files"][0]["dags"] == ["hello"]
    assert [d["dag_id"] for d in call(lf, "GET", "/api/dags")[1]["dags"]] == ["hello"]
    assert lf.lambda_handler(rec("ObjectRemoved:Delete"), None)["files"][0]["removed"] == ["hello"]
    assert call(lf, "GET", "/api/dags")[1]["dags"] == []


def test_the_pip_client_drives_the_console(lf, tmp_path, monkeypatch):
    """client/lambdaflow: every method reaches its route (the HTTPS transport replaced by an in-process call)."""
    import urllib.parse

    from lambdaflow import client

    def send(self, method, path, body):
        u = urllib.parse.urlsplit(path)
        resp = lf.lambda_handler({"requestContext": {"http": {"method": method}}, "rawPath": u.path, "body": body,
                                  "queryStringParameters": dict(urllib.parse.parse_qsl(u.query)) or None,
                                  "headers": {"authorization": "Basic " + self._auth}}, None)
        data = json.loads(resp["body"])
        if resp["statusCode"] >= 400:
            raise client.Error(resp["statusCode"], data.get("error", ""))
        return data
    monkeypatch.setattr(client.Connection, "_send", send)
    (tmp_path / "hello.py").write_text(DAG)
    c = client.connect("xxxx.lambda-url.us-east-2.on.aws", "admin", "pw")
    assert c.ping() is True
    assert c.upload(str(tmp_path / "hello.py"))["dags"] == ["hello"]
    assert [d["dag_id"] for d in c.dags()] == ["hello"] and c.import_errors() == []
    with pytest.raises(client.Error, match="paused"):
        c.trigger("hello")
    run_id = c.trigger("hello", conf={"who": "py"}, unpause=True)
    assert c.wait("hello", run_id, every=0) == "success"
    assert c.run("hello", run_id)["xcom"]["shout.return_value"] == "PY"
    assert [r["run_id"] for r in c.runs("hello")] == [run_id]
    assert "hello py" in c.log("hello", run_id, "greet")
    assert c.clear("hello", run_id, ["shout"]) == ["shout"]
    assert "hello" in c.cli("dags list")
    assert c.pause("hello")["paused"] is True and c.unpause("hello")["paused"] is False
    assert c.last["ms"] is not None
