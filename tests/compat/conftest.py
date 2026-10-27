"""Compatibility harness: the same DAG run on a real Airflow (Docker) and on lambdaflow, outcomes compared.

A test is one feature; its docstring "Category | Feature | note" is the report row. `pair.dag(dag_id, ...)`
triggers one run of tests/compat/dags/<dag_id>.py on both sides and asserts the outcomes are equal:
the run's state, every task's (state, try_number) and every XCom value. Wall-clock order is not compared
(parallel tasks may finish in any order); instead each side is checked to respect the dependencies.

Real side: AIRFLOW_URL=http://admin:admin@127.0.0.1:8088 (tests/compat/docker-compose.yml, service
airflow). Each real outcome is also written to tests/compat/expected/<test name>.json, so what Airflow does
is visible in git and in review. Without AIRFLOW_URL the lambdaflow side is compared with those recorded outcomes.
"""
import datetime
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request

import pytest

import compat_report

HERE = os.path.dirname(os.path.abspath(__file__))
LOGICAL_DATE = "2026-01-01T00:00:00Z"   # fixed, so templated values ({{ ds }}) are comparable
DONE = {"success", "failed"}


class Airflow:
    """The Airflow 3 REST API (/api/v2, JWT from /auth/token), standard library only."""

    def __init__(self, url):
        u = urllib.parse.urlsplit(url)
        self.base = f"{u.scheme}://{u.hostname}:{u.port or 80}"
        self.user, self.password = u.username or "admin", u.password or "admin"
        self.token = None

    def _req(self, method, path, body=None, auth=True):
        headers = {"Content-Type": "application/json"}
        if auth:
            headers["Authorization"] = "Bearer " + self._token()
        req = urllib.request.Request(self.base + path, method=method, headers=headers,
                                     data=json.dumps(body).encode() if body is not None else None)
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                raw = r.read()
                return r.status, json.loads(raw) if raw else None
        except urllib.error.HTTPError as e:
            raw = e.read()
            try:
                return e.code, json.loads(raw)
            except ValueError:
                return e.code, raw.decode(errors="replace")

    def _token(self):
        if self.token is None:
            status, data = self._req("POST", "/auth/token", {"username": self.user, "password": self.password}, auth=False)
            if status >= 300 or not isinstance(data, dict) or "access_token" not in data:
                raise RuntimeError(f"Airflow login failed ({status}): {data}")
            self.token = data["access_token"]
        return self.token

    def get(self, path):
        status, data = self._req("GET", path)
        if status >= 300:
            raise RuntimeError(f"GET {path} -> {status}: {data}")
        return data

    def version(self):
        return self.get("/api/v2/version")["version"]

    def wait_parsed(self, dag_id, timeout=120):
        """The DAG file is picked up by the dag processor within seconds; an import error fails at once."""
        end = time.time() + timeout
        while time.time() < end:
            status, _ = self._req("GET", f"/api/v2/dags/{dag_id}")
            if status == 200:
                return
            for e in self.get("/api/v2/importErrors").get("import_errors", []):
                if e.get("filename", "").endswith(f"/{dag_id}.py"):
                    raise RuntimeError(f"Airflow could not import {dag_id}.py:\n{e.get('stack_trace')}")
            time.sleep(2)
        raise RuntimeError(f"DAG {dag_id} not parsed after {timeout}s")

    def run(self, dag_id, conf=None, logical_date=LOGICAL_DATE, timeout=300):
        """Trigger one run and wait for it; returns the normalised outcome."""
        self.wait_parsed(dag_id)
        for r in self.get(f"/api/v2/dags/{dag_id}/dagRuns?limit=100").get("dag_runs", []):   # runs left by an earlier test run
            self._req("DELETE", f"/api/v2/dags/{dag_id}/dagRuns/{urllib.parse.quote(r['dag_run_id'])}")
        self._req("PATCH", f"/api/v2/dags/{dag_id}", {"is_paused": False})
        run_id = f"compat__{int(time.time() * 1000)}"
        status, data = self._req("POST", f"/api/v2/dags/{dag_id}/dagRuns",
                                 {"dag_run_id": run_id, "logical_date": logical_date, "conf": conf or {}})
        if status >= 300:
            raise RuntimeError(f"trigger {dag_id} -> {status}: {data}")
        rid = urllib.parse.quote(run_id)
        end = time.time() + timeout
        while True:
            run = self.get(f"/api/v2/dags/{dag_id}/dagRuns/{rid}")
            if run["state"] in DONE:
                break
            if time.time() > end:
                raise RuntimeError(f"{dag_id} still {run['state']} after {timeout}s")
            time.sleep(1)
        tis = self.get(f"/api/v2/dags/{dag_id}/dagRuns/{rid}/taskInstances?limit=1000")["task_instances"]
        xcom = {}
        for e in self.get(f"/api/v2/dags/{dag_id}/dagRuns/{rid}/taskInstances/~/xcomEntries?limit=1000").get("xcom_entries", []):
            v = self.get(f"/api/v2/dags/{dag_id}/dagRuns/{rid}/taskInstances/{urllib.parse.quote(e['task_id'])}"
                         f"/xcomEntries/{urllib.parse.quote(e['key'])}?deserialize=true")
            xcom[f"{e['task_id']}.{e['key']}"] = v.get("value")
        return outcome(run["state"], [(t["task_id"], t["state"], t["try_number"], t.get("start_date"), t.get("end_date"))
                                      for t in tis], xcom)


def outcome(state, tis, xcom):
    """{state, tasks: {task: [state, try]}, xcom: {task.key: value}, spans: {task: [start, end]}} (spans not compared)."""
    return {"state": state,
            "tasks": {t: [s, n] for t, s, n, _a, _b in sorted(tis)},
            "xcom": dict(sorted(xcom.items())),
            "spans": {t: [a, b] for t, _s, _n, a, b in sorted(tis)}}


INTERNAL_XCOM = ("skipmixin_key",)   # Airflow's own bookkeeping (which branch was skipped), not user data


def comparable(o):
    out = {k: v for k, v in o.items() if k != "spans"}
    out["xcom"] = {k: v for k, v in o["xcom"].items() if k.split(".", 1)[1] not in INTERNAL_XCOM}
    return out


def respects(o, edges):
    """Every task that ran started after all of its upstream tasks ended."""
    ts = lambda x: datetime.datetime.fromisoformat(x.replace("Z", "+00:00")) if x else None  # noqa: E731
    for up, down in edges:
        a, b = o["spans"].get(up, [None, None]), o["spans"].get(down, [None, None])
        if ts(a[1]) and ts(b[0]):
            assert ts(b[0]) >= ts(a[1]), f"{down} started before {up} ended"


@pytest.fixture(scope="session")
def real():
    url = os.environ.get("AIRFLOW_URL")
    if not url:   # no Docker: compare with what the real Airflow recorded last time (tests/compat/expected/)
        compat_report.versions["airflow"] = "Apache Airflow 3.3.1 — recorded outcomes in tests/compat/expected/ (no AIRFLOW_URL)"
        return None
    af = Airflow(url)
    try:   # one check, no waiting: a container that is still starting fails fast with the reason
        health = json.loads(urllib.request.urlopen(af.base + "/api/v2/monitor/health", timeout=5).read())
    except (OSError, ValueError) as e:
        pytest.exit(f"Airflow at {af.base} is not up ({e}); wait until "
                    "`docker compose -f tests/compat/docker-compose.yml ps` shows healthy", returncode=1)
    down = [k for k, v in health.items() if isinstance(v, dict) and v.get("status") not in (None, "healthy")]
    if down:
        pytest.exit(f"Airflow is up but not healthy yet: {', '.join(down)}; try again in a few seconds", returncode=1)
    compat_report.versions["airflow"] = f"Apache Airflow {af.version()} (Docker)"
    return af


class Ours:
    """lambdaflow in-process: moto's DynamoDB and S3, the DAG file uploaded and parsed, run_local() to the end."""

    def __init__(self, db, s3):
        from engine import Engine
        db.create_table(TableName="lambdaflow", BillingMode="PAY_PER_REQUEST",
                        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}, {"AttributeName": "sk", "KeyType": "RANGE"}],
                        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"},
                                              {"AttributeName": "sk", "AttributeType": "S"}])
        s3.create_bucket(Bucket="lambdaflow", CreateBucketConfiguration={"LocationConstraint": "us-east-2"})
        self.engine = Engine("lambdaflow", "lambdaflow", db=db, s3=s3)

    def run(self, dag_id, conf=None):
        from engine import run_local
        with open(os.path.join(HERE, "dags", f"{dag_id}.py"), encoding="utf-8") as f:
            parsed = self.engine.put_dag_file(f"{dag_id}.py", f.read())
        if dag_id not in parsed:
            err = self.engine.table.get_item(Key={"pk": "err", "sk": f"dags/{dag_id}.py"}).get("Item", {})
            raise RuntimeError(f"lambdaflow could not import {dag_id}.py:\n{err.get('trace', '?')}")
        run_id = run_local(self.engine, dag_id, LOGICAL_DATE, conf, sleep=time.sleep)
        run, tis, xcom = self.engine.run_state(dag_id, run_id)
        return outcome(run["state"], [(t, i["state"], int(i.get("try_number", 0)), i.get("start"), i.get("end"))
                                      for t, i in tis.items()], xcom)


@pytest.fixture
def ours(tmp_path, monkeypatch):
    import boto3
    from moto import mock_aws
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-2")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("LAMBDAFLOW_CACHE", str(tmp_path))
    with mock_aws():
        yield Ours(boto3.resource("dynamodb"), boto3.client("s3"))


@pytest.fixture
def pair(real, ours, request):
    class Pair:
        def dag(self, dag_id, conf=None, edges=()):
            r = None
            if real is not None:
                r = real.run(dag_id, conf)
                os.makedirs(os.path.join(HERE, "expected"), exist_ok=True)
                with open(os.path.join(HERE, "expected", f"{request.node.name}.json"), "w", encoding="utf-8") as f:
                    json.dump(comparable(r), f, indent=1, sort_keys=True)
                    f.write("\n")
                respects(r, edges)
            else:
                with open(os.path.join(HERE, "expected", f"{request.node.name}.json"), encoding="utf-8") as f:
                    r = json.load(f)
            o = ours.run(dag_id, conf)
            respects(o, edges)
            assert comparable(o) == comparable(r), f"{dag_id}\n  real: {comparable(r)}\n  ours: {comparable(o)}"
            return o
    return Pair()
