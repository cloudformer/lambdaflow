"""lambdaflow.client — your lambdaflow deployment from Python: upload DAG files, trigger runs, wait, read logs.

Standard library only. The transport below (keep-alive connection, strict retry policy: re-send only
when the request provably never reached the server) is shared across the series; the methods on top
are the console's JSON API, plus cli() for the airflow commands the console's command box understands.

    from lambdaflow import client
    lf = client.connect("https://xxxx.lambda-url.us-east-2.on.aws", "admin", "secret")
    lf.upload("my_dag.py")
    run_id = lf.trigger("my_dag", conf={"x": 1}, unpause=True)
    print(lf.wait("my_dag", run_id))          # "success" | "failed"
"""
import base64
import http.client
import json
import os
import socket
import time
import urllib.parse

__version__ = "0.1.0"


class Error(Exception):
    def __init__(self, status, message):
        super().__init__(status, message)
        self.status = status
        self.message = message


class NotSupportedError(Error):
    """The feature needs Airflow / MWAA; refused explicitly, never silently different."""


class _NotDelivered(Exception):
    def __init__(self, cause, reused):
        super().__init__(cause)
        self.reused = reused


class Connection:
    def __init__(self, url, user="", password="", timeout=30):
        p = urllib.parse.urlparse(url.rstrip("/"))
        self._host, self._base_path = p.netloc, p.path
        self._auth = base64.b64encode(f"{user}:{password}".encode()).decode()
        self.timeout = timeout
        self._conn = None
        self.last = {}   # server-side metrics of the last call

    # ---- transport (shared) ----

    def close(self):
        try:
            if self._conn:
                self._conn.close()
        finally:
            self._conn = None

    def _send(self, method, path, body):
        reused = self._conn is not None
        if self._conn is None:
            self._conn = http.client.HTTPSConnection(self._host, timeout=self.timeout)
        headers = {"content-type": "application/json", "authorization": f"Basic {self._auth}"}
        try:
            self._conn.request(method, self._base_path + path, body, headers)
        except (socket.timeout, TimeoutError):
            raise
        except (OSError, http.client.HTTPException) as e:
            raise _NotDelivered(e, reused) from None
        try:
            resp = self._conn.getresponse()
        except (http.client.RemoteDisconnected, http.client.BadStatusLine) as e:
            raise _NotDelivered(e, reused) from None
        raw = resp.read()
        try:
            data = json.loads(raw)
        except ValueError:
            data = {"error": raw[:200].decode(errors="replace")}
        if resp.status >= 400:
            raise Error(resp.status, data.get("error", ""))
        return data

    def _call(self, method, path, payload=None):
        body = json.dumps(payload) if payload is not None else None
        try:
            data = self._send(method, path, body)
        except _NotDelivered as e:
            self.close()
            if not e.reused:
                raise Error(0, f"request not delivered: {e}") from None
            data = self._send(method, path, body)   # an idle keep-alive connection was closed: safe to re-send once
        except (socket.timeout, TimeoutError) as e:
            self.close()
            raise Error(0, f"timeout: result unknown ({e})") from None
        self.last = {"ms": data.get("ms")}
        return data

    # ---- the API: the console's routes ----

    def ping(self):
        return self._call("GET", "/api/ping").get("ok", False)

    def dags(self):
        """Every DAG: dag_id, paused, schedule, next_run, recent runs."""
        return self._call("GET", "/api/dags")["dags"]

    def import_errors(self):
        return self._call("GET", "/api/import-errors")["import_errors"]

    def upload(self, path, name=None):
        """Upload a DAG file; parsed at once. Returns {"dags": [...], "removed": [...], "error": None | {...}}."""
        with open(path, encoding="utf-8") as f:
            source = f.read()
        return self._call("PUT", "/api/files/" + _q(name or os.path.basename(path)), {"source": source})

    def pause(self, dag_id, paused=True):
        return self._call("POST", f"/api/dags/{_q(dag_id)}/pause", {"paused": paused})

    def unpause(self, dag_id):
        return self.pause(dag_id, False)

    def trigger(self, dag_id, conf=None, logical_date=None, unpause=False):
        """Start a run (a paused DAG needs unpause=True); returns its run_id."""
        return self._call("POST", f"/api/dags/{_q(dag_id)}/runs",
                          {"conf": conf or {}, "logical_date": logical_date, "unpause": unpause})["run_id"]

    def runs(self, dag_id, limit=25):
        """The DAG's runs, oldest first, each with its task instances ("tis")."""
        return self._call("GET", f"/api/dags/{_q(dag_id)}?runs={int(limit)}")["runs"]

    def run(self, dag_id, run_id):
        """{"run": {...}, "tis": {task_id: {...}}, "xcom": {"task.key": value}}"""
        return self._call("GET", f"/api/dags/{_q(dag_id)}/runs/{_q(run_id)}")

    def wait(self, dag_id, run_id, timeout=900, every=3):
        """Poll until the run is done; returns "success" or "failed"."""
        end = time.time() + timeout
        while True:
            state = self.run(dag_id, run_id)["run"]["state"]
            if state in ("success", "failed"):
                return state
            if time.time() > end:
                raise Error(0, f"run {run_id} still {state} after {timeout}s")
            time.sleep(every)

    def log(self, dag_id, run_id, task_id, try_number=None):
        q = f"?try={int(try_number)}" if try_number else ""
        return self._call("GET", f"/api/dags/{_q(dag_id)}/runs/{_q(run_id)}/tasks/{_q(task_id)}/log{q}")["log"]

    def clear(self, dag_id, run_id, task_ids=None, downstream=True, only_failed=False):
        return self._call("POST", f"/api/dags/{_q(dag_id)}/runs/{_q(run_id)}/clear",
                          {"task_ids": task_ids, "downstream": downstream, "only_failed": only_failed})["cleared"]

    def cli(self, command):
        """An airflow command, as typed in the console's command box: cli("dags list-runs my_dag")."""
        return self._call("POST", "/api/cli", {"command": command})["output"]


def _q(s):
    return urllib.parse.quote(str(s), safe="")


def connect(host, user="", password="", timeout=30, **_ignored):
    """host: the console_url terraform printed (with or without https://)."""
    url = host if host.startswith("http") else "https://" + host
    return Connection(url, user, password, timeout)
