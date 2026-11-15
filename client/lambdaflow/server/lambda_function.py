"""lambdaflow console: one Lambda behind a Function URL — JSON API, web console at GET /, S3 events for dags/.

The first half (JSON, request body, HTTP Basic auth, routing, error mapping, timing) is the same in every
project of the series. The console never imports a DAG file: parsing runs user code, so it is the worker's
job (a separate low-privilege Lambda; in-process when WORKER_FUNCTION is unset, i.e. locally).

    GET    /                                             console (index.html)
    GET    /api/ping
    GET    /api/dags                                     DAG list, recent runs, next run, import errors
    GET    /api/dags/<dag>?runs=25                       one DAG: spec + the grid (runs × task instances)
    GET    /api/dags/<dag>/source                        the DAG file
    PUT    /api/files/<file.py>        {source}          upload a DAG file; parsed at once
    DELETE /api/dags/<dag>                               the DAG, its runs and logs (and its file if alone in it)
    POST   /api/dags/<dag>/pause       {paused}
    POST   /api/dags/<dag>/runs        {conf, logical_date, unpause}   trigger
    GET    /api/dags/<dag>/runs/<run>                    run, task instances, XComs
    POST   /api/dags/<dag>/runs/<run>/clear   {task_ids | only_failed, downstream}
    POST   /api/dags/<dag>/runs/<run>/tasks/<task>/mark  {state: success | failed}
    GET    /api/dags/<dag>/runs/<run>/tasks/<task>/log?try=<n>
    POST   /api/cli                    {command}         a subset of the airflow CLI (the command box)
"""
import base64
import hmac
import json
import os
import queue
import re
import shlex
import threading
import time
import urllib.parse
import uuid

USER = os.environ.get("USER_NAME", "")
PASSWORD = os.environ.get("PASSWORD", "")
CONSOLE = os.environ.get("CONSOLE", "1") == "1"

with open(os.path.join(os.path.dirname(__file__), "index.html"), encoding="utf-8") as f:
    INDEX_HTML = f.read()


class BadRequest(Exception):
    pass


class Forbidden(Exception):
    pass


# ---------------- shared: the same in every project ----------------

def _json(status, data):
    return {"statusCode": status, "headers": {"content-type": "application/json"},
            "body": json.dumps(data, ensure_ascii=False, default=str)}


def _body(event):
    """The JSON request body; 400 when it is not a JSON object."""
    raw = event.get("body") or "{}"
    try:
        if event.get("isBase64Encoded"):
            raw = base64.b64decode(raw)
        data = json.loads(raw)
    except ValueError:
        raise BadRequest("request body is not valid JSON") from None
    if not isinstance(data, dict):
        raise BadRequest("request body must be a JSON object")
    return data


def _authed(event):
    """HTTP Basic, constant-time compare. No USER_NAME configured = no auth (do not do that)."""
    if not USER:
        return True
    auth = (event.get("headers") or {}).get("authorization", "")
    if not auth.lower().startswith("basic "):
        return False
    try:
        user, _, password = base64.b64decode(auth.split(None, 1)[1]).decode().partition(":")
    except Exception:
        return False
    return hmac.compare_digest(user.encode(), USER.encode()) & hmac.compare_digest(password.encode(), PASSWORD.encode())


ERRORS = [(BadRequest, 400), (Forbidden, 403), (KeyError, 404), (ValueError, 400)]   # exception class -> HTTP status


def lambda_handler(event, context):
    if "Records" in event:                       # S3 events for dags/ (a file copied in with `aws s3 cp`)
        return s3_event(event)
    http = (event.get("requestContext") or {}).get("http") or {}
    method, path = http.get("method", "GET"), event.get("rawPath", "/")
    if CONSOLE and method == "GET" and path in ("/", "/index.html"):
        return {"statusCode": 200, "headers": {"content-type": "text/html; charset=utf-8"}, "body": INDEX_HTML}
    if not _authed(event):
        return _json(401, {"error": "bad credentials"})
    t0 = time.perf_counter()
    status, data = 404, {"error": f"no route: {method} {path}"}
    try:
        for route_method, pattern, handler in ROUTES:
            match = pattern.fullmatch(path)
            if match and route_method == method:
                out = handler(event, **{k: urllib.parse.unquote(v) for k, v in match.groupdict().items()})
                status, data = out if isinstance(out, tuple) else (200, out)
                break
    except Exception as e:  # noqa: BLE001 - every error becomes a JSON response
        status = 400 if type(e).__name__ == "NotSupportedError" else next((s for cls, s in ERRORS if isinstance(e, cls)), 500)
        msg = e.args[0] if isinstance(e, KeyError) and e.args else e
        data = {"error": str(msg) if status < 500 else f"{type(e).__name__}: {e}"}
    data.setdefault("ms", round((time.perf_counter() - t0) * 1000, 1))
    return _json(status, data)


# ---------------- the project's API ----------------

ENGINE = None
DAG_ID = r"[A-Za-z0-9_.\-]{1,200}"
FILE = r"[A-Za-z0-9_.\-]{1,200}\.py"


def engine():
    """The console's engine keeps EventBridge Scheduler in sync (when deployed with a state machine)."""
    global ENGINE
    if ENGINE is None:
        import boto3

        from engine import Engine
        schedules = None
        if os.environ.get("STATE_MACHINE_ARN") and os.environ.get("SCHEDULE_GROUP"):
            schedules = {"client": boto3.client("scheduler"), "group": os.environ["SCHEDULE_GROUP"],
                         "target": os.environ["STATE_MACHINE_ARN"], "role": os.environ["SCHEDULER_ROLE_ARN"],
                         "dlq": os.environ.get("DLQ_ARN")}
        ENGINE = Engine(os.environ["TABLE"], os.environ["BUCKET"], schedules=schedules)
    return ENGINE


def worker(payload):
    """The worker Lambda (synchronous), or the worker module in this process when running locally."""
    name = os.environ.get("WORKER_FUNCTION")
    if not name:
        import worker as w
        w.ENGINE = engine()
        return w.lambda_handler(payload)
    import boto3
    r = boto3.client("lambda").invoke(FunctionName=name, Payload=json.dumps(payload).encode())
    out = json.loads(r["Payload"].read() or b"null")
    if r.get("FunctionError"):
        raise RuntimeError(f"worker: {out.get('errorType')}: {out.get('errorMessage')}")
    return out


def _local_runner():
    """Locally: one background thread plays the state machine, one run at a time (task logs capture stdout)."""
    jobs = queue.Queue()

    def serve():
        import worker as w
        while True:
            payload = jobs.get()
            try:
                w.ENGINE = engine()
                w.loop(payload)
            except Exception:  # noqa: BLE001 - a broken run must not stop the runner
                import traceback
                traceback.print_exc()
    threading.Thread(target=serve, daemon=True).start()
    return jobs.put


RUNNER = None   # tests replace it with a synchronous call


def start(payload):
    """Start the run's loop: a Step Functions execution, or the local runner."""
    global RUNNER
    arn = os.environ.get("STATE_MACHINE_ARN")
    if arn:
        import boto3
        name = re.sub(r"[^A-Za-z0-9_-]", "_", payload["dag_id"])[:60] + "-" + uuid.uuid4().hex[:12]
        boto3.client("stepfunctions").start_execution(stateMachineArn=arn, name=name, input=json.dumps(payload))
        return "StartExecution"
    if RUNNER is None:
        RUNNER = _local_runner()
    RUNNER(payload)
    return "local runner"


def _parse_file(key):
    e = engine()
    before = e.dags_of_file(key)
    after = worker({"action": "parse", "key": key})["dags"]
    for dag_id in sorted(set(before) | set(after)):
        e.sync_schedule(dag_id)
    err = next((x for x in e.import_errors() if x["file"] == key), None)
    return {"file": key, "dags": after, "removed": sorted(set(before) - set(after)), "error": err}


def s3_event(event):
    out = []
    for r in event["Records"]:
        key = urllib.parse.unquote_plus(r["s3"]["object"]["key"])
        if not (key.startswith("dags/") and key.endswith(".py")):
            continue
        if r["eventName"].startswith("ObjectCreated"):
            out.append(_parse_file(key))
        else:
            gone = engine().forget_file(key)
            for dag_id in gone:
                engine().sync_schedule(dag_id)
            out.append({"file": key, "removed": gone})
    return {"files": out}


def _dag_summary(e, d, runs=10):
    s = d["spec"]
    return {"dag_id": d["dag_id"], "file": d["file"], "paused": d["paused"], "parsed": d["parsed"],
            "description": s.get("description"), "tags": s.get("tags", []), "schedule": s.get("schedule"),
            "expression": s.get("expression"), "timezone": s.get("timezone"),
            "next_run": None if d["paused"] else e.next_run(s), "tasks": len(s["tasks"]),
            "runs": [{k: r[k] for k in ("run_id", "state", "logical_date", "run_type")} for r in e.runs(d["dag_id"], runs)]}


def ping(event):
    return {"ok": True}


def list_dags(event):
    e = engine()
    return {"dags": sorted((_dag_summary(e, d) for d in e.dags()), key=lambda x: x["dag_id"]),
            "import_errors": e.import_errors()}


def get_dag(event, dag):
    e = engine()
    item = e.meta(dag)
    spec = json.loads(item["spec"])
    n = int((event.get("queryStringParameters") or {}).get("runs", 25))
    runs = list(reversed(e.runs(dag, n)))                       # oldest first, as Airflow's grid
    for r in runs:
        _, tis, _ = e.run_state(dag, r["run_id"])
        r["tis"] = {t: {"state": i.get("state"), "try_number": int(i.get("try_number", 0)),
                        "max_tries": int(i.get("max_tries", spec["tasks"].get(t, {}).get("retries", 0))),
                        "start": i.get("start"), "end": i.get("end")} for t, i in tis.items()}
    return {"dag": dict(spec, file=item["file"], paused=bool(item.get("paused")),
                        next_run=None if item.get("paused") else e.next_run(spec)), "runs": runs}


def dag_source(event, dag):
    e = engine()
    key = e.meta(dag)["file"]
    return {"file": key, "source": e.source(key)}


def put_file(event, name):
    source = _body(event).get("source")
    if not isinstance(source, str):
        raise BadRequest("source (the DAG file's text) is required")
    engine().s3.put_object(Bucket=engine().bucket, Key="dags/" + name, Body=source.encode())
    return _parse_file("dags/" + name)


def delete_dag(event, dag):
    engine().delete_dag(dag)
    return {"deleted": dag}


def pause(event, dag):
    paused = bool(_body(event).get("paused", True))
    engine().set_paused(dag, paused)
    return {"dag_id": dag, "paused": paused}


def trigger(event, dag):
    b, e = _body(event), engine()
    conf = b.get("conf") or {}
    if isinstance(conf, str):
        conf = json.loads(conf or "{}")
    if not isinstance(conf, dict):
        raise BadRequest("conf must be a JSON object")
    if e.meta(dag).get("paused"):
        if not b.get("unpause"):
            raise BadRequest(f"DAG {dag} is paused: unpause it first (a run of a paused DAG would not start)")
        e.set_paused(dag, False)
    run_id, created = e.create_run(dag, b.get("logical_date") or None, conf)
    if not created:
        raise BadRequest(f"run {run_id} exists already")
    return 201, {"run_id": run_id, "backend": start({"action": "resume", "dag_id": dag, "run_id": run_id})}


def get_run(event, dag, run):
    e = engine()
    r, tis, xcom = e.run_state(dag, run)
    return {"run": {k: v for k, v in r.items() if k not in ("pk", "sk")} | {"run_id": run,
                                                                             "conf": json.loads(r.get("conf") or "{}")},
            "tis": {t: {k: (int(v) if k in ("try_number", "max_tries") else v) for k, v in i.items() if k not in ("pk", "sk")}
                    for t, i in tis.items()},
            "xcom": xcom}


def clear(event, dag, run):
    b, e = _body(event), engine()
    task_ids = b.get("task_ids")
    if b.get("only_failed"):
        _, tis, _ = e.run_state(dag, run)
        task_ids = [t for t, i in tis.items() if i.get("state") in ("failed", "upstream_failed")]
    elif not task_ids:
        task_ids = list(e.spec(dag)[0]["tasks"])
    if not task_ids:
        raise BadRequest("nothing to clear")
    cleared = e.clear(dag, run, task_ids, downstream=bool(b.get("downstream", True)))
    return {"cleared": cleared, "backend": start({"action": "resume", "dag_id": dag, "run_id": run})}


def mark(event, dag, run, task):
    state = _body(event).get("state")
    engine().mark(dag, run, task, state)
    return {"task_id": task, "state": state, "backend": start({"action": "resume", "dag_id": dag, "run_id": run})}


def task_log(event, dag, run, task):
    e = engine()
    q = event.get("queryStringParameters") or {}
    ti = e.run_state(dag, run)[1].get(task, {})
    n = int(q.get("try") or ti.get("try_number") or 0)
    log = e.read_log(dag, run, task, n) if n else None
    return {"try_number": n, "tries": int(ti.get("try_number", 0)),
            "log": log if log is not None else ("" if n else "This task has not run.")}


def import_errors(event):
    return {"import_errors": engine().import_errors()}


# ---------------- the command box: a subset of the airflow CLI ----------------

def _table(rows, cols):
    if not rows:
        return "No data found"
    w = [max(len(c), *(len(str(r.get(c, ""))) for r in rows)) for c in cols]
    line = lambda vals: " | ".join(str(v).ljust(n) for v, n in zip(vals, w)).rstrip()  # noqa: E731
    return "\n".join([line(cols), "=+=".join("=" * n for n in w)] + [line([r.get(c, "") for c in cols]) for r in rows])


CLI_HELP = """lambdaflow understands these airflow commands ("airflow" is optional):
  dags list                                  dags list-import-errors
  dags trigger <dag> [-c JSON] [-l DATE]     dags pause <dag> | dags unpause <dag>
  dags list-runs <dag>                       dags state <dag> <run_id | logical date>
  dags next-execution <dag>                  dags delete <dag> -y
  tasks list <dag>                           tasks states-for-dag-run <dag> <run_id>
  tasks clear <dag> -t <task> -r <run_id> [--no-downstream]"""


def _flags(args, spec):
    """-c x / --conf x style options; spec maps every spelling to a name. Returns (options, positionals)."""
    opts, pos, i = {}, [], 0
    while i < len(args):
        a = args[i]
        if a in spec:
            name, takes = spec[a]
            if takes:
                if i + 1 >= len(args):
                    raise BadRequest(f"{a} needs a value")
                opts[name] = args[i + 1]
                i += 1
            else:
                opts[name] = True
        elif a.startswith("-"):
            raise BadRequest(f"unknown option {a}\n{CLI_HELP}")
        else:
            pos.append(a)
        i += 1
    return opts, pos


def cli(event):
    body = _body(event)
    if not isinstance(body.get("command"), str):
        raise BadRequest(f"no command in the request body (keys: {sorted(body) or 'none'}, "
                         f"base64: {bool(event.get('isBase64Encoded'))}, {len(event.get('body') or '')} bytes)")
    line = body["command"].strip()
    try:
        args = shlex.split(line)
    except ValueError as err:
        raise BadRequest(f"cannot parse: {err}") from None
    if args[:1] == ["airflow"]:
        args = args[1:]
    if not args or args[0] in ("help", "-h", "--help"):
        return {"output": CLI_HELP}
    e, group, cmd, rest = engine(), args[0], (args[1:2] or [""])[0], args[2:]

    def need(n):
        if len(rest) <= n:
            raise BadRequest(f"usage:\n{CLI_HELP}")
        return rest[n]
    if group == "dags" and cmd == "list":
        rows = [{"dag_id": d["dag_id"], "fileloc": d["file"], "is_paused": str(d["paused"]),
                 "schedule": d["spec"].get("schedule")} for d in sorted(e.dags(), key=lambda d: d["dag_id"])]
        return {"output": _table(rows, ["dag_id", "fileloc", "is_paused", "schedule"])}
    if group == "dags" and cmd == "list-import-errors":
        return {"output": _table(e.import_errors(), ["file", "error"])}
    if group == "dags" and cmd == "trigger":
        o, pos = _flags(rest, {"-c": ("conf", 1), "--conf": ("conf", 1), "-l": ("logical_date", 1),
                               "--logical-date": ("logical_date", 1), "-r": ("run_id", 1), "--run-id": ("run_id", 1)})
        if not pos:
            raise BadRequest("usage: dags trigger <dag> [-c JSON] [-l DATE]")
        conf = json.loads(o.get("conf") or "{}")
        dag = pos[0]
        if e.meta(dag).get("paused"):
            raise BadRequest(f"DAG {dag} is paused: run `dags unpause {dag}` first")
        run_id, created = e.create_run(dag, o.get("logical_date"), conf, o.get("run_id"))
        if not created:
            raise BadRequest(f"run {run_id} exists already")
        start({"action": "resume", "dag_id": dag, "run_id": run_id})
        return {"output": f"Created <DagRun {dag} @ {run_id}: queued>", "dag_id": dag, "run_id": run_id}
    if group == "dags" and cmd in ("pause", "unpause"):
        e.set_paused(need(0), cmd == "pause")
        return {"output": f"Dag: {rest[0]}, paused: {cmd == 'pause'}", "dag_id": rest[0]}
    if group == "dags" and cmd == "list-runs":
        rows = [dict(r, dag_id=need(0)) for r in e.runs(need(0))]
        return {"output": _table(rows, ["dag_id", "run_id", "state", "logical_date", "start", "end"]), "dag_id": rest[0]}
    if group == "dags" and cmd == "state":
        runs = [r for r in e.runs(need(0)) if need(1) in (r["run_id"], r["logical_date"])]
        if not runs:
            raise KeyError(f"no run {rest[1]} of DAG {rest[0]}")
        return {"output": runs[0]["state"], "dag_id": rest[0]}
    if group == "dags" and cmd == "next-execution":
        d = e.meta(need(0))
        spec = json.loads(d["spec"])
        return {"output": "None" if d.get("paused") else str(e.next_run(spec)), "dag_id": rest[0]}
    if group == "dags" and cmd == "delete":
        o, pos = _flags(rest, {"-y": ("yes", 0), "--yes": ("yes", 0)})
        if not pos or not o.get("yes"):
            raise BadRequest("usage: dags delete <dag> -y   (deletes its runs, logs and, if alone in it, its file)")
        e.delete_dag(pos[0])
        return {"output": f"Removed DAG {pos[0]}"}
    if group == "tasks" and cmd == "list":
        return {"output": "\n".join(e.spec(need(0))[0]["tasks"]), "dag_id": rest[0]}
    if group == "tasks" and cmd == "states-for-dag-run":
        _, tis, _ = e.run_state(need(0), need(1))
        rows = [{"task_id": t, "state": i.get("state"), "try_number": int(i.get("try_number", 0)),
                 "start_date": i.get("start"), "end_date": i.get("end")} for t, i in sorted(tis.items())]
        return {"output": _table(rows, ["task_id", "state", "try_number", "start_date", "end_date"]), "dag_id": rest[0]}
    if group == "tasks" and cmd == "clear":
        o, pos = _flags(rest, {"-t": ("task", 1), "--task-regex": ("task", 1), "-r": ("run_id", 1),
                               "--run-id": ("run_id", 1), "--no-downstream": ("nodown", 0)})
        if not pos or not o.get("task") or not o.get("run_id"):
            raise BadRequest("usage: tasks clear <dag> -t <task regex> -r <run_id> [--no-downstream]")
        tasks = [t for t in e.spec(pos[0])[0]["tasks"] if re.search(o["task"], t)]
        if not tasks:
            raise BadRequest(f"no task matches {o['task']!r}")
        cleared = e.clear(pos[0], o["run_id"], tasks, downstream=not o.get("nodown"))
        start({"action": "resume", "dag_id": pos[0], "run_id": o["run_id"]})
        return {"output": "Cleared: " + ", ".join(cleared), "dag_id": pos[0]}
    raise BadRequest(f"not supported: {line}\n{CLI_HELP}")


ROUTES = [
    ("GET",    re.compile(r"/api/ping"), ping),
    ("GET",    re.compile(r"/api/dags"), list_dags),
    ("GET",    re.compile(r"/api/import-errors"), import_errors),
    ("POST",   re.compile(r"/api/cli"), cli),
    ("PUT",    re.compile(rf"/api/files/(?P<name>{FILE})"), put_file),
    ("GET",    re.compile(rf"/api/dags/(?P<dag>{DAG_ID})"), get_dag),
    ("DELETE", re.compile(rf"/api/dags/(?P<dag>{DAG_ID})"), delete_dag),
    ("GET",    re.compile(rf"/api/dags/(?P<dag>{DAG_ID})/source"), dag_source),
    ("POST",   re.compile(rf"/api/dags/(?P<dag>{DAG_ID})/pause"), pause),
    ("POST",   re.compile(rf"/api/dags/(?P<dag>{DAG_ID})/runs"), trigger),
    ("GET",    re.compile(rf"/api/dags/(?P<dag>{DAG_ID})/runs/(?P<run>[^/]+)"), get_run),
    ("POST",   re.compile(rf"/api/dags/(?P<dag>{DAG_ID})/runs/(?P<run>[^/]+)/clear"), clear),
    ("POST",   re.compile(rf"/api/dags/(?P<dag>{DAG_ID})/runs/(?P<run>[^/]+)/tasks/(?P<task>[^/]+)/mark"), mark),
    ("GET",    re.compile(rf"/api/dags/(?P<dag>{DAG_ID})/runs/(?P<run>[^/]+)/tasks/(?P<task>[^/]+)/log"), task_log),
]
