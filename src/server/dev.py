"""lambdaflow on a laptop: the console, the worker and a scheduler in one process, AWS mocked by moto.

    python3 src/server/dev.py [dags folder] [--port 8080]

Opens http://127.0.0.1:8080 (user admin, password admin). The DAG files of the folder are uploaded at start
(default: tests/compat/dags); everything else is done in the console. Nothing is kept: the table and the
bucket live in memory. The state machine is a background thread, EventBridge Scheduler is a thread that
fires unpaused DAGs by schedule.next_fire(), the same evaluator the console shows as "next run".
Needs: pip install boto3 moto jinja2
"""
import argparse
import datetime
import glob
import http.server
import json
import os
import sys
import threading
import time
import urllib.parse

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("dags", nargs="?", default=os.path.join(HERE, "..", "..", "tests", "compat", "dags"))
    ap.add_argument("--port", type=int, default=8080)
    args = ap.parse_args()
    for k, v in {"AWS_DEFAULT_REGION": "us-east-2", "AWS_ACCESS_KEY_ID": "dev", "AWS_SECRET_ACCESS_KEY": "dev",
                 "TABLE": "lambdaflow-dev", "BUCKET": "lambdaflow-dev", "USER_NAME": "admin", "PASSWORD": "admin",
                 "LAMBDAFLOW_CACHE": os.path.join(os.environ.get("TMPDIR", "/tmp"), "lambdaflow-dev")}.items():
        os.environ.setdefault(k, v)
    for k in ("STATE_MACHINE_ARN", "WORKER_FUNCTION", "SCHEDULE_GROUP"):
        os.environ.pop(k, None)

    import boto3
    from moto import mock_aws
    mock_aws().start()
    boto3.resource("dynamodb").create_table(
        TableName=os.environ["TABLE"], BillingMode="PAY_PER_REQUEST",
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}, {"AttributeName": "sk", "KeyType": "RANGE"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}, {"AttributeName": "sk", "AttributeType": "S"}])
    boto3.client("s3").create_bucket(Bucket=os.environ["BUCKET"],
                                     CreateBucketConfiguration={"LocationConstraint": os.environ["AWS_DEFAULT_REGION"]})

    import lambda_function as lf
    for path in sorted(glob.glob(os.path.join(args.dags, "*.py"))):
        with open(path, encoding="utf-8") as f:
            r = lf.lambda_handler({"requestContext": {"http": {"method": "PUT"}}, "body": json.dumps({"source": f.read()}),
                                   "rawPath": "/api/files/" + os.path.basename(path), "headers": _auth()}, None)
        print(f"  {os.path.basename(path)}: {json.loads(r['body']).get('dags')}")
    threading.Thread(target=_scheduler, args=(lf,), daemon=True).start()

    class Handler(http.server.BaseHTTPRequestHandler):
        def _serve(self):
            u = urllib.parse.urlsplit(self.path)
            n = int(self.headers.get("content-length") or 0)
            event = {"requestContext": {"http": {"method": self.command}}, "rawPath": u.path,
                     "headers": {k.lower(): v for k, v in self.headers.items()},
                     "queryStringParameters": dict(urllib.parse.parse_qsl(u.query)) or None,
                     "body": self.rfile.read(n).decode() if n else None}
            r = lf.lambda_handler(event, None)
            body = r["body"].encode()
            self.send_response(r["statusCode"])
            for k, v in r["headers"].items():
                self.send_header(k, v)
            self.send_header("content-length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        do_GET = do_POST = do_PUT = do_DELETE = _serve

        def log_message(self, fmt, *a):
            if not self.path.startswith("/api/dags/") or self.command != "GET":   # the console polls: keep quiet
                sys.stderr.write(f"{self.command} {self.path} -> {a[1] if len(a) > 1 else ''}\n")

    print(f"lambdaflow dev console: http://127.0.0.1:{args.port}   (admin / admin; Ctrl+C stops, nothing is kept)")
    http.server.ThreadingHTTPServer(("127.0.0.1", args.port), Handler).serve_forever()


def _auth():
    import base64
    return {"authorization": "Basic " + base64.b64encode(b"admin:admin").decode()}


def _scheduler(lf):
    """EventBridge Scheduler stand-in: every unpaused DAG with a schedule fires when its next time comes."""
    import schedule
    seen = datetime.datetime.now(datetime.timezone.utc)
    while True:
        time.sleep(5)
        now = datetime.datetime.now(datetime.timezone.utc)
        for d in lf.engine().dags():
            s = d["spec"]
            if d["paused"] or not s.get("expression"):
                continue
            t = schedule.next_fire(s["expression"], s["timezone"], seen)
            if t and t <= now:
                print(f"  schedule fired: {d['dag_id']} @ {t.isoformat()}")
                lf.start({"action": "start", "dag_id": d["dag_id"], "logical_date": t.isoformat().replace("+00:00", "Z")})
        seen = now + datetime.timedelta(microseconds=1)


if __name__ == "__main__":
    main()
