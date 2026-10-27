"""lambdaflow's engine: DAG files in S3, all state in one DynamoDB table, Airflow's scheduling rules.

    S3   dags/<file>.py                                         the DAG files (same layout as MWAA)
         logs/dag_id=<d>/run_id=<r>/task_id=<t>/attempt=<n>.log  task logs (Airflow's remote-log layout)
    DynamoDB (pk, sk):
         dags                  <dag_id>       one DAG: file, spec (tasks, upstream ids, trigger rules, schedule), paused
         dag#<dag_id>          run#<run_id>   one DAG run: state, logical date, conf, start / end
         run#<dag_id>#<run_id> ti#<task_id>   one task instance: state, try number, start / end, next try
         run#<dag_id>#<run_id> xcom#<task>#<key>   one XCom value (JSON)
         err                   <file>         the import error of a DAG file

One DAG run is a loop of three steps — the Step Functions state machine in AWS, run_local() on a laptop:
    step()      plan: apply trigger rules (skip / upstream_failed), claim and return the tasks that can run now
    run_task()  execute one task (the worker Lambda), write its state, XCom and log
    wait        when only retries (or max_active_runs) are pending, wait until it is worth looking again

Every write that decides something is conditional, so an at-least-once trigger or a second state-machine
execution of the same run never creates a run twice nor runs a task twice.
"""
import contextlib
import copy
import datetime
import io
import json
import logging
import os
import sys
import time
import traceback
import types
import uuid

import boto3
import jinja2
from boto3.dynamodb.conditions import Attr, Key
from botocore.exceptions import ClientError

import schedule as sched

TERMINAL = {"success", "failed", "skipped", "upstream_failed"}
ACTIVE = {"queued", "running"}
PAUSED_AT_CREATION = os.environ.get("PAUSED_AT_CREATION", "1") == "1"   # Airflow's default


def _now():
    return time.time()


def _iso(t):
    return datetime.datetime.fromtimestamp(t, datetime.timezone.utc).isoformat().replace("+00:00", "Z")


def _date(s):
    return datetime.datetime.fromisoformat(s.replace("Z", "+00:00"))


def _conflict(e):
    return e.response["Error"]["Code"] == "ConditionalCheckFailedException"


def evaluate(rule, upstream_states):
    """Airflow's trigger-rule decision for a task that has not started: run | wait | skipped | upstream_failed."""
    n = len(upstream_states)
    if n == 0 or rule == "always":
        return "run"
    s, sk = upstream_states.count("success"), upstream_states.count("skipped")
    f, uf = upstream_states.count("failed"), upstream_states.count("upstream_failed")
    done = s + sk + f + uf == n
    if rule == "all_success":
        return "upstream_failed" if f or uf else "skipped" if sk else "run" if s == n else "wait"
    if rule == "all_failed":
        return "skipped" if s or sk else "run" if f + uf == n else "wait"
    if rule == "all_done":
        return "run" if done else "wait"
    if rule == "all_skipped":
        return "skipped" if s or f or uf else "run" if sk == n else "wait"
    if rule == "one_success":
        return "run" if s else ("skipped" if sk == n else "upstream_failed") if done else "wait"
    if rule == "one_failed":
        return "run" if f or uf else "skipped" if done else "wait"
    if rule == "one_done":
        return "run" if s or f else "skipped" if done else "wait"
    if rule == "none_failed":
        return "upstream_failed" if f or uf else "run" if done else "wait"
    if rule == "none_failed_min_one_success":
        return "upstream_failed" if f or uf else ("skipped" if sk == n else "run") if done else "wait"
    if rule == "none_skipped":
        return "skipped" if sk else "run" if done else "wait"
    raise ValueError(f"unknown trigger rule {rule!r}")


def _macros():
    def ds_add(ds, days):
        return (datetime.date.fromisoformat(ds) + datetime.timedelta(days=int(days))).isoformat()

    def ds_format(ds, input_format, output_format):
        return datetime.datetime.strptime(ds, input_format).strftime(output_format)
    return types.SimpleNamespace(ds_add=ds_add, ds_format=ds_format, datetime=datetime.datetime,
                                 timedelta=datetime.timedelta, json=json, time=time, uuid=uuid)


_JINJA = jinja2.Environment()


def render(value, context):
    """Render Jinja templates inside strings, lists, tuples and dicts (a template field's value)."""
    if isinstance(value, str):
        return _JINJA.from_string(value).render(**context) if "{{" in value or "{%" in value else value
    if isinstance(value, (list, tuple)):
        return type(value)(render(v, context) for v in value)
    if isinstance(value, dict):
        return {k: render(v, context) for k, v in value.items()}
    return value


class _Tee(io.TextIOBase):
    """A task's print() / traceback go to its log and still to stdout (CloudWatch)."""

    def __init__(self, *streams):
        self.streams = streams

    def write(self, s):
        for st in self.streams:
            st.write(s)
        return len(s)

    def flush(self):
        for st in self.streams:
            st.flush()


LOG_FORMAT = "[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s"   # Airflow's task log line


class Engine:
    def __init__(self, table_name, bucket, db=None, s3=None, schedules=None):
        """schedules: {"client", "group", "target", "role"[, "dlq"]} to keep EventBridge Scheduler in sync
        (the console's engine); None = leave schedules alone (the worker, the tests of the planner)."""
        self.db = db or boto3.resource("dynamodb")
        self.table = self.db.Table(table_name)
        self.s3 = s3 or boto3.client("s3")
        self.bucket = bucket
        self.schedules = schedules
        self._modules = {}   # S3 key -> (ETag, {dag_id: DAG}): a warm worker imports a DAG file once

    def _query(self, **kw):
        items = []
        while True:
            r = self.table.query(ConsistentRead=True, **kw)
            items += r["Items"]
            if "LastEvaluatedKey" not in r:
                return items
            kw["ExclusiveStartKey"] = r["LastEvaluatedKey"]

    # ---------------- DAG files ----------------

    def put_dag_file(self, name, source):
        """Upload and parse (local runs and tests; in AWS the console uploads and the worker parses)."""
        self.s3.put_object(Bucket=self.bucket, Key="dags/" + name, Body=source.encode())
        return self.parse("dags/" + name)

    def source(self, key):
        return self.s3.get_object(Bucket=self.bucket, Key=key)["Body"].read().decode()

    def _dags_in(self, key):
        """Import the DAG file (lambdaflow's `airflow` package is on the path) and return its DAGs."""
        obj = self.s3.get_object(Bucket=self.bucket, Key=key)
        etag = obj["ETag"]
        cached = self._modules.get(key)
        if cached and cached[0] == etag:
            return cached[1]
        import airflow.sdk as sdk
        path = os.path.join(os.environ.get("LAMBDAFLOW_CACHE", "/tmp/lambdaflow"), key)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        source = obj["Body"].read().decode()
        with open(path, "w", encoding="utf-8") as f:   # so tracebacks show the DAG file's lines
            f.write(source)
        module = types.ModuleType(os.path.splitext(os.path.basename(key))[0])
        module.__file__ = path
        del sdk.LOADED[:]
        try:
            exec(compile(source, path, "exec"), module.__dict__)
            dags = {d.dag_id: d for d in sdk.LOADED}
        finally:
            del sdk.LOADED[:]
        self._modules[key] = (etag, dags)
        return dags

    def dags_of_file(self, key):
        return sorted(i["sk"] for i in self._query(KeyConditionExpression=Key("pk").eq("dags"),
                                                   FilterExpression=Attr("file").eq(key), ProjectionExpression="sk"))

    def parse(self, key):
        """Read a DAG file into the table (one item per DAG); an import error is stored, not raised.
        DAGs the file no longer defines are removed (their runs stay until the DAG is deleted)."""
        try:
            dags = self._dags_in(key)
            specs = {d.dag_id: self._spec(d) for d in dags.values()}   # schedule refusals happen here
        except Exception as e:  # noqa: BLE001 - any import error is the user's, shown in the console
            self.table.put_item(Item={"pk": "err", "sk": key, "error": f"{type(e).__name__}: {e}",
                                      "trace": traceback.format_exc()[-4000:], "at": _iso(_now())})
            return []
        self.table.delete_item(Key={"pk": "err", "sk": key})
        for gone in set(self.dags_of_file(key)) - set(specs):
            self.table.delete_item(Key={"pk": "dags", "sk": gone})
        for dag_id, spec in specs.items():
            self.table.update_item(Key={"pk": "dags", "sk": dag_id},
                                   UpdateExpression="SET #f = :f, #s = :s, #p = if_not_exists(#p, :p), #at = :at",
                                   ExpressionAttributeNames={"#f": "file", "#s": "spec", "#p": "paused", "#at": "parsed"},
                                   ExpressionAttributeValues={":f": key, ":s": json.dumps(spec, default=str),
                                                              ":p": PAUSED_AT_CREATION, ":at": _iso(_now())})
        return sorted(specs)

    def forget_file(self, key):
        """A DAG file was deleted from S3: its DAGs and its import error go (runs stay until the DAG is deleted)."""
        gone = self.dags_of_file(key)
        for dag_id in gone:
            self.table.delete_item(Key={"pk": "dags", "sk": dag_id})
        self.table.delete_item(Key={"pk": "err", "sk": key})
        self._modules.pop(key, None)
        return gone

    @staticmethod
    def _spec(d):
        when = sched.translate(d.schedule, d.start_date, d.catchup)
        iso = lambda t: t.isoformat() if t else None  # noqa: E731
        return {"dag_id": d.dag_id, "params": d.params, "tags": d.tags, "description": d.description,
                "max_active_runs": d.max_active_runs, "start_date": iso(d.start_date), "end_date": iso(d.end_date),
                "doc_md": d.doc_md, **when,
                "tasks": {t.task_id: {"upstream": sorted(t.upstream_task_ids), "trigger_rule": t.trigger_rule,
                                      "retries": t.retries, "retry_delay": t.retry_delay,
                                      "operator": type(t).__name__} for t in d.tasks.values()}}

    def meta(self, dag_id):
        item = self.table.get_item(Key={"pk": "dags", "sk": dag_id}, ConsistentRead=True).get("Item")
        if not item:
            raise KeyError(f"no DAG {dag_id!r}")
        return item

    def spec(self, dag_id):
        item = self.meta(dag_id)
        return json.loads(item["spec"]), item["file"]

    def dags(self):
        """Every DAG: {dag_id, file, paused, spec}."""
        return [{"dag_id": i["sk"], "file": i["file"], "paused": bool(i.get("paused")), "parsed": i.get("parsed"),
                 "spec": json.loads(i["spec"])} for i in self._query(KeyConditionExpression=Key("pk").eq("dags"))]

    def import_errors(self):
        return [{"file": i["sk"], "error": i["error"], "trace": i.get("trace", ""), "at": i.get("at")}
                for i in self._query(KeyConditionExpression=Key("pk").eq("err"))]

    def set_paused(self, dag_id, paused):
        self.meta(dag_id)
        self.table.update_item(Key={"pk": "dags", "sk": dag_id}, UpdateExpression="SET paused = :p",
                               ExpressionAttributeValues={":p": bool(paused)})
        self.sync_schedule(dag_id)

    def delete_dag(self, dag_id):
        """The DAG, its runs, task instances, XComs and logs; the file too when no other DAG lives in it."""
        item = self.meta(dag_id)
        for run in self.runs(dag_id):
            pk = f"run#{dag_id}#{run['run_id']}"
            with self.table.batch_writer() as b:
                for i in self._query(KeyConditionExpression=Key("pk").eq(pk), ProjectionExpression="pk, sk"):
                    b.delete_item(Key={"pk": i["pk"], "sk": i["sk"]})
                b.delete_item(Key={"pk": f"dag#{dag_id}", "sk": f"run#{run['run_id']}"})
        self.table.delete_item(Key={"pk": "dags", "sk": dag_id})
        prefix = f"logs/dag_id={dag_id}/"
        for page in self.s3.get_paginator("list_objects_v2").paginate(Bucket=self.bucket, Prefix=prefix):
            for o in page.get("Contents", []):
                self.s3.delete_object(Bucket=self.bucket, Key=o["Key"])
        if not self.dags_of_file(item["file"]):
            self.s3.delete_object(Bucket=self.bucket, Key=item["file"])
            self.table.delete_item(Key={"pk": "err", "sk": item["file"]})
        self.sync_schedule(dag_id)

    # ---------------- EventBridge Scheduler: one schedule per DAG, in the group ----------------

    def sync_schedule(self, dag_id):
        """Make the DAG's schedule match the table: created, updated, disabled when paused, deleted when gone.
        Returns the schedule's state ("ENABLED" / "DISABLED") or None when the DAG has none."""
        if not self.schedules:
            return None
        c, group = self.schedules["client"], self.schedules["group"]
        item = self.table.get_item(Key={"pk": "dags", "sk": dag_id}, ConsistentRead=True).get("Item")
        spec = json.loads(item["spec"]) if item else {}
        expr, now = spec.get("expression"), datetime.datetime.now(datetime.timezone.utc)
        end = spec.get("end_date") and datetime.datetime.fromisoformat(spec["end_date"])
        if end and end.tzinfo is None:
            end = end.replace(tzinfo=datetime.timezone.utc)
        if expr and expr.startswith("at(") and not sched.next_fire(expr, spec["timezone"], now):
            # @once in the past: run it once now, unless it ran already
            expr = None if self.runs(dag_id, limit=1) else (now + datetime.timedelta(minutes=1)).astimezone(
                sched._zone(spec["timezone"])).strftime("at(%Y-%m-%dT%H:%M:00)")
        if not expr or (end and end <= now):
            try:
                c.delete_schedule(Name=dag_id, GroupName=group)
            except ClientError as e:
                if e.response["Error"]["Code"] != "ResourceNotFoundException":
                    raise
            return None
        target = {"Arn": self.schedules["target"], "RoleArn": self.schedules["role"],
                  "Input": json.dumps({"action": "start", "dag_id": dag_id, "logical_date": "<aws.scheduler.scheduled-time>"}),
                  "RetryPolicy": {"MaximumEventAgeInSeconds": 3600, "MaximumRetryAttempts": 10}}
        if self.schedules.get("dlq"):
            target["DeadLetterConfig"] = {"Arn": self.schedules["dlq"]}
        state = "DISABLED" if item.get("paused") else "ENABLED"
        want = {"Name": dag_id, "GroupName": group, "ScheduleExpression": expr,
                "ScheduleExpressionTimezone": spec["timezone"], "FlexibleTimeWindow": {"Mode": "OFF"},
                "State": state, "Target": target, "Description": f"lambdaflow DAG {dag_id}: {spec.get('schedule')}"}
        start = spec.get("start_date") and datetime.datetime.fromisoformat(spec["start_date"])
        if start and start.tzinfo is None:
            start = start.replace(tzinfo=datetime.timezone.utc)
        if start and start > now:          # EventBridge refuses a start date in the past
            want["StartDate"] = start
        if end:
            want["EndDate"] = end
        try:
            c.update_schedule(**want)
        except ClientError as e:
            if e.response["Error"]["Code"] != "ResourceNotFoundException":
                raise
            c.create_schedule(**want)
        return state

    def next_run(self, spec, after=None):
        """When the schedule fires next: not before start_date, not after end_date (EventBridge's StartDate / EndDate)."""
        bound = lambda k: spec.get(k) and _date(spec[k]).replace(  # noqa: E731
            tzinfo=_date(spec[k]).tzinfo or datetime.timezone.utc)
        after = after or datetime.datetime.now(datetime.timezone.utc)
        if bound("start_date") and bound("start_date") > after:
            after = bound("start_date")
        t = sched.next_fire(spec.get("expression"), spec.get("timezone") or "UTC", after)
        if not t or (bound("end_date") and t > bound("end_date")):
            return None
        return _iso(t.timestamp())

    # ---------------- runs ----------------

    def create_run(self, dag_id, logical_date=None, conf=None, run_id=None, run_type="manual"):
        """Create a DAG run -> (run_id, created). A run id that exists already is not created twice."""
        self.spec(dag_id)
        logical_date = logical_date or _iso(_now())
        run_id = run_id or f"{run_type}__{_date(logical_date).isoformat()}"
        try:
            self.table.put_item(Item={"pk": f"dag#{dag_id}", "sk": f"run#{run_id}", "state": "queued", "run_type": run_type,
                                      "logical_date": logical_date, "conf": json.dumps(conf or {}), "queued_at": _iso(_now())},
                                ConditionExpression="attribute_not_exists(sk)")
            return run_id, True
        except ClientError as e:
            if not _conflict(e):
                raise
            return run_id, False

    def trigger(self, dag_id, logical_date=None, conf=None, run_id=None):
        return self.create_run(dag_id, logical_date, conf, run_id)[0]

    def _run(self, dag_id, run_id):
        item = self.table.get_item(Key={"pk": f"dag#{dag_id}", "sk": f"run#{run_id}"}, ConsistentRead=True).get("Item")
        if not item:
            raise KeyError(f"no run {run_id!r} of DAG {dag_id!r}")
        return item

    def runs(self, dag_id, limit=None):
        """The DAG's runs, newest logical date first."""
        items = self._query(KeyConditionExpression=Key("pk").eq(f"dag#{dag_id}") & Key("sk").begins_with("run#"))
        out = [{"run_id": i["sk"][4:], "state": i["state"], "run_type": i.get("run_type", "manual"),
                "logical_date": i["logical_date"], "conf": json.loads(i.get("conf") or "{}"),
                "queued_at": i.get("queued_at"), "start": i.get("start"), "end": i.get("end")} for i in items]
        out.sort(key=lambda r: (r["logical_date"], r["queued_at"] or ""), reverse=True)
        return out[:limit] if limit else out

    def _tis(self, dag_id, run_id):
        items = self._query(KeyConditionExpression=Key("pk").eq(f"run#{dag_id}#{run_id}") & Key("sk").begins_with("ti#"))
        return {i["sk"][3:]: i for i in items}

    def _set_ti(self, dag_id, run_id, task_id, only_if_new=False, expect=None, remove=(), **attrs):
        """Write a task instance's fields. only_if_new: only if nothing has decided its state yet;
        expect: only if its state is still this one. False when the condition did not hold."""
        names = {f"#a{i}": k for i, k in enumerate(attrs)}
        vals = {f":v{i}": v for i, v in enumerate(attrs.values())}
        expr = "SET " + ", ".join(f"#a{i} = :v{i}" for i in range(len(attrs))) if attrs else ""
        if remove:
            names.update({f"#r{i}": k for i, k in enumerate(remove)})
            expr += " REMOVE " + ", ".join(f"#r{i}" for i in range(len(remove)))
        kw = {"Key": {"pk": f"run#{dag_id}#{run_id}", "sk": f"ti#{task_id}"}, "UpdateExpression": expr.strip(),
              "ExpressionAttributeNames": names}
        if vals:
            kw["ExpressionAttributeValues"] = vals
        if only_if_new:
            kw["ConditionExpression"] = "attribute_not_exists(#s)"
            names["#s"] = "state"
        elif expect:
            kw["ConditionExpression"] = "#s = :expect"
            names["#s"] = "state"
            kw.setdefault("ExpressionAttributeValues", {})[":expect"] = expect
        try:
            self.table.update_item(**kw)
            return True
        except ClientError as e:
            if _conflict(e):
                return False
            raise

    def _set_run(self, dag_id, run_id, expect=None, remove=(), **attrs):
        names = {f"#a{i}": k for i, k in enumerate(attrs)}
        kw = {"Key": {"pk": f"dag#{dag_id}", "sk": f"run#{run_id}"},
              "UpdateExpression": ("SET " + ", ".join(f"#a{i} = :v{i}" for i in range(len(attrs))) if attrs else "")
              + (" REMOVE " + ", ".join(f"#r{i}" for i in range(len(remove))) if remove else ""),
              "ExpressionAttributeNames": dict(names, **{f"#r{i}": k for i, k in enumerate(remove)}),
              "ExpressionAttributeValues": {f":v{i}": v for i, v in enumerate(attrs.values())}}
        if expect:
            kw["ConditionExpression"] = "#st = :expect"
            kw["ExpressionAttributeNames"]["#st"] = "state"
            kw["ExpressionAttributeValues"][":expect"] = expect
        if not kw["ExpressionAttributeValues"]:
            del kw["ExpressionAttributeValues"]
        try:
            self.table.update_item(**kw)
            return True
        except ClientError as e:
            if _conflict(e):
                return False
            raise

    def _running_count(self, dag_id):
        return len(self._query(KeyConditionExpression=Key("pk").eq(f"dag#{dag_id}") & Key("sk").begins_with("run#"),
                               FilterExpression=Attr("state").eq("running"), ProjectionExpression="sk"))

    def step(self, dag_id, run_id):
        """Plan one round: {"tasks": [...]} to run now, {"wait": seconds} until something is due, or {"done", "state"}."""
        spec, _ = self.spec(dag_id)
        tasks = spec["tasks"]
        run = self._run(dag_id, run_id)
        if run["state"] in ("success", "failed"):
            return {"done": True, "state": run["state"]}
        if run["state"] == "queued":
            if self._running_count(dag_id) >= int(spec.get("max_active_runs") or 16):
                return {"wait": 30}                      # Airflow keeps it queued until a slot is free
            self._set_run(dag_id, run_id, expect="queued", state="running", start=run.get("start") or _iso(_now()))
        tis = self._tis(dag_id, run_id)
        state = lambda t: tis.get(t, {}).get("state")  # noqa: E731
        changed = True
        while changed:   # skipped / upstream_failed cascade through the graph
            changed = False
            for t, info in tasks.items():
                if state(t) is None:
                    decision = evaluate(info["trigger_rule"], [state(u) for u in info["upstream"]])
                    if decision in ("skipped", "upstream_failed"):
                        self._set_ti(dag_id, run_id, t, only_if_new=True, state=decision, try_number=0)
                        tis.setdefault(t, {})["state"] = decision
                        changed = True
        now = _now()
        new = [t for t, info in tasks.items() if state(t) is None
               and evaluate(info["trigger_rule"], [state(u) for u in info["upstream"]]) == "run"]
        due = [t for t in tasks if state(t) == "up_for_retry" and float(tis[t].get("next_try", 0)) <= now]
        ready = [t for t in new if self._set_ti(dag_id, run_id, t, only_if_new=True, state="queued")]
        ready += [t for t in due if self._set_ti(dag_id, run_id, t, expect="up_for_retry", state="queued")]
        if ready:
            return {"tasks": ready}
        if all(state(t) in TERMINAL for t in tasks):
            leaves = [t for t in tasks if not any(t in i["upstream"] for i in tasks.values())]
            final = "failed" if any(state(t) in ("failed", "upstream_failed") for t in leaves) else "success"
            self._set_run(dag_id, run_id, state=final, end=_iso(_now()))
            return {"done": True, "state": final}
        retries = [float(tis[t]["next_try"]) for t in tasks if state(t) == "up_for_retry"]
        return {"wait": max(1, int(min(retries) - now + 0.999)) if retries else 1}

    # ---------------- one task ----------------

    def log_key(self, dag_id, run_id, task_id, try_number):
        return f"logs/dag_id={dag_id}/run_id={run_id}/task_id={task_id}/attempt={try_number}.log"

    def run_task(self, dag_id, run_id, task_id):
        spec, key = self.spec(dag_id)
        op = self._dags_in(key)[dag_id].tasks[task_id]
        run = self._run(dag_id, run_id)
        prev = self._tis(dag_id, run_id).get(task_id, {})
        try_number = int(prev.get("try_number", 0)) + 1
        max_tries = int(prev.get("max_tries", op.retries))   # a clear gives the task its retries again
        start = _now()
        self._set_ti(dag_id, run_id, task_id, state="running", try_number=try_number, start=_iso(start), remove=("end",))
        ti = TaskInstance(self, spec, dag_id, run_id, task_id, try_number)
        context = self._context(op, run, ti)
        from airflow.exceptions import AirflowFailException, AirflowSkipException
        buf = io.StringIO()
        handler = logging.StreamHandler(buf)
        handler.setFormatter(logging.Formatter(LOG_FORMAT))
        root, level = logging.getLogger(), logging.getLogger().level
        root.addHandler(handler)
        root.setLevel(logging.INFO)
        log = logging.getLogger("airflow.task")
        try:
            with contextlib.redirect_stdout(_Tee(buf, sys.__stdout__)), contextlib.redirect_stderr(_Tee(buf, sys.__stderr__)):
                log.info("Executing <Task(%s): %s> on %s, try %d of %d", type(op).__name__, task_id,
                         run["logical_date"], try_number, max_tries + 1)
                try:
                    task = copy.copy(op)   # render templates on a copy: the imported DAG is reused by the next task
                    for field in op.template_fields:
                        setattr(task, field, render(getattr(op, field), context))
                    context["task"] = task
                    result = task.execute(context)
                    if result is not None and op.pushes_return_value and op.do_xcom_push:
                        if op.multiple_outputs and isinstance(result, dict):
                            for k, v in result.items():
                                ti.xcom_push(k, v)
                        ti.xcom_push("return_value", result)
                        log.info("Done. Returned value was: %s", str(result)[:1000])
                    final = "success"
                except AirflowSkipException as e:
                    log.info("Skipping task: %s", e)
                    final = "skipped"
                except AirflowFailException:
                    log.exception("Task failed with exception (no retries: AirflowFailException)")
                    final = "failed"
                except Exception:  # noqa: BLE001 - a task failure, retried if retries are left
                    log.exception("Task failed with exception")
                    final = "up_for_retry" if try_number <= max_tries else "failed"
                log.info("Marking task as %s.", final.upper())
        finally:
            root.removeHandler(handler)
            root.setLevel(level)
            self.s3.put_object(Bucket=self.bucket, Key=self.log_key(dag_id, run_id, task_id, try_number),
                               Body=buf.getvalue().encode(), ContentType="text/plain; charset=utf-8")
        attrs = {"state": final, "end": _iso(_now())}
        if final == "up_for_retry":
            attrs["next_try"] = str(_now() + op.retry_delay)
        self._set_ti(dag_id, run_id, task_id, **attrs)
        return final

    def fail_task(self, dag_id, run_id, task_id, error):
        """The worker died (timeout, out of memory): the state machine reports it; retried like any failure."""
        spec, _ = self.spec(dag_id)
        ti = self._tis(dag_id, run_id).get(task_id, {})
        if ti.get("state") not in ("queued", "running"):
            return ti.get("state")
        try_number = int(ti.get("try_number", 0)) or 1
        info = spec["tasks"][task_id]
        final = "up_for_retry" if try_number <= int(ti.get("max_tries", info["retries"])) else "failed"
        attrs = {"state": final, "end": _iso(_now()), "try_number": try_number}
        if final == "up_for_retry":
            attrs["next_try"] = str(_now() + float(info["retry_delay"]))
        key = self.log_key(dag_id, run_id, task_id, try_number)
        self.s3.put_object(Bucket=self.bucket, Key=key, Body=(self.read_log(dag_id, run_id, task_id, try_number) or "")
                           .encode() + f"\nThe worker did not finish: {error}\nMarking task as {final.upper()}.\n".encode())
        self._set_ti(dag_id, run_id, task_id, **attrs)
        return final

    def _context(self, op, run, ti):
        ld = _date(run["logical_date"])
        conf = json.loads(run.get("conf") or "{}")
        params = dict(op.dag.params, **conf)   # [core] dag_run_conf_overrides_params = True
        dag_run = types.SimpleNamespace(dag_id=ti.dag_id, run_id=ti.run_id, conf=conf, logical_date=ld,
                                        run_type=run.get("run_type", "manual"))
        return {"ti": ti, "task_instance": ti, "task": op, "dag": op.dag, "dag_run": dag_run, "run_id": ti.run_id,
                "logical_date": ld, "data_interval_start": ld, "data_interval_end": ld,
                "ds": ld.strftime("%Y-%m-%d"), "ds_nodash": ld.strftime("%Y%m%d"),
                "ts": ld.isoformat(), "ts_nodash": ld.strftime("%Y%m%dT%H%M%S"),
                "params": params, "macros": _macros(), "try_number": ti.try_number}

    # ---------------- changing a run from the console ----------------

    def _downstream(self, tasks, roots):
        out, todo = set(), list(roots)
        while todo:
            t = todo.pop()
            if t not in out:
                out.add(t)
                todo += [d for d, i in tasks.items() if t in i["upstream"]]
        return out

    def clear(self, dag_id, run_id, task_ids, downstream=True):
        """Airflow's "Clear": the tasks (and their downstream) run again; try numbers keep counting and the
        retries start over (max_tries = try_number + retries). The run goes back to queued; the caller
        starts the loop again (resume)."""
        tasks = self.spec(dag_id)[0]["tasks"]
        self._run(dag_id, run_id)
        unknown = set(task_ids) - set(tasks)
        if unknown:
            raise KeyError(f"no task {', '.join(sorted(unknown))} in DAG {dag_id!r}")
        todo = self._downstream(tasks, task_ids) if downstream else set(task_ids)
        pk = f"run#{dag_id}#{run_id}"
        tis = self._tis(dag_id, run_id)
        for t in sorted(todo):
            tries = int(tis.get(t, {}).get("try_number", 0))
            self._set_ti(dag_id, run_id, t, max_tries=tries + int(tasks[t]["retries"]), remove=("state", "start", "end", "next_try"))
            for x in self._query(KeyConditionExpression=Key("pk").eq(pk) & Key("sk").begins_with(f"xcom#{t}#"),
                                 ProjectionExpression="pk, sk"):
                self.table.delete_item(Key={"pk": x["pk"], "sk": x["sk"]})
        self._set_run(dag_id, run_id, state="queued", remove=("end",))
        return sorted(todo)

    def mark(self, dag_id, run_id, task_id, state):
        """Airflow's "Mark success / failed" on one task instance; the run is re-evaluated (resume)."""
        if state not in ("success", "failed"):
            raise ValueError("state must be success or failed")
        if task_id not in self.spec(dag_id)[0]["tasks"]:
            raise KeyError(f"no task {task_id!r} in DAG {dag_id!r}")
        self._run(dag_id, run_id)
        self._set_ti(dag_id, run_id, task_id, state=state, end=_iso(_now()), remove=("next_try",))
        self._set_run(dag_id, run_id, state="running", remove=("end",))

    # ---------------- reading back ----------------

    def xcom(self, dag_id, run_id, task_id, key="return_value"):
        item = self.table.get_item(Key={"pk": f"run#{dag_id}#{run_id}", "sk": f"xcom#{task_id}#{key}"},
                                   ConsistentRead=True).get("Item")
        return json.loads(item["value"]) if item else None

    def run_state(self, dag_id, run_id):
        """The run, its task instances and its XComs (what the console and the compatibility tests read)."""
        run = self._run(dag_id, run_id)
        items = self._query(KeyConditionExpression=Key("pk").eq(f"run#{dag_id}#{run_id}"))
        tis = {i["sk"][3:]: i for i in items if i["sk"].startswith("ti#")}
        xcom = {}
        for i in items:
            if i["sk"].startswith("xcom#"):
                task, key = i["sk"][5:].split("#", 1)
                xcom[f"{task}.{key}"] = json.loads(i["value"])
        return run, tis, xcom

    def read_log(self, dag_id, run_id, task_id, try_number):
        try:
            return self.source(self.log_key(dag_id, run_id, task_id, try_number))
        except ClientError as e:
            if e.response["Error"]["Code"] in ("NoSuchKey", "404"):
                return None
            raise


class TaskInstance:
    """The `ti` a task sees: xcom_pull / xcom_push, try_number, and skipping for the branch operators."""

    def __init__(self, engine, spec, dag_id, run_id, task_id, try_number):
        self.engine, self.spec = engine, spec
        self.dag_id, self.run_id, self.task_id, self.try_number = dag_id, run_id, task_id, try_number
        self.map_index = -1

    def xcom_push(self, key, value):
        self.engine.table.put_item(Item={"pk": f"run#{self.dag_id}#{self.run_id}", "sk": f"xcom#{self.task_id}#{key}",
                                         "value": json.dumps(value)})

    def xcom_pull(self, task_ids=None, key="return_value", default=None, **_kw):
        if task_ids is None:
            task_ids = self.task_id
        if isinstance(task_ids, str):
            v = self.engine.xcom(self.dag_id, self.run_id, task_ids, key)
            return default if v is None else v
        return [self.engine.xcom(self.dag_id, self.run_id, t, key) for t in task_ids]

    def skip_downstream(self, follow=None, all_descendants=False):
        tasks = self.spec["tasks"]
        down = lambda t: [d for d, i in tasks.items() if t in i["upstream"]]  # noqa: E731
        if all_descendants:
            skip = self.engine._downstream(tasks, down(self.task_id))
        else:
            skip = {t for t in down(self.task_id) if t not in (follow or set())}
        for t in sorted(skip):
            self.engine._set_ti(self.dag_id, self.run_id, t, only_if_new=True, state="skipped", try_number=0)


def drive(engine, dag_id, run_id, sleep=time.sleep):
    """The state machine's loop on this machine: step, run the tasks, wait; until the run is done."""
    while True:
        s = engine.step(dag_id, run_id)
        if s.get("done"):
            return s["state"]
        for t in s.get("tasks", []):
            engine.run_task(dag_id, run_id, t)
        if s.get("wait"):
            sleep(s["wait"])


def run_local(engine, dag_id, logical_date=None, conf=None, sleep=time.sleep):
    """One DAG run on this machine, start to end."""
    run_id = engine.trigger(dag_id, logical_date, conf)
    drive(engine, dag_id, run_id, sleep)
    return run_id
