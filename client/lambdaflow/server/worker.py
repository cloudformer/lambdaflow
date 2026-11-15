"""lambdaflow worker: the only Lambda that imports DAG files and runs task code (low privilege: the table,
the bucket and the task policies; no scheduler, no IAM).

Called by the state machine, one action per state, and by the console for parsing:

    {"action": "start",  "dag_id", "logical_date"}         EventBridge Scheduler fired: create the scheduled run
    {"action": "resume", "dag_id", "run_id"}                the console triggered / cleared a run
        -> {"dag_id", "run_id", "go"}                       go = false: the run existed already (a duplicate fire)
    {"action": "step", "dag_id", "run_id"}                  -> {"tasks": [...]} | {"wait": s} | {"done", "state"}
    {"action": "run_task", "dag_id", "run_id", "task_id"}   -> {"state"}
    {"action": "task_failed", "dag_id", "run_id", "task_id", "error"}   the run_task call itself died
    {"action": "parse", "key"}                              -> {"dags": [...]}
"""
import os
import time

from engine import Engine, _date, drive

ENGINE = None


def engine():
    global ENGINE
    if ENGINE is None:
        ENGINE = Engine(os.environ["TABLE"], os.environ["BUCKET"])
    return ENGINE


def lambda_handler(event, context=None):
    e, a = engine(), event.get("action")
    ids = {"dag_id": event.get("dag_id"), "run_id": event.get("run_id")}
    if a == "start":
        ld = _date(event["logical_date"]).isoformat().replace("+00:00", "Z")
        try:
            run_id, created = e.create_run(event["dag_id"], ld, run_type="scheduled")
        except KeyError as err:                      # the DAG was deleted after the schedule fired
            return dict(ids, go=False, reason=str(err))
        return dict(ids, run_id=run_id, go=created)
    if a == "resume":
        return dict(ids, go=True)
    if a == "step":
        return dict(ids, **e.step(event["dag_id"], event["run_id"]))
    if a == "run_task":
        return {"state": e.run_task(event["dag_id"], event["run_id"], event["task_id"])}
    if a == "task_failed":
        return {"state": e.fail_task(event["dag_id"], event["run_id"], event["task_id"], str(event.get("error"))[:2000])}
    if a == "parse":
        return {"dags": e.parse(event["key"])}
    raise ValueError(f"unknown action {a!r}")


def loop(payload, sleep=time.sleep):
    """The state machine on this machine (local dev, tests): the first state, then step / run / wait."""
    first = lambda_handler(payload)
    if first.get("go"):
        drive(engine(), first["dag_id"], first["run_id"], sleep)
    return first
