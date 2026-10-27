"""Compatibility report plugin (tests/compat/REPORT.md), wired in by tests/conftest.py.

Shared across the series; a project edits only CONFIG. Every test under tests/compat/ is one feature:
its docstring is "Category | Feature | short note", an xfail marker's reason is the gap (naming the task
in TASKS.md that closes it), @pytest.mark.quirk + xfail = documented difference we do not imitate,
@pytest.mark.unsupported = the project refuses it on purpose. Outcomes of the whole run are rendered at
session end, with the actual diff for rows that fail unexpectedly.
"""
import collections
import datetime
import os
import platform
import re

CONFIG = {
    "title": "lambdaflow compatibility report",
    "report": os.path.join(os.path.dirname(__file__), "compat", "REPORT.md"),
    # (key, section title, substring of the test file name, reference description or callable)
    "suites": [
        ("airflow", "lambdaflow vs Airflow", "test_airflow",
         lambda: versions.get("airflow") or "Apache Airflow 3.3 (not run)"),
    ],
    "version": lambda: os.environ["PACKAGE"] + " " + __import__(os.environ["PACKAGE"]).__version__,
    "categories": ["DAGs", "Execution", "Templates", "Scheduling", "Variables", "Unsupported", "General"],
    # Optional "Test scope" table: (business step, how it is tested, ["file.py::test_name", ...], still to do).
    # Status is computed from the run: every listed test (all parametrized cases) must pass.
    "scope": [
        ("1. A DAG file reaches S3 (console upload or `aws s3 cp`) and is parsed",
         "moto S3: unsupported arguments refused (`pool`, `dagrun_timeout`), import errors stored and cleared once "
         "fixed, DAGs removed from a file disappear, pause survives a re-parse, S3 events parse / forget",
         ["test_engine.py::test_unknown_arguments_are_refused_at_parse_time",
          "test_engine.py::test_import_error_is_stored_and_cleared_when_fixed",
          "test_engine.py::test_a_dag_removed_from_its_file_disappears_and_pause_survives_a_reparse",
          "test_server.py::test_s3_events_parse_and_forget"], None),
        ("2. A run is created once; each task runs once",
         "moto DynamoDB, conditional writes: a duplicate trigger returns the same run, a second loop planning the "
         "same run cannot claim a task again; the worker drops a duplicate scheduled start",
         ["test_engine.py::test_a_run_is_created_once_and_runs_to_the_end",
          "test_engine.py::test_a_task_is_claimed_once_when_two_loops_plan_the_same_run",
          "test_engine.py::test_the_scheduled_start_is_deduplicated_by_the_worker"], None),
        ("3. Tasks run by dependency: order, fan-out/in, trigger rules, branching, skips, retries",
         "each task's state and try_number compared with Airflow; trigger rules beyond the compat DAG unit-tested",
         ["test_airflow.py::test_chain", "test_airflow.py::test_fan_out_in", "test_airflow.py::test_retries",
          "test_airflow.py::test_trigger_rules", "test_airflow.py::test_branching",
          "test_airflow.py::test_branching_conf", "test_engine.py::test_trigger_rules_beyond_the_compat_dag"], None),
        ("4. Data between tasks: XCom, TaskFlow return values, multiple_outputs",
         "XCom values compared with Airflow",
         ["test_airflow.py::test_chain", "test_airflow.py::test_fan_out_in", "test_airflow.py::test_taskflow"], None),
        ("5. Templates: `{{ ds }}`, params, conf, macros; Bash output",
         "rendered values compared with Airflow",
         ["test_airflow.py::test_templates", "test_airflow.py::test_branching_conf"], None),
        ("6. Scheduling: Airflow schedule → EventBridge Scheduler, pause, max_active_runs",
         "translation and refusals unit-tested; the first run after start_date of 19 schedules compared with "
         "Airflow's next_dagrun_run_after; moto Scheduler: the DAG's schedule is created, updated, disabled when "
         "paused, deleted with the DAG; a run waits while max_active_runs are running",
         ["test_schedule.py::test_cron_to_eventbridge", "test_schedule.py::test_timedelta_to_rate",
          "test_schedule.py::test_once_and_timezone_come_from_start_date", "test_schedule.py::test_refused_with_the_reason",
          "test_schedule.py::test_next_fire", "test_schedule.py::test_next_fire_of_at_and_rate",
          "test_airflow.py::test_schedules", "test_airflow.py::test_schedule_timedelta",
          "test_airflow.py::test_schedule_refused", "test_engine.py::test_an_unsupported_schedule_is_an_import_error",
          "test_engine.py::test_the_schedule_follows_the_dag", "test_engine.py::test_max_active_runs_keeps_a_run_queued"],
         None),
        ("7. Task logs, retries, a worker that dies",
         "print / logging / traceback captured to S3 per attempt (Airflow's log path); a timed-out worker is "
         "reported by the state machine and retried like any failure",
         ["test_engine.py::test_task_logs_retries_and_a_dead_worker"], None),
        ("8. Console actions and the pip client: trigger, pause, clear, mark, delete, the airflow CLI box",
         "console Lambda in-process with moto, the state machine replaced by the worker loop; clear gives the "
         "task its retries again (Airflow's max_tries)",
         ["test_server.py::test_console_is_public_and_api_needs_auth",
          "test_server.py::test_upload_trigger_and_read_back_a_run", "test_server.py::test_clear_failed_and_mark",
          "test_server.py::test_import_errors_unknown_dags_and_delete", "test_server.py::test_pause_unpause",
          "test_server.py::test_the_airflow_cli_box", "test_server.py::test_the_pip_client_drives_the_console",
          "test_engine.py::test_clear_reruns_the_task_and_its_downstream_with_the_next_try",
          "test_engine.py::test_mark_success_on_the_failed_leaf_makes_the_run_succeed",
          "test_engine.py::test_delete_dag_removes_runs_logs_and_the_file"],
         "the page itself: by hand in `python3 src/server/dev.py`"),
        ("9. Variables / Connections; ssm and secretsmanager backends",
         "moto", [], "later phase"),
        ("10. Real orchestration: Step Functions loop calling Lambda, EventBridge firing, IAM, real DynamoDB and S3",
         "not testable locally: `drive()` makes the same `step` / `run_task` calls the state machine makes, "
         "`worker.loop()` the same first state; verified on AWS after `terraform apply`",
         [], "after deploy"),
    ],
}

features = {}   # nodeid -> {"suite", "feature", "gap", "unsupported", "quirk", "fail"}
outcomes = {}   # nodeid -> verified | gap | regression | fixed | unsupported | quirk | not run
versions = {}   # suite key -> reference version string (set by the compat conftest once connected)
results = {}    # every nodeid -> passed | failed | skipped | xfailed (for the scope table)
suite = {"passed": 0, "failed": 0, "skipped": 0, "xfailed": 0}

STATUS = {"verified": "✅", "gap": "❌ gap", "regression": "🔴 regression", "fixed": "🟡 fixed",
          "unsupported": "⛔", "quirk": "⚪ quirk", "not run": "⏭"}
LEGEND = ("✅ verified · ❌ gap — known difference, the note names the task that closes it · "
          "⚪ quirk — documented, deliberately not aligned · ⛔ unsupported by design, refused with "
          "NotSupportedError · 🟡 fixed — xfail marker to remove · 🔴 regression · ⏭ not run")


def pytest_configure(config):
    config.addinivalue_line("markers", "unsupported(reason): the feature is rejected on purpose "
                            "(NotSupportedError); the test passes when the project refuses it cleanly")
    config.addinivalue_line("markers", "quirk: together with xfail -- a documented difference that is "
                            "deliberately not aligned (not a gap to fix)")


def pytest_collection_modifyitems(items):
    for item in items:
        nodeid = item.nodeid.replace(os.sep, "/")
        if "tests/compat/" not in nodeid:
            continue
        kind = next((k for k, _t, match, _r in CONFIG["suites"] if match in nodeid), None)
        if not kind:
            continue
        xfail = item.get_closest_marker("xfail")
        unsupported = item.get_closest_marker("unsupported")
        features[item.nodeid] = {
            "suite": kind,
            "feature": (item.function.__doc__ or item.name).strip().splitlines()[0],
            "gap": xfail.kwargs.get("reason") if xfail else
                   (unsupported.kwargs.get("reason") or (unsupported.args[0] if unsupported.args else "")) if unsupported else None,
            "unsupported": bool(unsupported),
            "quirk": item.get_closest_marker("quirk") is not None,
        }


def pytest_runtest_logreport(report):
    if report.when == "setup" and report.outcome == "skipped":
        suite["skipped"] += 1
    elif report.when == "call":
        if hasattr(report, "wasxfail") and report.outcome == "skipped":
            suite["xfailed"] += 1
        elif report.outcome in ("passed", "failed"):
            suite[report.outcome] += 1
    if report.outcome == "failed" or report.when == "call" or (report.when == "setup" and report.outcome == "skipped"):
        results[report.nodeid] = "xfailed" if hasattr(report, "wasxfail") else report.outcome
    if report.nodeid not in features:
        return
    if report.when == "setup" and report.outcome == "skipped":
        # xfail(run=False) is reported as skipped at setup: still a known gap, not "not run"
        outcomes[report.nodeid] = "gap" if hasattr(report, "wasxfail") else "not run"
    elif report.when == "call":
        xfail = hasattr(report, "wasxfail")
        if report.outcome == "failed":   # keep the reference-vs-project lines so the report shows the difference
            text = report.longreprtext
            lines = [ln.strip() for ln in text.splitlines() if ln.strip().startswith(("real:", "ours:", "E   "))]
            features[report.nodeid]["fail"] = " ".join(lines)[:800] or text.strip().splitlines()[-1][:800]
        outcomes[report.nodeid] = (
            "quirk" if report.outcome == "skipped" and xfail and features[report.nodeid]["quirk"] else
            "gap" if report.outcome == "skipped" and xfail else
            "fixed" if report.outcome == "passed" and xfail else
            "unsupported" if report.outcome == "passed" and features[report.nodeid]["unsupported"] else
            "verified" if report.outcome == "passed" else
            "not run" if report.outcome == "skipped" else "regression")   # pytest.skip() inside the test body


def _open_tasks():
    c = collections.Counter()
    for nid, f in features.items():
        if outcomes.get(nid) == "gap":
            m = re.search(r"task (\d+)", f["feature"] + " " + (f["gap"] or ""))
            if m:
                c[int(m.group(1))] += 1
    return ", ".join(f"task {t} ({n})" for t, n in sorted(c.items())) or "none"


def _counts(kind):
    c = collections.Counter(outcomes.get(nid, "not run") for nid, f in features.items() if f["suite"] == kind)
    total = sum(c.values())
    supported = c["verified"] + c["quirk"] + c["fixed"]
    pct = f"**Support {100 * supported // total}%** ({supported} / {total} features) · " if total else ""
    return pct + " · ".join(f"{n} {k}" for k, n in sorted(c.items()))


def _scope():
    def matches(spec):
        file, name = spec.split("::")
        return [n for n in results
                if n.split("::")[0].endswith("/" + file) and n.split("::")[-1].split("[")[0] == name]
    lines = ["## Test scope", "",
             "What each business step is checked by. ✅ every listed test passed · 🔴 one failed · "
             "⏭ some not run (filtered, or an Airflow outcome not recorded yet) · ⏳ still to do.", "",
             "| Business step | How it is tested | Tests | Status |", "|---|---|---|---|"]
    for step, how, tests, todo in CONFIG["scope"]:
        ran = [r for spec in tests for r in matches(spec)]
        got = [results[n] for n in ran]
        passed = got.count("passed") + got.count("xfailed")      # a quirk's expected failure is its pass
        status = ("🔴 " if "failed" in got else "✅ " if got and passed == len(got) else "⏭ ") + \
                 f"{passed}/{len(got)}" if tests else ""
        if todo:
            status = (status + " · " if status else "") + f"⏳ {todo}"
        names = "<br>".join(f"`{s.split('::')[1]}`" for s in tests) or "—"
        lines.append(f"| {step} | {how} | {names} | {status} |")
    return lines + [""]


def render():
    order ={"verified": 0, "fixed": 1, "regression": 2, "gap": 3, "quirk": 4, "unsupported": 5, "not run": 6}
    cats = CONFIG["categories"]
    lines = [
        f"# {CONFIG['title']} · [tasks](TASKS.md)", "",
        f"Generated {datetime.date.today()} by `pytest tests` · {CONFIG['version']()} · Python {platform.python_version()}", "",
        "Each row is one feature exercised the same way on the reference and on the project; results, types and "
        "errors must match. The feature list only grows (a removed case would show in git), so the support "
        "percentage can only move up.", "",
        f"Whole test run: **{suite['passed']} passed**, {suite['failed']} failed, {suite['xfailed']} expected failures "
        f"(known gaps), {suite['skipped']} skipped — regression suite (`tests/`) and compatibility suite (`tests/compat/`) together.", "",
        f"Open tasks: {_open_tasks()} — see [TASKS.md](TASKS.md).", "",
    ] + (_scope() if CONFIG.get("scope") else [])
    for kind, title, _match, ref in CONFIG["suites"]:
        lines += [f"## {title} — reference: {ref() if callable(ref) else ref}", "", _counts(kind), "", LEGEND, ""]
        groups = {}
        for nid, f in features.items():
            if f["suite"] != kind:
                continue
            parts = [p.strip() for p in f["feature"].split(" | ")]
            cat, feat, note = (parts + ["", ""])[:3] if len(parts) >= 2 else ("General", parts[0], "")
            if f.get("fail"):
                note = f["fail"].replace("|", "\\|")
            groups.setdefault(cat, []).append((nid, feat, note))
        lines += ["| Feature | Status | Note |", "|---|---|---|"]
        for cat in sorted(groups, key=lambda c: (cats.index(c) if c in cats else 99, c)):
            lines.append(f"| **{cat}** | | |")
            for nid, feat, note in sorted(groups[cat], key=lambda x: (order[outcomes.get(x[0], "not run")], x[1])):
                lines.append(f"| {feat} | {STATUS[outcomes.get(nid, 'not run')]} | {note} |")
        lines.append("")
    return "\n".join(lines)


def _ran():
    return any(outcomes.get(n) not in (None, "not run") for n in features)


def pytest_sessionfinish(session):
    if _ran():
        with open(CONFIG["report"], "w", encoding="utf-8") as fh:
            fh.write(render())


def pytest_terminal_summary(terminalreporter):
    if not _ran():
        return
    terminalreporter.section("compatibility")
    for kind, _t, _m, _r in CONFIG["suites"]:
        terminalreporter.write_line(f"{kind:10s} {_counts(kind)}")
    terminalreporter.write_line(f"open tasks: {_open_tasks()}  (tests/compat/TASKS.md)")
    terminalreporter.write_line(f"report: {os.path.relpath(CONFIG['report'])}")
