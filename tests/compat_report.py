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
    "title": "<project> compatibility report",
    "report": os.path.join(os.path.dirname(__file__), "compat", "REPORT.md"),
    # (key, section title, substring of the test file name, reference description or callable)
    "suites": [
        ("reference", "<project>.<client> vs <replaces>", "test_reference",
         lambda: versions.get("reference") or "<replaces> (not run)"),
    ],
    "version": lambda: os.environ["PACKAGE"] + " " + __import__(os.environ["PACKAGE"]).__version__,
    "categories": ["Basics", "Data", "Queries", "Errors", "Metadata", "Unsupported", "General"],
}

features = {}   # nodeid -> {"suite", "feature", "gap", "unsupported", "quirk", "fail"}
outcomes = {}   # nodeid -> verified | gap | regression | fixed | unsupported | quirk | not run
versions = {}   # suite key -> reference version string (set by the compat conftest once connected)
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
            "verified" if report.outcome == "passed" else "regression")


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


def render():
    order = {"verified": 0, "fixed": 1, "regression": 2, "gap": 3, "quirk": 4, "unsupported": 5, "not run": 6}
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
    ]
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
