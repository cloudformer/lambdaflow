"""Compatibility harness: the same operations on the real <replaces> (Docker) and on the project, compared.

Template. Keep the shape: a `real_<ref>` fixture that skips without the DSN env var (or runs the project
side only with COMPAT_DRY=1), an `ours` fixture that calls the server in-process, and a `pair` fixture
whose `run(steps)` executes each step on both sides and asserts the outcomes are equal. A test is one
feature; its docstring "Category | Feature | note" is the report row.
"""
import base64
import json
import os
from dataclasses import dataclass

import pytest

import lambda_function as lf
import compat_report
import importlib
ours_client = importlib.import_module(os.environ["PACKAGE"] + ".client")   # <package>.<client>; rename `client` with the submodule

AUTH = "Basic " + base64.b64encode(b"admin:pw").decode()


@dataclass
class Outcome:
    value: object          # the operation's result, normalised to plain Python
    error: object = None   # (class name, code) when it raised


def run_step(conn, op):
    """op = (method name, args). Normalise the result so both sides compare equal when they agree."""
    name, args = op[0], op[1:]
    try:
        return Outcome(getattr(conn, name)(*args))
    except Exception as e:  # noqa: BLE001 - we compare whatever each side raises
        code = e.args[0] if e.args and isinstance(e.args[0], int) else None
        return Outcome(None, (type(e).__name__, code))


@pytest.fixture
def real():
    dsn = os.environ.get("REFERENCE_DSN")          # e.g. REDIS_URL=redis://127.0.0.1:6379/0
    if not dsn and os.environ.get("COMPAT_DRY"):
        return None                                # dry run: project side only, print what it does
    if not dsn:
        pytest.skip("set REFERENCE_DSN (see tests/compat/docker-compose.yml) to run against the real service")
    client = pytest.importorskip("<client>")       # the real client library
    conn = client.connect(dsn)                     # <- adapt
    compat_report.versions["reference"] = "<replaces> <version> (Docker)"   # <- query it once
    return conn


@pytest.fixture
def ours(monkeypatch):
    """The project's client with its HTTP layer calling the Lambda handler in-process."""
    c = ours_client.connect(host="in-process.test", user="admin", password="pw")

    def call(method, path, payload=None):
        event = {"requestContext": {"http": {"method": method}}, "rawPath": path,
                 "headers": {"authorization": AUTH}, "body": json.dumps(payload) if payload is not None else None}
        resp = lf.lambda_handler(event, None)
        data = json.loads(resp["body"])
        if resp["statusCode"] >= 400:
            raise ours_client.Error(resp["statusCode"], data.get("error", ""))
        return data

    monkeypatch.setattr(c, "_call", call)
    return c


@pytest.fixture
def pair(real, ours):
    dry = real is None

    def both(op):
        o = run_step(ours, op)
        if dry:
            print(f"  {'ERR ' + str(o.error) if o.error else 'ok  '} {op}")
            return o, o
        return run_step(real, op), o

    def run(steps):
        for op in steps:
            r, o = both(op)
            if not dry:
                assert r == o, f"{op}\n  real: {r}\n  ours: {o}"
    run.both = both

    def unsupported(op):
        """The real service does it; the project must refuse it with NotSupportedError."""
        r, o = both(op)
        assert dry or r.error is None, f"the reference rejected it itself: {r.error}"
        assert o.error and o.error[0] == "NotSupportedError", f"{op}\n  not refused: {o}"
    run.unsupported = unsupported
    return run
