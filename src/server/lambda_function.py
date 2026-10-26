"""<project> server: one Lambda behind a Function URL — JSON API for the driver, web console at GET /.

Template. The first half (JSON, request body, HTTP Basic auth, routing, error mapping, timing) is the
same in every project of the series; replace the routes in the second half.

    GET  /                      console (index.html)
    GET  /api/ping              {"ok": true}
    GET  /api/items             example list
    POST /api/items             example create {name}
"""
import base64
import hmac
import json
import os
import re
import time

USER = os.environ.get("USER_NAME", "")
PASSWORD = os.environ.get("PASSWORD", "")
CONSOLE = os.environ.get("CONSOLE", "1") == "1"

with open(os.path.join(os.path.dirname(__file__), "index.html"), encoding="utf-8") as f:
    INDEX_HTML = f.read()

NAME = r"[A-Za-z0-9_-]{1,64}"   # the pattern for names in paths


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


ERRORS = [(BadRequest, 400), (Forbidden, 403)]   # exception class -> HTTP status; add the project's own


def lambda_handler(event, context):
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
                out = handler(event, **match.groupdict())
                status, data = out if isinstance(out, tuple) else (200, out)
                break
    except Exception as e:  # noqa: BLE001 - every error becomes a JSON response
        status = next((s for cls, s in ERRORS if isinstance(e, cls)), 500)
        data = {"error": str(e) if status < 500 else f"{type(e).__name__}: {e}"}
    data.setdefault("ms", round((time.perf_counter() - t0) * 1000, 1))
    return _json(status, data)


# ---------------- the project's API ----------------

_ITEMS = []   # example state; a real project talks to its AWS primitive here


def ping(event):
    return {"ok": True}


def list_items(event):
    return {"items": _ITEMS}


def create_item(event):
    name = _body(event).get("name")
    if not name or not re.fullmatch(NAME, name):
        raise BadRequest("name must match " + NAME)
    _ITEMS.append({"name": name})
    return 201, {"item": {"name": name}}


ROUTES = [
    ("GET",  re.compile(r"/api/ping$"), ping),
    ("GET",  re.compile(r"/api/items$"), list_items),
    ("POST", re.compile(r"/api/items$"), create_item),
]
