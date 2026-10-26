"""The server Lambda, called in-process: auth, routing, errors, timing."""
import base64
import json

import lambda_function as lf

AUTH = "Basic " + base64.b64encode(b"admin:pw").decode()


def call(method, path, body=None, auth=AUTH):
    event = {"requestContext": {"http": {"method": method}}, "rawPath": path,
             "headers": {"authorization": auth} if auth else {}, "body": json.dumps(body) if body else None}
    resp = lf.lambda_handler(event, None)
    return resp["statusCode"], json.loads(resp["body"]) if resp["headers"]["content-type"].startswith("application/json") else resp["body"]


def test_console_is_public_and_api_needs_auth():
    status, body = call("GET", "/", auth=None)
    assert status == 200 and "<html" in body
    assert call("GET", "/api/ping", auth=None)[0] == 401
    assert call("GET", "/api/ping", auth="Basic " + base64.b64encode(b"admin:wrong").decode())[0] == 401


def test_routes_errors_and_timing():
    status, data = call("GET", "/api/ping")
    assert status == 200 and data["ok"] is True and isinstance(data["ms"], float)
    assert call("GET", "/api/nope")[0] == 404
    status, data = call("POST", "/api/items", {"name": "bad name!"})
    assert status == 400 and "name" in data["error"]
    status, data = call("POST", "/api/items", {"name": "ok"})
    assert status == 201 and data["item"] == {"name": "ok"}
