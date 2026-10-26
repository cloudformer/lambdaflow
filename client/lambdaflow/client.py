"""<project>.<client> — the same API as `<client>`, talking HTTP (Basic auth, JSON) to the Lambda URL.

Standard library only. The transport below (keep-alive connection, strict retry policy: re-send only
when the request provably never reached the server) is shared across the series; the API on top of it
is whatever the standard client looks like.
"""
import base64
import http.client
import json
import socket
import urllib.parse

__version__ = "0.1.0"


class Error(Exception):
    def __init__(self, status, message):
        super().__init__(status, message)
        self.status = status
        self.message = message


class NotSupportedError(Error):
    """The operation needs the real <replaces>; refused explicitly, never silently different."""


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

    # ---- the API: shaped like the standard client ----

    def ping(self):
        return self._call("GET", "/api/ping").get("ok", False)


def connect(host, user="", password="", timeout=30, **_ignored):
    """Same signature as the standard client's connect; extra arguments are accepted and ignored."""
    url = host if host.startswith("http") else "https://" + host
    return Connection(url, user, password, timeout)
