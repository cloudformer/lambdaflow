"""The reference service in Docker for the compatibility tests — shared across the series, a pytest plugin
loaded by tests/conftest.py (`pytest_plugins = ["compat_docker"]`). Copy it unchanged; it has no settings.

The reference service (the real one, in Docker) — the same four commands in every project:

    python3 -m pytest tests -q                 leaves Docker alone: live against the service if it is running
                                               and healthy, otherwise the compatibility tests are skipped or use
                                               recorded outcomes (the project decides) and the summary says so
    python3 -m pytest tests -q --docker up     starts it (or reuses it), waits until healthy, tests; keeps it running
    python3 -m pytest tests -q --docker run    the same, then stops it if everything passed; on a failure it keeps
                                               running for a look, and the summary says so
    python3 -m pytest tests -q --docker down   the same, then stops it whatever the result

The container and its env var are the two values CI uses (`compose-service`, `test-env` in
.github/workflows/test.yml), so a local live run is exactly the CI run. A project whose reference is not a
Docker container (empty compose-service) takes the same commands; --docker then only says so.
"""
import json
import os
import re
import subprocess
import time

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
COMPOSE = ["docker", "compose", "-f", os.path.join(ROOT, "tests", "compat", "docker-compose.yml")]
WAIT = 300   # seconds --docker waits for the container's healthcheck
docker = {"mode": None, "service": None, "env": {}, "live": False, "note": None, "exit": None}


def pytest_addoption(parser):
    parser.addoption("--docker", choices=["up", "run", "down"], default=None,
                     help="the reference service in Docker (tests/compat/docker-compose.yml): up = start, test, keep "
                          "running; run = start, test, stop if all passed; down = start, test, always stop. "
                          "Without it Docker is left alone.")


def _ci_values():
    """The service and the env var CI uses, read from .github/workflows/test.yml (the one place they are set)."""
    with open(os.path.join(ROOT, ".github", "workflows", "test.yml"), encoding="utf-8") as f:
        text = f.read()
    service = re.search(r'^\s*compose-service:\s*"([^"]*)"', text, re.M).group(1)   # not the example in the comment
    env = re.search(r'^\s*test-env:\s*"([^"]*)"', text, re.M).group(1)
    return service, dict(kv.split("=", 1) for kv in env.split() if "=" in kv)


def _compose(*args, capture=False):
    return subprocess.run(COMPOSE + list(args), capture_output=capture, text=True)


def _state(service):
    """(state, health) of the service's container; (None, None) when there is none or no Docker at all."""
    try:
        r = subprocess.run(COMPOSE + ["ps", "-a", "--format", "json", service], capture_output=True, text=True, timeout=20)
    except (OSError, subprocess.TimeoutExpired):
        return None, None
    text = r.stdout.strip()
    if r.returncode or not text:
        return None, None
    rows = json.loads(text) if text.startswith("[") else [json.loads(x) for x in text.splitlines() if x.strip()]
    return (rows[0].get("State"), rows[0].get("Health") or "") if rows else (None, None)


def _say(config, text):
    with config.pluginmanager.getplugin("capturemanager").global_and_fixture_disabled():
        print(text, flush=True)


def _docker_setup(config):
    mode = config.getoption("--docker", None)
    service, env = _ci_values()
    docker.update(mode=mode, service=service, env=env)
    if not service:
        if mode:
            _say(config, f"\n--docker {mode}: this project's reference is not a Docker container "
                         "(no compose-service in .github/workflows/test.yml); running the tests as usual")
        return
    if mode is None:
        if env and all(os.environ.get(k) for k in env):       # CI, or set by hand: the service is someone else's
            docker["live"] = True
            return
        state, health = _state(service)
        if state == "running" and health in ("healthy", ""):
            os.environ.update(env)
            docker["live"] = True
        else:
            docker["note"] = (f"{service} is {state or 'not running'}{'/' + health if health else ''}: the compatibility "
                              "tests ran without it (skipped, or against recorded outcomes). "
                              "Live: python3 -m pytest tests -q --docker up | run | down")
        return
    state, _ = _state(service)
    _say(config, f"\n--docker {mode}: {'using the running' if state == 'running' else 'starting'} {service}, "
                 f"waiting until it is healthy (up to {WAIT}s) ...")
    t0 = time.time()
    if _compose("up", "-d", "--wait", "--wait-timeout", str(WAIT), service).returncode:
        state, health = _state(service)
        pytest.exit(f"--docker {mode}: {service} is not healthy yet ({state}/{health or 'no health'}) and keeps "
                          f"starting; run the same command again in a minute (logs: docker compose -f "
                          f"tests/compat/docker-compose.yml logs {service})", returncode=1)
    _say(config, f"--docker {mode}: {service} healthy after {time.time() - t0:.0f}s")
    os.environ.update(env)
    docker["live"] = True


def _docker_plan():
    """What happens to the container after the run: (stop it?, the line for the summary)."""
    mode, service, ok = docker["mode"], docker["service"], docker["exit"] == 0
    if not service or not mode:
        return False, docker["note"] or (f"{service}: compared live" if service and docker["live"] else None)
    if docker["exit"] is None:          # the container never got healthy: nothing ran, leave it starting
        return False, None
    if mode == "up":
        return False, f"{service} keeps running (--docker up); stop it with --docker down"
    if mode == "down" or ok:
        return True, f"stopping {service} (--docker {mode}{', all passed' if ok else ''})"
    state, health = _state(service)
    return False, (f"tests failed: {service} keeps running ({state}/{health or 'no health'}) for a look — "
                   f"logs: docker compose -f tests/compat/docker-compose.yml logs {service}; "
                   f"rerun with --docker run (stops it once all pass) or --docker down")


def pytest_unconfigure(config):
    stop, _ = _docker_plan()
    if stop:
        with config.pluginmanager.getplugin("capturemanager").global_and_fixture_disabled():
            _compose("down")


def pytest_configure(config):
    _docker_setup(config)


def pytest_sessionfinish(session, exitstatus):
    docker["exit"] = int(exitstatus)


def pytest_terminal_summary(terminalreporter):
    _, line = _docker_plan()
    if line:
        terminalreporter.section("docker")
        terminalreporter.write_line(line)
