"""tests/compat_docker.py (shared across the series): the CI values it reads, what --docker does after a run."""
import pytest

import compat_docker as cr


def test_the_ci_values_are_read_from_the_workflow_not_its_comment():
    service, env = cr._ci_values()
    with open(cr.os.path.join(cr.ROOT, ".github", "workflows", "test.yml"), encoding="utf-8") as f:
        text = f.read()
    assert f'compose-service: "{service}"' in text and all(f"{k}={v}" in text for k, v in env.items())
    assert service != "mysql" or "mysql" in text.split("compose-service:")[-1]   # the comment's example is not taken


@pytest.mark.parametrize("mode,exit,stop,says", [
    ("up", 0, False, "keeps running"),
    ("up", 1, False, "keeps running"),
    ("run", 0, True, "stopping"),
    ("run", 1, False, "tests failed"),
    ("down", 0, True, "stopping"),
    ("down", 1, True, "stopping"),
    ("down", None, False, None),            # never got healthy: nothing ran, the container is left starting
    (None, 0, False, "compared live"),
])
def test_what_happens_to_the_container_after_the_run(monkeypatch, mode, exit, stop, says):
    monkeypatch.setattr(cr, "docker", dict(cr.docker, mode=mode, service="svc", live=True, note=None, exit=exit))
    monkeypatch.setattr(cr, "_state", lambda service: ("running", "healthy"))
    s, line = cr._docker_plan()
    assert s is stop and (line is None if says is None else says in line)


def test_no_docker_reference_means_the_same_commands_still_work(monkeypatch):
    monkeypatch.setattr(cr, "docker", dict(cr.docker, mode="run", service="", note=None, exit=0))
    assert cr._docker_plan() == (False, None)
