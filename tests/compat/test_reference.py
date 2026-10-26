"""<project>.<client> vs the real <replaces>: the compatibility matrix.

One test = one feature; docstring "Category | Feature | note". The list only grows. Run with:

    docker compose -f tests/compat/docker-compose.yml up -d <service>
    REFERENCE_DSN=<dsn> python3 -m pytest tests -q
    COMPAT_DRY=1 python3 -m pytest tests/compat -q -s      # project side only, no Docker
"""
import pytest

# The production engine may be older than the local one (e.g. the Lambda runtime's SQLite): record
# what was checked on the deployed service and mark features it lacks with xfail(run=False).
# PRODUCTION_VERSION = (x, y, z)


def test_ping(pair):
    """Basics | ping"""
    pair([("ping",)])


# ---- known gaps: each marker names the task in TASKS.md that closes it ----

# @pytest.mark.xfail(strict=True, reason="task 1: ...")
# def test_something(pair):
#     """Data | something | task 1"""


# ---- unsupported by design: the real service does it, the project refuses with NotSupportedError ----

# @pytest.mark.unsupported("needs a real server")
# def test_blocking_refused(pair):
#     """Unsupported | BLPOP | needs a real server"""
#     pair.unsupported(("blpop", "k"))
