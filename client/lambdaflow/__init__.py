"""<project> — a truly serverless <replaces>. Submodules are named after the standard client they mirror."""
from . import client  # noqa: F401   rename to the standard client's module name (pymysql, redis, ...)

__version__ = "0.1.0"
