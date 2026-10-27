"""Exceptions a DAG can raise or see, named as in Airflow."""


class AirflowException(Exception):
    """A task failure (retried if the task has retries left)."""


class AirflowSkipException(AirflowException):
    """Raised inside a task: the task ends as skipped."""


class AirflowFailException(AirflowException):
    """Raised inside a task: the task fails at once, without retries."""


class NotSupportedError(AirflowException):
    """lambdaflow refuses this on purpose (it needs a running Airflow server); move to Airflow / MWAA for it."""
