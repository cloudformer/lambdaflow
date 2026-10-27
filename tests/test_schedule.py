"""Airflow schedule → EventBridge Scheduler expression, refusals, and the evaluator of the expressions.

Whether an expression fires when Airflow would is compared with a real Airflow in
tests/compat/test_airflow.py::test_schedules; these pin the translation itself.
"""
import datetime
import zoneinfo

import pytest

import schedule as s
from airflow.exceptions import NotSupportedError

UTC = datetime.timezone.utc


@pytest.mark.parametrize("cron,expr", [
    ("0 */4 * * *", "cron(0 0/4 * * ? *)"),
    ("*/20 * * * *", "cron(0/20 * * * ? *)"),
    ("30 9 * * 1-5", "cron(30 9 ? * MON-FRI *)"),            # cron 1 = Monday; EventBridge 1 = Sunday
    ("15 6 * * 0", "cron(15 6 ? * SUN *)"),
    ("0 0 * * 7", "cron(0 0 ? * SUN *)"),                     # 7 is Sunday too
    ("0 0 * * 5-7", "cron(0 0 ? * FRI,SAT,SUN *)"),
    ("0 8 * * mon,wed,fri", "cron(0 8 ? * MON,WED,FRI *)"),
    ("0 0 1,15 * *", "cron(0 0 1,15 * ? *)"),
    ("0 12 1 */3 *", "cron(0 12 1 1/3 ? *)"),
    ("0 0 * jan-mar *", "cron(0 0 * JAN-MAR ? *)"),
    ("@hourly", "cron(0 * * * ? *)"),
    ("@daily", "cron(0 0 * * ? *)"),
    ("@weekly", "cron(0 0 ? * SUN *)"),
    ("@monthly", "cron(0 0 1 * ? *)"),
    ("@yearly", "cron(0 0 1 JAN ? *)"),
])
def test_cron_to_eventbridge(cron, expr):
    assert s.cron_to_eventbridge(cron) == expr


@pytest.mark.parametrize("delta,expr", [
    (datetime.timedelta(minutes=15), "rate(15 minutes)"),
    (datetime.timedelta(minutes=90), "rate(90 minutes)"),
    (datetime.timedelta(hours=1), "rate(1 hour)"),
    (datetime.timedelta(days=2), "rate(2 days)"),
])
def test_timedelta_to_rate(delta, expr):
    assert s.translate(delta)["expression"] == expr


def test_once_and_timezone_come_from_start_date():
    start = datetime.datetime(2030, 3, 4, 5, 6, tzinfo=zoneinfo.ZoneInfo("Europe/Berlin"))
    assert s.translate("@once", start) == {"schedule": "@once", "expression": "at(2030-03-04T05:06:00)",
                                           "timezone": "Europe/Berlin"}
    assert s.translate("@daily", datetime.datetime(2030, 1, 1))["timezone"] == "UTC"            # naive = UTC
    assert s.translate("@daily", datetime.datetime(2030, 1, 1, tzinfo=UTC))["timezone"] == "UTC"
    assert s.translate(None) == {"schedule": None, "expression": None, "timezone": "UTC"}


@pytest.mark.parametrize("schedule,kw,why", [
    ("0 0 1 * 1", {}, "both day-of-month and day-of-week"),
    ("0 0 * * */2", {}, "step"),
    ("0 0 L * *", {}, "cron"),
    ("0 0 * * 5#3", {}, "cron"),
    ("0 0 * *", {}, "5-field"),
    ("61 * * * *", {}, "out of range"),
    ("@continuous", {}, "@continuous"),
    (datetime.timedelta(seconds=30), {}, "whole number of minutes"),
    ("@daily", {"catchup": True}, "catchup"),
    (object(), {}, "timetables"),
    ("@daily", {"start_date": datetime.datetime(2030, 1, 1, tzinfo=datetime.timezone(datetime.timedelta(hours=2)))}, "no name"),
])
def test_refused_with_the_reason(schedule, kw, why):
    with pytest.raises(NotSupportedError, match=why):
        s.translate(schedule, kw.get("start_date"), kw.get("catchup", False))


@pytest.mark.parametrize("cron,tz,after,first", [
    ("0 */4 * * *", "UTC", datetime.datetime(2030, 1, 2, 13, 7, tzinfo=UTC), "2030-01-02 16:00"),
    ("30 9 * * 1-5", "America/New_York", datetime.datetime(2030, 1, 5, 15, 0, tzinfo=UTC), "2030-01-07 14:30"),  # Sat → Mon, EST
    ("30 9 * * 1-5", "America/New_York", datetime.datetime(2030, 7, 5, 12, 0, tzinfo=UTC), "2030-07-05 13:30"),  # EDT
    ("0 12 1 */3 *", "UTC", datetime.datetime(2030, 2, 10, tzinfo=UTC), "2030-04-01 12:00"),
    ("@weekly", "UTC", datetime.datetime(2030, 1, 2, 13, 7, tzinfo=UTC), "2030-01-06 00:00"),
    ("0 0 * * 5-7", "UTC", datetime.datetime(2030, 1, 1, 1, tzinfo=UTC), "2030-01-04 00:00"),
    ("0 6 * * *", "UTC", datetime.datetime(2030, 1, 1, 6, 0, tzinfo=UTC), "2030-01-01 06:00"),      # on the dot: fires
])
def test_next_fire(cron, tz, after, first):
    t = s.next_fire(s.cron_to_eventbridge(cron), tz, after)
    assert t.strftime("%Y-%m-%d %H:%M") == first and t.tzinfo == UTC


def test_next_fire_of_at_and_rate():
    assert s.next_fire("at(2030-03-04T05:06:00)", "Europe/Berlin", datetime.datetime(2030, 1, 1, tzinfo=UTC)) == \
        datetime.datetime(2030, 3, 4, 4, 6, tzinfo=UTC)
    assert s.next_fire("at(2020-03-04T05:06:00)", "UTC", datetime.datetime(2030, 1, 1, tzinfo=UTC)) is None   # past
    assert s.next_fire("rate(5 minutes)", "UTC") is None      # AWS picks the phase
