"""Schedules: one DAG per case, compared by the first run after start_date (Airflow's next_dagrun_run_after).

Start dates are in the future, so nothing runs, and off the schedule's boundaries. Cron day-of-week numbers
(0 and 7 = Sunday) differ from EventBridge's (1 = Sunday); timezones come from the start date.
"""
import datetime
import zoneinfo

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG

UTC = datetime.timezone.utc
NY, BERLIN = zoneinfo.ZoneInfo("America/New_York"), zoneinfo.ZoneInfo("Europe/Berlin")
T = datetime.datetime(2030, 1, 2, 13, 7, tzinfo=UTC)   # a Wednesday afternoon

CASES = {
    "sched_every_4_hours": ("0 */4 * * *", T),
    "sched_every_20_min": ("*/20 * * * *", T),
    "sched_weekdays_ny": ("30 9 * * 1-5", datetime.datetime(2030, 1, 5, 10, 0, tzinfo=NY)),         # Sat → Mon, EST
    "sched_weekdays_ny_summer": ("30 9 * * 1-5", datetime.datetime(2030, 7, 5, 8, 0, tzinfo=NY)),   # EDT
    "sched_sunday_0": ("15 6 * * 0", T),
    "sched_sunday_7": ("15 6 * * 7", T),
    "sched_fri_to_sun": ("0 0 * * 5-7", datetime.datetime(2030, 1, 1, 1, tzinfo=UTC)),
    "sched_day_names": ("0 8 * * mon,wed,fri", datetime.datetime(2030, 1, 3, 9, tzinfo=UTC)),
    "sched_days_of_month": ("0 0 1,15 * *", T),
    "sched_quarterly_step": ("0 12 1 */3 *", datetime.datetime(2030, 2, 10, tzinfo=UTC)),
    "sched_month_names": ("0 0 1 jan-mar *", datetime.datetime(2030, 3, 2, tzinfo=UTC)),
    "sched_hourly": ("@hourly", T),
    "sched_daily": ("@daily", T),
    "sched_daily_berlin": ("@daily", datetime.datetime(2030, 1, 2, 13, 7, tzinfo=BERLIN)),
    "sched_weekly": ("@weekly", T),
    "sched_monthly": ("@monthly", T),
    "sched_yearly": ("@yearly", T),
    "sched_once": ("@once", datetime.datetime(2030, 3, 4, 5, 6, tzinfo=UTC)),
    "sched_once_berlin": ("@once", datetime.datetime(2030, 3, 4, 5, 6, tzinfo=BERLIN)),
}

for dag_id, (schedule, start) in CASES.items():
    with DAG(dag_id, start_date=start, schedule=schedule, catchup=False):
        EmptyOperator(task_id="noop")
