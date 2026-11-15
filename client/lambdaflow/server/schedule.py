"""Airflow schedules → EventBridge Scheduler expressions, and when such an expression fires.

    DAG(schedule=...)                    EventBridge Scheduler (one schedule per DAG in the group)
    None                                 no schedule: manual triggers only
    "@once"                              at(<start_date>)
    "@hourly" … "@yearly"                the cron below
    "m h dom mon dow" (5 fields)         cron(m h dom mon dow *): "?" in the unused day field, days of the
                                         week as names (cron 0 and 7 = Sunday; EventBridge 1 = SUN)
    timedelta(minutes=n)                 rate(n minutes) — EventBridge picks the phase (quirk)

Refused at parse time (NotSupportedError): day-of-month and day-of-week both restricted (cron ORs them,
EventBridge cannot), a step in day-of-week, L / W / #, @continuous, timetable and asset schedules, a
timedelta that is not whole minutes, catchup=True. The timezone is the start_date's, as in Airflow
(naive or UTC → "UTC").

next_fire() evaluates an EventBridge expression the way AWS documents it; the console shows it as the
next run, the compatibility tests compare it with Airflow's own next run, the local dev server fires by it.
"""
import datetime
import re

from airflow.exceptions import NotSupportedError

PRESETS = {"@hourly": "0 * * * *", "@daily": "0 0 * * *", "@midnight": "0 0 * * *", "@weekly": "0 0 * * 0",
           "@monthly": "0 0 1 * *", "@quarterly": "0 0 1 */3 *", "@yearly": "0 0 1 1 *", "@annually": "0 0 1 1 *"}
DAYS = ["SUN", "MON", "TUE", "WED", "THU", "FRI", "SAT"]            # EventBridge 1..7
MONTHS = ["JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"]
UTC = datetime.timezone.utc


def _refuse(why):
    raise NotSupportedError(f"schedule: {why} — not supported by lambdaflow; see tests/compat/REPORT.md")


def tz_name(start_date):
    """Airflow's rule: a DAG's timezone is its start_date's."""
    tz = start_date.tzinfo if start_date else None
    if tz is None:
        return "UTC"
    key = getattr(tz, "key", None) or getattr(tz, "name", None)     # zoneinfo / pendulum
    if key and key != "+00:00":
        return key
    if start_date.utcoffset() == datetime.timedelta(0):
        return "UTC"
    _refuse(f"start_date timezone {tz} has no name; use zoneinfo.ZoneInfo('Region/City')")


def _cron_field(value, lo, hi, names=None, dow=False):
    """One Airflow cron field → EventBridge. Numbers, names, ranges, lists and steps; nothing else."""
    out = []
    for item in value.split(","):
        part, _, step = item.partition("/")
        if step and (dow or not step.isdigit()):
            _refuse(f"step {item!r} in day-of-week" if dow else f"bad step {item!r}")
        if part in ("*", "?"):
            out.append(f"{lo}/{step}" if step else "*")
            continue
        bounds = part.split("-")
        if len(bounds) > 2 or not all(re.fullmatch(r"[0-9]+|[A-Za-z]{3}", b) for b in bounds):
            _refuse(f"cron field {value!r} (only numbers, names, ranges, lists and steps)")
        nums = []
        for b in bounds:
            if b.isdigit():
                n = int(b)
            elif names and b.upper() in names:
                n = names.index(b.upper()) + (0 if dow else 1)
            else:
                _refuse(f"cron value {b!r}")
            if dow and n == 7:
                n = 0 if len(bounds) == 1 else 7
            if not (lo <= n <= hi or (dow and n == 7)):
                _refuse(f"cron value {b!r} out of range")
            nums.append(n)
        if dow:
            if len(nums) == 1:
                out.append(DAYS[nums[0]])
            elif nums[0] <= nums[1] <= 6:
                out.append(f"{DAYS[nums[0]]}-{DAYS[nums[1]]}")
            else:                                                # 5-7: FRI,SAT,SUN
                out.extend(DAYS[n % 7] for n in range(nums[0], nums[1] + 1))
        elif names:                                              # months: names read better
            out.append("-".join(MONTHS[n - 1] for n in nums) + (f"/{step}" if step else ""))
        else:
            out.append("-".join(map(str, nums)) + (f"/{step}" if step else ""))
    return ",".join(out)


def cron_to_eventbridge(cron):
    fields = PRESETS.get(cron, cron).split()
    if len(fields) != 5:
        _refuse(f"{cron!r} is not a 5-field cron or a preset")
    m, h, dom, mon, dow = fields
    m, h = _cron_field(m, 0, 59), _cron_field(h, 0, 23)
    dom, mon = _cron_field(dom, 1, 31), _cron_field(mon, 1, 12, MONTHS)
    dow = _cron_field(dow, 0, 6, DAYS, dow=True)
    if dom != "*" and dow != "*":
        _refuse(f"{cron!r} restricts both day-of-month and day-of-week (cron runs on either; EventBridge cannot)")
    if dow == "*":
        dow = "?"
    else:
        dom = "?"
    return f"cron({m} {h} {dom} {mon} {dow} *)"


def translate(schedule, start_date=None, catchup=False):
    """DAG arguments → {"schedule": as written, "expression": EventBridge or None, "timezone": name}."""
    if catchup:
        _refuse("catchup=True (EventBridge does not backfill; trigger past dates from the console)")
    tz = tz_name(start_date)
    if schedule is None:
        return {"schedule": None, "expression": None, "timezone": tz}
    if isinstance(schedule, datetime.timedelta):
        minutes, rest = divmod(schedule.total_seconds(), 60)
        if rest or minutes < 1:
            _refuse(f"timedelta {schedule} is not a whole number of minutes")
        n, unit = next((int(minutes // k), u) for k, u in ((1440, "day"), (60, "hour"), (1, "minute")) if minutes % k == 0)
        return {"schedule": f"timedelta({schedule})", "expression": f"rate({n} {unit}{'s' if n > 1 else ''})", "timezone": tz}
    if not isinstance(schedule, str):
        _refuse(f"{type(schedule).__name__} (timetables and assets are not supported)")
    if schedule == "@once":
        if start_date is None:
            _refuse("@once needs a start_date")
        return {"schedule": schedule, "expression": f"at({start_date.strftime('%Y-%m-%dT%H:%M:%S')})", "timezone": tz}
    if schedule == "@continuous":
        _refuse("@continuous (a run right after the previous one needs a running scheduler)")
    return {"schedule": schedule, "expression": cron_to_eventbridge(schedule), "timezone": tz}


# ---------------- when an EventBridge expression fires ----------------

def _values(field, lo, hi, names=None, dow=False):
    if field in ("*", "?"):
        return set(range(lo, hi + 1))
    out = set()
    for item in field.split(","):
        part, _, step = item.partition("/")
        if part == "*":
            a, b = lo, hi
        else:
            ends = [(names.index(x) + 1 if names and x in names else int(x)) for x in part.split("-")]
            a, b = ends[0], ends[-1] if len(ends) > 1 else (hi if step else ends[0])
        out.update(range(a, b + 1, int(step) if step else 1))
    return out


def _zone(name):
    if name == "UTC":
        return UTC
    import zoneinfo
    return zoneinfo.ZoneInfo(name)


def next_fire(expression, timezone="UTC", after=None):
    """The first time ≥ after (UTC) the expression fires; None for rate() (AWS picks its phase) or never."""
    after = (after or datetime.datetime.now(UTC)).astimezone(UTC)
    tz = _zone(timezone)
    if not expression or expression.startswith("rate("):
        return None
    if expression.startswith("at("):
        t = datetime.datetime.fromisoformat(expression[3:-1]).replace(tzinfo=tz).astimezone(UTC)
        return t if t >= after else None
    m, h, dom, mon, dow, year = expression[5:-1].split()
    minutes, hours = sorted(_values(m, 0, 59)), sorted(_values(h, 0, 23))
    doms, mons = _values(dom, 1, 31), _values(mon, 1, 12, MONTHS)
    dows, years = _values(dow, 1, 7, DAYS, dow=True), _values(year, 1970, 2199)
    day = after.astimezone(tz).date()
    for _ in range(366 * 8):
        if (day.month in mons and day.year in years and day.day in doms
                and (day.weekday() + 1) % 7 + 1 in dows):
            for hh in hours:
                for mm in minutes:
                    t = datetime.datetime(day.year, day.month, day.day, hh, mm, tzinfo=tz).astimezone(UTC)
                    if t >= after:
                        return t
        day += datetime.timedelta(days=1)
    return None
