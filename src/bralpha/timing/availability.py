from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from bralpha.domain.b3_calendar import next_business_day

DEFAULT_TIMING_TIMEZONE = "America/Sao_Paulo"
DEFAULT_DECISION_CUTOFF_TIME = time(18, 30)
FOCUS_DATE_ONLY_AVAILABILITY_NOTE = "date_only_next_business_day_until_publication_calendar"


def decision_cutoff_datetime(ref_date: date, *, cutoff_time: time, tz_name: str) -> datetime:
    return datetime.combine(ref_date, cutoff_time, tzinfo=_local_timezone(tz_name))


def usable_date_from_available_datetime(
    available_datetime_local: datetime,
    *,
    cutoff_time: time,
    holidays: set[date] | None = None,
) -> date:
    local_date = available_datetime_local.date()
    cutoff = datetime.combine(local_date, cutoff_time)
    if available_datetime_local.tzinfo is not None:
        cutoff = cutoff.replace(tzinfo=available_datetime_local.tzinfo)
    if available_datetime_local <= cutoff:
        return local_date
    return next_business_day(local_date, holidays)


def usable_date_from_date_only(
    release_date: date,
    *,
    holidays: set[date] | None = None,
) -> date:
    return next_business_day(release_date, holidays)


def _local_timezone(tz_name: str):
    try:
        return ZoneInfo(tz_name)
    except ZoneInfoNotFoundError:
        if tz_name == DEFAULT_TIMING_TIMEZONE:
            return timezone(timedelta(hours=-3), name=tz_name)
        raise
