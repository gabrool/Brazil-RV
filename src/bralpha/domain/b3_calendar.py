from __future__ import annotations

from datetime import date, timedelta


def is_business_day(value: date, holidays: set[date] | None = None) -> bool:
    holidays = holidays or set()
    return value.weekday() < 5 and value not in holidays


def next_business_day(value: date, holidays: set[date] | None = None) -> date:
    candidate = value + timedelta(days=1)
    while not is_business_day(candidate, holidays):
        candidate += timedelta(days=1)
    return candidate


def previous_business_day(value: date, holidays: set[date] | None = None) -> date:
    candidate = value - timedelta(days=1)
    while not is_business_day(candidate, holidays):
        candidate -= timedelta(days=1)
    return candidate
