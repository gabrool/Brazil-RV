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


def add_business_days(
    ref_date: date,
    days: int,
    holidays: set[date] | None = None,
) -> date:
    if days < 0:
        raise ValueError("days must be nonnegative")
    candidate = ref_date
    remaining = days
    while remaining:
        candidate += timedelta(days=1)
        if is_business_day(candidate, holidays):
            remaining -= 1
    return candidate


def business_days_between(
    start: date,
    end: date,
    holidays: set[date] | None = None,
) -> int:
    if end <= start:
        return 0
    count = 0
    current = start + timedelta(days=1)
    while current <= end:
        if is_business_day(current, holidays):
            count += 1
        current += timedelta(days=1)
    return count
