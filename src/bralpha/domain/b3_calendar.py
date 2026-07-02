from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml

DEFAULT_B3_CALENDAR_PATH = Path("configs/calendars/b3_trading_holidays.yaml")


@dataclass(frozen=True)
class B3TradingCalendar:
    calendar_id: str
    start_year: int
    end_year: int
    holidays: frozenset[date]
    source_notes: tuple[str, ...] = ()

    def ensure_covered(self, value: date) -> None:
        if value.year < self.start_year or value.year > self.end_year:
            raise ValueError(
                f"{self.calendar_id} calendar does not cover {value:%Y-%m-%d}; "
                f"configured coverage is {self.start_year}-{self.end_year}"
            )

    def is_business_day(self, value: date) -> bool:
        self.ensure_covered(value)
        return value.weekday() < 5 and value not in self.holidays


def is_business_day(value: date, holidays: set[date] | None = None) -> bool:
    if holidays is not None:
        return value.weekday() < 5 and value not in holidays
    return default_calendar().is_business_day(value)


def next_business_day(value: date, holidays: set[date] | None = None) -> date:
    _ensure_optional_coverage(value, holidays)
    candidate = value + timedelta(days=1)
    while not is_business_day(candidate, holidays):
        candidate += timedelta(days=1)
    return candidate


def previous_business_day(value: date, holidays: set[date] | None = None) -> date:
    _ensure_optional_coverage(value, holidays)
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
        return _subtract_business_days(ref_date, abs(days), holidays)
    _ensure_optional_coverage(ref_date, holidays)
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
        _ensure_optional_coverage(start, holidays)
        _ensure_optional_coverage(end, holidays)
        return 0
    return len(business_days(start + timedelta(days=1), end, holidays))


def business_days(
    start: date,
    end: date,
    holidays: set[date] | None = None,
) -> list[date]:
    if start > end:
        _ensure_optional_coverage(start, holidays)
        _ensure_optional_coverage(end, holidays)
        return []
    days: list[date] = []
    current = start
    while current <= end:
        if is_business_day(current, holidays):
            days.append(current)
        current += timedelta(days=1)
    return days


@lru_cache(maxsize=1)
def default_calendar() -> B3TradingCalendar:
    return load_b3_trading_calendar()


def load_b3_trading_calendar(path: Path | None = None) -> B3TradingCalendar:
    calendar_path = path or _repo_root() / DEFAULT_B3_CALENDAR_PATH
    if not calendar_path.exists():
        raise FileNotFoundError(f"B3 trading calendar config not found: {calendar_path}")
    with calendar_path.open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle)
    if not isinstance(payload, dict):
        raise ValueError(f"B3 trading calendar config must be a mapping: {calendar_path}")

    coverage = _mapping(payload.get("coverage"), "coverage")
    start_year = int(coverage["start_year"])
    end_year = int(coverage["end_year"])
    years = _mapping(payload.get("years"), "years")
    holidays: set[date] = set()
    for year in range(start_year, end_year + 1):
        raw_dates = years.get(year) or years.get(str(year))
        if raw_dates is None:
            raise ValueError(f"B3 trading calendar missing year {year}")
        holidays.update(_parse_holiday_dates(raw_dates, year=year))

    return B3TradingCalendar(
        calendar_id=str(payload.get("calendar_id", "B3")),
        start_year=start_year,
        end_year=end_year,
        holidays=frozenset(holidays),
        source_notes=tuple(str(note) for note in payload.get("source_notes", [])),
    )


def _subtract_business_days(
    ref_date: date,
    days: int,
    holidays: set[date] | None,
) -> date:
    _ensure_optional_coverage(ref_date, holidays)
    candidate = ref_date
    remaining = days
    while remaining:
        candidate -= timedelta(days=1)
        if is_business_day(candidate, holidays):
            remaining -= 1
    return candidate


def _ensure_optional_coverage(value: date, holidays: set[date] | None) -> None:
    if holidays is None:
        default_calendar().ensure_covered(value)


def _parse_holiday_dates(values: Any, *, year: int) -> set[date]:
    if not isinstance(values, list):
        raise ValueError(f"B3 trading calendar year {year} must be a list")
    parsed: set[date] = set()
    for value in values:
        parsed_date = value if isinstance(value, date) else date.fromisoformat(str(value))
        if parsed_date.year != year:
            raise ValueError(
                f"B3 trading calendar year {year} contains {parsed_date:%Y-%m-%d}"
            )
        parsed.add(parsed_date)
    return parsed


def _mapping(value: Any, name: str) -> dict[Any, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"B3 trading calendar config requires mapping: {name}")
    return value


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]
