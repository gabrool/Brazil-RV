from __future__ import annotations

from collections.abc import Iterable
from datetime import date
from math import log, sqrt
from statistics import stdev
from typing import Any

from bralpha.domain.b3_calendar import previous_business_day


def as_date(value: Any) -> date:
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


def optional_date(value: Any) -> date | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    return as_date(value)


def optional_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def safe_log(value: Any) -> float | None:
    number = optional_float(value)
    if number is None or number <= 0:
        return None
    return log(number)


def safe_log1p(value: Any) -> float | None:
    number = optional_float(value)
    if number is None or number < 0:
        return None
    return log(number + 1.0)


def safe_ratio(numerator: Any, denominator: Any) -> float | None:
    top = optional_float(numerator)
    bottom = optional_float(denominator)
    if top is None or bottom in {None, 0.0}:
        return None
    return top / bottom


def safe_log_return(current: Any, previous: Any) -> float | None:
    current_float = optional_float(current)
    previous_float = optional_float(previous)
    if current_float is None or previous_float is None or current_float <= 0 or previous_float <= 0:
        return None
    return log(current_float / previous_float)


def safe_pct_change(current: Any, previous: Any) -> float | None:
    current_float = optional_float(current)
    previous_float = optional_float(previous)
    if current_float is None or previous_float in {None, 0.0}:
        return None
    return 100.0 * (current_float / previous_float - 1.0)


def diff(current: Any, previous: Any) -> float | None:
    current_float = optional_float(current)
    previous_float = optional_float(previous)
    if current_float is None or previous_float is None:
        return None
    return current_float - previous_float


def realized_vol_ann(log_returns: Iterable[float | None], window: int) -> float | None:
    values = list(log_returns)[-window:]
    if len(values) < window or any(value is None for value in values):
        return None
    if window < 2:
        return None
    return stdev(value for value in values if value is not None) * sqrt(252.0)


def feature_warmup_start(start: date, business_days: int) -> date:
    candidate = start
    for _ in range(business_days):
        candidate = previous_business_day(candidate)
    return candidate


def in_output_window(ref_date: date, start: date | None, end: date | None) -> bool:
    if start is not None and ref_date < start:
        return False
    return not (end is not None and ref_date > end)


def join_versions(*versions: Any) -> str:
    unique = sorted({str(version) for version in versions if version})
    return "|".join(unique) if unique else "v0"


def max_date(*values: Any) -> date | None:
    dates = [optional_date(value) for value in values]
    non_null = [value for value in dates if value is not None]
    if not non_null:
        return None
    return max(non_null)


def feature_row(
    *,
    ref_date: date,
    source_family: str,
    feature_id: str,
    value_name: str,
    value: float | bool | int | None,
    unit: str | None,
    observation_ref_date: date | None,
    observation_available_date: date | None,
    source_version: str | None,
) -> dict[str, Any]:
    available_date = observation_available_date or ref_date
    return {
        "ref_date": ref_date,
        "available_date": available_date,
        "source_family": source_family,
        "feature_id": feature_id,
        "value_name": value_name,
        "value": _feature_value(value),
        "unit": unit,
        "observation_ref_date": observation_ref_date,
        "observation_available_date": observation_available_date,
        "is_available": observation_available_date is not None,
        "staleness_days": 0,
        "source_version": source_version or "v0",
    }


def _feature_value(value: float | bool | int | None) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return 1.0 if value else 0.0
    return float(value)
