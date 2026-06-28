from __future__ import annotations

from datetime import date, timedelta

import polars as pl


def business_days_mon_fri(start: date, end: date) -> list[date]:
    if start > end:
        return []
    days: list[date] = []
    current = start
    while current <= end:
        if current.weekday() < 5:
            days.append(current)
        current += timedelta(days=1)
    return days


def business_day_frame(start: date, end: date) -> pl.DataFrame:
    return pl.DataFrame({"ref_date": business_days_mon_fri(start, end)})
