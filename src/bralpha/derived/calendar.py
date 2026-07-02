from __future__ import annotations

from datetime import date

import polars as pl

from bralpha.domain.b3_calendar import business_days


def business_days_b3(start: date, end: date) -> list[date]:
    return business_days(start, end)


def business_day_frame(start: date, end: date) -> pl.DataFrame:
    return pl.DataFrame({"ref_date": business_days_b3(start, end)})
