from __future__ import annotations

from datetime import date

import polars as pl

from bralpha.derived.fred.daily_long import build_fred_daily_long


def test_fred_daily_long_includes_non_null_asof_rows_only():
    asof = pl.DataFrame(
        [
            _asof_row(date(2024, 1, 2), "fred|dgs10", 4.0, "ok", True),
            _asof_row(date(2024, 1, 2), "fred|sp500", None, "missing", False),
        ]
    )

    panel = build_fred_daily_long(asof_daily=asof, include_observations=True)

    assert panel.height == 1
    row = panel.row(0, named=True)
    assert row["source_family"] == "fred"
    assert row["feature_id"] == "fred|dgs10"
    assert row["value_name"] == "value"
    assert row["value"] == 4.0
    assert row["has_value"] is True
    assert "series_name" not in panel.columns
    assert "return" not in panel.columns
    assert "spread" not in panel.columns


def test_fred_daily_long_upholds_long_primary_key_by_last_value():
    asof = pl.DataFrame(
        [
            _asof_row(date(2024, 1, 2), "fred|dgs10", 4.0, "ok", True),
            _asof_row(date(2024, 1, 2), "fred|dgs10", 4.1, "ok", True),
        ]
    )

    panel = build_fred_daily_long(asof_daily=asof, include_observations=True)

    assert panel.height == 1
    assert panel["value"].item() == 4.1
    assert panel.select(["ref_date", "source_family", "feature_id", "value_name"]).n_unique() == 1


def _asof_row(
    ref_date: date,
    feature_id: str,
    value: float | None,
    value_status: str,
    has_value: bool,
) -> dict[str, object]:
    return {
        "ref_date": ref_date,
        "available_date": ref_date,
        "feature_id": feature_id,
        "series_id": feature_id.removeprefix("fred|").upper(),
        "series_name": feature_id,
        "category": "treasury_nominal",
        "frequency": "daily",
        "unit": "percent",
        "observation_ref_date": date(2024, 1, 1),
        "observation_available_date": ref_date,
        "availability_policy": "date_only_next_business_day",
        "value": value,
        "raw_value": "." if value is None else str(value),
        "value_status": value_status,
        "has_value": has_value,
        "realtime_start": date(2024, 1, 1),
        "realtime_end": date(2024, 1, 1),
        "is_available": True,
        "is_observed_on_ref_date": False,
        "staleness_days": 0,
        "source_version": "v0",
    }
