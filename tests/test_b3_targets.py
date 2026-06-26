from __future__ import annotations

from datetime import date

import polars as pl

from bralpha.derived.b3.targets import build_targets_daily


def test_targets_use_future_values_only_in_separate_target_panel():
    targets = build_targets_daily(
        continuous_futures_daily=pl.DataFrame(
            [
                _continuous_row(date(2024, 1, 2), date(2024, 1, 3), 10.0),
                _continuous_row(date(2024, 1, 3), date(2024, 1, 4), 11.0),
                _continuous_row(date(2024, 1, 4), date(2024, 1, 5), 12.0),
            ]
        ),
        horizons=[1, 2],
        target_types=["quote_diff", "quote_pct_change"],
    )
    one_day = targets.filter(
        (pl.col("ref_date") == date(2024, 1, 2))
        & (pl.col("horizon") == 1)
        & (pl.col("target_type") == "quote_diff")
    ).row(0, named=True)

    assert one_day["target_value"] == 1.0
    assert one_day["target_end_date"] == date(2024, 1, 3)
    assert one_day["label_available_date"] == date(2024, 1, 4)
    assert "target_value" in targets.columns


def _continuous_row(ref_date: date, available_date: date, settlement: float):
    return {
        "ref_date": ref_date,
        "available_date": available_date,
        "continuous_id": "DI1_R1",
        "settlement": settlement,
        "source_version": "v0",
    }
