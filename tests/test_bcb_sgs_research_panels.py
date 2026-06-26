from __future__ import annotations

from datetime import date

import polars as pl

from bralpha.derived.bcb.sgs import build_sgs_asof_daily, build_sgs_observation_daily


def test_sgs_observation_panel_preserves_raw_fields_and_filters_model_usable():
    silver = pl.DataFrame(
        [
            _sgs_row(date(2024, 1, 1), date(2024, 1, 2), 11, "selic_over", 10.0, True),
            _sgs_row(date(2024, 1, 1), date(2024, 1, 2), 999, "unused", 1.0, False),
        ]
    )

    panel = build_sgs_observation_daily(
        silver,
        include_model_usable_only=True,
        start=date(2024, 1, 1),
        end=date(2024, 1, 31),
    )

    row = panel.row(0, named=True)
    assert panel.height == 1
    assert row["value"] == 10.0
    assert row["unit"] == "percent_annualized"
    assert row["category"] == "rates"
    assert row["frequency"] == "daily"


def test_sgs_asof_uses_latest_available_observation_and_staleness():
    observations = pl.DataFrame(
        [
            _sgs_row(date(2024, 1, 1), date(2024, 1, 2), 11, "selic_over", 10.0, True),
            _sgs_row(date(2024, 1, 3), date(2024, 1, 4), 11, "selic_over", 11.0, True),
        ]
    )

    panel = build_sgs_asof_daily(
        observations,
        start=date(2024, 1, 1),
        end=date(2024, 1, 5),
    ).sort("ref_date")

    assert panel["ref_date"].to_list() == [
        date(2024, 1, 2),
        date(2024, 1, 3),
        date(2024, 1, 4),
        date(2024, 1, 5),
    ]
    assert panel["value"].to_list() == [10.0, 10.0, 11.0, 11.0]
    assert panel["staleness_days"].to_list() == [0, 1, 0, 1]
    assert panel.filter(pl.col("ref_date") == date(2024, 1, 1)).is_empty()
    assert panel.filter(pl.col("observation_available_date") > pl.col("ref_date")).is_empty()


def test_sgs_panels_do_not_create_alpha_feature_columns():
    silver = pl.DataFrame(
        [_sgs_row(date(2024, 1, 1), date(2024, 1, 2), 11, "selic_over", 10.0, True)]
    )
    observation = build_sgs_observation_daily(
        silver,
        include_model_usable_only=True,
        start=date(2024, 1, 1),
        end=date(2024, 1, 31),
    )
    asof = build_sgs_asof_daily(
        observation,
        start=date(2024, 1, 1),
        end=date(2024, 1, 5),
    )

    banned = {"change", "zscore", "revision", "surprise", "rolling_mean"}
    assert banned.isdisjoint(observation.columns)
    assert banned.isdisjoint(asof.columns)


def _sgs_row(
    ref_date: date,
    available_date: date,
    series_id: int,
    slug: str,
    value: float,
    model_usable: bool,
) -> dict[str, object]:
    return {
        "ref_date": ref_date,
        "available_date": available_date,
        "series_id": series_id,
        "series_slug": slug,
        "series_name": slug,
        "category": "rates",
        "frequency": "daily",
        "value": value,
        "unit": "percent_annualized",
        "availability_policy": "next_business_day",
        "model_usable": model_usable,
        "source_version": "v0",
    }
