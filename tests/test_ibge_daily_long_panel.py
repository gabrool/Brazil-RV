from __future__ import annotations

from datetime import date, datetime

import polars as pl

from bralpha.derived.ibge.daily_long import build_daily_long


def test_daily_long_includes_only_sidra_value_rows_and_drops_nulls():
    panel = build_daily_long(
        sidra_asof_daily=pl.DataFrame(
            [
                _sidra_asof_row(feature_id="ibge_sidra:ipca:7060:63:1:315=7169", value=0.42),
                _sidra_asof_row(
                    feature_id="ibge_sidra:ipca:7060:64:1:315=7169",
                    value=None,
                    raw_value="X",
                    value_status="withheld",
                    has_value=False,
                ),
            ]
        ),
        include_sidra=True,
    )

    assert panel.height == 1
    row = panel.row(0, named=True)
    assert row["source_family"] == "ibge_sidra"
    assert row["feature_id"] == "ibge_sidra:ipca:7060:63:1:315=7169"
    assert row["value_name"] == "value"
    assert row["value"] == 0.42
    assert row["observation_available_date"] == date(2024, 2, 9)
    assert row["is_available"] is True
    assert row["has_value"] is True


def test_daily_long_uses_long_primary_key_and_does_not_pivot_wide():
    panel = build_daily_long(
        sidra_asof_daily=pl.DataFrame(
            [
                _sidra_asof_row(feature_id="ibge_sidra:ipca:7060:63:1:315=7169", value=0.42),
                _sidra_asof_row(feature_id="ibge_sidra:inpc:7063:63:1:315=7169", value=0.57),
            ]
        ),
        include_sidra=True,
    )

    keys = ["ref_date", "source_family", "feature_id", "value_name"]
    assert panel.group_by(keys).len().height == panel.height
    assert {"calendar_title", "news_title", "ipca_value", "inpc_value"}.isdisjoint(
        panel.columns
    )
    assert panel["source_family"].unique().to_list() == ["ibge_sidra"]
    assert panel["value_name"].unique().to_list() == ["value"]


def test_daily_long_can_disable_sidra_and_returns_empty_long_schema():
    panel = build_daily_long(
        sidra_asof_daily=pl.DataFrame([_sidra_asof_row()]),
        include_sidra=False,
    )

    assert panel.is_empty()
    assert {
        "ref_date",
        "available_date",
        "source_family",
        "feature_id",
        "value_name",
        "value",
    }.issubset(panel.columns)


def _sidra_asof_row(
    *,
    feature_id: str = "ibge_sidra:ipca:7060:63:1:315=7169",
    value: float | None = 0.42,
    raw_value: str = "0.42",
    value_status: str = "ok",
    has_value: bool = True,
) -> dict[str, object]:
    return {
        "ref_date": date(2024, 2, 9),
        "available_date": date(2024, 2, 9),
        "feature_id": feature_id,
        "dataset_slug": "ipca",
        "aggregate_id": 7060,
        "variable_id": "63",
        "variable_name": "IPCA monthly variation",
        "unit": "%",
        "frequency": "monthly",
        "observation_ref_date": date(2024, 1, 31),
        "observation_available_date": date(2024, 2, 9),
        "ref_period_start": date(2024, 1, 1),
        "ref_period_end": date(2024, 1, 31),
        "period_code": "202401",
        "period_label": "202401",
        "release_date": date(2024, 2, 9),
        "available_datetime_local": datetime(2024, 2, 9, 9),
        "available_datetime_utc": datetime(2024, 2, 9, 12),
        "geography_level": "N1",
        "geography_id": "1",
        "geography_name": "Brasil",
        "classification_key": "315=7169",
        "classifications_json": "[]",
        "value": value,
        "raw_value": raw_value,
        "value_status": value_status,
        "has_value": has_value,
        "is_available": True,
        "is_observed_on_ref_date": False,
        "staleness_days": 0,
        "source_version": "v0",
    }
