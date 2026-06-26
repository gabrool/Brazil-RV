from __future__ import annotations

from datetime import date, datetime

import polars as pl

from bralpha.derived.bcb.daily_long import build_daily_long


def test_daily_long_includes_sgs_ptax_and_focus_rows_and_drops_nulls():
    panel = build_daily_long(
        sgs_asof_daily=_sgs_asof(),
        ptax_selected_daily=_ptax_selected(),
        focus_expectation_asof_daily=_focus_asof(),
        include_sgs=True,
        include_ptax=True,
        include_focus=True,
    )

    rows = panel.select(["source_family", "feature_id", "value_name", "value"]).to_dicts()

    assert {
        ("sgs", "sgs:selic_over", "value"),
        ("ptax", "ptax:USD", "bid_rate"),
        ("ptax", "ptax:USD", "ask_rate"),
        ("focus", "focus:focus-key", "mean"),
        ("focus", "focus:focus-key", "respondents"),
    }.issubset(
        {(row["source_family"], row["feature_id"], row["value_name"]) for row in rows}
    )
    assert not any(row["value"] is None for row in rows)
    assert "std_dev" not in [row["value_name"] for row in rows]


def test_daily_long_uses_long_primary_key_and_does_not_pivot_wide():
    panel = build_daily_long(
        sgs_asof_daily=_sgs_asof(),
        ptax_selected_daily=_ptax_selected(),
        focus_expectation_asof_daily=_focus_asof(),
        include_sgs=True,
        include_ptax=True,
        include_focus=True,
    )

    keys = ["ref_date", "source_family", "feature_id", "value_name"]
    assert panel.group_by(keys).len().height == panel.height
    assert {"selic_over", "usd_bid_rate", "focus_mean"}.isdisjoint(panel.columns)


def _sgs_asof() -> pl.DataFrame:
    return pl.DataFrame(
        [
            {
                "ref_date": date(2024, 1, 2),
                "available_date": date(2024, 1, 2),
                "series_id": 11,
                "series_slug": "selic_over",
                "series_name": "Selic",
                "category": "rates",
                "frequency": "daily",
                "observation_ref_date": date(2024, 1, 1),
                "observation_available_date": date(2024, 1, 2),
                "value": 10.0,
                "unit": "percent_annualized",
                "is_available": True,
                "is_observed_on_ref_date": False,
                "staleness_days": 0,
                "availability_policy": "next_business_day",
                "model_usable": True,
                "source_version": "v0",
            }
        ]
    )


def _ptax_selected() -> pl.DataFrame:
    return pl.DataFrame(
        [
            {
                "ref_date": date(2024, 1, 2),
                "available_date": date(2024, 1, 2),
                "currency_code": "USD",
                "currency_name": "US Dollar",
                "selected_bulletin_type": "Fechamento",
                "quote_datetime": datetime(2024, 1, 2, 13),
                "bid_rate": 5.0,
                "ask_rate": 5.1,
                "bid_parity": None,
                "ask_parity": None,
                "has_quote": True,
                "source_version": "v0",
            }
        ]
    )


def _focus_asof() -> pl.DataFrame:
    return pl.DataFrame(
        [
            {
                "ref_date": date(2024, 1, 2),
                "available_date": date(2024, 1, 2),
                "expectation_key": "focus-key",
                "observation_ref_date": date(2024, 1, 2),
                "observation_available_date": date(2024, 1, 2),
                "endpoint": "ExpectativasMercadoAnuais",
                "indicator": "IPCA",
                "indicator_detail": None,
                "reference_period": "2025",
                "reference_year": 2025,
                "reference_month": None,
                "meeting": None,
                "horizon_label": "2025",
                "is_top5": False,
                "calculation_type": None,
                "statistic_scope": "1",
                "mean": 4.0,
                "median": None,
                "std_dev": None,
                "min_value": None,
                "max_value": None,
                "respondents": 50,
                "base_calculation": 1,
                "is_available": True,
                "is_observed_on_ref_date": True,
                "staleness_days": 0,
                "availability_note": "first_pass_available_date_equals_data",
                "source_dataset": "bcb_focus_expectations",
                "source_version": "v0",
            }
        ]
    )
