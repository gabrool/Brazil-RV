from __future__ import annotations

from datetime import date, datetime

import polars as pl

from bralpha.derived.bcb.daily_long import build_daily_long


def test_daily_long_includes_sgs_ptax_and_focus_rows_and_drops_nulls():
    panel = build_daily_long(
        sgs_asof_daily=_sgs_asof(),
        sgs_feature_daily=_sgs_feature_daily(),
        ptax_selected_daily=_ptax_selected(),
        focus_expectation_asof_daily=_focus_asof(),
        include_sgs=True,
        include_ptax=True,
        include_focus=True,
    )

    rows = panel.select(["source_family", "feature_id", "value_name", "value"]).to_dicts()

    assert {
        ("sgs", "sgs:selic_over", "value"),
        (
            "bcb_sgs_feature",
            "bcb_sgs_feature:external_reserves:reserves_log_change_5bd",
            "reserves_log_change_5bd",
        ),
        ("ptax", "ptax:USD", "bid_rate"),
        ("ptax", "ptax:USD", "ask_rate"),
        ("focus", "focus:focus-key", "mean"),
        ("focus", "focus:focus-key", "respondents"),
    }.issubset(
        {(row["source_family"], row["feature_id"], row["value_name"]) for row in rows}
    )
    assert not any(row["value"] is None for row in rows)
    assert "std_dev" not in [row["value_name"] for row in rows]
    ptax_bid = panel.filter(
        (pl.col("source_family") == "ptax") & (pl.col("value_name") == "bid_rate")
    ).row(0, named=True)
    assert ptax_bid["ref_date"] == date(2024, 1, 3)
    assert ptax_bid["observation_ref_date"] == date(2024, 1, 2)
    assert ptax_bid["observation_available_date"] == date(2024, 1, 3)
    sgs = panel.filter(pl.col("source_family") == "sgs").row(0, named=True)
    assert sgs["model_usable"] is True
    assert sgs["availability_basis"] == "source_date_only"
    assert sgs["revision_policy"] == "unrevised"


def test_daily_long_uses_long_primary_key_and_does_not_pivot_wide():
    panel = build_daily_long(
        sgs_asof_daily=_sgs_asof(),
        sgs_feature_daily=_sgs_feature_daily(),
        ptax_selected_daily=_ptax_selected(),
        focus_expectation_asof_daily=_focus_asof(),
        include_sgs=True,
        include_ptax=True,
        include_focus=True,
    )

    keys = ["ref_date", "source_family", "feature_id", "value_name"]
    assert panel.group_by(keys).len().height == panel.height
    assert {"selic_over", "usd_bid_rate", "focus_mean"}.isdisjoint(panel.columns)


def test_daily_long_includes_model_ready_sgs_features_and_excludes_reference_only_sgs():
    panel = build_daily_long(
        sgs_asof_daily=_sgs_asof(),
        sgs_feature_daily=_sgs_feature_daily(),
        ptax_selected_daily=None,
        focus_expectation_asof_daily=None,
        include_sgs=True,
        include_ptax=False,
        include_focus=False,
    )

    feature_ids = set(panel["feature_id"])

    assert "sgs:m2_new" not in feature_ids
    assert "bcb_sgs_feature:rates:selic_over_level_pa" in feature_ids
    assert "bcb_sgs_feature:inflation:ipca_12m_sum_pct" in feature_ids
    assert "bcb_sgs_feature:external_reserves:reserves_log_change_5bd" in feature_ids
    assert panel.filter(pl.col("model_usable") != True).is_empty()  # noqa: E712


def _sgs_asof() -> pl.DataFrame:
    return pl.DataFrame(
        [
            {
                "ref_date": date(2024, 1, 2),
                "available_date": date(2024, 1, 3),
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
                "availability_basis": "source_date_only",
                "revision_policy": "unrevised",
                "model_usable": True,
                "source_version": "v0",
            },
            {
                "ref_date": date(2024, 1, 2),
                "available_date": date(2024, 1, 3),
                "series_id": 27810,
                "series_slug": "m2_new",
                "series_name": "M2",
                "category": "monetary_liquidity",
                "frequency": "monthly",
                "observation_ref_date": date(2024, 1, 1),
                "observation_available_date": date(2024, 1, 2),
                "value": 1000.0,
                "unit": "brl_thousands",
                "is_available": True,
                "is_observed_on_ref_date": False,
                "staleness_days": 0,
                "availability_policy": "unknown",
                "availability_basis": "unknown",
                "revision_policy": "current_snapshot_reference_only",
                "model_usable": False,
                "source_version": "v0",
            }
        ]
    )


def _ptax_selected() -> pl.DataFrame:
    return pl.DataFrame(
        [
            {
                "ref_date": date(2024, 1, 2),
                "available_date": date(2024, 1, 3),
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


def _sgs_feature_daily() -> pl.DataFrame:
    return pl.DataFrame(
        [
            _feature_row(
                "bcb_sgs_feature:rates:selic_over_level_pa",
                "selic_over_level_pa",
                10.0,
                "percent_pa",
            ),
            _feature_row(
                "bcb_sgs_feature:inflation:ipca_12m_sum_pct",
                "ipca_12m_sum_pct",
                5.0,
                "percent",
            ),
            _feature_row(
                "bcb_sgs_feature:external_reserves:reserves_log_change_5bd",
                "reserves_log_change_5bd",
                0.01,
                "log_change",
            ),
            {
                **_feature_row(
                    "bcb_sgs_feature:credit:reference_only",
                    "reference_only",
                    1.0,
                    "index",
                ),
                "model_usable": False,
            },
        ]
    )


def _feature_row(
    feature_id: str,
    value_name: str,
    value: float,
    unit: str,
) -> dict[str, object]:
    return {
        "ref_date": date(2024, 1, 2),
        "available_date": date(2024, 1, 2),
        "source_family": "bcb_sgs_feature",
        "feature_id": feature_id,
        "value_name": value_name,
        "value": value,
        "unit": unit,
        "observation_ref_date": date(2024, 1, 2),
        "observation_available_date": date(2024, 1, 2),
        "availability_policy": "date_only_next_business_day",
        "availability_basis": "source_date_only",
        "revision_policy": "unrevised",
        "model_usable": True,
        "is_available": True,
        "staleness_days": 0,
        "source_version": "v0",
    }


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
                "availability_note": "date_only_next_business_day_until_publication_calendar",
                "source_dataset": "bcb_focus_expectations",
                "source_version": "v0",
            }
        ]
    )
