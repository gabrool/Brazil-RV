from __future__ import annotations

from datetime import date, timedelta
from math import isclose, log

import polars as pl

from bralpha.derived.bcb.sgs_features import build_sgs_feature_daily
from bralpha.timing.vintages import assert_pit_model_ready_panel


def test_sgs_feature_panel_calculates_selic_features():
    ref_dates = _business_dates(date(2024, 1, 2), 6)
    rows = []
    target_values = [10.0, 10.25, 10.25, 10.25, 10.25, 10.5]
    over_values = [10.1, 10.2, 10.22, 10.3, 10.25, 10.55]
    for ref_date, target, over in zip(ref_dates, target_values, over_values, strict=True):
        rows.append(_asof_row(ref_date, "selic_target", "rates", target, "percent_annualized"))
        rows.append(_asof_row(ref_date, "selic_over", "rates", over, "percent_annualized"))

    panel = build_sgs_feature_daily(pl.DataFrame(rows))

    spread = _value(panel, ref_dates[2], "bcb_sgs_feature:rates:selic_over_minus_target_bp")
    step = _value(panel, ref_dates[1], "bcb_sgs_feature:rates:selic_policy_step_flag")
    change_5 = _value(panel, ref_dates[5], "bcb_sgs_feature:rates:selic_target_change_5bd_bp")

    assert isclose(spread, -3.0)
    assert step == 1.0
    assert isclose(change_5, 50.0)
    assert _value(panel, ref_dates[0], "bcb_sgs_feature:rates:selic_over_level_pa") == 10.1
    assert_pit_model_ready_panel(panel)


def test_sgs_feature_panel_calculates_ipca_from_available_observation_history():
    rows = []
    for month in range(1, 13):
        observation_ref_date = date(2023, month, 1)
        ref_date = date(2024, month, 15)
        rows.append(
            _asof_row(
                ref_date,
                "ipca",
                "inflation",
                float(month),
                "percent_monthly",
                observation_ref_date=observation_ref_date,
                observation_available_date=ref_date,
                staleness_days=0,
            )
        )
    rows.append(
        _asof_row(
            last_ref_date := date(2024, 12, 15),
            "selic_target",
            "rates",
            15.0,
            "percent_annualized",
        )
    )

    panel = build_sgs_feature_daily(pl.DataFrame(rows))

    assert _value(panel, last_ref_date, "bcb_sgs_feature:inflation:ipca_monthly_pct") == 12.0
    assert _value(panel, last_ref_date, "bcb_sgs_feature:inflation:ipca_3m_sum_pct") == 33.0
    assert _value(panel, last_ref_date, "bcb_sgs_feature:inflation:ipca_12m_sum_pct") == 78.0
    assert isclose(
        _value(panel, last_ref_date, "bcb_sgs_feature:inflation:ipca_3m_ann_pct"),
        ((1 + 33.0 / 100) ** 4 - 1) * 100,
    )
    assert isclose(
        _value(panel, last_ref_date, "bcb_sgs_feature:rates:real_policy_rate_12m_ipca_bp"),
        (15.0 - 78.0) * 100.0,
    )
    assert isclose(
        _value(panel, last_ref_date, "bcb_sgs_feature:rates:real_policy_rate_3m_ann_ipca_bp"),
        (15.0 - ((1 + 33.0 / 100) ** 4 - 1) * 100) * 100.0,
    )
    assert panel.filter(
        (pl.col("ref_date") == date(2024, 2, 15))
        & (pl.col("feature_id") == "bcb_sgs_feature:inflation:ipca_3m_sum_pct")
    ).is_empty()


def test_sgs_feature_panel_calculates_reserves_features():
    ref_dates = _business_dates(date(2024, 1, 2), 22)
    levels = [100.0 + index for index in range(21)] + [110.0]
    rows = [
        _asof_row(
            ref_date,
            "international_reserves_liquidity",
            "external_reserves",
            level,
            "usd_millions",
        )
        for ref_date, level in zip(ref_dates, levels, strict=True)
    ]

    panel = build_sgs_feature_daily(pl.DataFrame(rows))
    ref_20 = ref_dates[20]
    ref_21 = ref_dates[21]

    assert _value(panel, ref_20, "bcb_sgs_feature:external_reserves:reserves_usd_mn_level") == 120.0
    assert isclose(
        _value(panel, ref_20, "bcb_sgs_feature:external_reserves:reserves_log_change_5bd"),
        log(120.0) - log(115.0),
    )
    assert isclose(
        _value(panel, ref_21, "bcb_sgs_feature:external_reserves:reserves_log_change_21bd"),
        log(110.0) - log(100.0),
    )
    assert isclose(
        _value(panel, ref_20, "bcb_sgs_feature:external_reserves:reserves_pct_change_20bd"),
        20.0,
    )
    assert isclose(
        _value(
            panel,
            ref_21,
            "bcb_sgs_feature:external_reserves:reserves_drawdown_from_252bd_high_pct",
        ),
        (110.0 / 120.0 - 1) * 100,
    )
    assert isclose(
        _value(
            panel,
            ref_21,
            "bcb_sgs_feature:external_reserves:reserves_drawdown_from_504bd_high_pct",
        ),
        (110.0 / 120.0 - 1) * 100,
    )
    assert_pit_model_ready_panel(panel)


def _asof_row(
    ref_date: date,
    slug: str,
    category: str,
    value: float,
    unit: str,
    *,
    observation_ref_date: date | None = None,
    observation_available_date: date | None = None,
    staleness_days: int = 0,
) -> dict[str, object]:
    observation_ref_date = observation_ref_date or ref_date
    observation_available_date = observation_available_date or ref_date
    return {
        "ref_date": ref_date,
        "available_date": ref_date,
        "series_id": hash(slug) % 100000,
        "series_slug": slug,
        "series_name": slug,
        "category": category,
        "frequency": "daily",
        "observation_ref_date": observation_ref_date,
        "observation_available_date": observation_available_date,
        "value": value,
        "unit": unit,
        "is_available": True,
        "is_observed_on_ref_date": observation_ref_date == ref_date,
        "staleness_days": staleness_days,
        "availability_policy": "date_only_next_business_day",
        "availability_basis": "source_date_only",
        "revision_policy": "unrevised",
        "model_usable": True,
        "source_version": "v0",
    }


def _business_dates(start: date, count: int) -> list[date]:
    dates = []
    current = start
    while len(dates) < count:
        if current.weekday() < 5:
            dates.append(current)
        current += timedelta(days=1)
    return dates


def _value(panel: pl.DataFrame, ref_date: date, feature_id: str) -> float:
    return panel.filter(
        (pl.col("ref_date") == ref_date) & (pl.col("feature_id") == feature_id)
    )["value"].item()
