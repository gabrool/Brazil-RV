from __future__ import annotations

from datetime import date
from typing import Any

import polars as pl

from bralpha.derived.feature_utils import as_date, in_output_window, join_versions, max_date

SOURCE_FAMILY = "br_rv_cross_feature"

BR_RV_CROSS_FEATURE_COLUMNS = [
    "ref_date",
    "available_date",
    "source_family",
    "feature_id",
    "value_name",
    "value",
    "unit",
    "observation_ref_date",
    "observation_available_date",
    "is_available",
    "staleness_days",
    "source_version",
]


def build_br_rv_cross_feature_daily(
    *,
    b3_di_feature_daily: pl.DataFrame | None = None,
    b3_index_feature_daily: pl.DataFrame | None = None,
    bcb_sgs_feature_daily: pl.DataFrame | None = None,
    bcb_ptax_feature_daily: pl.DataFrame | None = None,
    fred_rate_feature_daily: pl.DataFrame | None = None,
    fred_market_feature_daily: pl.DataFrame | None = None,
    start: date | None = None,
    end: date | None = None,
) -> pl.DataFrame:
    inputs = _indexed_inputs(
        [
            b3_di_feature_daily,
            b3_index_feature_daily,
            bcb_sgs_feature_daily,
            bcb_ptax_feature_daily,
            fred_rate_feature_daily,
            fred_market_feature_daily,
        ]
    )
    ref_dates = sorted(
        ref_date
        for ref_date in {key[0] for key in inputs}
        if in_output_window(ref_date, start, end)
    )
    rows = []
    for ref_date in ref_dates:
        rows.extend(_policy_rows(ref_date, inputs))
        rows.extend(_rates_rows(ref_date, inputs))
        rows.extend(_fx_rows(ref_date, inputs))
        rows.extend(_equity_rows(ref_date, inputs))
    if not rows:
        return pl.DataFrame(schema={column: pl.Null for column in BR_RV_CROSS_FEATURE_COLUMNS})
    return (
        pl.DataFrame(rows)
        .select(BR_RV_CROSS_FEATURE_COLUMNS)
        .unique(subset=["ref_date", "source_family", "feature_id", "value_name"], keep="last")
        .sort(["ref_date", "feature_id", "value_name"])
    )


def _policy_rows(
    ref_date: date,
    inputs: dict[tuple[date, str, str, str], dict[str, Any]],
) -> list[dict[str, Any]]:
    selic = _get(
        inputs,
        ref_date,
        "bcb_sgs_feature",
        "bcb_sgs_feature:rates:selic_target_level_pa",
        "selic_target_level_pa",
    )
    fed_mid = _get(inputs, ref_date, "fred_rate_feature", "fred_rate:curve", "fed_target_mid_bp")
    dff = _get(inputs, ref_date, "fred_rate_feature", "fred_rate:dff", "level_bp")
    return [
        _row(
            ref_date,
            "br_rv_cross:policy",
            "brl_policy_carry_vs_fed_mid_bp",
            _sub(_scale(selic, 100.0), _value(fed_mid)),
            "bp",
            [selic, fed_mid],
        ),
        _row(
            ref_date,
            "br_rv_cross:policy",
            "brl_policy_carry_vs_fed_funds_bp",
            _sub(_scale(selic, 100.0), _value(dff)),
            "bp",
            [selic, dff],
        ),
    ]


def _rates_rows(
    ref_date: date,
    inputs: dict[tuple[date, str, str, str], dict[str, Any]],
) -> list[dict[str, Any]]:
    di_2y = _get(inputs, ref_date, "b3_di_curve_feature", "b3_di_curve:DI1:504bd", "rate_level_bp")
    di_5y = _get(inputs, ref_date, "b3_di_curve_feature", "b3_di_curve:DI1:1260bd", "rate_level_bp")
    ust_2y = _get(inputs, ref_date, "fred_rate_feature", "fred_rate:dgs2", "level_bp")
    ust_5y = _get(inputs, ref_date, "fred_rate_feature", "fred_rate:dgs5", "level_bp")
    ipca_12m = _get(
        inputs,
        ref_date,
        "bcb_sgs_feature",
        "bcb_sgs_feature:inflation:ipca_12m_sum_pct",
        "ipca_12m_sum_pct",
    )
    return [
        _row(
            ref_date,
            "br_rv_cross:rates",
            "brl_di_2y_minus_ust_2y_bp",
            _sub(_value(di_2y), _value(ust_2y)),
            "bp",
            [di_2y, ust_2y],
        ),
        _row(
            ref_date,
            "br_rv_cross:rates",
            "brl_di_5y_minus_ust_5y_bp",
            _sub(_value(di_5y), _value(ust_5y)),
            "bp",
            [di_5y, ust_5y],
        ),
        _row(
            ref_date,
            "br_rv_cross:rates",
            "brl_di_2y_real_minus_us_2y_proxy_bp",
            _sub(_sub(_value(di_2y), _scale(ipca_12m, 100.0)), _value(ust_2y)),
            "bp",
            [di_2y, ipca_12m, ust_2y],
        ),
    ]


def _fx_rows(
    ref_date: date,
    inputs: dict[tuple[date, str, str, str], dict[str, Any]],
) -> list[dict[str, Any]]:
    rows = []
    for horizon in [1, 5, 21]:
        brl = _get(
            inputs,
            ref_date,
            "bcb_ptax_feature",
            "bcb_ptax:USD",
            f"log_return_{horizon}bd",
        )
        dollar = _get(
            inputs,
            ref_date,
            "fred_market_feature",
            "fred_market:dtwexemegs",
            f"log_return_{horizon}bd",
        )
        value_name = (
            "brl_usd_minus_em_dollar_log_return_1bd"
            if horizon == 1
            else f"brl_fx_idiosyncratic_return_{horizon}bd"
        )
        rows.append(
            _row(
                ref_date,
                "br_rv_cross:fx",
                value_name,
                _sub(_value(brl), _value(dollar)),
                "log_return",
                [brl, dollar],
            )
        )
    return rows


def _equity_rows(
    ref_date: date,
    inputs: dict[tuple[date, str, str, str], dict[str, Any]],
) -> list[dict[str, Any]]:
    rows = []
    for horizon in [1, 21]:
        ibov = _get(
            inputs,
            ref_date,
            "b3_index_feature",
            "b3_index:IBOV",
            f"log_return_{horizon}bd",
        )
        sp500 = _get(
            inputs,
            ref_date,
            "fred_market_feature",
            "fred_market:sp500",
            f"log_return_{horizon}bd",
        )
        value_name = (
            "ibov_minus_sp500_log_return_1bd"
            if horizon == 1
            else "ibov_sp500_relative_return_21bd"
        )
        rows.append(
            _row(
                ref_date,
                "br_rv_cross:equity",
                value_name,
                _sub(_value(ibov), _value(sp500)),
                "log_return",
                [ibov, sp500],
            )
        )
    return rows


def _row(
    ref_date: date,
    feature_id: str,
    value_name: str,
    value: float | None,
    unit: str,
    inputs: list[dict[str, Any] | None],
) -> dict[str, Any]:
    present = [row for row in inputs if row is not None]
    observation_available_date = max_date(
        *(row.get("observation_available_date") for row in present)
    )
    available_date = max_date(
        ref_date,
        *(row.get("available_date") for row in present),
        *(row.get("observation_available_date") for row in present),
    )
    return {
        "ref_date": ref_date,
        "available_date": available_date or ref_date,
        "source_family": SOURCE_FAMILY,
        "feature_id": feature_id,
        "value_name": value_name,
        "value": value,
        "unit": unit,
        "observation_ref_date": max_date(*(row.get("observation_ref_date") for row in present)),
        "observation_available_date": observation_available_date,
        "is_available": value is not None,
        "staleness_days": _max_int(*(row.get("staleness_days") for row in present)),
        "source_version": join_versions(*(row.get("source_version") for row in present)),
    }


def _indexed_inputs(
    frames: list[pl.DataFrame | None],
) -> dict[tuple[date, str, str, str], dict[str, Any]]:
    indexed = {}
    for frame in frames:
        if frame is None or frame.is_empty():
            continue
        for row in frame.to_dicts():
            ref_date = as_date(row["ref_date"])
            key = (
                ref_date,
                str(row.get("source_family")),
                str(row.get("feature_id")),
                str(row.get("value_name")),
            )
            indexed[key] = {**row, "ref_date": ref_date}
    return indexed


def _get(
    inputs: dict[tuple[date, str, str, str], dict[str, Any]],
    ref_date: date,
    source_family: str,
    feature_id: str,
    value_name: str = "value",
) -> dict[str, Any] | None:
    return inputs.get((ref_date, source_family, feature_id, value_name))


def _value(row: dict[str, Any] | None) -> float | None:
    if row is None or row.get("value") is None:
        return None
    return float(row["value"])


def _scale(row: dict[str, Any] | None, multiplier: float) -> float | None:
    value = _value(row)
    if value is None:
        return None
    return value * multiplier


def _sub(left: float | None, right: float | None) -> float | None:
    if left is None or right is None:
        return None
    return left - right


def _max_int(*values: Any) -> int | None:
    ints = [int(value) for value in values if value is not None]
    if not ints:
        return None
    return max(ints)
