from __future__ import annotations

from collections import defaultdict
from datetime import date
from math import exp
from typing import Any

import polars as pl

from bralpha.derived.b3.quality import validate_panel
from bralpha.derived.b3.schemas import (
    DI_CURVE_CONTRACT_DAILY_COLUMNS,
    DI_CURVE_GRID_DAILY_COLUMNS,
    PANEL_PRIMARY_KEYS,
)
from bralpha.domain.di_futures import (
    annual_rate_from_discount_factor,
    annual_rate_from_pu,
    discount_factor_from_pu,
    log_discount_factor_from_pu,
)


def build_di_curve_contract_daily(
    futures_contract_daily: pl.DataFrame,
    *,
    source_roots: list[str],
    start: date | None = None,
    end: date | None = None,
) -> pl.DataFrame:
    roots = {root.upper() for root in source_roots}
    rows = []
    for row in futures_contract_daily.to_dicts():
        ref_date = _as_date(row["ref_date"])
        if start is not None and ref_date < start:
            continue
        if end is not None and ref_date > end:
            continue
        root = str(row.get("root") or row.get("commodity") or "").upper()
        if root not in roots:
            continue
        raw_settlement_pu = row.get("settlement")
        business_days = row.get("business_days_to_maturity")
        discount_factor = discount_factor_from_pu(raw_settlement_pu)
        log_discount_factor = log_discount_factor_from_pu(raw_settlement_pu)
        implied_rate = annual_rate_from_pu(raw_settlement_pu, business_days)
        rows.append(
            {
                "ref_date": ref_date,
                "available_date": _as_date(row["available_date"]),
                "contract_id": row.get("contract_id"),
                "maturity_code": row.get("maturity_code"),
                "maturity_date": _optional_date(row.get("maturity_date")),
                "days_to_maturity_calendar": row.get("days_to_maturity_calendar"),
                "business_days_to_maturity": business_days,
                "calendar_source": row.get("calendar_source"),
                "contract_rank_by_maturity": row.get("contract_rank_by_maturity"),
                "raw_settlement_pu": raw_settlement_pu,
                "discount_factor": discount_factor,
                "log_discount_factor": log_discount_factor,
                "implied_annual_rate": implied_rate,
                "implied_annual_rate_bp": _bp(implied_rate),
                "curve_value": implied_rate,
                "curve_value_kind": "implied_annual_rate",
                "curve_value_diff_1d": None,
                "curve_value_pct_change_1d": None,
                "implied_annual_rate_bp_change_1d": None,
                "log_discount_factor_change_1d": None,
                "volume": row.get("volume"),
                "open_interest": row.get("open_interest"),
                "is_observed": raw_settlement_pu is not None,
                "source_version": row.get("source_version") or "v0",
            }
        )
    rows = _add_contract_changes(rows)
    frame = _contract_frame(rows)
    validate_panel(
        frame,
        required_columns=DI_CURVE_CONTRACT_DAILY_COLUMNS,
        primary_keys=PANEL_PRIMARY_KEYS["di_curve_contract_daily"],
        nonnegative_columns=["volume", "open_interest"],
    )
    return frame


def build_di_curve_grid_daily(
    di_curve_contract_daily: pl.DataFrame,
    *,
    tenor_days: list[int] | None = None,
    tenor_business_days: list[int] | None = None,
    interpolation_method: str,
    start: date | None = None,
    end: date | None = None,
) -> pl.DataFrame:
    tenors = tenor_business_days if tenor_business_days is not None else tenor_days
    if tenors is None:
        raise ValueError("tenor_business_days or tenor_days must be supplied")
    grouped: dict[date, list[dict[str, Any]]] = defaultdict(list)
    for row in di_curve_contract_daily.to_dicts():
        ref_date = _as_date(row["ref_date"])
        if start is not None and ref_date < start:
            continue
        if end is not None and ref_date > end:
            continue
        grouped[ref_date].append(row)

    rows = []
    for ref_date, contract_rows in sorted(grouped.items()):
        observed = sorted(
            [
                row
                for row in contract_rows
                if row.get("log_discount_factor") is not None
                and row.get("business_days_to_maturity") is not None
                and row.get("business_days_to_maturity") > 0
            ],
            key=lambda row: row["business_days_to_maturity"],
        )
        fallback_available = max(_as_date(row["available_date"]) for row in contract_rows)
        for tenor in tenors:
            rows.append(
                _grid_row(
                    ref_date,
                    tenor,
                    observed,
                    fallback_available,
                    interpolation_method,
                )
            )
    frame = _grid_frame(rows)
    validate_panel(
        frame,
        required_columns=DI_CURVE_GRID_DAILY_COLUMNS,
        primary_keys=PANEL_PRIMARY_KEYS["di_curve_grid_daily"],
    )
    return frame


def _add_contract_changes(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_contract: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_contract[str(row["contract_id"])].append(row)
    for contract_rows in by_contract.values():
        contract_rows.sort(key=lambda item: item["ref_date"])
        previous = None
        for row in contract_rows:
            if previous is not None:
                row["curve_value_diff_1d"] = _diff(row["curve_value"], previous["curve_value"])
                row["curve_value_pct_change_1d"] = _pct(row["curve_value"], previous["curve_value"])
                row["implied_annual_rate_bp_change_1d"] = _diff(
                    row["implied_annual_rate_bp"],
                    previous["implied_annual_rate_bp"],
                )
                row["log_discount_factor_change_1d"] = _diff(
                    row["log_discount_factor"],
                    previous["log_discount_factor"],
                )
            previous = row
    return rows


def _grid_row(
    ref_date: date,
    tenor: int,
    observed: list[dict[str, Any]],
    fallback_available: date,
    interpolation_method: str,
) -> dict[str, Any]:
    base = {
        "ref_date": ref_date,
        "available_date": fallback_available,
        "curve_id": "DI1",
        "tenor_days": tenor,
        "tenor_business_days": tenor,
        "curve_value": None,
        "curve_value_kind": "implied_annual_rate",
        "discount_factor": None,
        "log_discount_factor": None,
        "implied_annual_rate": None,
        "implied_annual_rate_bp": None,
        "interpolation_method": interpolation_method,
        "left_contract_id": None,
        "right_contract_id": None,
        "left_days_to_maturity": None,
        "right_days_to_maturity": None,
        "is_interpolated": False,
        "is_extrapolated": False,
        "has_curve_value": False,
        "source_version": "v0",
    }
    if not observed:
        return base

    left = None
    right = None
    for row in observed:
        days = row["business_days_to_maturity"]
        if days <= tenor:
            left = row
        if days >= tenor and right is None:
            right = row
    if left is None or right is None:
        return base
    base["left_contract_id"] = left["contract_id"]
    base["right_contract_id"] = right["contract_id"]
    base["left_days_to_maturity"] = left["business_days_to_maturity"]
    base["right_days_to_maturity"] = right["business_days_to_maturity"]
    base["available_date"] = max(
        _as_date(left["available_date"]),
        _as_date(right["available_date"]),
    )
    base["source_version"] = _join_versions(left.get("source_version"), right.get("source_version"))
    if left["business_days_to_maturity"] == right["business_days_to_maturity"]:
        base["log_discount_factor"] = left["log_discount_factor"]
    else:
        width = right["business_days_to_maturity"] - left["business_days_to_maturity"]
        weight = (tenor - left["business_days_to_maturity"]) / width
        base["log_discount_factor"] = (
            left["log_discount_factor"]
            + (right["log_discount_factor"] - left["log_discount_factor"]) * weight
        )
        base["is_interpolated"] = True
    base["discount_factor"] = _exp(base["log_discount_factor"])
    base["implied_annual_rate"] = annual_rate_from_discount_factor(base["discount_factor"], tenor)
    base["implied_annual_rate_bp"] = _bp(base["implied_annual_rate"])
    base["curve_value"] = base["implied_annual_rate"]
    base["has_curve_value"] = base["curve_value"] is not None
    return base


def _join_versions(left: Any, right: Any) -> str:
    versions = sorted({str(value) for value in [left, right] if value})
    return "|".join(versions) if versions else "v0"


def _diff(value: float | None, previous: float | None) -> float | None:
    if value is None or previous is None:
        return None
    return value - previous


def _pct(value: float | None, previous: float | None) -> float | None:
    if value is None or previous in {None, 0}:
        return None
    return (value / previous) - 1


def _bp(value: float | None) -> float | None:
    if value is None:
        return None
    return value * 10_000.0


def _exp(value: float | None) -> float | None:
    if value is None:
        return None
    return exp(value)


def _contract_frame(rows: list[dict[str, Any]]) -> pl.DataFrame:
    if not rows:
        return pl.DataFrame(schema={column: pl.Null for column in DI_CURVE_CONTRACT_DAILY_COLUMNS})
    return pl.DataFrame(rows).select(DI_CURVE_CONTRACT_DAILY_COLUMNS)


def _grid_frame(rows: list[dict[str, Any]]) -> pl.DataFrame:
    if not rows:
        return pl.DataFrame(schema={column: pl.Null for column in DI_CURVE_GRID_DAILY_COLUMNS})
    return pl.DataFrame(rows).select(DI_CURVE_GRID_DAILY_COLUMNS)


def _as_date(value: Any) -> date:
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


def _optional_date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, date):
        return value
    text = str(value).strip()
    if not text:
        return None
    return date.fromisoformat(text[:10])
