from __future__ import annotations

from collections import defaultdict
from datetime import date
from typing import Any

import polars as pl

from bralpha.derived.b3.quality import assert_no_banned_feature_columns, validate_panel
from bralpha.derived.b3.schemas import (
    DI_CURVE_CONTRACT_DAILY_COLUMNS,
    DI_CURVE_GRID_DAILY_COLUMNS,
    PANEL_PRIMARY_KEYS,
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
        rows.append(
            {
                "ref_date": ref_date,
                "available_date": _as_date(row["available_date"]),
                "contract_id": row.get("contract_id"),
                "maturity_code": row.get("maturity_code"),
                "maturity_date": _optional_date(row.get("maturity_date")),
                "days_to_maturity_calendar": row.get("days_to_maturity_calendar"),
                "business_days_to_maturity": row.get("business_days_to_maturity"),
                "contract_rank_by_maturity": row.get("contract_rank_by_maturity"),
                "curve_value": row.get("settlement"),
                "curve_value_diff_1d": None,
                "curve_value_pct_change_1d": None,
                "volume": row.get("volume"),
                "open_interest": row.get("open_interest"),
                "is_observed": row.get("settlement") is not None,
                "source_version": row.get("source_version") or "v0",
            }
        )
    rows = _add_contract_changes(rows)
    frame = _contract_frame(rows)
    assert_no_banned_feature_columns(frame)
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
    tenor_days: list[int],
    interpolation_method: str,
    start: date | None = None,
    end: date | None = None,
) -> pl.DataFrame:
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
                if row.get("curve_value") is not None
                and row.get("days_to_maturity_calendar") is not None
            ],
            key=lambda row: row["days_to_maturity_calendar"],
        )
        fallback_available = max(_as_date(row["available_date"]) for row in contract_rows)
        for tenor in tenor_days:
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
    assert_no_banned_feature_columns(frame)
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
        "curve_value": None,
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
        days = row["days_to_maturity_calendar"]
        if days <= tenor:
            left = row
        if days >= tenor and right is None:
            right = row
    if left is None or right is None:
        return base
    base["left_contract_id"] = left["contract_id"]
    base["right_contract_id"] = right["contract_id"]
    base["left_days_to_maturity"] = left["days_to_maturity_calendar"]
    base["right_days_to_maturity"] = right["days_to_maturity_calendar"]
    base["available_date"] = max(
        _as_date(left["available_date"]),
        _as_date(right["available_date"]),
    )
    base["source_version"] = _join_versions(left.get("source_version"), right.get("source_version"))
    if left["days_to_maturity_calendar"] == right["days_to_maturity_calendar"]:
        base["curve_value"] = left["curve_value"]
    else:
        width = right["days_to_maturity_calendar"] - left["days_to_maturity_calendar"]
        weight = (tenor - left["days_to_maturity_calendar"]) / width
        base["curve_value"] = (
            left["curve_value"] + (right["curve_value"] - left["curve_value"]) * weight
        )
        base["is_interpolated"] = True
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
