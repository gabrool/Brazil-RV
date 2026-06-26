from __future__ import annotations

from collections import defaultdict
from datetime import date
from typing import Any

import polars as pl

from bralpha.derived.b3.quality import assert_no_banned_feature_columns, validate_panel
from bralpha.derived.b3.schemas import CONTINUOUS_FUTURES_DAILY_COLUMNS, PANEL_PRIMARY_KEYS


def build_continuous_futures_daily(
    contract_panel: pl.DataFrame,
    *,
    roots: list[str],
    max_front_rank: int,
    min_days_to_maturity: int,
    prefer_liquidity_when_available: bool,
    roll_policy: str,
    start: date | None = None,
    end: date | None = None,
) -> pl.DataFrame:
    allowed_roots = {root.upper() for root in roots}
    grouped: dict[tuple[date, str], list[dict[str, Any]]] = defaultdict(list)
    for row in contract_panel.to_dicts():
        ref_date = _as_date(row["ref_date"])
        if start is not None and ref_date < start:
            continue
        if end is not None and ref_date > end:
            continue
        root = str(row.get("root") or row.get("commodity") or "").upper()
        if root not in allowed_roots:
            continue
        if not row.get("is_tradeable"):
            continue
        days = row.get("days_to_maturity_calendar")
        if days is not None and days < min_days_to_maturity:
            continue
        if prefer_liquidity_when_available and not _has_same_date_liquidity(row):
            continue
        grouped[(ref_date, root)].append(row)

    rows = []
    for (ref_date, root), contracts in sorted(grouped.items()):
        ranked = sorted(contracts, key=_selection_key)
        for ordinal, contract in enumerate(ranked[:max_front_rank], start=1):
            rows.append(_continuous_row(ref_date, root, ordinal, contract, roll_policy))

    rows = _add_one_day_changes(rows)
    frame = _frame(rows)
    assert_no_banned_feature_columns(frame)
    validate_panel(
        frame,
        required_columns=CONTINUOUS_FUTURES_DAILY_COLUMNS,
        primary_keys=PANEL_PRIMARY_KEYS["continuous_futures_daily"],
        nonnegative_columns=["volume", "open_interest"],
    )
    return frame


def _selection_key(row: dict[str, Any]) -> tuple[int, int, str]:
    contract_rank = row.get("contract_rank_by_maturity") or 9999
    days = row.get("days_to_maturity_calendar") or 9999
    return (contract_rank, days, str(row.get("contract_id") or ""))


def _has_same_date_liquidity(row: dict[str, Any]) -> bool:
    volume = row.get("volume")
    open_interest = row.get("open_interest")
    if volume is None and open_interest is None:
        return True
    return (volume is not None and volume > 0) or (open_interest is not None and open_interest > 0)


def _continuous_row(
    ref_date: date,
    root: str,
    rank: int,
    contract: dict[str, Any],
    roll_policy: str,
) -> dict[str, Any]:
    return {
        "ref_date": ref_date,
        "available_date": _as_date(contract["available_date"]),
        "continuous_id": f"{root}_R{rank}",
        "root": root,
        "rank": rank,
        "selected_contract_id": contract.get("contract_id"),
        "selected_maturity_code": contract.get("maturity_code"),
        "selected_maturity_date": _optional_date(contract.get("maturity_date")),
        "days_to_maturity_calendar": contract.get("days_to_maturity_calendar"),
        "business_days_to_maturity": contract.get("business_days_to_maturity"),
        "roll_policy": roll_policy,
        "is_roll_date": False,
        "previous_contract_id": None,
        "settlement": contract.get("settlement"),
        "quote_diff_1d": None,
        "quote_pct_change_1d": None,
        "same_contract_quote_diff_1d": None,
        "same_contract_quote_pct_change_1d": None,
        "volume": contract.get("volume"),
        "open_interest": contract.get("open_interest"),
        "is_tradeable": contract.get("is_tradeable"),
        "source_version": contract.get("source_version") or "v0",
    }


def _add_one_day_changes(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_id: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_id[row["continuous_id"]].append(row)
    for series_rows in by_id.values():
        series_rows.sort(key=lambda item: item["ref_date"])
        previous = None
        for row in series_rows:
            if previous is not None:
                row["previous_contract_id"] = previous["selected_contract_id"]
                row["is_roll_date"] = (
                    row["selected_contract_id"] != previous["selected_contract_id"]
                )
                row["quote_diff_1d"] = _diff(row["settlement"], previous["settlement"])
                row["quote_pct_change_1d"] = _pct(row["settlement"], previous["settlement"])
                if not row["is_roll_date"]:
                    row["same_contract_quote_diff_1d"] = row["quote_diff_1d"]
                    row["same_contract_quote_pct_change_1d"] = row["quote_pct_change_1d"]
            previous = row
    return rows


def _diff(value: float | None, previous: float | None) -> float | None:
    if value is None or previous is None:
        return None
    return value - previous


def _pct(value: float | None, previous: float | None) -> float | None:
    if value is None or previous in {None, 0}:
        return None
    return (value / previous) - 1


def _frame(rows: list[dict[str, Any]]) -> pl.DataFrame:
    if not rows:
        return pl.DataFrame(schema={column: pl.Null for column in CONTINUOUS_FUTURES_DAILY_COLUMNS})
    return pl.DataFrame(rows).select(CONTINUOUS_FUTURES_DAILY_COLUMNS)


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
