from __future__ import annotations

from collections import defaultdict
from datetime import date
from typing import Any

import polars as pl

from bralpha.derived.b3.quality import validate_panel
from bralpha.derived.b3.schemas import FUTURES_CONTRACT_DAILY_COLUMNS, PANEL_PRIMARY_KEYS
from bralpha.domain.b3_calendar import business_days_between
from bralpha.domain.b3_contracts import build_b3_contract_id
from bralpha.domain.b3_month_codes import parse_b3_maturity_code
from bralpha.domain.instruments import asset_class_for_root


def build_futures_contract_daily(
    *,
    settlements: pl.DataFrame,
    open_interest: pl.DataFrame | None = None,
    trade_summary: pl.DataFrame | None = None,
    contract_master: pl.DataFrame | None = None,
    holiday_calendar: pl.DataFrame | None = None,
    holidays: set[date] | None = None,
    start: date | None = None,
    end: date | None = None,
) -> pl.DataFrame:
    holiday_dates, calendar_source = _calendar_inputs(
        holiday_calendar=holiday_calendar,
        holidays=holidays,
    )
    buckets: dict[tuple[date, str, str, str], dict[str, Any]] = {}
    seen_source_keys: set[tuple[str, date, str, str, str]] = set()

    for source_name, frame in [
        ("b3_futures_settlements", settlements),
        ("b3_derivatives_open_interest", open_interest),
        ("b3_derivatives_trade_summary", trade_summary),
    ]:
        if frame is None or frame.is_empty():
            continue
        for row in _filter_rows(frame, start=start, end=end):
            prepared = _prepared_contract_row(row, default_source=source_name)
            key = _merge_key(prepared)
            source_key = (prepared["source_dataset"], *key)
            if source_key in seen_source_keys:
                raise ValueError(
                    f"duplicate source merge rows for {prepared['source_dataset']}: {key}"
                )
            seen_source_keys.add(source_key)
            bucket = buckets.setdefault(key, _base_bucket(prepared))
            _merge_bucket(bucket, prepared, source_name)

    master_by_contract = _contract_master_by_id(contract_master)
    for key, bucket in buckets.items():
        master = master_by_contract.get(key[3], {})
        _merge_master(bucket, master)
        _finalize_bucket(bucket, holidays=holiday_dates, calendar_source=calendar_source)

    rows = list(buckets.values())
    _assign_contract_ranks(rows)
    frame = _frame(rows)
    validate_panel(
        frame,
        required_columns=FUTURES_CONTRACT_DAILY_COLUMNS,
        primary_keys=PANEL_PRIMARY_KEYS["futures_contract_daily"],
        nonnegative_columns=["volume", "open_interest"],
    )
    return frame


def _filter_rows(
    frame: pl.DataFrame,
    *,
    start: date | None,
    end: date | None,
) -> list[dict[str, Any]]:
    rows = frame.to_dicts()
    if start is None and end is None:
        return rows
    filtered = []
    for row in rows:
        ref_date = _as_date(row.get("ref_date"))
        if start is not None and ref_date < start:
            continue
        if end is not None and ref_date > end:
            continue
        filtered.append(row)
    return filtered


def _prepared_contract_row(row: dict[str, Any], *, default_source: str) -> dict[str, Any]:
    ref_date = _as_date(row.get("ref_date"))
    commodity = _text(row.get("commodity") or row.get("root"))
    maturity_code = _text(row.get("maturity_code"))
    contract_id = _text(row.get("contract_id"))
    if not contract_id and commodity and maturity_code:
        contract_id = build_b3_contract_id(commodity, maturity_code)
    if not commodity and contract_id and "_" in contract_id:
        commodity = contract_id.split("_", 1)[0]
    if not maturity_code and contract_id and "_" in contract_id:
        maturity_code = contract_id.split("_", 1)[1]
    if not commodity or not maturity_code or not contract_id:
        raise ValueError(
            "futures source row must identify commodity, maturity_code, and contract_id"
        )
    return {
        **row,
        "ref_date": ref_date,
        "available_date": _optional_date(row.get("available_date")) or ref_date,
        "commodity": commodity,
        "maturity_code": maturity_code,
        "contract_id": contract_id,
        "source_dataset": _source_dataset(row.get("source_dataset")) or default_source,
    }


def _merge_key(row: dict[str, Any]) -> tuple[date, str, str, str]:
    return (row["ref_date"], row["commodity"], row["maturity_code"], row["contract_id"])


def _base_bucket(row: dict[str, Any]) -> dict[str, Any]:
    commodity = row["commodity"]
    maturity_code = row["maturity_code"]
    return {
        "ref_date": row["ref_date"],
        "available_date": row["available_date"],
        "root": commodity,
        "commodity": commodity,
        "maturity_code": maturity_code,
        "contract_id": row["contract_id"],
        "symbol": _text(row.get("symbol")) or f"{commodity}{maturity_code}",
        "asset_class": _text(row.get("asset_class")) or asset_class_for_root(commodity),
        "maturity_date": None,
        "days_to_maturity_calendar": None,
        "business_days_to_maturity": None,
        "calendar_source": None,
        "contract_rank_by_maturity": None,
        "settlement": None,
        "previous_settlement": None,
        "price_change": None,
        "settlement_value": None,
        "volume": None,
        "financial_volume": None,
        "number_of_trades": None,
        "open_interest": None,
        "currency": _text(row.get("currency")) or "BRL",
        "unit": row.get("unit"),
        "quote_convention": row.get("quote_convention"),
        "is_tradeable": False,
        "has_settlement": False,
        "has_volume": False,
        "has_open_interest": False,
        "_source_datasets": set(),
        "_source_versions": set(),
    }


def _merge_bucket(bucket: dict[str, Any], row: dict[str, Any], source_name: str) -> None:
    bucket["available_date"] = max(bucket["available_date"], row["available_date"])
    bucket["_source_datasets"].add(row["source_dataset"])
    if row.get("source_version"):
        bucket["_source_versions"].add(str(row["source_version"]))

    for column in ["symbol", "asset_class", "currency", "unit", "quote_convention"]:
        if bucket.get(column) is None and row.get(column) is not None:
            bucket[column] = row.get(column)

    if source_name == "b3_futures_settlements":
        for column in [
            "settlement",
            "previous_settlement",
            "price_change",
            "settlement_value",
            "volume",
            "financial_volume",
            "number_of_trades",
            "open_interest",
        ]:
            _coalesce(bucket, row, column)
    elif source_name == "b3_derivatives_open_interest":
        _coalesce(bucket, row, "open_interest")
    elif source_name == "b3_derivatives_trade_summary":
        for column in ["volume", "financial_volume", "number_of_trades"]:
            _coalesce(bucket, row, column)


def _merge_master(bucket: dict[str, Any], master: dict[str, Any]) -> None:
    if not master:
        return
    contributed = False
    maturity_date = _optional_date(master.get("maturity_date"))
    if maturity_date is not None and bucket.get("maturity_date") != maturity_date:
        bucket["maturity_date"] = maturity_date
        contributed = True
    for column in ["asset_class", "currency", "unit", "quote_convention"]:
        if bucket.get(column) is None and master.get(column) is not None:
            bucket[column] = master[column]
            contributed = True
    if not contributed:
        return

    # Manual contract-master rows without available_date are treated as already available
    # at the market-row availability for v0; future dated masters should supply available_date.
    master_available_date = _optional_date(master.get("available_date")) or bucket["available_date"]
    bucket["available_date"] = max(bucket["available_date"], master_available_date)
    bucket["_source_datasets"].add("b3_futures_contract_master")
    bucket["_source_versions"].add(str(master.get("source_version") or "v0"))


def _finalize_bucket(
    bucket: dict[str, Any],
    *,
    holidays: set[date] | None,
    calendar_source: str,
) -> None:
    maturity_date = _optional_date(bucket.get("maturity_date"))
    bucket["maturity_date"] = maturity_date
    bucket["calendar_source"] = calendar_source
    if maturity_date is not None:
        bucket["days_to_maturity_calendar"] = (maturity_date - bucket["ref_date"]).days
        bucket["business_days_to_maturity"] = business_days_between(
            bucket["ref_date"],
            maturity_date,
            holidays=holidays,
        )
    bucket["has_settlement"] = bucket.get("settlement") is not None
    bucket["has_volume"] = bucket.get("volume") is not None
    bucket["has_open_interest"] = bucket.get("open_interest") is not None
    bucket["is_tradeable"] = _is_tradeable(bucket)
    bucket["source_datasets"] = "|".join(sorted(bucket["_source_datasets"]))
    versions = sorted(bucket["_source_versions"])
    bucket["source_version"] = "|".join(versions) if versions else "v0"
    del bucket["_source_datasets"]
    del bucket["_source_versions"]


def _assign_contract_ranks(rows: list[dict[str, Any]]) -> None:
    groups: dict[tuple[date, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        groups[(row["ref_date"], row["root"])].append(row)
    for group_rows in groups.values():
        sorted_rows = sorted(group_rows, key=_maturity_sort_key)
        for rank, row in enumerate(sorted_rows, start=1):
            row["contract_rank_by_maturity"] = rank


def _maturity_sort_key(row: dict[str, Any]) -> tuple[int, int, str]:
    maturity_date = _optional_date(row.get("maturity_date"))
    if maturity_date is not None:
        return (maturity_date.year, maturity_date.timetuple().tm_yday, row["contract_id"])
    try:
        maturity = parse_b3_maturity_code(row["maturity_code"])
        return (maturity.year, maturity.month, row["contract_id"])
    except ValueError:
        return (9999, 12, row["contract_id"])


def _contract_master_by_id(frame: pl.DataFrame | None) -> dict[str, dict[str, Any]]:
    if frame is None or frame.is_empty():
        return {}
    rows = {}
    for row in frame.to_dicts():
        contract_id = _text(row.get("contract_id"))
        if contract_id:
            rows[contract_id] = row
    return rows


def _calendar_inputs(
    *,
    holiday_calendar: pl.DataFrame | None,
    holidays: set[date] | None,
) -> tuple[set[date] | None, str]:
    if holidays is not None:
        return holidays, "configured_holidays"
    if holiday_calendar is None or holiday_calendar.is_empty():
        return None, "canonical_b3_calendar"
    rows = holiday_calendar.to_dicts()
    return {
        _as_date(row["ref_date"])
        for row in rows
        if not bool(row.get("is_business_day", False))
    }, "b3_holiday_calendar"


def _is_tradeable(row: dict[str, Any]) -> bool:
    days = row.get("days_to_maturity_calendar")
    if days is not None and days < 0:
        return False
    if row.get("settlement") is None:
        return False
    volume = row.get("volume")
    open_interest = row.get("open_interest")
    return (volume is None or volume > 0) or (open_interest is None or open_interest > 0)


def _coalesce(bucket: dict[str, Any], row: dict[str, Any], column: str) -> None:
    if row.get(column) is not None:
        bucket[column] = row[column]


def _frame(rows: list[dict[str, Any]]) -> pl.DataFrame:
    if not rows:
        return pl.DataFrame(schema={column: pl.Null for column in FUTURES_CONTRACT_DAILY_COLUMNS})
    return pl.DataFrame(rows).select(FUTURES_CONTRACT_DAILY_COLUMNS)


def _as_date(value: Any) -> date:
    if isinstance(value, date):
        return value
    if value is None:
        raise ValueError("date value is required")
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


def _text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip().upper()
    return text or None


def _source_dataset(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None
