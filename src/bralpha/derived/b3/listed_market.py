from __future__ import annotations

from datetime import date
from typing import Any

import polars as pl

from bralpha.derived.b3.quality import assert_no_banned_feature_columns, validate_panel
from bralpha.derived.b3.schemas import (
    INDEX_COMPOSITION_DAILY_COLUMNS,
    INDEX_DAILY_COLUMNS,
    LISTED_MARKET_DAILY_COLUMNS,
    PANEL_PRIMARY_KEYS,
)


def build_listed_market_daily(
    *,
    cotahist_yearly: pl.DataFrame | None = None,
    cotahist_daily: pl.DataFrame | None = None,
    traded_securities: pl.DataFrame | None = None,
    isin_database: pl.DataFrame | None = None,
    start: date | None = None,
    end: date | None = None,
) -> pl.DataFrame:
    reference = _security_reference(traded_securities, isin_database)
    rows_by_key: dict[tuple[date, str, str], dict[str, Any]] = {}
    for precedence, frame in [(0, cotahist_yearly), (1, cotahist_daily)]:
        if frame is None or frame.is_empty():
            continue
        for row in frame.to_dicts():
            ref_date = _as_date(row["ref_date"])
            if start is not None and ref_date < start:
                continue
            if end is not None and ref_date > end:
                continue
            symbol = _text(row.get("symbol"))
            market_type = _text(row.get("market_type"))
            if not symbol or not market_type:
                continue
            key = (ref_date, symbol, market_type)
            current = rows_by_key.get(key)
            if current is not None and current["_precedence"] > precedence:
                continue
            ref = reference.get((symbol, market_type), {})
            cotahist_available_date = _as_date(row["available_date"])
            isin, used_isin_reference = _reference_value(_text(row.get("isin")), ref, "isin")
            name, used_name_reference = _reference_value(row.get("name"), ref, "name")
            asset_class, used_asset_class_reference = _reference_value(
                _lower_text(row.get("asset_class")),
                ref,
                "asset_class",
            )
            used_reference = (
                used_isin_reference or used_name_reference or used_asset_class_reference
            )
            available_date = cotahist_available_date
            source_version = row.get("source_version") or "v0"
            if used_reference:
                reference_available_date = _optional_date(ref.get("available_date"))
                if reference_available_date is not None:
                    available_date = max(cotahist_available_date, reference_available_date)
                source_version = _join_versions(source_version, ref.get("source_version"))
            rows_by_key[key] = {
                "ref_date": ref_date,
                "available_date": available_date,
                "symbol": symbol,
                "isin": isin,
                "market_type": market_type,
                "asset_class": asset_class,
                "name": name,
                "open": row.get("open"),
                "high": row.get("high"),
                "low": row.get("low"),
                "close": row.get("close"),
                "average": row.get("average"),
                "best_bid": row.get("best_bid"),
                "best_ask": row.get("best_ask"),
                "volume": row.get("volume"),
                "financial_volume": row.get("financial_volume"),
                "number_of_trades": row.get("number_of_trades"),
                "source_dataset": _source_dataset(row.get("source_dataset"))
                or "b3_cotahist_yearly",
                "source_version": source_version,
                "_precedence": precedence,
            }
    rows = [
        {key: value for key, value in row.items() if key != "_precedence"}
        for row in rows_by_key.values()
    ]
    frame = _frame(rows, LISTED_MARKET_DAILY_COLUMNS)
    assert_no_banned_feature_columns(frame)
    validate_panel(
        frame,
        required_columns=LISTED_MARKET_DAILY_COLUMNS,
        primary_keys=PANEL_PRIMARY_KEYS["listed_market_daily"],
        nonnegative_columns=["open", "high", "low", "close", "volume"],
    )
    return frame


def build_index_daily(
    indexes_historical_data: pl.DataFrame,
    *,
    start: date | None = None,
    end: date | None = None,
) -> pl.DataFrame:
    rows = []
    for row in indexes_historical_data.to_dicts():
        ref_date = _as_date(row["ref_date"])
        if start is not None and ref_date < start:
            continue
        if end is not None and ref_date > end:
            continue
        index_id = _text(row.get("index_id") or row.get("symbol"))
        if not index_id:
            continue
        rows.append(
            {
                "ref_date": ref_date,
                "available_date": _as_date(row["available_date"]),
                "index_id": index_id,
                "close": row.get("close"),
                "open": row.get("open"),
                "high": row.get("high"),
                "low": row.get("low"),
                "volume": row.get("volume"),
                "financial_volume": row.get("financial_volume"),
                "number_of_trades": row.get("number_of_trades"),
                "currency": _text(row.get("currency")) or "BRL",
                "unit": row.get("unit") or "points",
                "source_version": row.get("source_version") or "v0",
            }
        )
    frame = _frame(rows, INDEX_DAILY_COLUMNS)
    assert_no_banned_feature_columns(frame)
    validate_panel(
        frame,
        required_columns=INDEX_DAILY_COLUMNS,
        primary_keys=PANEL_PRIMARY_KEYS["index_daily"],
        nonnegative_columns=["close", "volume"],
    )
    return frame


def build_index_composition_daily(
    *,
    indexes_composition: pl.DataFrame | None = None,
    indexes_current_portfolio: pl.DataFrame | None = None,
    indexes_theoretical_portfolio: pl.DataFrame | None = None,
    start: date | None = None,
    end: date | None = None,
) -> pl.DataFrame:
    rows_by_key = {}
    for default_source, frame in [
        ("b3_indexes_composition", indexes_composition),
        ("b3_indexes_current_portfolio", indexes_current_portfolio),
        ("b3_indexes_theoretical_portfolio", indexes_theoretical_portfolio),
    ]:
        if frame is None or frame.is_empty():
            continue
        for row in frame.to_dicts():
            ref_date = _as_date(row["ref_date"])
            if start is not None and ref_date < start:
                continue
            if end is not None and ref_date > end:
                continue
            index_id = _text(row.get("index_id"))
            symbol = _text(row.get("symbol"))
            source_dataset = _source_dataset(row.get("source_dataset")) or default_source
            if not index_id or not symbol:
                continue
            rows_by_key[(ref_date, index_id, symbol, source_dataset)] = {
                "ref_date": ref_date,
                "available_date": _as_date(row["available_date"]),
                "index_id": index_id,
                "symbol": symbol,
                "isin": _text(row.get("isin")),
                "security_id": _text(row.get("security_id")),
                "name": row.get("name"),
                "weight": row.get("weight"),
                "theoretical_quantity": row.get("theoretical_quantity"),
                "source_dataset": source_dataset,
                "source_version": row.get("source_version") or "v0",
            }
    frame = _frame(list(rows_by_key.values()), INDEX_COMPOSITION_DAILY_COLUMNS)
    assert_no_banned_feature_columns(frame)
    validate_panel(
        frame,
        required_columns=INDEX_COMPOSITION_DAILY_COLUMNS,
        primary_keys=PANEL_PRIMARY_KEYS["index_composition_daily"],
        nonnegative_columns=["weight"],
    )
    return frame


def _security_reference(
    traded_securities: pl.DataFrame | None,
    isin_database: pl.DataFrame | None,
) -> dict[tuple[str, str], dict[str, Any]]:
    reference: dict[tuple[str, str], dict[str, Any]] = {}
    for frame in [traded_securities, isin_database]:
        if frame is None or frame.is_empty():
            continue
        for row in frame.to_dicts():
            symbol = _text(row.get("symbol"))
            market_type = _text(row.get("market_type"))
            if not symbol or not market_type:
                continue
            reference[(symbol, market_type)] = {
                "isin": _text(row.get("isin")),
                "name": row.get("name"),
                "asset_class": _lower_text(row.get("asset_class")),
                "available_date": _optional_date(row.get("available_date")),
                "source_version": row.get("source_version") or "v0",
            }
    return reference


def _frame(rows: list[dict[str, Any]], columns: list[str]) -> pl.DataFrame:
    if not rows:
        return pl.DataFrame(schema={column: pl.Null for column in columns})
    return pl.DataFrame(rows).select(columns)


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


def _reference_value(
    value: Any,
    reference: dict[str, Any],
    field: str,
) -> tuple[Any, bool]:
    if value is not None and str(value).strip():
        return value, False
    reference_value = reference.get(field)
    if reference_value is not None and str(reference_value).strip():
        return reference_value, True
    return value, False


def _join_versions(*versions: Any) -> str:
    unique = sorted({str(version) for version in versions if version})
    return "|".join(unique) if unique else "v0"


def _text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip().upper()
    return text or None


def _lower_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip().lower()
    return text or None


def _source_dataset(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None
