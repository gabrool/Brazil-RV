from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl
import yaml

from bralpha.domain.b3_calendar import next_business_day
from bralpha.parsing.common import normalize_column_name, parse_decimal, write_source_partitioned

REFERENCE_CONTRACT_COLUMNS = [
    "contract_id",
    "symbol_root",
    "commodity",
    "asset_class",
    "maturity_code",
    "maturity_date",
    "first_trade_date",
    "last_trade_date",
    "expiry_date",
    "contract_multiplier",
    "tick_size",
    "currency",
    "quote_convention",
    "settlement_method",
    "source",
    "source_version",
]

REFERENCE_CALENDAR_COLUMNS = [
    "calendar_id",
    "ref_date",
    "available_date",
    "is_business_day",
    "holiday_name",
    "source",
    "source_version",
]

INDEX_COMPOSITION_COLUMNS = [
    "ref_date",
    "available_date",
    "index_id",
    "symbol",
    "security_id",
    "isin",
    "name",
    "weight",
    "theoretical_quantity",
    "source",
    "source_dataset",
    "download_timestamp_utc",
    "raw_path",
    "sha256",
    "source_version",
]

REFERENCE_SECURITY_COLUMNS = [
    "security_id",
    "symbol",
    "isin",
    "name",
    "market_type",
    "asset_class",
    "issuer",
    "currency",
    "source",
    "source_dataset",
    "download_timestamp_utc",
    "raw_path",
    "sha256",
    "source_version",
]

ISIN_DATABASE_COLUMNS = [
    "ref_date",
    "available_date",
    "security_id",
    "symbol",
    "isin",
    "name",
    "market_type",
    "asset_class",
    "issuer",
    "currency",
    "source",
    "source_dataset",
    "download_timestamp_utc",
    "raw_path",
    "sha256",
    "source_version",
]

TRADING_PARAMETERS_COLUMNS = [
    "ref_date",
    "available_date",
    "symbol",
    "market_segment",
    "asset_class",
    "round_lot",
    "tick_size",
    "price_limits",
    "trading_status",
    "security_id",
    "isin",
    "market_type",
    "price_limit_lower",
    "price_limit_upper",
    "source",
    "source_dataset",
    "download_timestamp_utc",
    "raw_path",
    "sha256",
    "source_version",
]


def normalize_contract_master(
    rows: list[dict[str, object]],
    *,
    source_version: str = "v0",
) -> pl.DataFrame:
    normalized = []
    for row in rows:
        normalized.append(
            {
                "contract_id": _text(row.get("contract_id")),
                "symbol_root": _text(row.get("symbol_root")),
                "commodity": _text(row.get("commodity")),
                "asset_class": _text(row.get("asset_class")),
                "maturity_code": _text(row.get("maturity_code")),
                "maturity_date": _optional_date(row.get("maturity_date")),
                "first_trade_date": _optional_date(row.get("first_trade_date")),
                "last_trade_date": _optional_date(row.get("last_trade_date")),
                "expiry_date": _optional_date(row.get("expiry_date")),
                "contract_multiplier": parse_decimal(row.get("contract_multiplier")),
                "tick_size": parse_decimal(row.get("tick_size")),
                "currency": _text(row.get("currency")) or "BRL",
                "quote_convention": row.get("quote_convention"),
                "settlement_method": row.get("settlement_method"),
                "source": row.get("source", "b3"),
                "source_version": source_version,
            }
        )
    return _frame(normalized, REFERENCE_CONTRACT_COLUMNS)


def normalize_holiday_calendar(
    rows: list[dict[str, object]],
    *,
    source_version: str = "v0",
) -> pl.DataFrame:
    normalized = []
    for row in rows:
        ref_date = _required_date(row.get("ref_date"))
        normalized.append(
            {
                "calendar_id": _text(row.get("calendar_id")) or "B3",
                "ref_date": ref_date,
                "available_date": _optional_date(row.get("available_date")) or ref_date,
                "is_business_day": bool(row.get("is_business_day", False)),
                "holiday_name": row.get("holiday_name"),
                "source": row.get("source", "b3"),
                "source_version": source_version,
            }
        )
    return _frame(normalized, REFERENCE_CALENDAR_COLUMNS)


def normalize_index_composition(
    bronze: pl.DataFrame,
    *,
    holidays: set[date] | None = None,
    source_version: str = "v0",
) -> pl.DataFrame:
    rows = []
    for row in bronze.to_dicts():
        ref_date = _required_date(row.get("ref_date"))
        rows.append(
            {
                "ref_date": ref_date,
                "available_date": _optional_date(row.get("available_date"))
                or next_business_day(ref_date, holidays),
                "index_id": _text(_value(row, "index_id", "indice")),
                "symbol": _text(_value(row, "symbol", "codigo", "codneg", "ticker")),
                "security_id": _text(row.get("security_id"))
                or _text(_value(row, "symbol", "codigo", "codneg", "ticker")),
                "isin": _text(_value(row, "isin", "codisi")),
                "name": _first_text(row, "name", "nome", "empresa"),
                "weight": parse_decimal(_value(row, "weight", "participacao", "part")),
                "theoretical_quantity": parse_decimal(
                    _value(row, "theoretical_quantity", "quantidade_teorica", "qtd_teorica")
                ),
                "source": row.get("source", "b3"),
                "source_dataset": row.get("source_dataset", "b3_indexes_composition"),
                "download_timestamp_utc": row.get("download_timestamp_utc"),
                "raw_path": row.get("raw_path"),
                "sha256": row.get("sha256"),
                "source_version": source_version,
            }
        )
    return _frame(rows, INDEX_COMPOSITION_COLUMNS)


def normalize_traded_securities(
    bronze: pl.DataFrame,
    *,
    source_version: str = "v0",
) -> pl.DataFrame:
    rows = []
    for row in bronze.to_dicts():
        symbol = _text(_value(row, "symbol", "codigo", "codneg", "ticker"))
        market_type = _text(_value(row, "market_type", "tipo_mercado", "mercado"))
        rows.append(
            {
                "security_id": _text(row.get("security_id")) or f"{symbol}_{market_type}",
                "symbol": symbol,
                "isin": _text(_value(row, "isin", "codisi")),
                "name": _first_text(row, "name", "nome"),
                "market_type": market_type,
                "asset_class": _text(_value(row, "asset_class")) or "listed_security",
                "issuer": _first_text(row, "issuer", "emissor"),
                "currency": _text(row.get("currency")) or "BRL",
                "source": row.get("source", "b3"),
                "source_dataset": row.get("source_dataset", "b3_traded_securities"),
                "download_timestamp_utc": row.get("download_timestamp_utc"),
                "raw_path": row.get("raw_path"),
                "sha256": row.get("sha256"),
                "source_version": source_version,
            }
        )
    return _frame(rows, REFERENCE_SECURITY_COLUMNS)


def normalize_isin_database(
    bronze: pl.DataFrame,
    *,
    holidays: set[date] | None = None,
    source_version: str = "v0",
) -> pl.DataFrame:
    rows = []
    for row in bronze.to_dicts():
        ref_date = _required_date(row.get("ref_date"))
        symbol = _text(_value(row, "symbol", "codigo", "codneg", "ticker"))
        market_type = _text(_value(row, "market_type", "tipo_mercado", "mercado"))
        rows.append(
            {
                "ref_date": ref_date,
                "available_date": _optional_date(row.get("available_date"))
                or next_business_day(ref_date, holidays),
                "security_id": _text(row.get("security_id")) or f"{symbol}_{market_type}",
                "symbol": symbol,
                "isin": _text(_value(row, "isin", "codisi")),
                "name": _first_text(row, "name", "nome"),
                "market_type": market_type,
                "asset_class": _text(_value(row, "asset_class")) or "listed_security",
                "issuer": _first_text(row, "issuer", "emissor"),
                "currency": _text(row.get("currency")) or "BRL",
                "source": row.get("source", "b3"),
                "source_dataset": row.get("source_dataset", "b3_isin_database"),
                "download_timestamp_utc": row.get("download_timestamp_utc"),
                "raw_path": row.get("raw_path"),
                "sha256": row.get("sha256"),
                "source_version": source_version,
            }
        )
    return _frame(rows, ISIN_DATABASE_COLUMNS)


def normalize_trading_parameters(
    bronze: pl.DataFrame,
    *,
    holidays: set[date] | None = None,
    source_version: str = "v0",
) -> pl.DataFrame:
    rows = []
    for row in bronze.to_dicts():
        ref_date = _required_date(row.get("ref_date"))
        symbol = _text(_value(row, "symbol", "codigo", "codneg", "ticker"))
        market_type = _text(_value(row, "market_type", "tipo_mercado", "mercado"))
        price_limit_lower = parse_decimal(_value(row, "price_limit_lower"))
        price_limit_upper = parse_decimal(_value(row, "price_limit_upper"))
        rows.append(
            {
                "ref_date": ref_date,
                "available_date": _optional_date(row.get("available_date"))
                or next_business_day(ref_date, holidays),
                "symbol": symbol,
                "market_segment": _text(_value(row, "market_segment", "segmento", "mercado")),
                "asset_class": _text(_value(row, "asset_class", "classe_ativo")),
                "round_lot": parse_decimal(
                    _value(row, "round_lot", "lot_size", "lote_padrao", "lote")
                ),
                "tick_size": parse_decimal(_value(row, "tick_size", "tick", "variacao_minima")),
                "price_limits": _price_limits(row, price_limit_lower, price_limit_upper),
                "trading_status": _text(_value(row, "trading_status", "status")),
                "security_id": _text(row.get("security_id")) or f"{symbol}_{market_type}",
                "isin": _text(_value(row, "isin", "codisi")),
                "market_type": market_type,
                "price_limit_lower": price_limit_lower,
                "price_limit_upper": price_limit_upper,
                "source": row.get("source", "b3"),
                "source_dataset": row.get("source_dataset", "b3_trading_parameters"),
                "download_timestamp_utc": row.get("download_timestamp_utc"),
                "raw_path": row.get("raw_path"),
                "sha256": row.get("sha256"),
                "source_version": source_version,
            }
        )
    return _frame(rows, TRADING_PARAMETERS_COLUMNS)


def load_contract_master_yaml(path: Path) -> list[dict[str, object]]:
    data = _load_manual_yaml(path)
    rows = data.get("contracts")
    if not isinstance(rows, list):
        raise ValueError("manual contract master YAML must contain a contracts list")
    return rows


def load_holiday_calendar_yaml(path: Path) -> list[dict[str, object]]:
    data = _load_manual_yaml(path)
    rows = data.get("holidays")
    if not isinstance(rows, list):
        raise ValueError("manual holiday calendar YAML must contain a holidays list")
    return rows


def write_reference_table(
    frame: pl.DataFrame,
    output_root: Path,
    *,
    primary_keys: list[str],
    ref_date_col: str | None = None,
) -> list[Path]:
    if frame.is_empty():
        return []
    if ref_date_col and ref_date_col in frame.columns:
        return write_source_partitioned(
            frame,
            output_root,
            ref_date_col=ref_date_col,
            primary_keys=primary_keys,
        )

    output_root.mkdir(parents=True, exist_ok=True)
    path = output_root / "data.parquet"
    part = frame
    if path.exists():
        part = pl.concat([pl.read_parquet(path), frame], how="diagonal_relaxed")
    part = part.unique(subset=primary_keys, keep="last", maintain_order=True)
    part.write_parquet(path)
    return [path]


def _frame(rows: list[dict[str, object]], columns: list[str]) -> pl.DataFrame:
    if not rows:
        return pl.DataFrame(schema={column: pl.Null for column in columns})
    return pl.DataFrame(rows).select(columns)


def _load_manual_yaml(path: Path) -> dict[str, object]:
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(data, dict):
        raise ValueError("manual YAML input must be a mapping")
    return data


def _required_date(value: object) -> date:
    parsed = _optional_date(value)
    if parsed is None:
        raise ValueError("date value is required")
    return parsed


def _optional_date(value: object) -> date | None:
    if value is None:
        return None
    if isinstance(value, date):
        return value
    text = str(value).strip()
    if not text:
        return None
    return date.fromisoformat(text[:10])


def _value(row: dict[str, object], *aliases: str) -> object:
    for alias in aliases:
        normalized = normalize_column_name(alias)
        if normalized in row:
            return row[normalized]
    return None


def _first_text(row: dict[str, object], *aliases: str) -> str | None:
    for alias in aliases:
        value = _value(row, alias)
        if value is not None and str(value).strip():
            return str(value).strip()
    return None


def _price_limits(
    row: dict[str, object],
    price_limit_lower: float | None,
    price_limit_upper: float | None,
) -> str | None:
    explicit = _first_text(row, "price_limits")
    if explicit:
        return explicit
    if price_limit_lower is not None or price_limit_upper is not None:
        return f"{price_limit_lower or ''}/{price_limit_upper or ''}"
    return None


def _text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip().upper()
    return text or None
