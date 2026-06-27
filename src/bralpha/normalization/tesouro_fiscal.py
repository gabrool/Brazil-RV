from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import polars as pl

from bralpha.ingestion.tesouro.common import write_partitioned_frame
from bralpha.parsing.common import normalize_column_name, parse_decimal
from bralpha.timing.availability import usable_date_from_date_only

TESOURO_DPF_STOCK_COLUMNS = [
    "ref_date",
    "available_date",
    "debt_category",
    "instrument_type",
    "indexer",
    "holder_or_maturity_bucket",
    "stock_value",
    "unit",
    "source",
    "source_dataset",
    "download_timestamp_utc",
    "raw_path",
    "sha256",
    "source_version",
]

FISCAL_SILVER_COLUMNS_BY_DATASET = {
    "tesouro_dpf_stock": TESOURO_DPF_STOCK_COLUMNS,
}


def normalize_dpf_stock_to_silver(
    bronze: pl.DataFrame,
    *,
    source_version: str = "v0",
) -> pl.DataFrame:
    rows = []
    for row in bronze.to_dicts():
        ref_date = _date_field(row, "ref_date", "data_base", "data_referencia", "mes")
        rows.append(
            {
                "ref_date": ref_date,
                "available_date": _lagged_available_date(ref_date, days=45),
                "debt_category": _text_field(
                    row,
                    "debt_category",
                    "categoria_divida",
                    "tipo_divida",
                    "categoria",
                ),
                "instrument_type": _text_field(
                    row,
                    "instrument_type",
                    "tipo_titulo",
                    "instrumento",
                    "titulo",
                ),
                "indexer": _text_field(row, "indexer", "indexador", "remuneracao"),
                "holder_or_maturity_bucket": _text_field(
                    row,
                    "holder_or_maturity_bucket",
                    "detentor",
                    "prazo",
                    "vencimento",
                ),
                "stock_value": _decimal_field(
                    row,
                    "stock_value",
                    "valor",
                    "estoque",
                    "estoque_r_milhoes",
                    "estoque_r_bilhoes",
                ),
                "unit": _text_field(row, "unit", "unidade") or "BRL",
                **_lineage(row, source_version=source_version),
            }
        )
    return _frame(rows, TESOURO_DPF_STOCK_COLUMNS)


def write_tesouro_fiscal_silver(
    frame: pl.DataFrame,
    output_root: Path,
    *,
    primary_keys: list[str],
    partition_cols: list[str],
) -> list[Path]:
    return write_partitioned_frame(
        frame,
        output_root,
        primary_keys=primary_keys,
        ref_date_col="ref_date",
        partition_cols=partition_cols,
    )


def _field(row: dict[str, Any], *aliases: str) -> Any:
    for alias in aliases:
        for key in (alias, f"raw_{normalize_column_name(alias)}"):
            value = row.get(key)
            if _has_value(value):
                return value
    raw_fields = row.get("raw_fields_json")
    if isinstance(raw_fields, str) and raw_fields:
        try:
            payload = json.loads(raw_fields)
        except json.JSONDecodeError:
            payload = {}
        for alias in aliases:
            normalized_alias = normalize_column_name(alias)
            for key, value in payload.items():
                if normalize_column_name(key) == normalized_alias and _has_value(value):
                    return value
    return None


def _text_field(row: dict[str, Any], *aliases: str) -> str | None:
    value = _field(row, *aliases)
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _decimal_field(row: dict[str, Any], *aliases: str) -> float | None:
    return parse_decimal(_field(row, *aliases))


def _date_field(row: dict[str, Any], *aliases: str) -> date | None:
    return _parse_date(_field(row, *aliases))


def _parse_date(value: Any) -> date | None:
    if value is None:
        return None
    if hasattr(value, "date") and not isinstance(value, date):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value).strip()
    if not text:
        return None
    if "/" in text:
        parts = text[:10].split("/")
        if len(parts) == 2:
            month, year = parts
            return date(int(year), int(month), 1)
        day, month, year = parts
        return date(int(year), int(month), int(day))
    return date.fromisoformat(text[:10])


def _lagged_available_date(ref_date: date | None, *, days: int) -> date | None:
    if ref_date is None:
        return None
    return usable_date_from_date_only(ref_date + timedelta(days=days))


def _has_value(value: Any) -> bool:
    return value is not None and value != ""


def _lineage(row: dict[str, Any], *, source_version: str) -> dict[str, Any]:
    return {
        "source": row.get("source", "tesouro"),
        "source_dataset": row.get("source_dataset"),
        "download_timestamp_utc": row.get("download_timestamp_utc"),
        "raw_path": row.get("raw_path"),
        "sha256": row.get("sha256"),
        "source_version": source_version,
    }


def _frame(rows: list[dict[str, object]], columns: list[str]) -> pl.DataFrame:
    if not rows:
        return pl.DataFrame(schema={column: pl.Null for column in columns})
    return pl.DataFrame(rows).select(columns)
