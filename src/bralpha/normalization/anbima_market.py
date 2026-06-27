from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

import polars as pl

from bralpha.ingestion.anbima.common import write_partitioned_frame
from bralpha.parsing.common import normalize_column_name, parse_decimal, parse_int
from bralpha.timing.availability import usable_date_from_date_only

ANBIMA_SOVEREIGN_SECONDARY_MARKET_COLUMNS = [
    "ref_date",
    "available_date",
    "security_id",
    "security_type",
    "security_name",
    "maturity_date",
    "days_to_maturity",
    "indicative_rate",
    "bid_rate",
    "ask_rate",
    "price",
    "pu",
    "duration",
    "convexity",
    "indexer",
    "currency",
    "source",
    "source_dataset",
    "download_timestamp_utc",
    "raw_path",
    "sha256",
    "source_version",
]

ANBIMA_YIELD_CURVE_COLUMNS = [
    "ref_date",
    "available_date",
    "curve_type",
    "tenor_days",
    "tenor_label",
    "rate",
    "unit",
    "source",
    "source_dataset",
    "download_timestamp_utc",
    "raw_path",
    "sha256",
    "source_version",
]

ANBIMA_VNA_COLUMNS = [
    "ref_date",
    "available_date",
    "security_type",
    "indexer",
    "vna",
    "unit",
    "source",
    "source_dataset",
    "download_timestamp_utc",
    "raw_path",
    "sha256",
    "source_version",
]

ANBIMA_FIXED_INCOME_INDEX_COLUMNS = [
    "ref_date",
    "available_date",
    "index_id",
    "index_family",
    "index_name",
    "index_value",
    "return_1d_official",
    "duration",
    "yield_rate",
    "market_value",
    "unit",
    "source",
    "source_dataset",
    "download_timestamp_utc",
    "raw_path",
    "sha256",
    "source_version",
]

ANBIMA_INFLATION_PROJECTION_COLUMNS = [
    "ref_date",
    "available_date",
    "indicator",
    "reference_period",
    "statistic",
    "projection_value",
    "unit",
    "source",
    "source_dataset",
    "download_timestamp_utc",
    "raw_path",
    "sha256",
    "source_version",
]

SILVER_COLUMNS_BY_DATASET = {
    "anbima_sovereign_secondary_market": ANBIMA_SOVEREIGN_SECONDARY_MARKET_COLUMNS,
    "anbima_sovereign_yield_curves": ANBIMA_YIELD_CURVE_COLUMNS,
    "anbima_vna": ANBIMA_VNA_COLUMNS,
    "anbima_fixed_income_indices": ANBIMA_FIXED_INCOME_INDEX_COLUMNS,
    "anbima_inflation_projections": ANBIMA_INFLATION_PROJECTION_COLUMNS,
}


def normalize_anbima_to_silver(
    dataset_id: str,
    bronze: pl.DataFrame,
    *,
    source_version: str = "v0",
) -> pl.DataFrame:
    if dataset_id == "anbima_sovereign_secondary_market":
        return normalize_sovereign_secondary_market_to_silver(
            bronze,
            source_version=source_version,
        )
    if dataset_id == "anbima_sovereign_yield_curves":
        return normalize_yield_curves_to_silver(bronze, source_version=source_version)
    if dataset_id == "anbima_vna":
        return normalize_vna_to_silver(bronze, source_version=source_version)
    if dataset_id == "anbima_fixed_income_indices":
        return normalize_fixed_income_indices_to_silver(
            bronze,
            source_version=source_version,
        )
    if dataset_id == "anbima_inflation_projections":
        return normalize_inflation_projections_to_silver(
            bronze,
            source_version=source_version,
        )
    raise NotImplementedError(f"ANBIMA silver normalizer is not implemented for {dataset_id}")


def normalize_sovereign_secondary_market_to_silver(
    bronze: pl.DataFrame,
    *,
    source_version: str = "v0",
) -> pl.DataFrame:
    rows = []
    for row in bronze.to_dicts():
        ref_date = _date_field(row, "ref_date", "data_referencia", "data_base", "data")
        rows.append(
            {
                "ref_date": ref_date,
                "available_date": _available_date(ref_date),
                "security_id": _text_field(row, "security_id", "codigo_titulo", "codigo", "isin"),
                "security_type": _text_field(row, "security_type", "tipo_titulo", "titulo"),
                "security_name": _text_field(row, "security_name", "nome_titulo", "nome"),
                "maturity_date": _date_field(row, "maturity_date", "data_vencimento", "vencimento"),
                "days_to_maturity": _int_field(row, "days_to_maturity", "dias_uteis", "du"),
                "indicative_rate": _decimal_field(row, "indicative_rate", "taxa_indicativa"),
                "bid_rate": _decimal_field(row, "bid_rate", "taxa_compra"),
                "ask_rate": _decimal_field(row, "ask_rate", "taxa_venda"),
                "price": _decimal_field(row, "price", "preco"),
                "pu": _decimal_field(row, "pu"),
                "duration": _decimal_field(row, "duration", "duracao"),
                "convexity": _decimal_field(row, "convexity", "convexidade"),
                "indexer": _text_field(row, "indexer", "indexador"),
                "currency": _text_field(row, "currency", "moeda") or "BRL",
                **_lineage(row, source_version=source_version),
            }
        )
    return _frame(rows, ANBIMA_SOVEREIGN_SECONDARY_MARKET_COLUMNS)


def normalize_yield_curves_to_silver(
    bronze: pl.DataFrame,
    *,
    source_version: str = "v0",
) -> pl.DataFrame:
    rows = []
    for row in bronze.to_dicts():
        ref_date = _date_field(row, "ref_date", "data_referencia", "data_base", "data")
        rows.append(
            {
                "ref_date": ref_date,
                "available_date": _available_date(ref_date),
                "curve_type": _text_field(row, "curve_type", "tipo_curva", "curva"),
                "tenor_days": _int_field(row, "tenor_days", "prazo_dias", "du"),
                "tenor_label": _text_field(row, "tenor_label", "prazo", "vertice"),
                "rate": _decimal_field(row, "rate", "taxa"),
                "unit": _text_field(row, "unit", "unidade") or "percent_annualized",
                **_lineage(row, source_version=source_version),
            }
        )
    return _frame(rows, ANBIMA_YIELD_CURVE_COLUMNS)


def normalize_vna_to_silver(
    bronze: pl.DataFrame,
    *,
    source_version: str = "v0",
) -> pl.DataFrame:
    rows = []
    for row in bronze.to_dicts():
        ref_date = _date_field(row, "ref_date", "data_referencia", "data_base", "data")
        rows.append(
            {
                "ref_date": ref_date,
                "available_date": _available_date(ref_date),
                "security_type": _text_field(row, "security_type", "tipo_titulo", "titulo"),
                "indexer": _text_field(row, "indexer", "indexador"),
                "vna": _decimal_field(row, "vna", "valor_nominal_atualizado"),
                "unit": _text_field(row, "unit", "unidade") or "BRL",
                **_lineage(row, source_version=source_version),
            }
        )
    return _frame(rows, ANBIMA_VNA_COLUMNS)


def normalize_fixed_income_indices_to_silver(
    bronze: pl.DataFrame,
    *,
    source_version: str = "v0",
) -> pl.DataFrame:
    rows = []
    for row in bronze.to_dicts():
        ref_date = _date_field(row, "ref_date", "data_referencia", "data_base", "data")
        rows.append(
            {
                "ref_date": ref_date,
                "available_date": _available_date(ref_date),
                "index_id": _text_field(row, "index_id", "codigo_indice", "indice"),
                "index_family": _text_field(row, "index_family", "familia_indice", "familia"),
                "index_name": _text_field(row, "index_name", "nome_indice", "nome"),
                "index_value": _decimal_field(row, "index_value", "valor_indice"),
                "return_1d_official": _decimal_field(
                    row,
                    "return_1d_official",
                    "retorno_1d_oficial",
                    "rentabilidade_dia",
                ),
                "duration": _decimal_field(row, "duration", "duracao"),
                "yield_rate": _decimal_field(row, "yield_rate", "taxa"),
                "market_value": _decimal_field(row, "market_value", "valor_mercado"),
                "unit": _text_field(row, "unit", "unidade"),
                **_lineage(row, source_version=source_version),
            }
        )
    return _frame(rows, ANBIMA_FIXED_INCOME_INDEX_COLUMNS)


def normalize_inflation_projections_to_silver(
    bronze: pl.DataFrame,
    *,
    source_version: str = "v0",
) -> pl.DataFrame:
    rows = []
    for row in bronze.to_dicts():
        ref_date = _date_field(row, "ref_date", "data_referencia", "data_base", "data")
        rows.append(
            {
                "ref_date": ref_date,
                "available_date": _available_date(ref_date),
                "indicator": _text_field(row, "indicator", "indicador"),
                "reference_period": _text_field(row, "reference_period", "periodo_referencia"),
                "statistic": _text_field(row, "statistic", "estatistica") or "consensus",
                "projection_value": _decimal_field(row, "projection_value", "projecao"),
                "unit": _text_field(row, "unit", "unidade"),
                **_lineage(row, source_version=source_version),
            }
        )
    return _frame(rows, ANBIMA_INFLATION_PROJECTION_COLUMNS)


def write_anbima_silver(
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


def _int_field(row: dict[str, Any], *aliases: str) -> int | None:
    return parse_int(_field(row, *aliases))


def _date_field(row: dict[str, Any], *aliases: str) -> date | None:
    return _parse_date(_field(row, *aliases))


def _parse_date(value: Any) -> date | None:
    if value is None:
        return None
    if hasattr(value, "date") and not isinstance(value, date):
        return value.date()
    if isinstance(value, date):
        return value if not hasattr(value, "date") else value.date()
    text = str(value).strip()
    if not text:
        return None
    if "/" in text:
        day, month, year = text[:10].split("/")
        return date(int(year), int(month), int(day))
    return date.fromisoformat(text[:10])


def _available_date(ref_date: date | None) -> date | None:
    return usable_date_from_date_only(ref_date) if ref_date is not None else None


def _has_value(value: Any) -> bool:
    return value is not None and value != ""


def _lineage(row: dict[str, Any], *, source_version: str) -> dict[str, Any]:
    return {
        "source": row.get("source", "anbima"),
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
