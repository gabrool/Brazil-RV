from __future__ import annotations

import hashlib
from calendar import monthrange
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import polars as pl

from bralpha.ingestion.anp.common import write_partitioned_frame
from bralpha.parsing.common import normalize_column_name, parse_decimal
from bralpha.timing.availability import usable_date_from_date_only

ANP_WEEKLY_PRICE_AVAILABILITY_POLICY = (
    "anp_weekly_price_survey_conservative_7d_next_business_day"
)
ANP_MONTHLY_AVAILABILITY_POLICY = "anp_monthly_next_month_end_next_business_day"

ANP_FUEL_PRICES_WEEKLY_COLUMNS = [
    "observation_id",
    "ref_date",
    "available_date",
    "availability_policy",
    "region",
    "state",
    "municipality",
    "station_name",
    "station_cnpj",
    "street_name",
    "street_number",
    "address_complement",
    "neighborhood",
    "postal_code",
    "product",
    "sale_price",
    "purchase_price",
    "unit",
    "brand",
    "resource_family",
    "source",
    "source_dataset",
    "download_timestamp_utc",
    "raw_path",
    "sha256",
    "source_version",
]

ANP_FUEL_SALES_MONTHLY_COLUMNS = [
    "ref_date",
    "available_date",
    "availability_policy",
    "year",
    "month",
    "region",
    "state",
    "product",
    "sales_volume_m3",
    "unit",
    "source",
    "source_dataset",
    "download_timestamp_utc",
    "raw_path",
    "sha256",
    "source_version",
]

ANP_OIL_GAS_PRODUCTION_MONTHLY_COLUMNS = [
    "ref_date",
    "available_date",
    "availability_policy",
    "year",
    "month",
    "region",
    "state",
    "location",
    "product",
    "metric_type",
    "metric_value",
    "unit",
    "resource_family",
    "source",
    "source_dataset",
    "download_timestamp_utc",
    "raw_path",
    "sha256",
    "source_version",
]

ANP_SILVER_COLUMNS_BY_DATASET = {
    "anp_fuel_prices_weekly": ANP_FUEL_PRICES_WEEKLY_COLUMNS,
    "anp_fuel_sales_monthly": ANP_FUEL_SALES_MONTHLY_COLUMNS,
    "anp_oil_gas_production_monthly": ANP_OIL_GAS_PRODUCTION_MONTHLY_COLUMNS,
}


def normalize_anp_to_silver(
    dataset_id: str,
    bronze: pl.DataFrame,
    *,
    source_version: str = "v0",
) -> pl.DataFrame:
    if dataset_id == "anp_fuel_prices_weekly":
        return normalize_anp_fuel_prices_weekly(bronze, source_version=source_version)
    if dataset_id == "anp_fuel_sales_monthly":
        return normalize_anp_fuel_sales_monthly(bronze, source_version=source_version)
    if dataset_id == "anp_oil_gas_production_monthly":
        return normalize_anp_oil_gas_production_monthly(bronze, source_version=source_version)
    raise ValueError(f"Unsupported ANP silver dataset: {dataset_id}")


def normalize_anp_fuel_prices_weekly(
    bronze: pl.DataFrame,
    *,
    source_version: str = "v0",
) -> pl.DataFrame:
    if bronze.is_empty():
        return pl.DataFrame(schema={column: pl.Null for column in ANP_FUEL_PRICES_WEEKLY_COLUMNS})

    frame = bronze.with_columns(
        [
            _date_expr(bronze, "data_da_coleta").alias("ref_date"),
            _text_expr(bronze, "regiao_sigla").alias("region"),
            _text_expr(bronze, "estado_sigla").alias("state"),
            _text_expr(bronze, "municipio").alias("municipality"),
            _text_expr(bronze, "revenda").alias("station_name"),
            _text_expr(bronze, "cnpj_da_revenda").alias("station_cnpj"),
            _text_expr(bronze, "nome_da_rua").alias("street_name"),
            _text_expr(bronze, "numero_rua").alias("street_number"),
            _text_expr(bronze, "complemento").alias("address_complement"),
            _text_expr(bronze, "bairro").alias("neighborhood"),
            _text_expr(bronze, "cep").alias("postal_code"),
            _text_expr(bronze, "produto").alias("product"),
            _decimal_expr(bronze, "valor_de_venda").alias("sale_price"),
            _decimal_expr(bronze, "valor_de_compra").alias("purchase_price"),
            _text_expr(bronze, "unidade_de_medida").alias("unit"),
            _text_expr(bronze, "bandeira").alias("brand"),
            _existing_or_literal(bronze, "resource_family", None).alias("resource_family"),
            _existing_or_literal(bronze, "resource_name", None).alias("resource_name"),
            _existing_or_literal(bronze, "inner_filename", None).alias("inner_filename"),
            _existing_or_literal(bronze, "row_index", None).alias("row_index"),
            _existing_or_literal(bronze, "source", "anp").alias("source"),
            _existing_or_literal(bronze, "source_dataset", None).alias("source_dataset"),
            _existing_or_literal(bronze, "download_timestamp_utc", None).alias(
                "download_timestamp_utc"
            ),
            _existing_or_literal(bronze, "raw_path", None).alias("raw_path"),
            _existing_or_literal(bronze, "sha256", None).alias("sha256"),
            pl.lit(source_version).alias("source_version"),
        ]
    )
    frame = frame.with_columns(
        [
            pl.col("ref_date")
            .map_elements(_weekly_available_date, return_dtype=pl.Date)
            .alias("available_date"),
            pl.lit(ANP_WEEKLY_PRICE_AVAILABILITY_POLICY).alias("availability_policy"),
        ]
    )
    id_columns = [
        "source_dataset",
        "resource_family",
        "resource_name",
        "inner_filename",
        "row_index",
        "station_cnpj",
        "product",
        "ref_date",
        "sale_price",
        "purchase_price",
    ]
    frame = frame.with_columns(
        pl.struct(id_columns)
        .map_elements(_observation_id, return_dtype=pl.Utf8)
        .alias("observation_id")
    )
    return frame.select(ANP_FUEL_PRICES_WEEKLY_COLUMNS)


def normalize_anp_fuel_sales_monthly(
    bronze: pl.DataFrame,
    *,
    source_version: str = "v0",
) -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    for row in bronze.to_dicts():
        year = _int(_field(row, "ano", "year"))
        month = _month_number(_field(row, "mes", "month"))
        ref_date = _month_end(year, month)
        rows.append(
            {
                "ref_date": ref_date,
                "available_date": _monthly_available_date(ref_date),
                "availability_policy": ANP_MONTHLY_AVAILABILITY_POLICY,
                "year": year,
                "month": month,
                "region": _text(_field(row, "grande_regiao", "region")),
                "state": _text(_field(row, "unidade_da_federacao", "state")),
                "product": _text(_field(row, "produto", "product")),
                "sales_volume_m3": _decimal(_field(row, "vendas", "sales_volume_m3")),
                "unit": "m3",
                **_lineage(row, source_version=source_version),
            }
        )
    return _frame(rows, ANP_FUEL_SALES_MONTHLY_COLUMNS)


def normalize_anp_oil_gas_production_monthly(
    bronze: pl.DataFrame,
    *,
    source_version: str = "v0",
) -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    for row in bronze.to_dicts():
        year = _int(_field(row, "ano", "year"))
        month = _month_number(_field(row, "mes", "month"))
        ref_date = _month_end(year, month)
        resource_family = _text(row.get("resource_family"))
        rows.append(
            {
                "ref_date": ref_date,
                "available_date": _monthly_available_date(ref_date),
                "availability_policy": ANP_MONTHLY_AVAILABILITY_POLICY,
                "year": year,
                "month": month,
                "region": _text(_field(row, "grande_regiao", "region")),
                "state": _text(_field(row, "unidade_da_federacao", "state")),
                "location": _text(_field(row, "localizacao", "location")),
                "product": _text(_field(row, "produto", "product")),
                "metric_type": resource_family,
                "metric_value": _decimal(_field(row, "producao", "metric_value")),
                "unit": _production_unit(resource_family),
                "resource_family": resource_family,
                **_lineage(row, source_version=source_version),
            }
        )
    return _frame(rows, ANP_OIL_GAS_PRODUCTION_MONTHLY_COLUMNS)


def write_anp_silver(
    frame: pl.DataFrame,
    output_root: Path,
    *,
    primary_keys: list[str],
    partition_cols: list[str],
    ref_date_col: str = "ref_date",
) -> list[Path]:
    return write_partitioned_frame(
        frame,
        output_root,
        primary_keys=primary_keys,
        ref_date_col=ref_date_col,
        partition_cols=partition_cols,
    )


def _text_expr(frame: pl.DataFrame, *aliases: str) -> pl.Expr:
    return _coalesced_expr(frame, *aliases).map_elements(_text, return_dtype=pl.Utf8)


def _date_expr(frame: pl.DataFrame, *aliases: str) -> pl.Expr:
    return _coalesced_expr(frame, *aliases).map_elements(_parse_date, return_dtype=pl.Date)


def _decimal_expr(frame: pl.DataFrame, *aliases: str) -> pl.Expr:
    return _coalesced_expr(frame, *aliases).map_elements(_decimal, return_dtype=pl.Float64)


def _coalesced_expr(frame: pl.DataFrame, *aliases: str) -> pl.Expr:
    exprs: list[pl.Expr] = []
    for alias in aliases:
        normalized = normalize_column_name(alias)
        for candidate in (normalized, f"raw_{normalized}"):
            if candidate in frame.columns:
                exprs.append(pl.col(candidate).cast(pl.Utf8, strict=False))
    if not exprs:
        return pl.lit(None, dtype=pl.Utf8)
    if len(exprs) == 1:
        return exprs[0]
    return pl.coalesce(exprs)


def _existing_or_literal(frame: pl.DataFrame, column: str, value: object) -> pl.Expr:
    if column in frame.columns:
        return pl.col(column)
    return pl.lit(value)


def _field(row: dict[str, object], *aliases: str) -> object:
    for alias in aliases:
        normalized = normalize_column_name(alias)
        for candidate in (normalized, f"raw_{normalized}"):
            if candidate in row and row[candidate] is not None:
                return row[candidate]
    return None


def _lineage(row: dict[str, object], *, source_version: str) -> dict[str, object]:
    return {
        "source": row.get("source", "anp"),
        "source_dataset": row.get("source_dataset"),
        "download_timestamp_utc": row.get("download_timestamp_utc"),
        "raw_path": row.get("raw_path"),
        "sha256": row.get("sha256"),
        "source_version": source_version,
    }


def _weekly_available_date(value: date | None) -> date | None:
    if value is None:
        return None
    return usable_date_from_date_only(value + timedelta(days=7))


def _monthly_available_date(ref_date: date | None) -> date | None:
    if ref_date is None:
        return None
    next_month_year = ref_date.year + (1 if ref_date.month == 12 else 0)
    next_month = 1 if ref_date.month == 12 else ref_date.month + 1
    release_date = _month_end(next_month_year, next_month)
    return usable_date_from_date_only(release_date)


def _production_unit(resource_family: str | None) -> str | None:
    if resource_family in {"petroleum_production", "lgn_production"}:
        return "m3"
    if resource_family and resource_family.startswith("natural_gas"):
        return "mil_m3"
    return None


def _observation_id(values: dict[str, object]) -> str:
    payload = "|".join("" if value is None else str(value) for value in values.values())
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _decimal(value: object) -> float | None:
    return parse_decimal(value)


def _int(value: object) -> int | None:
    parsed = parse_decimal(value)
    return int(parsed) if parsed is not None else None


def _text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _parse_date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value).strip()
    if not text:
        return None
    if "/" in text:
        day, month, year = text[:10].split("/")
        return date(int(year), int(month), int(day))
    return date.fromisoformat(text[:10])


def _month_number(value: Any) -> int | None:
    if value is None:
        return None
    text = normalize_column_name(str(value))
    if not text:
        return None
    if text.isdigit():
        return int(text)
    months = {
        "janeiro": 1,
        "fevereiro": 2,
        "marco": 3,
        "abril": 4,
        "maio": 5,
        "junho": 6,
        "julho": 7,
        "agosto": 8,
        "setembro": 9,
        "outubro": 10,
        "novembro": 11,
        "dezembro": 12,
    }
    return months.get(text)


def _month_end(year: int | None, month: int | None) -> date | None:
    if year is None or month is None:
        return None
    return date(year, month, monthrange(year, month)[1])


def _frame(rows: list[dict[str, object]], columns: list[str]) -> pl.DataFrame:
    if not rows:
        return pl.DataFrame(schema={column: pl.Null for column in columns})
    return pl.DataFrame(rows).select(columns)
