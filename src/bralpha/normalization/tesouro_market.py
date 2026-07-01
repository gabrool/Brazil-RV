from __future__ import annotations

import json
from calendar import monthrange
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import polars as pl

from bralpha.domain.b3_calendar import add_business_days
from bralpha.ingestion.tesouro.common import write_partitioned_frame
from bralpha.normalization.tesouro_fiscal import (
    FISCAL_SILVER_COLUMNS_BY_DATASET,
    normalize_dpf_stock_to_silver,
)
from bralpha.parsing.common import normalize_column_name, parse_decimal, parse_int
from bralpha.timing.availability import usable_date_from_date_only

TESOURO_DATE_ONLY_NEXT_BUSINESS_DAY_POLICY = "date_only_next_business_day"
TESOURO_DIRETO_SALES_OFFICIAL_2BD_POLICY = "tesouro_direto_sales_official_2bd"
TESOURO_DIRETO_REDEMPTIONS_CONSERVATIVE_2BD_POLICY = (
    "tesouro_direto_redemptions_conservative_2bd"
)
TESOURO_DIRETO_STOCK_CONSERVATIVE_30D_POLICY = "tesouro_direto_stock_conservative_30d"
TESOURO_CONFIGURED_HOLIDAY_AVAILABILITY_BASIS = "configured_holiday_calendar"
TESOURO_CANONICAL_B3_AVAILABILITY_BASIS = "canonical_b3_calendar"

TESOURO_DIRETO_PRICES_RATES_COLUMNS = [
    "ref_date",
    "available_date",
    "availability_policy",
    "security_name",
    "security_type",
    "maturity_date",
    "buy_rate",
    "sell_rate",
    "buy_price",
    "sell_price",
    "unit",
    "source",
    "source_dataset",
    "download_timestamp_utc",
    "raw_path",
    "sha256",
    "source_version",
]

TESOURO_DIRETO_SALES_COLUMNS = [
    "ref_date",
    "available_date",
    "availability_policy",
    "availability_basis",
    "security_name",
    "security_type",
    "maturity_date",
    "quantity",
    "value",
    "investor_count",
    "unit",
    "source",
    "source_dataset",
    "download_timestamp_utc",
    "raw_path",
    "sha256",
    "source_version",
]

TESOURO_DIRETO_REDEMPTIONS_COLUMNS = [
    "ref_date",
    "available_date",
    "availability_policy",
    "availability_basis",
    "redemption_type",
    "security_name",
    "security_type",
    "maturity_date",
    "quantity",
    "value",
    "unit",
    "source",
    "source_dataset",
    "download_timestamp_utc",
    "raw_path",
    "sha256",
    "source_version",
]

TESOURO_DIRETO_STOCK_COLUMNS = [
    "ref_date",
    "available_date",
    "availability_policy",
    "security_name",
    "security_type",
    "maturity_date",
    "quantity",
    "stock_value",
    "investor_count",
    "unit",
    "source",
    "source_dataset",
    "download_timestamp_utc",
    "raw_path",
    "sha256",
    "source_version",
]

MARKET_SILVER_COLUMNS_BY_DATASET = {
    "tesouro_direto_prices_rates": TESOURO_DIRETO_PRICES_RATES_COLUMNS,
    "tesouro_direto_sales": TESOURO_DIRETO_SALES_COLUMNS,
    "tesouro_direto_redemptions": TESOURO_DIRETO_REDEMPTIONS_COLUMNS,
    "tesouro_direto_stock": TESOURO_DIRETO_STOCK_COLUMNS,
}

SILVER_COLUMNS_BY_DATASET = {
    **MARKET_SILVER_COLUMNS_BY_DATASET,
    **FISCAL_SILVER_COLUMNS_BY_DATASET,
}


def normalize_tesouro_to_silver(
    dataset_id: str,
    bronze: pl.DataFrame,
    *,
    source_version: str = "v0",
    holidays: set[date] | None = None,
) -> pl.DataFrame:
    if dataset_id == "tesouro_direto_prices_rates":
        return normalize_prices_rates_to_silver(bronze, source_version=source_version)
    if dataset_id == "tesouro_direto_sales":
        return normalize_sales_to_silver(
            bronze,
            source_version=source_version,
            holidays=holidays,
        )
    if dataset_id == "tesouro_direto_redemptions":
        return normalize_redemptions_to_silver(
            bronze,
            source_version=source_version,
            holidays=holidays,
        )
    if dataset_id == "tesouro_direto_stock":
        return normalize_tesouro_direto_stock_to_silver(bronze, source_version=source_version)
    if dataset_id == "tesouro_dpf_stock":
        return normalize_dpf_stock_to_silver(bronze, source_version=source_version)
    raise NotImplementedError(f"Tesouro silver normalizer is not implemented for {dataset_id}")


def normalize_prices_rates_to_silver(
    bronze: pl.DataFrame,
    *,
    source_version: str = "v0",
) -> pl.DataFrame:
    rows = []
    for row in bronze.to_dicts():
        ref_date = _date_field(row, "ref_date", "data_base", "data_referencia", "data")
        security_name = _text_field(row, "security_name", "tipo_titulo", "titulo")
        rows.append(
            {
                "ref_date": ref_date,
                "available_date": _available_date(ref_date),
                "availability_policy": TESOURO_DATE_ONLY_NEXT_BUSINESS_DAY_POLICY,
                "security_name": security_name,
                "security_type": _security_type(security_name),
                "maturity_date": _date_field(row, "maturity_date", "data_vencimento", "vencimento"),
                "buy_rate": _decimal_field(
                    row,
                    "buy_rate",
                    "taxa_compra_manha",
                    "taxa_compra",
                ),
                "sell_rate": _decimal_field(
                    row,
                    "sell_rate",
                    "taxa_venda_manha",
                    "taxa_venda",
                ),
                "buy_price": _decimal_field(
                    row,
                    "buy_price",
                    "pu_compra_manha",
                    "preco_compra_manha",
                    "pu_compra",
                ),
                "sell_price": _decimal_field(
                    row,
                    "sell_price",
                    "pu_venda_manha",
                    "preco_venda_manha",
                    "pu_venda",
                ),
                "unit": _text_field(row, "unit", "unidade") or "BRL",
                **_lineage(row, source_version=source_version),
            }
        )
    return _frame(rows, TESOURO_DIRETO_PRICES_RATES_COLUMNS)


def normalize_sales_to_silver(
    bronze: pl.DataFrame,
    *,
    source_version: str = "v0",
    holidays: set[date] | None = None,
) -> pl.DataFrame:
    rows = []
    for row in bronze.to_dicts():
        ref_date = _date_field(row, "ref_date", "data_venda", "data_base", "data")
        security_name = _text_field(row, "security_name", "tipo_titulo", "titulo")
        rows.append(
            {
                "ref_date": ref_date,
                "available_date": _business_day_lag_available_date(
                    ref_date,
                    days=2,
                    holidays=holidays,
                ),
                "availability_policy": TESOURO_DIRETO_SALES_OFFICIAL_2BD_POLICY,
                "availability_basis": _business_day_lag_availability_basis(holidays),
                "security_name": security_name,
                "security_type": _security_type(security_name),
                "maturity_date": _date_field(
                    row,
                    "maturity_date",
                    "vencimento_do_titulo",
                    "data_vencimento",
                    "vencimento",
                ),
                "quantity": _decimal_field(row, "quantity", "quantidade"),
                "value": _decimal_field(row, "value", "valor"),
                "investor_count": _int_field(row, "investor_count", "investidores"),
                "unit": _text_field(row, "unit", "unidade") or "BRL",
                **_lineage(row, source_version=source_version),
            }
        )
    return _frame(rows, TESOURO_DIRETO_SALES_COLUMNS)


def normalize_redemptions_to_silver(
    bronze: pl.DataFrame,
    *,
    source_version: str = "v0",
    holidays: set[date] | None = None,
) -> pl.DataFrame:
    rows = []
    for row in bronze.to_dicts():
        ref_date = _date_field(
            row,
            "ref_date",
            "data_resgate",
            "data_pagamento",
            "data_base",
            "data",
        )
        security_name = _text_field(row, "security_name", "tipo_titulo", "titulo")
        rows.append(
            {
                "ref_date": ref_date,
                "available_date": _business_day_lag_available_date(
                    ref_date,
                    days=2,
                    holidays=holidays,
                ),
                "availability_policy": TESOURO_DIRETO_REDEMPTIONS_CONSERVATIVE_2BD_POLICY,
                "availability_basis": _business_day_lag_availability_basis(holidays),
                "redemption_type": _redemption_type(row),
                "security_name": security_name,
                "security_type": _security_type(security_name),
                "maturity_date": _date_field(
                    row,
                    "maturity_date",
                    "vencimento_do_titulo",
                    "data_vencimento",
                    "vencimento",
                ),
                "quantity": _decimal_field(row, "quantity", "quantidade"),
                "value": _decimal_field(row, "value", "valor"),
                "unit": _text_field(row, "unit", "unidade") or "BRL",
                **_lineage(row, source_version=source_version),
            }
        )
    return _frame(rows, TESOURO_DIRETO_REDEMPTIONS_COLUMNS)


def normalize_tesouro_direto_stock_to_silver(
    bronze: pl.DataFrame,
    *,
    source_version: str = "v0",
) -> pl.DataFrame:
    rows = []
    for row in bronze.to_dicts():
        ref_date = _date_field(row, "ref_date", "data_base", "mes_estoque", "mes")
        security_name = _text_field(row, "security_name", "tipo_titulo", "titulo")
        rows.append(
            {
                "ref_date": ref_date,
                "available_date": _lagged_available_date(ref_date, days=30),
                "availability_policy": TESOURO_DIRETO_STOCK_CONSERVATIVE_30D_POLICY,
                "security_name": security_name,
                "security_type": _security_type(security_name),
                "maturity_date": _date_field(
                    row,
                    "maturity_date",
                    "vencimento_do_titulo",
                    "data_vencimento",
                    "vencimento",
                ),
                "quantity": _decimal_field(row, "quantity", "quantidade"),
                "stock_value": _decimal_field(row, "stock_value", "valor_estoque", "valor"),
                "investor_count": _int_field(row, "investor_count", "investidores"),
                "unit": _text_field(row, "unit", "unidade") or "BRL",
                **_lineage(row, source_version=source_version),
            }
        )
    return _frame(rows, TESOURO_DIRETO_STOCK_COLUMNS)


def write_tesouro_silver(
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
        return value
    text = str(value).strip()
    if not text:
        return None
    if "/" in text:
        parts = text[:10].split("/")
        if len(parts) == 2:
            month, year = parts
            return _month_end(int(year), int(month))
        day, month, year = parts
        return date(int(year), int(month), int(day))
    return date.fromisoformat(text[:10])


def _month_end(year: int, month: int) -> date:
    return date(year, month, monthrange(year, month)[1])


def _available_date(ref_date: date | None) -> date | None:
    return usable_date_from_date_only(ref_date) if ref_date is not None else None


def _business_day_lag_available_date(
    ref_date: date | None,
    *,
    days: int,
    holidays: set[date] | None,
) -> date | None:
    return add_business_days(ref_date, days, holidays) if ref_date is not None else None


def _business_day_lag_availability_basis(holidays: set[date] | None) -> str:
    if holidays is None:
        return TESOURO_CANONICAL_B3_AVAILABILITY_BASIS
    return TESOURO_CONFIGURED_HOLIDAY_AVAILABILITY_BASIS


def _lagged_available_date(ref_date: date | None, *, days: int) -> date | None:
    if ref_date is None:
        return None
    return usable_date_from_date_only(ref_date + timedelta(days=days))


def _security_type(security_name: str | None) -> str | None:
    if not security_name:
        return None
    return security_name.strip()


def _redemption_type(row: dict[str, Any]) -> str | None:
    explicit = _text_field(row, "redemption_type", "tipo_resgate")
    if explicit:
        return normalize_column_name(explicit)
    resource_name = normalize_column_name(str(row.get("resource_name") or ""))
    if "recompra" in resource_name:
        return "early_repurchase"
    if "vencimento" in resource_name:
        return "maturity"
    if "cupom" in resource_name:
        return "coupon"
    return None


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
