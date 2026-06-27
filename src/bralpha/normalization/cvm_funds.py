from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from typing import Any

import polars as pl

from bralpha.domain.b3_calendar import next_business_day
from bralpha.ingestion.cvm.common import write_partitioned_frame
from bralpha.parsing.common import normalize_column_name, parse_decimal

CVM_DAILY_AVAILABILITY_POLICY = "cvm_fund_daily_conservative_2bd"

CVM_FUND_DAILY_REPORTS_COLUMNS = [
    "ref_date",
    "available_date",
    "availability_policy",
    "fund_id",
    "fund_type",
    "portfolio_value",
    "nav",
    "quota_value",
    "subscriptions",
    "redemptions",
    "shareholder_count",
    "raw_vl_total",
    "raw_vl_patrim_liq",
    "raw_vl_quota",
    "raw_captc_dia",
    "raw_resg_dia",
    "raw_nr_cotst",
    "source",
    "source_dataset",
    "download_timestamp_utc",
    "raw_path",
    "sha256",
    "source_version",
]

CVM_FUND_REGISTRY_CURRENT_COLUMNS = [
    "fund_id",
    "fund_type",
    "fund_name",
    "cvm_code",
    "registration_date",
    "constitution_date",
    "cancellation_date",
    "status",
    "status_start_date",
    "activity_start_date",
    "class_name",
    "class_start_date",
    "benchmark_or_return_target",
    "condominium_type",
    "is_fund_of_funds",
    "is_exclusive",
    "is_long_term_tax",
    "public_target",
    "admin_id",
    "admin_name",
    "manager_id",
    "manager_name",
    "custodian_id",
    "custodian_name",
    "auditor_id",
    "auditor_name",
    "controller_id",
    "controller_name",
    "snapshot_date",
    "source",
    "source_dataset",
    "download_timestamp_utc",
    "raw_path",
    "sha256",
    "source_version",
]

CVM_SILVER_COLUMNS_BY_DATASET = {
    "cvm_fund_daily_reports": CVM_FUND_DAILY_REPORTS_COLUMNS,
    "cvm_fund_registry_current": CVM_FUND_REGISTRY_CURRENT_COLUMNS,
}


def normalize_cvm_to_silver(
    dataset_id: str,
    bronze: pl.DataFrame,
    *,
    source_version: str = "v0",
) -> pl.DataFrame:
    if dataset_id == "cvm_fund_daily_reports":
        return normalize_cvm_fund_daily_reports_to_silver(
            bronze, source_version=source_version
        )
    if dataset_id == "cvm_fund_registry_current":
        return normalize_cvm_fund_registry_current_to_silver(
            bronze, source_version=source_version
        )
    raise ValueError(f"Unsupported CVM silver dataset: {dataset_id}")


def normalize_cvm_fund_daily_reports_to_silver(
    bronze: pl.DataFrame,
    *,
    source_version: str = "v0",
) -> pl.DataFrame:
    if bronze.is_empty():
        return pl.DataFrame(schema={column: pl.Null for column in CVM_FUND_DAILY_REPORTS_COLUMNS})

    frame = bronze.with_columns(
        [
            _date_expr(bronze, "dt_comptc", "ref_date").alias("ref_date"),
            _text_expr(bronze, "cnpj_fundo", "cnpj_fundo_classe", "fund_id").alias("fund_id"),
            _text_expr(bronze, "tp_fundo", "tp_fundo_classe").alias("fund_type"),
            _decimal_expr(bronze, "vl_total").alias("portfolio_value"),
            _decimal_expr(bronze, "vl_patrim_liq").alias("nav"),
            _decimal_expr(bronze, "vl_quota").alias("quota_value"),
            _decimal_expr(bronze, "captc_dia").alias("subscriptions"),
            _decimal_expr(bronze, "resg_dia").alias("redemptions"),
            _int_expr(bronze, "nr_cotst").alias("shareholder_count"),
            _text_expr(bronze, "vl_total").alias("raw_vl_total"),
            _text_expr(bronze, "vl_patrim_liq").alias("raw_vl_patrim_liq"),
            _text_expr(bronze, "vl_quota").alias("raw_vl_quota"),
            _text_expr(bronze, "captc_dia").alias("raw_captc_dia"),
            _text_expr(bronze, "resg_dia").alias("raw_resg_dia"),
            _text_expr(bronze, "nr_cotst").alias("raw_nr_cotst"),
            _existing_or_literal(bronze, "source", "cvm").alias("source"),
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
            .map_elements(lambda value: _add_business_days(value, 2), return_dtype=pl.Date)
            .alias("available_date"),
            pl.lit(CVM_DAILY_AVAILABILITY_POLICY).alias("availability_policy"),
        ]
    )
    return frame.select(CVM_FUND_DAILY_REPORTS_COLUMNS)


def normalize_cvm_fund_registry_current_to_silver(
    bronze: pl.DataFrame,
    *,
    source_version: str = "v0",
) -> pl.DataFrame:
    rows = []
    for row in bronze.to_dicts():
        rows.append(
            {
                "fund_id": _text(_field(row, "cnpj_fundo", "cnpj", "fund_id")),
                "fund_type": _text(_field(row, "tp_fundo", "tipo_fundo", "fund_type")),
                "fund_name": _text(_field(row, "denom_social", "nome_fundo", "fund_name")),
                "cvm_code": _text(_field(row, "cd_cvm", "cod_cvm", "cvm_code")),
                "registration_date": _parse_date(_field(row, "dt_reg", "registration_date")),
                "constitution_date": _parse_date(_field(row, "dt_const", "constitution_date")),
                "cancellation_date": _parse_date(_field(row, "dt_cancel", "cancellation_date")),
                "status": _text(_field(row, "sit", "situacao", "status")),
                "status_start_date": _parse_date(_field(row, "dt_ini_sit", "status_start_date")),
                "activity_start_date": _parse_date(
                    _field(row, "dt_ini_ativ", "activity_start_date")
                ),
                "class_name": _text(_field(row, "classe", "classe_fundo", "class_name")),
                "class_start_date": _parse_date(_field(row, "dt_ini_classe", "class_start_date")),
                "benchmark_or_return_target": _text(
                    _field(row, "rentab_fundo", "benchmark", "benchmark_or_return_target")
                ),
                "condominium_type": _text(_field(row, "condom", "condominio", "condominium_type")),
                "is_fund_of_funds": _text(_field(row, "fundo_cotas", "is_fund_of_funds")),
                "is_exclusive": _text(_field(row, "fundo_exclusivo", "is_exclusive")),
                "is_long_term_tax": _text(
                    _field(row, "fundo_longo_prazo", "tribut_longo_prazo", "is_long_term_tax")
                ),
                "public_target": _text(_field(row, "publico_alvo", "public_target")),
                "admin_id": _text(_field(row, "cnpj_admin", "admin_id")),
                "admin_name": _text(_field(row, "admin", "administrador", "admin_name")),
                "manager_id": _text(
                    _field(row, "cpf_cnpj_gestor", "cnpj_gestor", "manager_id")
                ),
                "manager_name": _text(_field(row, "gestor", "manager_name")),
                "custodian_id": _text(_field(row, "cnpj_custodiante", "custodian_id")),
                "custodian_name": _text(_field(row, "custodiante", "custodian_name")),
                "auditor_id": _text(_field(row, "cnpj_auditor", "auditor_id")),
                "auditor_name": _text(_field(row, "auditor", "auditor_name")),
                "controller_id": _text(_field(row, "cnpj_controlador", "controller_id")),
                "controller_name": _text(_field(row, "controlador", "controller_name")),
                "snapshot_date": _snapshot_date(row),
                **_lineage(row, source_version=source_version),
            }
        )
    return _frame(rows, CVM_FUND_REGISTRY_CURRENT_COLUMNS)


def write_cvm_silver(
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


def _int_expr(frame: pl.DataFrame, *aliases: str) -> pl.Expr:
    return _coalesced_expr(frame, *aliases).map_elements(_int, return_dtype=pl.Int64)


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
        "source": row.get("source", "cvm"),
        "source_dataset": row.get("source_dataset"),
        "download_timestamp_utc": row.get("download_timestamp_utc"),
        "raw_path": row.get("raw_path"),
        "sha256": row.get("sha256"),
        "source_version": source_version,
    }


def _snapshot_date(row: dict[str, object]) -> date | None:
    direct = _parse_date(_field(row, "snapshot_date", "dt_referencia"))
    if direct is not None:
        return direct
    downloaded_at = row.get("download_timestamp_utc")
    if isinstance(downloaded_at, datetime):
        return downloaded_at.date()
    parsed = _parse_datetime(downloaded_at)
    return parsed.date() if parsed is not None else None


def _add_business_days(value: date | None, days: int) -> date | None:
    if value is None:
        return None
    current = value
    for _ in range(days):
        current = next_business_day(current)
    return current


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


def _parse_datetime(value: object) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    text = str(value).strip()
    if not text:
        return None
    return datetime.fromisoformat(text.replace("Z", "+00:00"))


def _frame(rows: list[dict[str, object]], columns: list[str]) -> pl.DataFrame:
    if not rows:
        return pl.DataFrame(schema={column: pl.Null for column in columns})
    return pl.DataFrame(rows).select(columns)
