from __future__ import annotations

from datetime import UTC, date, datetime
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any

import polars as pl

from bralpha.domain.b3_calendar import next_business_day
from bralpha.ingestion.cvm.common import write_partitioned_frame
from bralpha.parsing.common import normalize_column_name, parse_decimal
from bralpha.timing.vintages import (
    AVAILABILITY_CONSERVATIVE_HEURISTIC,
    AVAILABILITY_CURRENT_SNAPSHOT_NO_VINTAGE,
    AVAILABILITY_EXACT_SOURCE_TIMESTAMP,
    AVAILABILITY_FIRST_SEEN_DOWNLOAD_TIMESTAMP,
    AVAILABILITY_SOURCE_DATE_ONLY,
    REVISION_CURRENT_SNAPSHOT_REFERENCE_ONLY,
    REVISION_REVISED_USE_FIRST_SEEN,
    REVISION_REVISED_USE_VINTAGES,
    available_date_from_first_seen,
    available_date_from_official_date_only,
    available_date_from_source_datetime,
    choose_model_usable,
    make_vintage_id,
)

CVM_DAILY_DELIVERY_METADATA_POLICY = "cvm_delivery_metadata"
CVM_DAILY_FIRST_SEEN_POLICY = "cvm_first_seen_snapshot"
CVM_DAILY_REFERENCE_ONLY_POLICY = "cvm_fund_daily_conservative_2bd_reference_only"
CVM_REGISTRY_CURRENT_REFERENCE_POLICY = "cvm_fund_registry_current_reference_only"

CVM_PIT_COLUMNS = [
    "availability_basis",
    "revision_policy",
    "release_date",
    "source_publication_datetime_utc",
    "source_last_modified_utc",
    "first_seen_timestamp_utc",
    "vintage_id",
    "revision_sequence",
    "model_usable",
    "model_usable_reason",
]

CVM_FUND_DAILY_REPORTS_COLUMNS = [
    "ref_date",
    "available_date",
    "availability_policy",
    *CVM_PIT_COLUMNS,
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
    "available_date",
    "availability_policy",
    *CVM_PIT_COLUMNS,
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
    "resource_name",
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
            _existing_or_literal(bronze, "source_publication_datetime_utc", None).alias(
                "source_publication_datetime_utc"
            ),
            _source_last_modified_expr(bronze).alias("source_last_modified_utc"),
            _first_seen_expr(bronze).alias("first_seen_timestamp_utc"),
            _datetime_expr(
                bronze,
                "delivery_datetime_utc",
                "document_delivery_datetime_utc",
                "delivery_timestamp_utc",
                "document_delivery_timestamp_utc",
                "data_entrega_utc",
                "dt_entrega_utc",
            ).alias("delivery_datetime_utc"),
            _date_expr(
                bronze,
                "delivery_date",
                "document_delivery_date",
                "data_entrega",
                "dt_entrega",
            ).alias("delivery_date"),
            _existing_or_literal(bronze, "raw_path", None).alias("raw_path"),
            _existing_or_literal(bronze, "sha256", None).alias("sha256"),
            _existing_or_literal(bronze, "resource_name", None).alias("resource_name"),
            pl.lit(source_version).alias("source_version"),
        ]
    )
    frame = frame.with_columns(
        [
            pl.col("delivery_datetime_utc")
            .map_elements(available_date_from_source_datetime, return_dtype=pl.Date)
            .alias("delivery_datetime_available_date"),
            pl.col("delivery_date")
            .map_elements(available_date_from_official_date_only, return_dtype=pl.Date)
            .alias("delivery_date_available_date"),
            pl.col("first_seen_timestamp_utc")
            .map_elements(available_date_from_first_seen, return_dtype=pl.Date)
            .alias("first_seen_available_date"),
            pl.col("ref_date")
            .map_elements(lambda value: _add_business_days(value, 2), return_dtype=pl.Date)
            .alias("heuristic_available_date"),
        ]
    )
    has_delivery_datetime = pl.col("delivery_datetime_available_date").is_not_null()
    has_delivery_date = pl.col("delivery_date_available_date").is_not_null()
    has_first_seen = pl.col("first_seen_available_date").is_not_null()
    frame = frame.with_columns(
        [
            pl.when(has_delivery_datetime)
            .then(pl.col("delivery_datetime_available_date"))
            .when(has_delivery_date)
            .then(pl.col("delivery_date_available_date"))
            .when(has_first_seen)
            .then(pl.col("first_seen_available_date"))
            .otherwise(pl.col("heuristic_available_date"))
            .alias("available_date"),
            pl.when(has_delivery_datetime | has_delivery_date)
            .then(pl.lit(CVM_DAILY_DELIVERY_METADATA_POLICY))
            .when(has_first_seen)
            .then(pl.lit(CVM_DAILY_FIRST_SEEN_POLICY))
            .otherwise(pl.lit(CVM_DAILY_REFERENCE_ONLY_POLICY))
            .alias("availability_policy"),
            pl.when(has_delivery_datetime)
            .then(pl.lit(AVAILABILITY_EXACT_SOURCE_TIMESTAMP))
            .when(has_delivery_date)
            .then(pl.lit(AVAILABILITY_SOURCE_DATE_ONLY))
            .when(has_first_seen)
            .then(pl.lit(AVAILABILITY_FIRST_SEEN_DOWNLOAD_TIMESTAMP))
            .otherwise(pl.lit(AVAILABILITY_CONSERVATIVE_HEURISTIC))
            .alias("availability_basis"),
            pl.when(has_delivery_datetime | has_delivery_date)
            .then(pl.lit(REVISION_REVISED_USE_VINTAGES))
            .when(has_first_seen)
            .then(pl.lit(REVISION_REVISED_USE_FIRST_SEEN))
            .otherwise(pl.lit(REVISION_CURRENT_SNAPSHOT_REFERENCE_ONLY))
            .alias("revision_policy"),
            pl.coalesce(
                [
                    pl.col("delivery_datetime_utc").dt.date(),
                    pl.col("delivery_date"),
                ]
            ).alias("release_date"),
            pl.lit(0).alias("revision_sequence"),
            pl.when(has_delivery_datetime | has_delivery_date)
            .then(pl.lit(CVM_DAILY_DELIVERY_METADATA_POLICY))
            .when(has_first_seen)
            .then(pl.lit(CVM_DAILY_FIRST_SEEN_POLICY))
            .otherwise(pl.lit(CVM_DAILY_REFERENCE_ONLY_POLICY))
            .alias("model_usable_reason"),
        ]
    )
    frame = frame.with_columns(
        pl.struct(
            [
                "source_dataset",
                "resource_name",
                "raw_path",
                "sha256",
                "fund_id",
                "ref_date",
                "delivery_datetime_utc",
                "delivery_date",
                "first_seen_timestamp_utc",
            ]
        )
        .map_elements(_daily_vintage_id, return_dtype=pl.Utf8)
        .alias("vintage_id")
    )
    frame = frame.with_columns(
        pl.struct(
            [
                "availability_basis",
                "revision_policy",
                "available_date",
                "vintage_id",
                "first_seen_timestamp_utc",
                "delivery_datetime_utc",
                "delivery_date",
            ]
        )
        .map_elements(_daily_model_usable, return_dtype=pl.Boolean)
        .alias("model_usable")
    )
    frame = frame.drop(
        [
            "delivery_datetime_utc",
            "delivery_date",
            "delivery_datetime_available_date",
            "delivery_date_available_date",
            "first_seen_available_date",
            "heuristic_available_date",
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
        pit = _registry_pit_fields(row)
        rows.append(
            {
                "fund_id": _text(_field(row, "cnpj_fundo", "cnpj", "fund_id")),
                **pit,
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


def _datetime_expr(frame: pl.DataFrame, *aliases: str) -> pl.Expr:
    return _coalesced_expr(frame, *aliases).map_elements(
        _parse_datetime, return_dtype=pl.Datetime
    )


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
        "resource_name": row.get("resource_name"),
        "source_version": source_version,
    }


def _registry_pit_fields(row: dict[str, object]) -> dict[str, object]:
    first_seen = _datetime_value(
        row.get("first_seen_timestamp_utc") or row.get("download_timestamp_utc")
    )
    available_date = available_date_from_first_seen(first_seen) or _snapshot_date(row)
    vintage_id = make_vintage_id(
        source="cvm",
        dataset_id=_text(row.get("source_dataset")) or "cvm_fund_registry_current",
        resource_id=_text(row.get("resource_name")) or _text(row.get("raw_path")) or "cad_fi",
        observation_key=_text(_field(row, "cnpj_fundo", "cnpj", "fund_id")) or "",
        publication_timestamp=None,
        first_seen_timestamp_utc=None
        if _text(row.get("sha256"))
        else first_seen,
        content_hash=_text(row.get("sha256")),
    )
    return {
        "available_date": available_date,
        "availability_policy": CVM_REGISTRY_CURRENT_REFERENCE_POLICY,
        "availability_basis": AVAILABILITY_CURRENT_SNAPSHOT_NO_VINTAGE,
        "revision_policy": REVISION_CURRENT_SNAPSHOT_REFERENCE_ONLY,
        "release_date": None,
        "source_publication_datetime_utc": row.get("source_publication_datetime_utc"),
        "source_last_modified_utc": _source_last_modified(row),
        "first_seen_timestamp_utc": first_seen,
        "vintage_id": vintage_id,
        "revision_sequence": 0,
        "model_usable": False,
        "model_usable_reason": CVM_REGISTRY_CURRENT_REFERENCE_POLICY,
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
    try:
        timestamp = parsedate_to_datetime(text)
    except (TypeError, ValueError):
        timestamp = datetime.fromisoformat(text.replace("Z", "+00:00"))
    if timestamp.tzinfo is None:
        return timestamp
    return timestamp.astimezone(UTC).replace(tzinfo=None)


def _source_last_modified_expr(frame: pl.DataFrame) -> pl.Expr:
    exprs = [
        pl.col(column)
        for column in (
            "resource_last_modified",
            "ckan_resource_last_modified",
            "http_last_modified",
            "resource_updated_at",
        )
        if column in frame.columns
    ]
    if not exprs:
        return pl.lit(None, dtype=pl.Datetime)
    return pl.coalesce(exprs).map_elements(_parse_datetime, return_dtype=pl.Datetime)


def _first_seen_expr(frame: pl.DataFrame) -> pl.Expr:
    exprs = [
        pl.col(column)
        for column in ("first_seen_timestamp_utc", "download_timestamp_utc")
        if column in frame.columns
    ]
    if not exprs:
        return pl.lit(None, dtype=pl.Datetime)
    return pl.coalesce(exprs).map_elements(_parse_datetime, return_dtype=pl.Datetime)


def _source_last_modified(row: dict[str, object]) -> datetime | None:
    for column in (
        "resource_last_modified",
        "ckan_resource_last_modified",
        "http_last_modified",
        "resource_updated_at",
    ):
        parsed = _datetime_value(row.get(column))
        if parsed is not None:
            return parsed
    return None


def _datetime_value(value: object) -> datetime | None:
    return _parse_datetime(value)


def _daily_vintage_id(values: dict[str, object]) -> str:
    dataset_id = _text(values.get("source_dataset")) or "cvm_fund_daily_reports"
    resource_id = _text(values.get("resource_name")) or _text(values.get("raw_path")) or dataset_id
    content_hash = _text(values.get("sha256"))
    publication_or_first_seen = (
        values.get("delivery_datetime_utc")
        or values.get("delivery_date")
        or (None if content_hash else values.get("first_seen_timestamp_utc"))
    )
    observation_key = "|".join(
        token
        for token in (
            _text(values.get("fund_id")),
            _stable_date_text(values.get("ref_date")),
        )
        if token
    )
    return make_vintage_id(
        source="cvm",
        dataset_id=dataset_id,
        resource_id=resource_id,
        observation_key=observation_key,
        publication_timestamp=publication_or_first_seen,
        first_seen_timestamp_utc=None if content_hash else values.get("first_seen_timestamp_utc"),
        content_hash=content_hash,
    )


def _daily_model_usable(values: dict[str, object]) -> bool:
    revision_policy = _text(values.get("revision_policy")) or ""
    availability_basis = _text(values.get("availability_basis"))
    is_delivery = revision_policy == REVISION_REVISED_USE_VINTAGES
    return choose_model_usable(
        configured_model_usable=True,
        availability_basis=availability_basis,
        revision_policy=revision_policy,
        available_date=values.get("available_date"),
        vintage_id=_text(values.get("vintage_id")),
        first_seen_timestamp_utc=values.get("first_seen_timestamp_utc"),
        model_usable_without_vintage=is_delivery,
    )


def _stable_date_text(value: object) -> str | None:
    if isinstance(value, (date, datetime)):
        return value.isoformat()[:10]
    text = _text(value)
    return text[:10] if text else None


def _frame(rows: list[dict[str, object]], columns: list[str]) -> pl.DataFrame:
    if not rows:
        return pl.DataFrame(schema={column: pl.Null for column in columns})
    return pl.DataFrame(rows).select(columns)
