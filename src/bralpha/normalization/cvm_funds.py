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
    "registry_status",
    "registration_date",
    "constitution_date",
    "cancellation_date",
    "status_start_date",
    "activity_start_date",
    "class_name",
    "class_start_date",
    "condominium_type",
    "benchmark_or_return_target",
    "exclusive_fund",
    "quota_fund",
    "target_public",
    "administrator_id",
    "administrator_name",
    "manager_id",
    "manager_name",
    "custodian_id",
    "custodian_name",
    "snapshot_date",
    "source",
    "source_dataset",
    "download_timestamp_utc",
    "raw_path",
    "sha256",
    "source_version",
]

CVM_FUND_REGISTRY_HISTORY_COLUMNS = [
    "fund_id",
    "registry_event_date",
    "registry_field",
    "registry_value",
    "previous_registry_value",
    "source",
    "source_dataset",
    "download_timestamp_utc",
    "raw_path",
    "sha256",
    "source_version",
]

CVM_FUND_CLASS_REGISTRY_COLUMNS = [
    "fund_id",
    "class_id",
    "subclass_id",
    "class_name",
    "subclass_name",
    "class_status",
    "subclass_status",
    "class_start_date",
    "subclass_start_date",
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
    "cvm_fund_registry_history": CVM_FUND_REGISTRY_HISTORY_COLUMNS,
    "cvm_fund_class_registry": CVM_FUND_CLASS_REGISTRY_COLUMNS,
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
    if dataset_id == "cvm_fund_registry_history":
        return normalize_cvm_fund_registry_history_to_silver(
            bronze, source_version=source_version
        )
    if dataset_id == "cvm_fund_class_registry":
        return normalize_cvm_fund_class_registry_to_silver(
            bronze, source_version=source_version
        )
    raise ValueError(f"Unsupported CVM silver dataset: {dataset_id}")


def normalize_cvm_fund_daily_reports_to_silver(
    bronze: pl.DataFrame,
    *,
    source_version: str = "v0",
) -> pl.DataFrame:
    rows = []
    for row in bronze.to_dicts():
        ref_date = _parse_date(_field(row, "dt_comptc", "ref_date"))
        rows.append(
            {
                "ref_date": ref_date,
                "available_date": _add_business_days(ref_date, 2),
                "availability_policy": CVM_DAILY_AVAILABILITY_POLICY,
                "fund_id": _text(_field(row, "cnpj_fundo", "cnpj_fundo_classe", "fund_id")),
                "fund_type": _text(_field(row, "tp_fundo", "tp_fundo_classe")),
                "portfolio_value": _decimal(_field(row, "vl_total")),
                "nav": _decimal(_field(row, "vl_patrim_liq")),
                "quota_value": _decimal(_field(row, "vl_quota")),
                "subscriptions": _decimal(_field(row, "captc_dia")),
                "redemptions": _decimal(_field(row, "resg_dia")),
                "shareholder_count": _int(_field(row, "nr_cotst")),
                "raw_vl_total": _text(_field(row, "vl_total")),
                "raw_vl_patrim_liq": _text(_field(row, "vl_patrim_liq")),
                "raw_vl_quota": _text(_field(row, "vl_quota")),
                "raw_captc_dia": _text(_field(row, "captc_dia")),
                "raw_resg_dia": _text(_field(row, "resg_dia")),
                "raw_nr_cotst": _text(_field(row, "nr_cotst")),
                **_lineage(row, source_version=source_version),
            }
        )
    return _frame(rows, CVM_FUND_DAILY_REPORTS_COLUMNS)


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
                "registry_status": _text(_field(row, "sit", "situacao", "registry_status")),
                "registration_date": _parse_date(_field(row, "dt_reg", "registration_date")),
                "constitution_date": _parse_date(_field(row, "dt_const", "constitution_date")),
                "cancellation_date": _parse_date(_field(row, "dt_cancel", "cancellation_date")),
                "status_start_date": _parse_date(_field(row, "dt_ini_sit", "status_start_date")),
                "activity_start_date": _parse_date(
                    _field(row, "dt_ini_ativ", "activity_start_date")
                ),
                "class_name": _text(_field(row, "classe", "classe_fundo", "class_name")),
                "class_start_date": _parse_date(_field(row, "dt_ini_classe", "class_start_date")),
                "condominium_type": _text(_field(row, "condom", "condominio", "condominium_type")),
                "benchmark_or_return_target": _text(
                    _field(row, "rentab_fundo", "benchmark", "benchmark_or_return_target")
                ),
                "exclusive_fund": _text(_field(row, "fundo_exclusivo", "exclusive_fund")),
                "quota_fund": _text(_field(row, "fundo_cotas", "quota_fund")),
                "target_public": _text(_field(row, "publico_alvo", "target_public")),
                "administrator_id": _text(_field(row, "cnpj_admin", "administrator_id")),
                "administrator_name": _text(
                    _field(row, "admin", "administrador", "administrator_name")
                ),
                "manager_id": _text(
                    _field(row, "cpf_cnpj_gestor", "cnpj_gestor", "manager_id")
                ),
                "manager_name": _text(_field(row, "gestor", "manager_name")),
                "custodian_id": _text(_field(row, "cnpj_custodiante", "custodian_id")),
                "custodian_name": _text(_field(row, "custodiante", "custodian_name")),
                "snapshot_date": _snapshot_date(row),
                **_lineage(row, source_version=source_version),
            }
        )
    return _frame(rows, CVM_FUND_REGISTRY_CURRENT_COLUMNS)


def normalize_cvm_fund_registry_history_to_silver(
    bronze: pl.DataFrame,
    *,
    source_version: str = "v0",
) -> pl.DataFrame:
    rows = []
    for row in bronze.to_dicts():
        event_date = _parse_date(
            _field(
                row,
                "dt_alter",
                "dt_alteracao",
                "dt_evento",
                "registry_event_date",
            )
        )
        rows.append(
            {
                "fund_id": _text(_field(row, "cnpj_fundo", "cnpj", "fund_id")),
                "registry_event_date": event_date,
                "registry_field": _text(
                    _field(row, "campo_alterado", "campo", "registry_field")
                ),
                "registry_value": _text(
                    _field(row, "valor_atual", "valor", "vl_atual", "registry_value")
                ),
                "previous_registry_value": _text(
                    _field(
                        row,
                        "valor_anterior",
                        "vl_anterior",
                        "previous_registry_value",
                    )
                ),
                **_lineage(row, source_version=source_version),
            }
        )
    return _frame(rows, CVM_FUND_REGISTRY_HISTORY_COLUMNS)


def normalize_cvm_fund_class_registry_to_silver(
    bronze: pl.DataFrame,
    *,
    source_version: str = "v0",
) -> pl.DataFrame:
    rows = []
    for row in bronze.to_dicts():
        rows.append(
            {
                "fund_id": _text(
                    _field(row, "cnpj_fundo", "cnpj_fundo_classe", "fund_id")
                ),
                "class_id": _text(
                    _field(row, "id_classe", "cd_classe", "cnpj_fundo_classe", "class_id")
                ),
                "subclass_id": _text(
                    _field(
                        row,
                        "id_subclasse",
                        "cd_subclasse",
                        "cnpj_fundo_subclasse",
                        "subclass_id",
                    )
                ),
                "class_name": _text(
                    _field(row, "denom_social_classe", "nome_classe", "class_name")
                ),
                "subclass_name": _text(
                    _field(row, "denom_social_subclasse", "nome_subclasse", "subclass_name")
                ),
                "class_status": _text(_field(row, "sit_classe", "status_classe", "class_status")),
                "subclass_status": _text(
                    _field(row, "sit_subclasse", "status_subclasse", "subclass_status")
                ),
                "class_start_date": _parse_date(
                    _field(row, "dt_ini_classe", "class_start_date")
                ),
                "subclass_start_date": _parse_date(
                    _field(row, "dt_ini_subclasse", "subclass_start_date")
                ),
                "snapshot_date": _snapshot_date(row),
                **_lineage(row, source_version=source_version),
            }
        )
    return _frame(rows, CVM_FUND_CLASS_REGISTRY_COLUMNS)


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
