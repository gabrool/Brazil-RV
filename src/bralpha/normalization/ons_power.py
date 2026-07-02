from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from typing import Any

import polars as pl

from bralpha.ingestion.ons.common import write_partitioned_frame
from bralpha.parsing.common import normalize_column_name, parse_decimal
from bralpha.timing.availability import usable_date_from_date_only
from bralpha.timing.vintages import (
    AVAILABILITY_CURRENT_SNAPSHOT_NO_VINTAGE,
    AVAILABILITY_EXACT_SOURCE_TIMESTAMP,
    AVAILABILITY_FIRST_SEEN_DOWNLOAD_TIMESTAMP,
    AVAILABILITY_SOURCE_LAST_MODIFIED,
    REVISION_CURRENT_SNAPSHOT_REFERENCE_ONLY,
    REVISION_REVISED_USE_FIRST_SEEN,
    REVISION_REVISED_USE_VINTAGES,
    available_date_from_first_seen,
    available_date_from_source_datetime,
    make_vintage_id,
)

ONS_AVAILABILITY_POLICY = "ons_conservative_next_business_day"
ONS_SOURCE_TIMESTAMP_POLICY = "ons_source_timestamp_cutoff"
ONS_RESOURCE_TIMESTAMP_POLICY = "ons_resource_timestamp_cutoff"
ONS_FIRST_SEEN_POLICY = "ons_first_seen_timestamp_cutoff"
ONS_AVAILABILITY_NOTE = (
    "timestampless_ons_snapshot_reference_only_requires_last_modified_or_first_seen"
)
ONS_TIMESTAMP_AVAILABILITY_NOTE = "ons_timestamp_cutoff_model_usable"
ONS_FIRST_SEEN_AVAILABILITY_NOTE = "ons_first_seen_cutoff_model_usable"

ONS_LINEAGE_COLUMNS = [
    "source",
    "source_dataset",
    "download_timestamp_utc",
    "raw_path",
    "sha256",
    "resource_name",
    "source_version",
]

ONS_PIT_SNAPSHOT_COLUMNS = [
    "vintage_id",
    "source_publication_datetime_utc",
    "resource_last_modified",
    "http_last_modified",
    "first_seen_timestamp_utc",
]

ONS_EAR_SUBSYSTEM_DAILY_COLUMNS = [
    "ref_date",
    "available_date",
    "availability_policy",
    "availability_basis",
    "revision_policy",
    "model_usable",
    *ONS_PIT_SNAPSHOT_COLUMNS,
    "availability_note",
    "subsystem_id",
    "subsystem",
    "stored_energy_mwmes",
    "stored_energy_percent",
    "stored_energy_max_mwmes",
    "unit",
    "raw_ear_verif_subsistema_mwmes",
    "raw_ear_verif_subsistema_percentual",
    "raw_ear_max_subsistema",
    *ONS_LINEAGE_COLUMNS,
]

ONS_ENA_SUBSYSTEM_DAILY_COLUMNS = [
    "ref_date",
    "available_date",
    "availability_policy",
    "availability_basis",
    "revision_policy",
    "model_usable",
    *ONS_PIT_SNAPSHOT_COLUMNS,
    "availability_note",
    "subsystem_id",
    "subsystem",
    "ena_type",
    "ena_value",
    "unit",
    "raw_ena_value",
    *ONS_LINEAGE_COLUMNS,
]

ONS_LOAD_DAILY_COLUMNS = [
    "ref_date",
    "available_date",
    "availability_policy",
    "availability_basis",
    "revision_policy",
    "model_usable",
    *ONS_PIT_SNAPSHOT_COLUMNS,
    "availability_note",
    "subsystem_id",
    "subsystem",
    "load_mwmed",
    "unit",
    "methodology_note",
    "raw_val_cargaenergiamwmed",
    *ONS_LINEAGE_COLUMNS,
]

ONS_CMO_WEEKLY_COLUMNS = [
    "ref_date",
    "available_date",
    "availability_policy",
    "availability_basis",
    "revision_policy",
    "model_usable",
    *ONS_PIT_SNAPSHOT_COLUMNS,
    "availability_note",
    "subsystem_id",
    "subsystem",
    "load_block",
    "cmo_brl_mwh",
    "unit",
    "raw_cmo_value",
    *ONS_LINEAGE_COLUMNS,
]

ONS_ENERGY_BALANCE_SUBSYSTEM_COLUMNS = [
    "ref_datetime",
    "ref_date",
    "available_date",
    "availability_policy",
    "availability_basis",
    "revision_policy",
    "model_usable",
    *ONS_PIT_SNAPSHOT_COLUMNS,
    "availability_note",
    "subsystem_id",
    "subsystem",
    "load_mwmed",
    "hydro_generation_mwmed",
    "thermal_generation_mwmed",
    "wind_generation_mwmed",
    "solar_generation_mwmed",
    "other_generation_mwmed",
    "interchange_mwmed",
    "unit",
    "raw_val_carga",
    "raw_val_gerhidraulica",
    "raw_val_gertermica",
    "raw_val_gereolica",
    "raw_val_gersolar",
    "raw_val_intercambio",
    *ONS_LINEAGE_COLUMNS,
]

ONS_INTERCHANGE_SUBSYSTEM_HOURLY_COLUMNS = [
    "ref_datetime",
    "ref_date",
    "available_date",
    "availability_policy",
    "availability_basis",
    "revision_policy",
    "model_usable",
    *ONS_PIT_SNAPSHOT_COLUMNS,
    "availability_note",
    "source_subsystem_id",
    "source_subsystem",
    "target_subsystem_id",
    "target_subsystem",
    "interchange_mwmed",
    "programmed_interchange_mwmed",
    "unit",
    "raw_val_intercambiomwmed",
    "raw_val_intercambioprogmwmed",
    *ONS_LINEAGE_COLUMNS,
]

ONS_SILVER_COLUMNS_BY_DATASET = {
    "ons_ear_subsystem_daily": ONS_EAR_SUBSYSTEM_DAILY_COLUMNS,
    "ons_ena_subsystem_daily": ONS_ENA_SUBSYSTEM_DAILY_COLUMNS,
    "ons_load_daily": ONS_LOAD_DAILY_COLUMNS,
    "ons_cmo_weekly": ONS_CMO_WEEKLY_COLUMNS,
    "ons_energy_balance_subsystem": ONS_ENERGY_BALANCE_SUBSYSTEM_COLUMNS,
    "ons_interchange_subsystem_hourly": ONS_INTERCHANGE_SUBSYSTEM_HOURLY_COLUMNS,
}


def normalize_ons_to_silver(
    dataset_id: str,
    bronze: pl.DataFrame,
    *,
    source_version: str = "v0",
) -> pl.DataFrame:
    if dataset_id == "ons_ear_subsystem_daily":
        return normalize_ons_ear_subsystem_daily(bronze, source_version=source_version)
    if dataset_id == "ons_ena_subsystem_daily":
        return normalize_ons_ena_subsystem_daily(bronze, source_version=source_version)
    if dataset_id == "ons_load_daily":
        return normalize_ons_load_daily(bronze, source_version=source_version)
    if dataset_id == "ons_cmo_weekly":
        return normalize_ons_cmo_weekly(bronze, source_version=source_version)
    if dataset_id == "ons_energy_balance_subsystem":
        return normalize_ons_energy_balance_subsystem(bronze, source_version=source_version)
    if dataset_id == "ons_interchange_subsystem_hourly":
        return normalize_ons_interchange_subsystem_hourly(bronze, source_version=source_version)
    raise ValueError(f"Unsupported ONS silver dataset: {dataset_id}")


def normalize_ons_ear_subsystem_daily(
    bronze: pl.DataFrame,
    *,
    source_version: str = "v0",
) -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    for row in bronze.to_dicts():
        ref_date = _parse_date(_field(row, "ear_data"))
        rows.append(
            {
                "ref_date": ref_date,
                **_pit_timing(row, ref_date),
                "subsystem_id": _text(_field(row, "id_subsistema")),
                "subsystem": _text(_field(row, "nom_subsistema")),
                "stored_energy_mwmes": _decimal(_field(row, "ear_verif_subsistema_mwmes")),
                "stored_energy_percent": _decimal(
                    _field(row, "ear_verif_subsistema_percentual")
                ),
                "stored_energy_max_mwmes": _decimal(_field(row, "ear_max_subsistema")),
                "unit": "MWmes",
                "raw_ear_verif_subsistema_mwmes": _text(
                    _field(row, "ear_verif_subsistema_mwmes")
                ),
                "raw_ear_verif_subsistema_percentual": _text(
                    _field(row, "ear_verif_subsistema_percentual")
                ),
                "raw_ear_max_subsistema": _text(_field(row, "ear_max_subsistema")),
                **_lineage(row, source_version=source_version),
            }
        )
    return _frame(rows, ONS_EAR_SUBSYSTEM_DAILY_COLUMNS)


def normalize_ons_ena_subsystem_daily(
    bronze: pl.DataFrame,
    *,
    source_version: str = "v0",
) -> pl.DataFrame:
    specs = [
        ("bruta_mwmed", "ena_bruta_regiao_mwmed", "MWmed"),
        ("bruta_percentual_mlt", "ena_bruta_regiao_percentualmlt", "percent_mlt"),
        ("armazenavel_mwmed", "ena_armazenavel_regiao_mwmed", "MWmed"),
        (
            "armazenavel_percentual_mlt",
            "ena_armazenavel_regiao_percentualmlt",
            "percent_mlt",
        ),
    ]
    rows: list[dict[str, object]] = []
    for row in bronze.to_dicts():
        ref_date = _parse_date(_field(row, "ena_data"))
        common = {
            "ref_date": ref_date,
            **_pit_timing(row, ref_date),
            "subsystem_id": _text(_field(row, "id_subsistema")),
            "subsystem": _text(_field(row, "nom_subsistema")),
            **_lineage(row, source_version=source_version),
        }
        for ena_type, source_column, unit in specs:
            raw_value = _field(row, source_column)
            rows.append(
                {
                    **common,
                    "ena_type": ena_type,
                    "ena_value": _decimal(raw_value),
                    "unit": unit,
                    "raw_ena_value": _text(raw_value),
                }
            )
    return _frame(rows, ONS_ENA_SUBSYSTEM_DAILY_COLUMNS)


def normalize_ons_load_daily(
    bronze: pl.DataFrame,
    *,
    source_version: str = "v0",
) -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    for row in bronze.to_dicts():
        ref_date = _parse_date(_field(row, "din_instante"))
        raw_load = _field(row, "val_cargaenergiamwmed")
        rows.append(
            {
                "ref_date": ref_date,
                **_pit_timing(row, ref_date),
                "subsystem_id": _text(_field(row, "id_subsistema")),
                "subsystem": _text(_field(row, "nom_subsistema")),
                "load_mwmed": _decimal(raw_load),
                "unit": "MWmed",
                "methodology_note": _load_methodology_note(ref_date),
                "raw_val_cargaenergiamwmed": _text(raw_load),
                **_lineage(row, source_version=source_version),
            }
        )
    return _frame(rows, ONS_LOAD_DAILY_COLUMNS)


def normalize_ons_cmo_weekly(
    bronze: pl.DataFrame,
    *,
    source_version: str = "v0",
) -> pl.DataFrame:
    specs = [
        ("media_semanal", "val_cmomediasemanal"),
        ("leve", "val_cmoleve"),
        ("media", "val_cmomedia"),
        ("pesada", "val_cmopesada"),
    ]
    rows: list[dict[str, object]] = []
    for row in bronze.to_dicts():
        ref_date = _parse_date(_field(row, "din_instante"))
        common = {
            "ref_date": ref_date,
            **_pit_timing(row, ref_date),
            "subsystem_id": _text(_field(row, "id_subsistema")),
            "subsystem": _text(_field(row, "nom_subsistema")),
            **_lineage(row, source_version=source_version),
        }
        for load_block, source_column in specs:
            raw_value = _field(row, source_column)
            rows.append(
                {
                    **common,
                    "load_block": load_block,
                    "cmo_brl_mwh": _decimal(raw_value),
                    "unit": "BRL/MWh",
                    "raw_cmo_value": _text(raw_value),
                }
            )
    return _frame(rows, ONS_CMO_WEEKLY_COLUMNS)


def normalize_ons_energy_balance_subsystem(
    bronze: pl.DataFrame,
    *,
    source_version: str = "v0",
) -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    for row in bronze.to_dicts():
        ref_datetime = _parse_datetime(_field(row, "din_instante"))
        ref_date = ref_datetime.date() if ref_datetime is not None else None
        raw_load = _field(row, "val_carga")
        raw_hydro = _field(row, "val_gerhidraulica")
        raw_thermal = _field(row, "val_gertermica")
        raw_wind = _field(row, "val_gereolica")
        raw_solar = _field(row, "val_gersolar")
        raw_interchange = _field(row, "val_intercambio")
        rows.append(
            {
                "ref_datetime": ref_datetime,
                "ref_date": ref_date,
                **_pit_timing(row, ref_date),
                "subsystem_id": _text(_field(row, "id_subsistema")),
                "subsystem": _text(_field(row, "nom_subsistema")),
                "load_mwmed": _decimal(raw_load),
                "hydro_generation_mwmed": _decimal(raw_hydro),
                "thermal_generation_mwmed": _decimal(raw_thermal),
                "wind_generation_mwmed": _decimal(raw_wind),
                "solar_generation_mwmed": _decimal(raw_solar),
                "other_generation_mwmed": None,
                "interchange_mwmed": _decimal(raw_interchange),
                "unit": "MWmed",
                "raw_val_carga": _text(raw_load),
                "raw_val_gerhidraulica": _text(raw_hydro),
                "raw_val_gertermica": _text(raw_thermal),
                "raw_val_gereolica": _text(raw_wind),
                "raw_val_gersolar": _text(raw_solar),
                "raw_val_intercambio": _text(raw_interchange),
                **_lineage(row, source_version=source_version),
            }
        )
    return _frame(rows, ONS_ENERGY_BALANCE_SUBSYSTEM_COLUMNS)


def normalize_ons_interchange_subsystem_hourly(
    bronze: pl.DataFrame,
    *,
    source_version: str = "v0",
) -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    for row in bronze.to_dicts():
        ref_datetime = _parse_datetime(_field(row, "din_instante"))
        ref_date = ref_datetime.date() if ref_datetime is not None else None
        raw_interchange = _field(row, "val_intercambiomwmed")
        raw_programmed = _field(row, "val_intercambioprogmwmed")
        rows.append(
            {
                "ref_datetime": ref_datetime,
                "ref_date": ref_date,
                **_pit_timing(row, ref_date),
                "source_subsystem_id": _text(_field(row, "id_subsistema_origem")),
                "source_subsystem": _text(_field(row, "nom_subsistema_origem")),
                "target_subsystem_id": _text(_field(row, "id_subsistema_destino")),
                "target_subsystem": _text(_field(row, "nom_subsistema_destino")),
                "interchange_mwmed": _decimal(raw_interchange),
                "programmed_interchange_mwmed": _decimal(raw_programmed),
                "unit": "MWmed",
                "raw_val_intercambiomwmed": _text(raw_interchange),
                "raw_val_intercambioprogmwmed": _text(raw_programmed),
                **_lineage(row, source_version=source_version),
            }
        )
    return _frame(rows, ONS_INTERCHANGE_SUBSYSTEM_HOURLY_COLUMNS)


def write_ons_silver(
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
        "source": row.get("source", "ons"),
        "source_dataset": row.get("source_dataset"),
        "download_timestamp_utc": row.get("download_timestamp_utc"),
        "raw_path": row.get("raw_path"),
        "sha256": row.get("sha256"),
        "resource_name": row.get("resource_name"),
        "source_version": source_version,
    }


def _pit_timing(row: dict[str, object], ref_date: date | None) -> dict[str, object]:
    timestamps = _pit_timestamp_fields(row)
    source_timestamp = timestamps["source_publication_datetime_utc"]
    if source_timestamp is not None:
        vintage_id = _vintage_id(row, source_timestamp, timestamps)
        return {
            "available_date": available_date_from_source_datetime(source_timestamp),
            "availability_policy": ONS_SOURCE_TIMESTAMP_POLICY,
            "availability_basis": AVAILABILITY_EXACT_SOURCE_TIMESTAMP,
            "revision_policy": REVISION_REVISED_USE_VINTAGES,
            "model_usable": True,
            "vintage_id": vintage_id,
            **timestamps,
            "availability_note": ONS_TIMESTAMP_AVAILABILITY_NOTE,
        }

    resource_timestamp = timestamps["resource_last_modified"] or timestamps["http_last_modified"]
    if resource_timestamp is not None:
        vintage_id = _vintage_id(row, resource_timestamp, timestamps)
        return {
            "available_date": available_date_from_source_datetime(resource_timestamp),
            "availability_policy": ONS_RESOURCE_TIMESTAMP_POLICY,
            "availability_basis": AVAILABILITY_SOURCE_LAST_MODIFIED,
            "revision_policy": REVISION_REVISED_USE_VINTAGES,
            "model_usable": True,
            "vintage_id": vintage_id,
            **timestamps,
            "availability_note": ONS_TIMESTAMP_AVAILABILITY_NOTE,
        }

    first_seen_timestamp = timestamps["first_seen_timestamp_utc"]
    if first_seen_timestamp is not None:
        vintage_id = _vintage_id(row, first_seen_timestamp, timestamps, content_snapshot=True)
        return {
            "available_date": available_date_from_first_seen(first_seen_timestamp),
            "availability_policy": ONS_FIRST_SEEN_POLICY,
            "availability_basis": AVAILABILITY_FIRST_SEEN_DOWNLOAD_TIMESTAMP,
            "revision_policy": REVISION_REVISED_USE_FIRST_SEEN,
            "model_usable": True,
            "vintage_id": vintage_id,
            **timestamps,
            "availability_note": ONS_FIRST_SEEN_AVAILABILITY_NOTE,
        }

    return {
        "available_date": _available_date(ref_date),
        "availability_policy": ONS_AVAILABILITY_POLICY,
        **_pit_reference_only(),
        **timestamps,
    }


def _pit_timestamp_fields(row: dict[str, object]) -> dict[str, datetime | None]:
    return {
        "source_publication_datetime_utc": _timestamp_field(
            row, "source_publication_datetime_utc"
        ),
        "resource_last_modified": _timestamp_field(row, "resource_last_modified"),
        "http_last_modified": _timestamp_field(row, "http_last_modified"),
        "first_seen_timestamp_utc": _timestamp_field(row, "first_seen_timestamp_utc"),
    }


def _timestamp_field(row: dict[str, object], *keys: str) -> datetime | None:
    for key in keys:
        value = row.get(key)
        if value is None or value == "":
            continue
        if isinstance(value, datetime):
            return value
        text = str(value).strip()
        if not text:
            continue
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    return None


def _vintage_id(
    row: dict[str, object],
    publication_timestamp: datetime,
    timestamps: dict[str, datetime | None],
    *,
    content_snapshot: bool = False,
) -> str:
    content_hash = str(row.get("sha256") or "")
    publication_for_id = None if content_snapshot and content_hash else publication_timestamp
    first_seen_for_id = (
        None
        if content_snapshot and content_hash
        else timestamps["first_seen_timestamp_utc"]
    )
    return make_vintage_id(
        source=str(row.get("source") or "ons"),
        dataset_id=str(row.get("source_dataset") or "ons"),
        resource_id=str(row.get("resource_name") or row.get("raw_path") or "ons_resource"),
        publication_timestamp=publication_for_id,
        first_seen_timestamp_utc=first_seen_for_id,
        content_hash=content_hash,
    )


def _available_date(value: date | None) -> date | None:
    if value is None:
        return None
    return usable_date_from_date_only(value)


def _pit_reference_only() -> dict[str, object]:
    return {
        "availability_basis": AVAILABILITY_CURRENT_SNAPSHOT_NO_VINTAGE,
        "revision_policy": REVISION_CURRENT_SNAPSHOT_REFERENCE_ONLY,
        "model_usable": False,
        "vintage_id": None,
        "availability_note": ONS_AVAILABILITY_NOTE,
    }


def _decimal(value: object) -> float | None:
    return parse_decimal(value)


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
        return value.replace(tzinfo=None)
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day)
    text = str(value).strip()
    if not text:
        return None
    if "/" in text:
        date_part, _, time_part = text.partition(" ")
        day, month, year = date_part.split("/")
        text = f"{year}-{month}-{day}" + (f" {time_part}" if time_part else "")
    return datetime.fromisoformat(text.replace("Z", "+00:00")).replace(tzinfo=None)


def _load_methodology_note(ref_date: date | None) -> str | None:
    if ref_date is None:
        return None
    if ref_date < date(2021, 3, 1):
        return "pre_2021_03_scada_dispatched_programmed_basis"
    if ref_date <= date(2023, 4, 28):
        return "2021_03_to_2023_04_28_includes_non_dispatched_generation_estimate"
    return "from_2023_04_29_includes_estimated_mmgd"


def _frame(rows: list[dict[str, object]], columns: list[str]) -> pl.DataFrame:
    if not rows:
        return pl.DataFrame(schema={column: pl.Null for column in columns})
    return pl.DataFrame(rows).select(columns)
