from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl

from bralpha.parsing.common import parse_decimal, parse_int, write_source_partitioned
from bralpha.timing.availability import usable_date_from_date_only

BCB_FOCUS_EXPECTATION_COLUMNS = [
    "ref_date",
    "available_date",
    "endpoint",
    "indicator",
    "indicator_detail",
    "reference_period",
    "reference_year",
    "reference_month",
    "meeting",
    "horizon_label",
    "is_top5",
    "calculation_type",
    "statistic_scope",
    "mean",
    "median",
    "std_dev",
    "min_value",
    "max_value",
    "respondents",
    "base_calculation",
    "source",
    "source_dataset",
    "download_timestamp_utc",
    "raw_path",
    "sha256",
    "source_version",
]

BCB_FOCUS_REFERENCE_DATE_COLUMNS = [
    "indicator",
    "period",
    "reference_date_type",
    "reference_date",
    "available_date",
    "raw_reference_date",
    "source",
    "source_dataset",
    "download_timestamp_utc",
    "raw_path",
    "sha256",
    "source_version",
]

FOCUS_REFERENCE_DATE_PRIMARY_KEYS = [
    "indicator",
    "period",
    "reference_date_type",
    "reference_date",
]

FOCUS_EXPECTATION_PRIMARY_KEYS = [
    "endpoint",
    "ref_date",
    "indicator",
    "indicator_detail",
    "reference_period",
    "meeting",
    "calculation_type",
    "base_calculation",
]


def normalize_focus_expectations_to_silver(
    bronze: pl.DataFrame,
    *,
    source_version: str = "v0",
) -> pl.DataFrame:
    rows = []
    for row in bronze.to_dicts():
        ref_date = _date(row.get("Data"))
        if ref_date is None:
            continue
        reference_period = _text(row.get("DataReferencia"))
        reference_year, reference_month = _reference_parts(reference_period)
        base_calculation = parse_int(row.get("baseCalculo"))
        rows.append(
            {
                "ref_date": ref_date,
                "available_date": usable_date_from_date_only(ref_date),
                "endpoint": row.get("endpoint"),
                "indicator": _text(row.get("Indicador") or row.get("indicador")),
                "indicator_detail": _text(row.get("IndicadorDetalhe") or row.get("Suavizada")),
                "reference_period": reference_period,
                "reference_year": reference_year,
                "reference_month": reference_month,
                "meeting": _text(row.get("Reuniao") or row.get("reuniao")),
                "horizon_label": _horizon_label(row.get("endpoint"), reference_period),
                "is_top5": "Top5" in str(row.get("endpoint") or ""),
                "calculation_type": _text(row.get("tipoCalculo")),
                "statistic_scope": str(base_calculation) if base_calculation is not None else None,
                "mean": parse_decimal(row.get("Media") or row.get("media")),
                "median": parse_decimal(row.get("Mediana") or row.get("mediana")),
                "std_dev": parse_decimal(row.get("DesvioPadrao") or row.get("desvioPadrao")),
                "min_value": parse_decimal(row.get("Minimo") or row.get("minimo")),
                "max_value": parse_decimal(row.get("Maximo") or row.get("maximo")),
                "respondents": parse_int(row.get("numeroRespondentes")),
                "base_calculation": base_calculation,
                "source": row.get("source", "bcb"),
                "source_dataset": row.get("source_dataset"),
                "download_timestamp_utc": row.get("download_timestamp_utc"),
                "raw_path": row.get("raw_path"),
                "sha256": row.get("sha256"),
                "source_version": source_version,
            }
        )
    frame = _expectation_frame(rows)
    if frame.is_empty():
        return frame
    return frame.unique(subset=FOCUS_EXPECTATION_PRIMARY_KEYS, keep="last", maintain_order=True)


def normalize_focus_reference_dates_to_silver(
    bronze: pl.DataFrame,
    *,
    source_version: str = "v0",
) -> pl.DataFrame:
    rows = []
    for row in bronze.to_dicts():
        for field in ["DataReferencia1", "DataReferencia2"]:
            reference_date = _date(row.get(field))
            if reference_date is None:
                continue
            rows.append(
                {
                    "indicator": _text(row.get("Indicador")),
                    "period": _text(row.get("periodo")),
                    "reference_date_type": field,
                    "reference_date": reference_date,
                    "available_date": reference_date,
                    "raw_reference_date": row.get(field),
                    "source": row.get("source", "bcb"),
                    "source_dataset": row.get("source_dataset"),
                    "download_timestamp_utc": row.get("download_timestamp_utc"),
                    "raw_path": row.get("raw_path"),
                    "sha256": row.get("sha256"),
                    "source_version": source_version,
                }
            )
    frame = _reference_frame(rows)
    if frame.is_empty():
        return frame
    return frame.unique(
        subset=FOCUS_REFERENCE_DATE_PRIMARY_KEYS,
        keep="last",
        maintain_order=True,
    )


def write_focus_expectations_silver(frame: pl.DataFrame, output_root: Path) -> list[Path]:
    return write_source_partitioned(
        frame,
        output_root,
        primary_keys=FOCUS_EXPECTATION_PRIMARY_KEYS,
    )


def write_focus_reference_dates_silver(frame: pl.DataFrame, output_root: Path) -> list[Path]:
    return write_source_partitioned(
        frame,
        output_root,
        ref_date_col="reference_date",
        primary_keys=FOCUS_REFERENCE_DATE_PRIMARY_KEYS,
    )


def _date(value: object) -> date | None:
    if value is None:
        return None
    if isinstance(value, date):
        return value
    text = str(value).strip()
    if not text:
        return None
    return date.fromisoformat(text[:10])


def _reference_parts(reference_period: str | None) -> tuple[int | None, int | None]:
    if not reference_period:
        return None, None
    parts = reference_period.split("/")
    if len(parts) == 2 and all(part.isdigit() for part in parts):
        first, second = (int(part) for part in parts)
        if first <= 12:
            return second, first
        return first, second
    if len(parts) == 1 and parts[0].isdigit() and len(parts[0]) == 4:
        return int(parts[0]), None
    return None, None


def _horizon_label(endpoint: object, reference_period: str | None) -> str | None:
    text = str(endpoint or "")
    if "Inflacao12Meses" in text:
        return "12m"
    if "Inflacao24Meses" in text:
        return "24m"
    return reference_period


def _text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _expectation_frame(rows: list[dict[str, object]]) -> pl.DataFrame:
    if not rows:
        return pl.DataFrame(schema={column: pl.Null for column in BCB_FOCUS_EXPECTATION_COLUMNS})
    return pl.DataFrame(rows).select(BCB_FOCUS_EXPECTATION_COLUMNS)


def _reference_frame(rows: list[dict[str, object]]) -> pl.DataFrame:
    if not rows:
        return pl.DataFrame(schema={column: pl.Null for column in BCB_FOCUS_REFERENCE_DATE_COLUMNS})
    return pl.DataFrame(rows).select(BCB_FOCUS_REFERENCE_DATE_COLUMNS)
