from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl

from bralpha.metadata.manifest import ManifestRecord
from bralpha.parsing.common import normalize_column_name, parse_decimal, write_source_partitioned

FEE_SCHEDULE_COLUMNS = [
    "ref_date",
    "available_date",
    "fee_id",
    "product",
    "investor_type",
    "fee_type",
    "fee_value",
    "fee_unit",
    "market_segment",
    "currency",
    "source",
    "source_dataset",
    "download_timestamp_utc",
    "raw_path",
    "sha256",
    "source_version",
]

PRODUCT_SPEC_METADATA_COLUMNS = [
    "download_date",
    "available_date",
    "product_root",
    "product_name",
    "page_url",
    "content_type",
    "source",
    "source_dataset",
    "download_timestamp_utc",
    "raw_path",
    "sha256",
    "source_version",
]

RAW_REPORT_METADATA_COLUMNS = [
    "ref_date",
    "download_date",
    "available_date",
    "report_name",
    "report_section",
    "report_category",
    "page_url",
    "content_type",
    "source",
    "source_dataset",
    "download_timestamp_utc",
    "raw_path",
    "sha256",
    "source_version",
]


def normalize_fee_schedule_table(
    bronze: pl.DataFrame,
    *,
    source_version: str = "v0",
) -> pl.DataFrame:
    rows = []
    for row in bronze.to_dicts():
        download_date = _required_date(row.get("download_date"))
        rows.append(
            {
                "ref_date": _optional_date(row.get("ref_date")) or download_date,
                "available_date": _optional_date(row.get("available_date")) or download_date,
                "fee_id": _text(row.get("fee_id")),
                "product": _first_text(row, "product", "produto", "contract", "contrato"),
                "investor_type": _first_upper(row, "investor_type", "tipo_investidor"),
                "fee_type": _first_upper(row, "fee_type", "type", "tipo", "tarifa"),
                "fee_value": parse_decimal(
                    _first_value(row, "fee_value", "value", "valor", "price")
                ),
                "fee_unit": _first_upper(row, "fee_unit", "unit", "unidade"),
                "market_segment": _first_upper(row, "market_segment", "segment", "mercado"),
                "currency": _first_upper(row, "currency", "moeda") or "BRL",
                "source": row.get("source", "b3"),
                "source_dataset": row.get("source_dataset", "b3_fee_schedules"),
                "download_timestamp_utc": row.get("download_timestamp_utc"),
                "raw_path": row.get("raw_path"),
                "sha256": row.get("sha256"),
                "source_version": source_version,
            }
        )
    return _frame(rows, FEE_SCHEDULE_COLUMNS)


def normalize_product_spec_metadata(
    record: ManifestRecord,
    *,
    product_root: str,
    product_name: str,
    page_url: str,
    source_version: str = "v0",
) -> pl.DataFrame:
    download_date = record.download_timestamp_utc.date()
    return _frame(
        [
            {
                "download_date": download_date,
                "available_date": download_date,
                "product_root": product_root,
                "product_name": product_name,
                "page_url": page_url,
                "content_type": record.content_type,
                "source": record.source,
                "source_dataset": record.dataset_id,
                "download_timestamp_utc": record.download_timestamp_utc,
                "raw_path": record.raw_path,
                "sha256": record.sha256,
                "source_version": source_version,
            }
        ],
        PRODUCT_SPEC_METADATA_COLUMNS,
    )


def normalize_raw_report_metadata(
    record: ManifestRecord,
    *,
    ref_date: date,
    report_name: str,
    report_section: str | None = None,
    report_category: str | None = None,
    page_url: str | None = None,
    source_version: str = "v0",
) -> pl.DataFrame:
    download_date = record.download_timestamp_utc.date()
    return _frame(
        [
            {
                "ref_date": ref_date,
                "download_date": download_date,
                "available_date": download_date,
                "report_name": report_name,
                "report_section": report_section,
                "report_category": report_category,
                "page_url": page_url or record.source_url,
                "content_type": record.content_type,
                "source": record.source,
                "source_dataset": record.dataset_id,
                "download_timestamp_utc": record.download_timestamp_utc,
                "raw_path": record.raw_path,
                "sha256": record.sha256,
                "source_version": source_version,
            }
        ],
        RAW_REPORT_METADATA_COLUMNS,
    )


def write_fee_schedule(
    frame: pl.DataFrame,
    output_root: Path,
    *,
    primary_keys: list[str],
) -> list[Path]:
    return _write_single_file_table(frame, output_root, primary_keys=primary_keys)


def write_product_spec_metadata(
    frame: pl.DataFrame,
    output_root: Path,
    *,
    primary_keys: list[str],
) -> list[Path]:
    return _write_single_file_table(frame, output_root, primary_keys=primary_keys)


def write_raw_report_metadata(
    frame: pl.DataFrame,
    output_root: Path,
    *,
    primary_keys: list[str],
) -> list[Path]:
    return write_source_partitioned(
        frame,
        output_root,
        ref_date_col="ref_date",
        primary_keys=primary_keys,
    )


def _write_single_file_table(
    frame: pl.DataFrame,
    output_root: Path,
    *,
    primary_keys: list[str],
) -> list[Path]:
    if frame.is_empty():
        return []
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


def _first_text(row: dict[str, object], *keys: str) -> str | None:
    for key in keys:
        text = _stripped(row.get(normalize_column_name(key)))
        if text:
            return text
    return None


def _first_upper(row: dict[str, object], *keys: str) -> str | None:
    text = _first_text(row, *keys)
    return text.upper() if text else None


def _first_value(row: dict[str, object], *keys: str) -> object:
    for key in keys:
        value = row.get(normalize_column_name(key))
        if value is not None and str(value).strip():
            return value
    return None


def _text(value: object) -> str | None:
    text = _stripped(value)
    return text.upper() if text else None


def _stripped(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None
