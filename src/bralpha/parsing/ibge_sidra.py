from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import polars as pl

from bralpha.ingestion.ibge.common import write_bronze_frame

IBGE_SIDRA_BRONZE_COLUMNS = [
    "dataset_slug",
    "aggregate_id",
    "variable_id",
    "variable_name",
    "unit",
    "period_code",
    "period_label",
    "geography_level",
    "geography_id",
    "geography_name",
    "classification_key",
    "classifications_json",
    "raw_value",
    "source",
    "source_dataset",
    "download_timestamp_utc",
    "raw_path",
    "sha256",
]


def parse_sidra_bytes(
    content: bytes,
    *,
    dataset_slug: str,
    aggregate_id: int,
    source_dataset: str,
    download_timestamp_utc: datetime,
    raw_path: Path,
    sha256: str,
) -> pl.DataFrame:
    rows = []
    timestamp = _naive_utc(download_timestamp_utc)
    for variable in _payload_rows(content):
        variable_id = str(variable.get("id", ""))
        variable_name = variable.get("variavel")
        unit = variable.get("unidade")
        for result in _list_of_dicts(variable.get("resultados")):
            classifications = _classification_entries(result.get("classificacoes"))
            classification_key = _classification_key(classifications)
            classifications_json = json.dumps(
                classifications,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
            for series in _list_of_dicts(result.get("series")):
                locality_value = series.get("localidade")
                locality = locality_value if isinstance(locality_value, dict) else {}
                geography_level = _nested(locality, "nivel", "id")
                geography_id = locality.get("id")
                geography_name = locality.get("nome")
                observations = series.get("serie") if isinstance(series.get("serie"), dict) else {}
                for period_code, raw_value in observations.items():
                    rows.append(
                        {
                            "dataset_slug": dataset_slug,
                            "aggregate_id": aggregate_id,
                            "variable_id": variable_id,
                            "variable_name": variable_name,
                            "unit": unit,
                            "period_code": str(period_code),
                            "period_label": None,
                            "geography_level": geography_level,
                            "geography_id": str(geography_id) if geography_id is not None else None,
                            "geography_name": geography_name,
                            "classification_key": classification_key,
                            "classifications_json": classifications_json,
                            "raw_value": None if raw_value is None else str(raw_value),
                            "source": "ibge",
                            "source_dataset": source_dataset,
                            "download_timestamp_utc": timestamp,
                            "raw_path": str(raw_path),
                            "sha256": sha256,
                        }
                    )
    return _frame(rows)


def parse_sidra_file(
    raw_path: Path,
    *,
    dataset_slug: str,
    aggregate_id: int,
    source_dataset: str,
    download_timestamp_utc: datetime,
    sha256: str,
) -> pl.DataFrame:
    return parse_sidra_bytes(
        raw_path.read_bytes(),
        dataset_slug=dataset_slug,
        aggregate_id=aggregate_id,
        source_dataset=source_dataset,
        download_timestamp_utc=download_timestamp_utc,
        raw_path=raw_path,
        sha256=sha256,
    )


def write_sidra_bronze(frame: pl.DataFrame, output_root: Path) -> list[Path]:
    return write_bronze_frame(
        frame,
        output_root,
        primary_keys=[
            "dataset_slug",
            "aggregate_id",
            "variable_id",
            "period_code",
            "geography_id",
            "classification_key",
        ],
        partition_cols=["dataset_slug"],
    )


def _payload_rows(content: bytes) -> list[dict[str, Any]]:
    payload = json.loads(content.decode("utf-8-sig"))
    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, dict)]
    if isinstance(payload, dict) and isinstance(payload.get("value"), list):
        return [row for row in payload["value"] if isinstance(row, dict)]
    raise ValueError("SIDRA JSON payload must be a list or contain a value list")


def _classification_entries(value: object) -> list[dict[str, str | None]]:
    entries = []
    for classification in _list_of_dicts(value):
        classification_id = _as_text(classification.get("id"))
        classification_name = _as_text(classification.get("nome"))
        categories = classification.get("categoria")
        if not isinstance(categories, dict):
            categories = classification.get("categorias")
        if isinstance(categories, dict):
            sorted_categories = sorted(categories.items(), key=lambda item: str(item[0]))
            for category_id, category_name in sorted_categories:
                entries.append(
                    {
                        "classification_id": classification_id,
                        "classification_name": classification_name,
                        "category_id": _as_text(category_id),
                        "category_name": _as_text(category_name),
                    }
                )
        else:
            entries.append(
                {
                    "classification_id": classification_id,
                    "classification_name": classification_name,
                    "category_id": None,
                    "category_name": None,
                }
            )
    return sorted(
        entries,
        key=lambda item: (item.get("classification_id") or "", item.get("category_id") or ""),
    )


def _classification_key(entries: list[dict[str, str | None]]) -> str:
    if not entries:
        return "__none__"
    return "|".join(
        f"{entry.get('classification_id') or ''}={entry.get('category_id') or ''}"
        for entry in entries
    )


def _list_of_dicts(value: object) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, dict)]


def _nested(row: dict[str, Any], *keys: str) -> object | None:
    current: object = row
    for key in keys:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def _as_text(value: object) -> str | None:
    return None if value is None else str(value)


def _frame(rows: list[dict[str, object]]) -> pl.DataFrame:
    if not rows:
        return pl.DataFrame(schema={column: pl.Null for column in IBGE_SIDRA_BRONZE_COLUMNS})
    return pl.DataFrame(rows).select(IBGE_SIDRA_BRONZE_COLUMNS)


def _naive_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value
    return value.astimezone(UTC).replace(tzinfo=None)
