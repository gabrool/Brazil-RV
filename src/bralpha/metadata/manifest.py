from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_serializer


class ManifestRecord(BaseModel):
    model_config = ConfigDict(extra="forbid")

    dataset_id: str
    source: str
    source_url: str
    request_params: dict[str, Any] = Field(default_factory=dict)
    download_timestamp_utc: datetime
    http_status: int | None = None
    content_type: str | None = None
    http_last_modified: datetime | None = None
    ckan_resource_last_modified: datetime | None = None
    resource_last_modified: datetime | None = None
    resource_updated_at: datetime | None = None
    source_publication_datetime_utc: datetime | None = None
    resource_url: str | None = None
    resource_name: str | None = None
    file_size_bytes: int = 0
    sha256: str | None = None
    raw_path: str | None = None
    license_note: str
    success: bool
    error_message: str | None = None

    @field_serializer("download_timestamp_utc")
    def serialize_timestamp(self, value: datetime) -> str:
        return value.astimezone(UTC).isoformat()


class ManifestWriter:
    """Append JSONL manifests for v0 single-process ingestion.

    This writer intentionally does not implement cross-process locking. Run
    independent ingestion jobs with separate manifest files or external
    serialization until a concurrent ingestion mode is explicitly designed.
    """

    def __init__(self, path: Path) -> None:
        self.path = path

    def append(self, record: ManifestRecord) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = record.model_dump(mode="json")
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, sort_keys=True, separators=(",", ":")))
            handle.write("\n")


def response_manifest_metadata(
    *,
    source_url: str,
    headers: Mapping[str, str],
    request_params: Mapping[str, Any],
) -> dict[str, object]:
    return {
        "http_last_modified": parse_metadata_datetime(_header(headers, "last-modified")),
        "ckan_resource_last_modified": parse_metadata_datetime(
            request_params.get("ckan_resource_last_modified")
        ),
        "resource_last_modified": parse_metadata_datetime(
            request_params.get("resource_last_modified")
            or request_params.get("resource_modified")
            or request_params.get("last_modified")
        ),
        "resource_updated_at": parse_metadata_datetime(
            request_params.get("resource_updated_at") or request_params.get("updated_at")
        ),
        "source_publication_datetime_utc": parse_metadata_datetime(
            request_params.get("source_publication_datetime_utc")
            or request_params.get("source_publication_datetime")
        ),
        "resource_url": str(request_params.get("resource_url") or source_url),
        "resource_name": _optional_text(request_params.get("resource_name")),
    }


def manifest_bronze_metadata(record: ManifestRecord) -> dict[str, object]:
    return {
        "http_last_modified": _naive_utc(record.http_last_modified),
        "ckan_resource_last_modified": _naive_utc(record.ckan_resource_last_modified),
        "resource_last_modified": _naive_utc(record.resource_last_modified),
        "resource_updated_at": _naive_utc(record.resource_updated_at),
        "source_publication_datetime_utc": _naive_utc(
            record.source_publication_datetime_utc
        ),
        "first_seen_timestamp_utc": _naive_utc(record.download_timestamp_utc),
        "resource_url": record.resource_url,
        "resource_name": record.resource_name,
    }


def parse_metadata_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        timestamp = value
    else:
        text = str(value).strip()
        if not text:
            return None
        try:
            timestamp = parsedate_to_datetime(text)
        except (TypeError, ValueError):
            timestamp = datetime.fromisoformat(text.replace("Z", "+00:00"))
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=UTC)
    return timestamp.astimezone(UTC)


def _header(headers: Mapping[str, str], name: str) -> str | None:
    normalized = name.lower()
    for key, value in headers.items():
        if key.lower() == normalized:
            return value
    return None


def _optional_text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _naive_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    timestamp = value if value.tzinfo is not None else value.replace(tzinfo=UTC)
    return timestamp.astimezone(UTC).replace(tzinfo=None)
