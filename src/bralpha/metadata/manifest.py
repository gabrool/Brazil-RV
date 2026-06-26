from __future__ import annotations

import json
from datetime import UTC, datetime
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
    def __init__(self, path: Path) -> None:
        self.path = path

    def append(self, record: ManifestRecord) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = record.model_dump(mode="json")
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, sort_keys=True, separators=(",", ":")))
            handle.write("\n")
