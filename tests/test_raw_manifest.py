from __future__ import annotations

import json
from datetime import UTC, datetime

from bralpha.infra.hashing import sha256_bytes, sha256_file
from bralpha.infra.raw_store import RawStore
from bralpha.metadata.manifest import (
    ManifestRecord,
    ManifestWriter,
    manifest_bronze_metadata,
    response_manifest_metadata,
)


def test_sha256_is_stable(tmp_path):
    path = tmp_path / "sample.bin"
    path.write_bytes(b"abc")

    assert sha256_bytes(b"abc") == sha256_file(path)


def test_raw_writes_are_atomic_and_deterministic(tmp_path):
    store = RawStore(tmp_path / "raw")
    downloaded_at = datetime(2024, 1, 2, 12, tzinfo=UTC)

    first = store.write_bytes("dataset", b"payload", "file.csv", downloaded_at)
    second = store.write_bytes("dataset", b"payload", "file.csv", downloaded_at)
    third = store.write_bytes("dataset", b"different", "file.csv", downloaded_at)

    assert first == second
    assert first.read_bytes() == b"payload"
    assert third != first
    assert third.read_bytes() == b"different"


def test_manifest_success_and_failure_records_serialize(tmp_path):
    manifest_path = tmp_path / "downloads.jsonl"
    writer = ManifestWriter(manifest_path)
    timestamp = datetime(2024, 1, 2, 12, tzinfo=UTC)

    writer.append(
        ManifestRecord(
            dataset_id="dataset",
            source="b3",
            source_url="https://example.test/success",
            request_params={"a": "b"},
            download_timestamp_utc=timestamp,
            http_status=200,
            content_type="text/csv",
            http_last_modified=datetime(2024, 1, 2, 13, tzinfo=UTC),
            resource_url="https://example.test/success",
            resource_name="success.csv",
            file_size_bytes=3,
            sha256=sha256_bytes(b"abc"),
            raw_path="data/raw/file.csv",
            license_note="test",
            success=True,
        )
    )
    writer.append(
        ManifestRecord(
            dataset_id="dataset",
            source="b3",
            source_url="https://example.test/fail",
            request_params={},
            download_timestamp_utc=timestamp,
            http_status=500,
            content_type="text/html",
            file_size_bytes=0,
            sha256=None,
            raw_path=None,
            license_note="test",
            success=False,
            error_message="server error",
        )
    )

    lines = [json.loads(line) for line in manifest_path.read_text(encoding="utf-8").splitlines()]
    assert lines[0]["success"] is True
    assert lines[0]["sha256"] == sha256_bytes(b"abc")
    assert lines[0]["http_last_modified"] == "2024-01-02T13:00:00Z"
    assert lines[0]["resource_name"] == "success.csv"
    assert lines[1]["success"] is False
    assert lines[1]["error_message"] == "server error"


def test_manifest_metadata_extracts_response_and_resource_timestamps():
    metadata = response_manifest_metadata(
        source_url="https://example.test/data.csv",
        headers={"Last-Modified": "Tue, 02 Jan 2024 13:30:00 GMT"},
        request_params={
            "resource_name": "data.csv",
            "resource_last_modified": "2024-01-02T12:00:00Z",
            "resource_updated_at": "2024-01-02T12:30:00Z",
            "source_publication_datetime_utc": "2024-01-02T11:00:00Z",
        },
    )

    assert metadata["http_last_modified"] == datetime(2024, 1, 2, 13, 30, tzinfo=UTC)
    assert metadata["resource_last_modified"] == datetime(2024, 1, 2, 12, tzinfo=UTC)
    assert metadata["resource_updated_at"] == datetime(2024, 1, 2, 12, 30, tzinfo=UTC)
    assert metadata["source_publication_datetime_utc"] == datetime(
        2024, 1, 2, 11, tzinfo=UTC
    )
    assert metadata["resource_url"] == "https://example.test/data.csv"
    assert metadata["resource_name"] == "data.csv"


def test_manifest_bronze_metadata_uses_download_timestamp_as_first_seen():
    record = ManifestRecord(
        dataset_id="dataset",
        source="ons",
        source_url="https://example.test/data.csv",
        request_params={},
        download_timestamp_utc=datetime(2024, 1, 3, 14, tzinfo=UTC),
        http_last_modified=datetime(2024, 1, 2, 13, tzinfo=UTC),
        resource_url="https://example.test/data.csv",
        resource_name="data.csv",
        file_size_bytes=3,
        license_note="test",
        success=True,
    )

    metadata = manifest_bronze_metadata(record)

    assert metadata["first_seen_timestamp_utc"] == datetime(2024, 1, 3, 14)
    assert metadata["http_last_modified"] == datetime(2024, 1, 2, 13)
    assert metadata["resource_url"] == "https://example.test/data.csv"
    assert metadata["resource_name"] == "data.csv"
