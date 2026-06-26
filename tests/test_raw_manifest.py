from __future__ import annotations

import json
from datetime import UTC, datetime

from bralpha.infra.hashing import sha256_bytes, sha256_file
from bralpha.infra.raw_store import RawStore
from bralpha.metadata.manifest import ManifestRecord, ManifestWriter


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
    assert lines[1]["success"] is False
    assert lines[1]["error_message"] == "server error"
