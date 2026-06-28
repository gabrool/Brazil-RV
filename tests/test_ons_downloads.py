from __future__ import annotations

import json
import shutil
from datetime import UTC, date, datetime

import pytest

from bralpha.infra.http import HttpResponse
from bralpha.ingestion.ons.downloads import ONSDatasetNotLiveError, download_ons_dataset


class MockONSClient:
    def __init__(self, *, status_code: int = 200) -> None:
        self.status_code = status_code
        self.requests = []

    def get_bytes(self, url, params=None, headers=None):
        self.requests.append({"url": url, "params": params or {}, "headers": headers or {}})
        year = "2025" if "2025" in url else "2024"
        content = (
            "id_subsistema;nom_subsistema;ear_data;ear_max_subsistema;"
            "ear_verif_subsistema_mwmes;ear_verif_subsistema_percentual\n"
            f"SE;Sudeste;{year}-01-01;100;50;50\n"
        ).encode()
        return HttpResponse(
            url=url,
            status_code=self.status_code,
            headers={"content-type": "text/csv"},
            content=content,
        )


def test_ons_downloads_write_raw_files_and_manifest_only(repo_root, tmp_path):
    shutil.copytree(repo_root / "configs", tmp_path / "configs")
    client = MockONSClient()

    results = download_ons_dataset(
        tmp_path,
        "ons_ear_subsystem_daily",
        start=date(2024, 12, 31),
        end=date(2025, 1, 1),
        client=client,
        downloaded_at=datetime(2025, 1, 2, 12, tzinfo=UTC),
    )

    assert len(results) == 2
    assert [request["url"].split("/")[-1] for request in client.requests] == [
        "EAR_DIARIO_SUBSISTEMA_2024.csv",
        "EAR_DIARIO_SUBSISTEMA_2025.csv",
    ]
    assert all(result.raw_path is not None for result in results)
    assert not (tmp_path / "data" / "bronze").exists()
    manifest = tmp_path / "data" / "manifests" / "ons" / "downloads.jsonl"
    records = [json.loads(line) for line in manifest.read_text().splitlines()]
    assert [record["request_params"]["year"] for record in records] == [2024, 2025]
    assert all(record["success"] is True for record in records)


def test_ons_http_failure_writes_failure_manifest_without_raw(repo_root, tmp_path):
    shutil.copytree(repo_root / "configs", tmp_path / "configs")

    results = download_ons_dataset(
        tmp_path,
        "ons_ear_subsystem_daily",
        start=date(2024, 1, 1),
        end=date(2024, 1, 2),
        client=MockONSClient(status_code=503),
        downloaded_at=datetime(2024, 1, 3, 12, tzinfo=UTC),
    )

    assert results[0].raw_path is None
    assert not (tmp_path / "data" / "raw").exists()
    manifest = tmp_path / "data" / "manifests" / "ons" / "downloads.jsonl"
    record = json.loads(manifest.read_text())
    assert record["success"] is False
    assert record["http_status"] == 503
    assert record["raw_path"] is None


def test_ons_source_map_only_dataset_fails_without_data_writes(repo_root, tmp_path):
    shutil.copytree(repo_root / "configs", tmp_path / "configs")

    with pytest.raises(ONSDatasetNotLiveError, match="source-map-only"):
        download_ons_dataset(
            tmp_path,
            "ons_ear_reservoir_daily",
            start=date(2024, 1, 1),
            end=date(2024, 1, 2),
            client=MockONSClient(),
        )

    assert not (tmp_path / "data").exists()


def test_ons_live_download_requires_dates(repo_root, tmp_path):
    shutil.copytree(repo_root / "configs", tmp_path / "configs")

    with pytest.raises(ValueError, match="requires start and end"):
        download_ons_dataset(tmp_path, "ons_ear_subsystem_daily", client=MockONSClient())

    assert not (tmp_path / "data").exists()
