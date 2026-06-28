from __future__ import annotations

import json
import shutil
from datetime import UTC, date, datetime

import pytest

from bralpha.infra.http import HttpResponse
from bralpha.ingestion.novo_caged.downloads import (
    NovoCagedDatasetNotLiveError,
    download_novo_caged_dataset,
)


class MockNovoCagedClient:
    def __init__(self, *, status_code: int = 200) -> None:
        self.status_code = status_code
        self.requests = []

    def get_bytes(self, url, params=None, headers=None):
        self.requests.append({"url": url, "params": params or {}, "headers": headers or {}})
        if "calendario-de-divulgacao" in url:
            content = "<li>03/03/2026 - Competência: janeiro de 2026;</li>".encode()
            return _response(url, content, status_code=self.status_code, content_type="text/html")
        content = b"not parsed by downloader"
        return _response(
            url,
            content,
            status_code=self.status_code,
            content_type="application/x-7z-compressed",
        )


def test_novo_caged_movement_download_writes_raw_and_manifest_only(repo_root, tmp_path):
    shutil.copytree(repo_root / "configs", tmp_path / "configs")
    client = MockNovoCagedClient()

    results = download_novo_caged_dataset(
        tmp_path,
        "novo_caged_movements_monthly",
        start=date(2024, 1, 1),
        end=date(2024, 1, 31),
        client=client,
        downloaded_at=datetime(2024, 2, 5, 12, tzinfo=UTC),
    )

    assert len(results) == 1
    assert results[0].raw_path is not None
    assert client.requests[0]["url"].endswith("/2024/202401/CAGEDMOV202401.7z")
    assert not (tmp_path / "data" / "bronze").exists()
    records = _manifest_records(tmp_path)
    assert records[0]["success"] is True
    assert records[0]["request_params"]["period"] == "202401"


def test_novo_caged_release_calendar_download_writes_html_raw(repo_root, tmp_path):
    shutil.copytree(repo_root / "configs", tmp_path / "configs")

    results = download_novo_caged_dataset(
        tmp_path,
        "novo_caged_release_calendar",
        client=MockNovoCagedClient(),
        downloaded_at=datetime(2026, 1, 5, 12, tzinfo=UTC),
    )

    assert len(results) == 1
    assert results[0].raw_path.name == "novo_caged_release_calendar.html"
    assert "calendario-de-divulgacao-do-novo-caged" in _manifest_records(tmp_path)[0]["source_url"]


def test_novo_caged_http_failure_writes_failure_manifest_without_raw(repo_root, tmp_path):
    shutil.copytree(repo_root / "configs", tmp_path / "configs")

    results = download_novo_caged_dataset(
        tmp_path,
        "novo_caged_movements_monthly",
        start=date(2024, 1, 1),
        end=date(2024, 1, 31),
        client=MockNovoCagedClient(status_code=503),
        downloaded_at=datetime(2024, 2, 5, 12, tzinfo=UTC),
    )

    assert results[0].raw_path is None
    assert not (tmp_path / "data" / "raw").exists()
    records = _manifest_records(tmp_path)
    assert records[0]["success"] is False
    assert records[0]["raw_path"] is None
    assert records[0]["error_message"] == "HTTP 503"


def test_novo_caged_source_map_only_dataset_fails_without_data_writes(repo_root, tmp_path):
    shutil.copytree(repo_root / "configs", tmp_path / "configs")

    with pytest.raises(NovoCagedDatasetNotLiveError, match="not live"):
        download_novo_caged_dataset(
            tmp_path,
            "novo_caged_late_declarations_monthly",
            client=MockNovoCagedClient(),
        )

    assert not (tmp_path / "data").exists()


def test_novo_caged_movement_download_requires_dates(repo_root, tmp_path):
    shutil.copytree(repo_root / "configs", tmp_path / "configs")

    with pytest.raises(ValueError, match="requires start and end"):
        download_novo_caged_dataset(
            tmp_path,
            "novo_caged_movements_monthly",
            client=MockNovoCagedClient(),
        )

    assert not (tmp_path / "data").exists()


def _manifest_records(tmp_path):
    manifest = tmp_path / "data" / "manifests" / "novo_caged" / "downloads.jsonl"
    return [json.loads(line) for line in manifest.read_text().splitlines()]


def _response(url: str, content: bytes, *, status_code: int, content_type: str) -> HttpResponse:
    return HttpResponse(
        url=url,
        status_code=status_code,
        headers={"content-type": content_type},
        content=content,
    )
