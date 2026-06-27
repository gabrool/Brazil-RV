from __future__ import annotations

import json
import shutil
from datetime import UTC, date, datetime

import pytest

from bralpha.infra.config import load_fred_dataset_registry
from bralpha.infra.http import HttpResponse
from bralpha.ingestion.fred.common import FredApiKeyMissingError
from bralpha.ingestion.fred.downloads import (
    build_fred_observations_request,
    download_fred_series_observations,
)


class MockFredClient:
    def __init__(self, content: bytes | None = None, *, status_code: int = 200) -> None:
        self.content = content or _fred_payload("DGS10").encode()
        self.status_code = status_code
        self.requests = []

    def get_bytes(self, url, params=None, headers=None):
        request_params = params or {}
        self.requests.append({"url": url, "params": request_params, "headers": headers or {}})
        series_id = request_params.get("series_id", "DGS10")
        response_url = (
            f"{url}?series_id={series_id}"
            f"&api_key={request_params.get('api_key', '')}"
            f"&file_type={request_params.get('file_type', '')}"
        )
        content = self.content
        if self.content is None:
            content = _fred_payload(str(series_id)).encode()
        return HttpResponse(
            url=response_url,
            status_code=self.status_code,
            headers={"content-type": "application/json"},
            content=content,
        )


def test_fred_url_construction_uses_official_parameters(repo_root):
    dataset = load_fred_dataset_registry(repo_root).get("fred_series_observations")

    url, params, filename = build_fred_observations_request(
        dataset,
        series_id="DGS10",
        start=date(2024, 1, 2),
        end=date(2024, 1, 31),
        api_key="test-key",
    )

    assert url == "https://api.stlouisfed.org/fred/series/observations"
    assert params == {
        "series_id": "DGS10",
        "api_key": "test-key",
        "file_type": "json",
        "observation_start": "2024-01-02",
        "observation_end": "2024-01-31",
        "sort_order": "asc",
    }
    assert filename == "fred_DGS10_20240102_20240131.json"


def test_fred_missing_api_key_raises_before_data_writes(repo_root, tmp_path, monkeypatch):
    shutil.copytree(repo_root / "configs", tmp_path / "configs")
    monkeypatch.delenv("FRED_API_KEY", raising=False)

    with pytest.raises(FredApiKeyMissingError, match="FRED_API_KEY"):
        download_fred_series_observations(
            tmp_path,
            start=date(2024, 1, 2),
            end=date(2024, 1, 2),
            series_ids=["DGS10"],
            client=MockFredClient(),
        )

    assert not (tmp_path / "data").exists()


def test_fred_mocked_download_writes_raw_file_and_manifest(repo_root, tmp_path):
    shutil.copytree(repo_root / "configs", tmp_path / "configs")
    client = MockFredClient(content=_fred_payload("DGS10").encode())

    results = download_fred_series_observations(
        tmp_path,
        start=date(2024, 1, 2),
        end=date(2024, 1, 3),
        series_ids=["DGS10"],
        api_key="test-key",
        client=client,
        downloaded_at=datetime(2024, 1, 4, 12, tzinfo=UTC),
    )

    assert len(results) == 1
    assert results[0].raw_path is not None
    assert results[0].raw_path.read_bytes() == _fred_payload("DGS10").encode()
    assert "data/raw/fred/fred_series_observations" in str(results[0].raw_path).replace("\\", "/")
    assert client.requests[0]["params"]["api_key"] == "test-key"
    assert client.requests[0]["params"]["file_type"] == "json"
    assert client.requests[0]["params"]["sort_order"] == "asc"
    assert not (tmp_path / "data" / "bronze").exists()
    manifest = tmp_path / "data" / "manifests" / "fred" / "downloads.jsonl"
    records = [json.loads(line) for line in manifest.read_text(encoding="utf-8").splitlines()]
    assert records[0]["success"] is True
    assert records[0]["request_params"]["series_id"] == "DGS10"
    assert records[0]["request_params"]["api_key"] == "<redacted>"
    assert "test-key" not in json.dumps(records[0], sort_keys=True)
    assert "test-key" not in records[0]["source_url"]
    assert (
        "api_key=%3Credacted%3E" in records[0]["source_url"]
        or "api_key=<redacted>" in records[0]["source_url"]
    )


def test_fred_http_failure_writes_failure_manifest_without_raw(repo_root, tmp_path):
    shutil.copytree(repo_root / "configs", tmp_path / "configs")
    client = MockFredClient(content=b"unavailable", status_code=503)

    results = download_fred_series_observations(
        tmp_path,
        start=date(2024, 1, 2),
        end=date(2024, 1, 2),
        series_ids=["DGS10"],
        api_key="test-key",
        client=client,
        downloaded_at=datetime(2024, 1, 4, 12, tzinfo=UTC),
    )

    assert results[0].raw_path is None
    assert client.requests[0]["params"]["api_key"] == "test-key"
    assert not (tmp_path / "data" / "raw").exists()
    manifest = tmp_path / "data" / "manifests" / "fred" / "downloads.jsonl"
    record = json.loads(manifest.read_text(encoding="utf-8"))
    assert record["success"] is False
    assert record["http_status"] == 503
    assert record["raw_path"] is None
    assert record["error_message"] == "HTTP 503"
    assert record["request_params"]["api_key"] == "<redacted>"
    assert "test-key" not in json.dumps(record, sort_keys=True)
    assert "test-key" not in record["source_url"]


def _fred_payload(series_id: str) -> str:
    return json.dumps(
        {
            "realtime_start": "2024-01-04",
            "realtime_end": "2024-01-04",
            "observation_start": "2024-01-02",
            "observation_end": "2024-01-03",
            "units": "lin",
            "output_type": 1,
            "file_type": "json",
            "order_by": "observation_date",
            "sort_order": "asc",
            "count": 1,
            "offset": 0,
            "limit": 100000,
            "observations": [
                {
                    "realtime_start": "2024-01-04",
                    "realtime_end": "2024-01-04",
                    "date": "2024-01-02",
                    "value": "4.25" if series_id == "DGS10" else "3.25",
                }
            ],
        }
    )
