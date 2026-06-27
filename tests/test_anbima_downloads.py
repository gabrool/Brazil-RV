from __future__ import annotations

import json
import shutil
from datetime import UTC, date, datetime

import polars as pl
import pytest
import yaml

from bralpha.infra.http import HttpResponse
from bralpha.ingestion.anbima.downloads import (
    ANBIMAEndpointNotVerifiedError,
    download_anbima_dataset,
)
from bralpha.pipelines.anbima_ingest import run_anbima_ingest


class MockClient:
    def __init__(self, content: bytes) -> None:
        self.content = content
        self.requests = []

    def get_bytes(self, url, params=None, headers=None):
        self.requests.append({"url": url, "params": params or {}, "headers": headers or {}})
        return HttpResponse(
            url=f"{url}?mock=1",
            status_code=200,
            headers={"content-type": "text/csv"},
            content=self.content,
        )


def test_anbima_non_live_dataset_raises_clear_error(repo_root):
    with pytest.raises(ANBIMAEndpointNotVerifiedError, match="no verified live endpoint"):
        download_anbima_dataset(
            repo_root,
            "anbima_sovereign_secondary_market",
            start=date(2024, 1, 2),
            end=date(2024, 1, 2),
        )


def test_anbima_mocked_live_dataset_writes_raw_file_and_manifest(repo_root, tmp_path):
    _copy_configs_with_live_anbima_dataset(tmp_path, repo_root)
    client = MockClient(content=b"not,parsed\nstill,raw\n")

    results = download_anbima_dataset(
        tmp_path,
        "anbima_sovereign_secondary_market",
        start=date(2024, 1, 2),
        end=date(2024, 1, 2),
        client=client,
        downloaded_at=datetime(2024, 1, 2, 12, tzinfo=UTC),
    )

    assert len(results) == 1
    assert results[0].raw_path is not None
    assert results[0].raw_path.read_bytes() == b"not,parsed\nstill,raw\n"
    assert "data/raw/anbima/anbima_sovereign_secondary_market" in str(
        results[0].raw_path
    ).replace("\\", "/")
    assert client.requests[0]["params"] == {
        "start": "2024-01-02",
        "end": "2024-01-02",
    }
    manifest = tmp_path / "data" / "manifests" / "anbima" / "downloads.jsonl"
    records = [json.loads(line) for line in manifest.read_text().splitlines()]
    assert records[0]["success"] is True
    assert records[0]["raw_path"].endswith("anbima_sovereign_20240102_20240102.csv")


def test_anbima_pipeline_rejects_committed_pending_dataset_without_writing(repo_root, tmp_path):
    shutil.copytree(repo_root / "configs", tmp_path / "configs")

    with pytest.raises(ANBIMAEndpointNotVerifiedError, match="no verified live endpoint"):
        run_anbima_ingest(
            repo_root=tmp_path,
            dataset_id="anbima_sovereign_secondary_market",
            start=date(2024, 1, 2),
            end=date(2024, 1, 2),
            client=MockClient(content=b""),
        )

    assert not (tmp_path / "data").exists()


def test_anbima_pipeline_mocked_live_config_writes_bronze_and_silver(repo_root, tmp_path):
    _copy_configs_with_live_anbima_dataset(tmp_path, repo_root)
    client = MockClient(
        content=(
            b"data_referencia,codigo_titulo,tipo_titulo,nome_titulo,data_vencimento,"
            b"taxa_indicativa,pu\n"
            b"2024-01-02,LTN_202501,LTN,LTN Jan 2025,2025-01-01,10.5,950.25\n"
        )
    )

    status = run_anbima_ingest(
        repo_root=tmp_path,
        dataset_id="anbima_sovereign_secondary_market",
        start=date(2024, 1, 2),
        end=date(2024, 1, 2),
        client=client,
    )

    silver = pl.read_parquet(
        tmp_path
        / "data"
        / "silver"
        / "anbima_sovereign_secondary_market"
        / "year=2024"
        / "data.parquet"
    )
    assert status == {"downloads": 1, "bronze_rows": 1, "silver_rows": 1}
    assert (tmp_path / "data" / "bronze" / "anbima" / "anbima_sovereign_secondary_market").exists()
    assert silver["security_id"].item() == "LTN_202501"
    assert silver["available_date"].item() == date(2024, 1, 3)


def _copy_configs_with_live_anbima_dataset(tmp_path, repo_root) -> None:
    shutil.copytree(repo_root / "configs", tmp_path / "configs")
    path = tmp_path / "configs" / "datasets" / "anbima.yaml"
    data = yaml.safe_load(path.read_text())
    row = data["datasets"][0]
    row["source_map_status"] = "live_download"
    row["raw_format"] = "csv"
    row["endpoint_verified"] = True
    row["endpoint_verification_note"] = "Fixture-only test endpoint."
    row["source_urls"] = [
        {
            "name": "fixture_csv",
            "url_template": "https://example.test/anbima/{dataset_id}.csv",
            "params": {
                "start": "{start:%Y-%m-%d}",
                "end": "{end:%Y-%m-%d}",
            },
            "filename_template": "anbima_sovereign_{start:%Y%m%d}_{end:%Y%m%d}.csv",
        }
    ]
    path.write_text(yaml.safe_dump(data, sort_keys=False), encoding="utf-8")
