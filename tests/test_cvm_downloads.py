from __future__ import annotations

import io
import json
import shutil
import zipfile
from datetime import UTC, date, datetime

import pytest

from bralpha.infra.http import HttpResponse
from bralpha.ingestion.cvm.downloads import CVMDatasetNotLiveError, download_cvm_dataset


class MockCVMClient:
    def __init__(self, *, status_code: int = 200) -> None:
        self.status_code = status_code
        self.requests = []

    def get_bytes(self, url, params=None, headers=None):
        self.requests.append({"url": url, "params": params or {}, "headers": headers or {}})
        if url.endswith(".zip"):
            content = _zip_bytes(
                "inf_diario.csv",
                "CNPJ_FUNDO;DT_COMPTC;VL_TOTAL\n00.000.000/0001-00;2024-01-31;10\n",
            )
            content_type = "application/zip"
        else:
            content = b"CNPJ_FUNDO;DENOM_SOCIAL\n00.000.000/0001-00;Fundo Teste\n"
            content_type = "text/csv"
        return HttpResponse(
            url=url,
            status_code=self.status_code,
            headers={"content-type": content_type},
            content=content,
        )


def test_cvm_daily_downloads_write_raw_files_and_manifest(repo_root, tmp_path):
    shutil.copytree(repo_root / "configs", tmp_path / "configs")
    client = MockCVMClient()

    results = download_cvm_dataset(
        tmp_path,
        "cvm_fund_daily_reports",
        start=date(2024, 1, 31),
        end=date(2024, 2, 1),
        client=client,
        downloaded_at=datetime(2024, 2, 5, 12, tzinfo=UTC),
    )

    assert len(results) == 2
    assert [request["url"].split("/")[-1] for request in client.requests] == [
        "inf_diario_fi_202401.zip",
        "inf_diario_fi_202402.zip",
    ]
    assert all(result.raw_path is not None for result in results)
    assert not (tmp_path / "data" / "bronze").exists()
    manifest = tmp_path / "data" / "manifests" / "cvm" / "downloads.jsonl"
    records = [json.loads(line) for line in manifest.read_text().splitlines()]
    assert [record["request_params"]["period_month"] for record in records] == [1, 2]
    assert all(record["success"] is True for record in records)


def test_cvm_registry_download_writes_one_configured_file(repo_root, tmp_path):
    shutil.copytree(repo_root / "configs", tmp_path / "configs")
    client = MockCVMClient()

    results = download_cvm_dataset(
        tmp_path,
        "cvm_fund_registry_current",
        client=client,
        downloaded_at=datetime(2024, 2, 5, 12, tzinfo=UTC),
    )

    assert len(results) == 1
    assert client.requests[0]["url"].endswith("/FI/CAD/DADOS/cad_fi.csv")
    assert results[0].raw_path is not None
    assert results[0].raw_path.name == "cad_fi.csv"
    assert not (tmp_path / "data" / "bronze").exists()


def test_cvm_non_live_dataset_fails_without_data_writes(repo_root, tmp_path):
    shutil.copytree(repo_root / "configs", tmp_path / "configs")

    with pytest.raises(CVMDatasetNotLiveError, match="not live"):
        download_cvm_dataset(tmp_path, "cvm_fund_portfolio_cda", client=MockCVMClient())

    assert not (tmp_path / "data").exists()


def test_cvm_http_failure_writes_failure_manifest_without_raw(repo_root, tmp_path):
    shutil.copytree(repo_root / "configs", tmp_path / "configs")
    client = MockCVMClient(status_code=503)

    results = download_cvm_dataset(
        tmp_path,
        "cvm_fund_registry_current",
        client=client,
        downloaded_at=datetime(2024, 2, 5, 12, tzinfo=UTC),
    )

    assert results[0].raw_path is None
    assert not (tmp_path / "data" / "raw").exists()
    manifest = tmp_path / "data" / "manifests" / "cvm" / "downloads.jsonl"
    record = json.loads(manifest.read_text())
    assert record["success"] is False
    assert record["http_status"] == 503
    assert record["raw_path"] is None


def test_cvm_daily_download_requires_dates(repo_root, tmp_path):
    shutil.copytree(repo_root / "configs", tmp_path / "configs")

    with pytest.raises(ValueError, match="requires start and end"):
        download_cvm_dataset(tmp_path, "cvm_fund_daily_reports", client=MockCVMClient())

    assert not (tmp_path / "data").exists()


def _zip_bytes(name: str, text: str) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr(name, text.encode("latin1"))
    return buffer.getvalue()
