from __future__ import annotations

import io
import shutil
import zipfile
from datetime import date

import polars as pl
import pytest

from bralpha.infra.http import HttpResponse
from bralpha.ingestion.cvm.downloads import CVMDatasetNotLiveError
from bralpha.pipelines import cvm_ingest
from bralpha.pipelines.cvm_ingest import run_cvm_ingest


class MockCVMClient:
    def __init__(self) -> None:
        self.requests = []

    def get_bytes(self, url, params=None, headers=None):
        self.requests.append({"url": url, "params": params or {}, "headers": headers or {}})
        if "202401" in url:
            content = _zip_bytes(
                "jan.csv",
                (
                    "CNPJ_FUNDO;TP_FUNDO;DT_COMPTC;VL_TOTAL;VL_PATRIM_LIQ;"
                    "VL_QUOTA;CAPTC_DIA;RESG_DIA;NR_COTST\n"
                    "00.000.000/0001-00;FI;2024-01-30;1;1;1;0;0;10\n"
                    "00.000.000/0001-00;FI;2024-01-31;2;2;2;1;0;10\n"
                ),
            )
            content_type = "application/zip"
        elif "202402" in url:
            content = _zip_bytes(
                "feb.csv",
                (
                    "CNPJ_FUNDO;TP_FUNDO;DT_COMPTC;VL_TOTAL;VL_PATRIM_LIQ;"
                    "VL_QUOTA;CAPTC_DIA;RESG_DIA;NR_COTST\n"
                    "00.000.000/0001-00;FI;2024-02-01;3;3;3;0;1;10\n"
                    "00.000.000/0001-00;FI;2024-02-02;4;4;4;0;0;10\n"
                ),
            )
            content_type = "application/zip"
        elif url.endswith(".zip"):
            content = _zip_bytes(
                "registro_fundo.csv",
                "CNPJ_FUNDO;DENOM_SOCIAL\n00.000.000/0001-00;Fundo Teste\n",
            )
            content_type = "application/zip"
        else:
            content = (
                "CNPJ_FUNDO;DENOM_SOCIAL;CD_CVM\n"
                "00.000.000/0001-00;Fundo Teste;00123\n"
            ).encode("latin1")
            content_type = "text/csv"
        return HttpResponse(
            url=url,
            status_code=200,
            headers={"content-type": content_type},
            content=content,
        )


def test_cvm_pipeline_mocked_daily_raw_to_bronze_to_silver_multiple_months(
    repo_root, tmp_path, monkeypatch
):
    shutil.copytree(repo_root / "configs", tmp_path / "configs")
    normalize_calls = []
    original_normalize = cvm_ingest.normalize_cvm_to_silver

    def spy_normalize(dataset_id, bronze):
        normalize_calls.append(bronze.height)
        return original_normalize(dataset_id, bronze)

    monkeypatch.setattr(cvm_ingest, "normalize_cvm_to_silver", spy_normalize)

    status = run_cvm_ingest(
        repo_root=tmp_path,
        dataset_id="cvm_fund_daily_reports",
        start=date(2024, 1, 31),
        end=date(2024, 2, 1),
        client=MockCVMClient(),
    )

    assert status == {"downloads": 2, "bronze_rows": 4, "silver_rows": 2}
    assert normalize_calls == [2, 2]
    assert (
        tmp_path
        / "data"
        / "bronze"
        / "cvm"
        / "cvm_fund_daily_reports"
        / "year=2024"
        / "month=1"
        / "data.parquet"
    ).exists()
    jan_silver = pl.read_parquet(
        tmp_path
        / "data"
        / "silver"
        / "cvm_fund_daily_reports"
        / "year=2024"
        / "month=1"
        / "data.parquet"
    )
    feb_silver = pl.read_parquet(
        tmp_path
        / "data"
        / "silver"
        / "cvm_fund_daily_reports"
        / "year=2024"
        / "month=2"
        / "data.parquet"
    )
    assert jan_silver["ref_date"].to_list() == [date(2024, 1, 31)]
    assert feb_silver["ref_date"].to_list() == [date(2024, 2, 1)]
    assert jan_silver["subscriptions"].to_list() == [1.0]
    assert feb_silver["redemptions"].to_list() == [1.0]


def test_cvm_pipeline_rerun_is_idempotent_for_silver_primary_key(repo_root, tmp_path):
    shutil.copytree(repo_root / "configs", tmp_path / "configs")

    for _ in range(2):
        run_cvm_ingest(
            repo_root=tmp_path,
            dataset_id="cvm_fund_daily_reports",
            start=date(2024, 1, 31),
            end=date(2024, 2, 1),
            client=MockCVMClient(),
        )

    frames = [
        pl.read_parquet(path)
        for path in (tmp_path / "data" / "silver" / "cvm_fund_daily_reports").glob(
            "year=2024/month=*/data.parquet"
        )
    ]
    silver = pl.concat(frames, how="diagonal_relaxed")

    assert silver.height == 2
    assert silver.group_by(["ref_date", "fund_id"]).len().height == 2


def test_cvm_registry_current_pipeline(repo_root, tmp_path):
    shutil.copytree(repo_root / "configs", tmp_path / "configs")

    status = run_cvm_ingest(
        repo_root=tmp_path,
        dataset_id="cvm_fund_registry_current",
        client=MockCVMClient(),
    )

    assert status == {"downloads": 1, "bronze_rows": 1, "silver_rows": 1}
    silver_paths = list(
        (tmp_path / "data" / "silver" / "cvm_fund_registry_current").glob(
            "snapshot_year=*/data.parquet"
        )
    )
    assert len(silver_paths) == 1
    silver = pl.read_parquet(silver_paths[0])
    assert silver["fund_name"].to_list() == ["Fundo Teste"]
    assert silver["cvm_code"].to_list() == ["00123"]


def test_cvm_raw_bronze_only_registry_pipeline_writes_no_silver(repo_root, tmp_path):
    shutil.copytree(repo_root / "configs", tmp_path / "configs")

    status = run_cvm_ingest(
        repo_root=tmp_path,
        dataset_id="cvm_fund_class_registry",
        client=MockCVMClient(),
    )

    assert status == {"downloads": 1, "bronze_rows": 1, "silver_rows": 0}
    assert (
        tmp_path
        / "data"
        / "bronze"
        / "cvm"
        / "cvm_fund_class_registry"
        / "data.parquet"
    ).exists()
    assert not (tmp_path / "data" / "silver" / "cvm_fund_class_registry").exists()


def test_cvm_source_map_only_pipeline_failure_writes_no_data(repo_root, tmp_path):
    shutil.copytree(repo_root / "configs", tmp_path / "configs")

    with pytest.raises(CVMDatasetNotLiveError):
        run_cvm_ingest(
            repo_root=tmp_path,
            dataset_id="cvm_fund_portfolio_cda",
            client=MockCVMClient(),
        )

    assert not (tmp_path / "data").exists()


def _zip_bytes(name: str, text: str) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr(name, text.encode("latin1"))
    return buffer.getvalue()
