from __future__ import annotations

import io
import shutil
from datetime import date

import polars as pl
import py7zr
import pytest

from bralpha.infra.http import HttpResponse
from bralpha.ingestion.novo_caged.downloads import NovoCagedDatasetNotLiveError
from bralpha.pipelines.novo_caged_ingest import run_novo_caged_ingest


class MockNovoCagedPipelineClient:
    def __init__(self) -> None:
        self.requests = []

    def get_bytes(self, url, params=None, headers=None):
        self.requests.append({"url": url, "params": params or {}, "headers": headers or {}})
        if "calendario-de-divulgacao" in url:
            return _response(
                url,
                (
                    "<li>03/03/2026 - Competência: janeiro de 2026;</li>"
                    "<li>30/06/2026 - Competência: maio de 2026;</li>"
                ).encode(),
                "text/html",
            )
        period = url.rsplit("CAGEDMOV", 1)[1].split(".7z", 1)[0]
        return _response(url, _movement_7z(period), "application/x-7z-compressed")


def test_novo_caged_pipeline_two_month_movement_raw_to_bronze_to_silver(repo_root, tmp_path):
    shutil.copytree(repo_root / "configs", tmp_path / "configs")

    status = run_novo_caged_ingest(
        repo_root=tmp_path,
        dataset_id="novo_caged_movements_monthly",
        start=date(2024, 1, 1),
        end=date(2024, 2, 29),
        client=MockNovoCagedPipelineClient(),
    )

    assert status == {"downloads": 2, "bronze_rows": 2, "silver_rows": 2}
    assert (
        tmp_path
        / "data"
        / "bronze"
        / "novo_caged"
        / "novo_caged_movements_monthly"
        / "year=2024"
        / "data.parquet"
    ).exists()
    silver = pl.read_parquet(
        tmp_path
        / "data"
        / "silver"
        / "novo_caged_movements_monthly"
        / "year=2024"
        / "data.parquet"
    )
    assert silver["ref_date"].to_list() == [date(2024, 1, 31), date(2024, 2, 29)]
    assert silver["wage"].to_list() == [2500.0, 2600.0]


def test_novo_caged_pipeline_movement_rerun_is_idempotent(repo_root, tmp_path):
    shutil.copytree(repo_root / "configs", tmp_path / "configs")

    for _ in range(2):
        run_novo_caged_ingest(
            repo_root=tmp_path,
            dataset_id="novo_caged_movements_monthly",
            start=date(2024, 1, 1),
            end=date(2024, 2, 29),
            client=MockNovoCagedPipelineClient(),
        )

    silver = pl.read_parquet(
        tmp_path
        / "data"
        / "silver"
        / "novo_caged_movements_monthly"
        / "year=2024"
        / "data.parquet"
    )
    assert silver.height == 2
    assert silver.group_by(["movement_record_id"]).len().height == 2


def test_novo_caged_pipeline_release_calendar_end_to_end(repo_root, tmp_path):
    shutil.copytree(repo_root / "configs", tmp_path / "configs")

    status = run_novo_caged_ingest(
        repo_root=tmp_path,
        dataset_id="novo_caged_release_calendar",
        client=MockNovoCagedPipelineClient(),
    )

    assert status == {"downloads": 1, "bronze_rows": 1, "silver_rows": 2}
    silver = pl.read_parquet(
        tmp_path
        / "data"
        / "silver"
        / "novo_caged_release_calendar"
        / "release_year=2026"
        / "data.parquet"
    )
    assert silver["ref_date"].to_list() == [date(2026, 1, 31), date(2026, 5, 31)]


def test_novo_caged_pipeline_source_map_only_failure_writes_no_data(repo_root, tmp_path):
    shutil.copytree(repo_root / "configs", tmp_path / "configs")

    with pytest.raises(NovoCagedDatasetNotLiveError):
        run_novo_caged_ingest(
            repo_root=tmp_path,
            dataset_id="pdet_powerbi_panel",
            client=MockNovoCagedPipelineClient(),
        )

    assert not (tmp_path / "data").exists()


def _movement_7z(period: str) -> bytes:
    month = int(period[-2:])
    wage = 2500 if month == 1 else 2600
    text = (
        "competenciamov;uf;município;subclasse;cbo2002ocupação;"
        "saldomovimentação;tipomovimentação;salário\n"
        f"{period};SP;3550308;4711302;411005;1;10;{wage},00\n"
    )
    buffer = io.BytesIO()
    with py7zr.SevenZipFile(buffer, "w") as archive:
        archive.writestr(text.encode("latin1"), f"CAGEDMOV{period}.txt")
    return buffer.getvalue()


def _response(url: str, content: bytes, content_type: str) -> HttpResponse:
    return HttpResponse(
        url=url,
        status_code=200,
        headers={"content-type": content_type},
        content=content,
    )
