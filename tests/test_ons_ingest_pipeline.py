from __future__ import annotations

import shutil
from datetime import date

import polars as pl
import pytest

from bralpha.infra.http import HttpResponse
from bralpha.ingestion.ons.downloads import ONSDatasetNotLiveError
from bralpha.pipelines import ons_ingest
from bralpha.pipelines.ons_ingest import run_ons_ingest


class MockONSPipelineClient:
    def __init__(self) -> None:
        self.requests = []

    def get_bytes(self, url, params=None, headers=None):
        self.requests.append({"url": url, "params": params or {}, "headers": headers or {}})
        if "2024" in url:
            row = "SE;Sudeste;2024-12-31;100;50;50\n"
        else:
            row = "SE;Sudeste;2025-01-01;100;60;60\n"
        content = (
            "id_subsistema;nom_subsistema;ear_data;ear_max_subsistema;"
            "ear_verif_subsistema_mwmes;ear_verif_subsistema_percentual\n"
            f"{row}"
        ).encode()
        return HttpResponse(
            url=url,
            status_code=200,
            headers={"content-type": "text/csv"},
            content=content,
        )


def test_ons_pipeline_mocked_two_year_raw_to_bronze_to_silver(repo_root, tmp_path, monkeypatch):
    shutil.copytree(repo_root / "configs", tmp_path / "configs")
    normalize_calls = []
    original_normalize = ons_ingest.normalize_ons_to_silver

    def spy_normalize(dataset_id, bronze):
        normalize_calls.append((dataset_id, bronze["year"].unique().to_list()))
        return original_normalize(dataset_id, bronze)

    monkeypatch.setattr(ons_ingest, "normalize_ons_to_silver", spy_normalize)

    status = run_ons_ingest(
        repo_root=tmp_path,
        dataset_id="ons_ear_subsystem_daily",
        start=date(2024, 12, 31),
        end=date(2025, 1, 1),
        client=MockONSPipelineClient(),
    )

    assert status == {"downloads": 2, "bronze_rows": 2, "silver_rows": 2}
    assert normalize_calls == [
        ("ons_ear_subsystem_daily", [2024]),
        ("ons_ear_subsystem_daily", [2025]),
    ]
    assert (
        tmp_path
        / "data"
        / "bronze"
        / "ons"
        / "ons_ear_subsystem_daily"
        / "year=2024"
        / "data.parquet"
    ).exists()
    silver_2024 = pl.read_parquet(
        tmp_path
        / "data"
        / "silver"
        / "ons_ear_subsystem_daily"
        / "year=2024"
        / "data.parquet"
    )
    silver_2025 = pl.read_parquet(
        tmp_path
        / "data"
        / "silver"
        / "ons_ear_subsystem_daily"
        / "year=2025"
        / "data.parquet"
    )
    assert silver_2024["ref_date"].to_list() == [date(2024, 12, 31)]
    assert silver_2025["stored_energy_mwmes"].to_list() == [60.0]


def test_ons_pipeline_rerun_is_idempotent_for_silver_primary_key(repo_root, tmp_path):
    shutil.copytree(repo_root / "configs", tmp_path / "configs")

    for _ in range(2):
        run_ons_ingest(
            repo_root=tmp_path,
            dataset_id="ons_ear_subsystem_daily",
            start=date(2024, 12, 31),
            end=date(2025, 1, 1),
            client=MockONSPipelineClient(),
        )

    frames = [
        pl.read_parquet(path)
        for path in (tmp_path / "data" / "silver" / "ons_ear_subsystem_daily").glob(
            "year=*/data.parquet"
        )
    ]
    silver = pl.concat(frames, how="diagonal_relaxed")

    assert silver.height == 2
    assert silver.group_by(["ref_date", "subsystem"]).len().height == 2


def test_ons_pipeline_source_map_only_failure_writes_no_data(repo_root, tmp_path):
    shutil.copytree(repo_root / "configs", tmp_path / "configs")

    with pytest.raises(ONSDatasetNotLiveError):
        run_ons_ingest(
            repo_root=tmp_path,
            dataset_id="ons_ear_reservoir_daily",
            start=date(2024, 1, 1),
            end=date(2024, 1, 2),
            client=MockONSPipelineClient(),
        )

    assert not (tmp_path / "data").exists()
