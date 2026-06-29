from __future__ import annotations

import shutil
from datetime import date

import polars as pl
import pytest

from bralpha.infra.http import HttpResponse
from bralpha.ingestion.receita.downloads import ReceitaDatasetNotLiveError
from bralpha.pipelines.receita_ingest import run_receita_ingest


class MockReceitaPipelineClient:
    def __init__(self, *, csv_content: bytes | None = None) -> None:
        self.requests = []
        self.csv_content = csv_content or (
            b"ANO;MES;CATEGORIA;CODIGO;DESCRICAO;VALOR\n"
            b"2024;1;IR;001;IRPJ;10,5\n"
            b"2024;2;IR;001;IRPJ;20,5\n"
        )

    def get_bytes(self, url, params=None, headers=None):
        self.requests.append({"url": url, "params": params or {}, "headers": headers or {}})
        if "conjuntos-dados" in url:
            return _response(
                url,
                (
                    '<a href="/dados/resultado-arrecadacao.csv">'
                    "Resultado da arrecadação CSV</a>"
                ).encode(),
                "text/html",
            )
        return _response(url, self.csv_content, "text/csv")


def test_receita_pipeline_mocked_raw_to_bronze_to_silver(repo_root, tmp_path):
    shutil.copytree(repo_root / "configs", tmp_path / "configs")

    status = run_receita_ingest(
        repo_root=tmp_path,
        dataset_id="receita_tax_collection_monthly",
        start=date(2024, 1, 1),
        end=date(2024, 1, 31),
        client=MockReceitaPipelineClient(),
    )

    assert status == {"downloads": 1, "bronze_rows": 2, "silver_rows": 1}
    assert (
        tmp_path
        / "data"
        / "bronze"
        / "receita"
        / "receita_tax_collection_monthly"
        / "year=2024"
        / "data.parquet"
    ).exists()
    silver = pl.read_parquet(
        tmp_path
        / "data"
        / "silver"
        / "receita_tax_collection_monthly"
        / "year=2024"
        / "data.parquet"
    )
    assert silver["ref_date"].to_list() == [date(2024, 1, 31)]
    assert silver["collection_amount_brl"].to_list() == [10.5]


def test_receita_pipeline_rerun_is_idempotent_for_silver_primary_keys(repo_root, tmp_path):
    shutil.copytree(repo_root / "configs", tmp_path / "configs")

    for _ in range(2):
        run_receita_ingest(
            repo_root=tmp_path,
            dataset_id="receita_tax_collection_monthly",
            start=date(2024, 1, 1),
            end=date(2024, 1, 31),
            client=MockReceitaPipelineClient(),
        )

    silver = pl.read_parquet(
        tmp_path
        / "data"
        / "silver"
        / "receita_tax_collection_monthly"
        / "year=2024"
        / "data.parquet"
    )
    assert silver.height == 1
    assert silver.group_by(
        [
            "ref_date",
            "collection_scope",
            "revenue_category",
            "revenue_code",
            "revenue_key",
            "table_kind",
        ]
    ).len().height == 1


def test_receita_pipeline_date_filtering_happens_after_normalization(repo_root, tmp_path):
    shutil.copytree(repo_root / "configs", tmp_path / "configs")

    status = run_receita_ingest(
        repo_root=tmp_path,
        dataset_id="receita_tax_collection_monthly",
        start=date(2024, 2, 1),
        end=date(2024, 2, 29),
        client=MockReceitaPipelineClient(),
    )

    assert status["bronze_rows"] == 2
    assert status["silver_rows"] == 1
    silver = pl.read_parquet(
        tmp_path
        / "data"
        / "silver"
        / "receita_tax_collection_monthly"
        / "year=2024"
        / "data.parquet"
    )
    assert silver["ref_date"].to_list() == [date(2024, 2, 29)]


def test_receita_pipeline_source_map_only_failure_writes_no_data(repo_root, tmp_path):
    shutil.copytree(repo_root / "configs", tmp_path / "configs")

    with pytest.raises(ReceitaDatasetNotLiveError):
        run_receita_ingest(
            repo_root=tmp_path,
            dataset_id="receita_tax_expenditures_annual",
            start=date(2024, 1, 1),
            end=date(2024, 1, 31),
            client=MockReceitaPipelineClient(),
        )

    assert not (tmp_path / "data").exists()


def _response(url: str, content: bytes, content_type: str) -> HttpResponse:
    return HttpResponse(
        url=url,
        status_code=200,
        headers={"content-type": content_type},
        content=content,
    )
