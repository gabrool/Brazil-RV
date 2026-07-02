from __future__ import annotations

import json
import shutil
from datetime import UTC, date, datetime

import polars as pl
import pytest

from bralpha.infra.http import HttpResponse
from bralpha.ingestion.tesouro.downloads import (
    TesouroDatasetNotLiveError,
    download_tesouro_dataset,
)
from bralpha.pipelines.tesouro_ingest import run_tesouro_ingest


class MockTesouroClient:
    def __init__(self, *, csv_content: bytes, resources: list[dict] | None = None) -> None:
        self.csv_content = csv_content
        self.resources = resources or [
            {
                "id": "prices",
                "name": "Taxas dos Titulos Ofertados pelo Tesouro Direto",
                "format": "CSV",
                "position": 0,
                "url": "https://example.test/prices.csv",
            }
        ]
        self.requests = []

    def get_bytes(self, url, params=None, headers=None):
        self.requests.append({"url": url, "params": params or {}, "headers": headers or {}})
        if url.endswith("/package_show"):
            content = json.dumps(
                {"success": True, "result": {"name": "fixture", "resources": self.resources}}
            ).encode()
            return HttpResponse(
                url=url,
                status_code=200,
                headers={"content-type": "application/json"},
                content=content,
            )
        return HttpResponse(
            url=url,
            status_code=200,
            headers={"content-type": "text/csv"},
            content=self.csv_content,
        )


def test_tesouro_mocked_live_dataset_writes_raw_file_and_manifest(repo_root, tmp_path):
    shutil.copytree(repo_root / "configs", tmp_path / "configs")
    client = MockTesouroClient(csv_content=b"Data Base;Tipo Titulo\n02/01/2024;Tesouro Selic\n")

    results = download_tesouro_dataset(
        tmp_path,
        "tesouro_direto_prices_rates",
        start=date(2024, 1, 2),
        end=date(2024, 1, 2),
        client=client,
        downloaded_at=datetime(2024, 1, 2, 12, tzinfo=UTC),
    )

    assert len(results) == 1
    assert results[0].raw_path is not None
    assert results[0].raw_path.read_bytes() == b"Data Base;Tipo Titulo\n02/01/2024;Tesouro Selic\n"
    assert "data/raw/tesouro/tesouro_direto_prices_rates" in str(
        results[0].raw_path
    ).replace("\\", "/")
    assert client.requests[0]["params"] == {"id": "taxas-dos-titulos-ofertados-pelo-tesouro-direto"}
    manifest = tmp_path / "data" / "manifests" / "tesouro" / "downloads.jsonl"
    records = [json.loads(line) for line in manifest.read_text().splitlines()]
    assert records[0]["success"] is True
    assert records[0]["request_params"]["resource_id"] == "prices"
    assert records[0]["request_params"]["resource_name"].startswith("Taxas dos Titulos")


def test_tesouro_pending_dataset_fails_without_data_writes(repo_root, tmp_path):
    shutil.copytree(repo_root / "configs", tmp_path / "configs")

    with pytest.raises(TesouroDatasetNotLiveError, match="not configured for live download"):
        download_tesouro_dataset(
            tmp_path,
            "tesouro_rtn_series",
            client=MockTesouroClient(csv_content=b""),
        )

    assert not (tmp_path / "data").exists()


def test_tesouro_pipeline_mocked_live_config_writes_bronze_and_silver(repo_root, tmp_path):
    shutil.copytree(repo_root / "configs", tmp_path / "configs")
    client = MockTesouroClient(
        csv_content=(
            "Data Base;Tipo Titulo;Data Vencimento;Taxa Compra Manha;"
            "Taxa Venda Manha;PU Compra Manha;PU Venda Manha\n"
            "02/01/2024;Tesouro Prefixado;01/01/2027;10,50;10,62;950,25;949,10\n"
        ).encode("latin1")
    )

    status = run_tesouro_ingest(
        repo_root=tmp_path,
        dataset_id="tesouro_direto_prices_rates",
        start=date(2024, 1, 2),
        end=date(2024, 1, 2),
        client=client,
    )

    silver_path = (
        tmp_path
        / "data"
        / "silver"
        / "tesouro_direto_prices_rates"
        / "year=2024"
        / "data.parquet"
    )
    silver = pl.read_parquet(silver_path)
    assert status == {"downloads": 1, "bronze_rows": 1, "silver_rows": 1}
    assert (tmp_path / "data" / "bronze" / "tesouro" / "tesouro_direto_prices_rates").exists()
    assert silver["buy_rate"].item() == 10.5
    assert silver["available_date"].item() == date(2024, 1, 3)


def test_tesouro_pipeline_passes_configured_holidays_to_flow_normalizer(repo_root, tmp_path):
    shutil.copytree(repo_root / "configs", tmp_path / "configs")
    _write_holiday_calendar(tmp_path, date(2024, 1, 3))
    client = MockTesouroClient(
        csv_content=(
            "Data Venda;Tipo Titulo;Vencimento do Titulo;Quantidade;Valor;Investidores\n"
            "02/01/2024;Tesouro Selic;01/01/2027;1,00;100,00;1\n"
        ).encode("latin1"),
        resources=[_resource("sales", "Vendas do Tesouro Direto")],
    )

    status = run_tesouro_ingest(
        repo_root=tmp_path,
        dataset_id="tesouro_direto_sales",
        start=date(2024, 1, 2),
        end=date(2024, 1, 2),
        client=client,
    )

    silver = pl.read_parquet(
        tmp_path
        / "data"
        / "silver"
        / "tesouro_direto_sales"
        / "year=2024"
        / "data.parquet"
    )
    assert status == {"downloads": 1, "bronze_rows": 1, "silver_rows": 1}
    assert silver["availability_basis"].item() == "configured_holiday_calendar"
    assert silver["available_date"].item() == date(2024, 1, 5)


def test_tesouro_pipeline_flow_calendar_fallback_is_explicit(repo_root, tmp_path):
    shutil.copytree(repo_root / "configs", tmp_path / "configs")
    client = MockTesouroClient(
        csv_content=(
            "Data Venda;Tipo Titulo;Vencimento do Titulo;Quantidade;Valor;Investidores\n"
            "02/01/2024;Tesouro Selic;01/01/2027;1,00;100,00;1\n"
        ).encode("latin1"),
        resources=[_resource("sales", "Vendas do Tesouro Direto")],
    )

    run_tesouro_ingest(
        repo_root=tmp_path,
        dataset_id="tesouro_direto_sales",
        start=date(2024, 1, 2),
        end=date(2024, 1, 2),
        client=client,
    )

    silver = pl.read_parquet(
        tmp_path
        / "data"
        / "silver"
        / "tesouro_direto_sales"
        / "year=2024"
        / "data.parquet"
    )
    assert silver["availability_basis"].item() == "canonical_b3_calendar"
    assert silver["available_date"].item() == date(2024, 1, 4)


def test_tesouro_silver_write_is_idempotent_for_same_primary_key(repo_root, tmp_path):
    shutil.copytree(repo_root / "configs", tmp_path / "configs")
    content = (
        "Data Base;Tipo Titulo;Data Vencimento;Taxa Compra Manha;"
        "Taxa Venda Manha;PU Compra Manha;PU Venda Manha\n"
        "02/01/2024;Tesouro Prefixado;01/01/2027;10,50;10,62;950,25;949,10\n"
    ).encode("latin1")

    run_tesouro_ingest(
        repo_root=tmp_path,
        dataset_id="tesouro_direto_prices_rates",
        client=MockTesouroClient(csv_content=content),
    )
    run_tesouro_ingest(
        repo_root=tmp_path,
        dataset_id="tesouro_direto_prices_rates",
        client=MockTesouroClient(csv_content=content),
    )

    silver = pl.read_parquet(
        tmp_path
        / "data"
        / "silver"
        / "tesouro_direto_prices_rates"
        / "year=2024"
        / "data.parquet"
    )
    assert silver.height == 1
    assert silver.group_by(["ref_date", "security_name", "maturity_date"]).len().height == 1


def _resource(resource_id: str, name: str) -> dict[str, object]:
    return {
        "id": resource_id,
        "name": name,
        "format": "CSV",
        "position": 0,
        "url": f"https://example.test/{resource_id}.csv",
    }


def _write_holiday_calendar(repo_root, holiday: date) -> None:
    root = repo_root / "data" / "silver" / "b3_holiday_calendar" / f"year={holiday.year}"
    root.mkdir(parents=True)
    pl.DataFrame(
        [
            {
                "ref_date": holiday,
                "calendar_id": "B3",
                "is_business_day": False,
                "holiday_name": "Fixture holiday",
            }
        ]
    ).write_parquet(root / "data.parquet")
