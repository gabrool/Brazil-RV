from __future__ import annotations

import shutil
from datetime import date

import polars as pl
import pytest

from bralpha.infra.http import HttpResponse
from bralpha.ingestion.anp.downloads import ANPDatasetNotLiveError
from bralpha.pipelines import anp_ingest
from bralpha.pipelines.anp_ingest import run_anp_ingest


class MockANPPipelineClient:
    def __init__(self) -> None:
        self.requests = []

    def get_bytes(self, url, params=None, headers=None):
        self.requests.append({"url": url, "params": params or {}, "headers": headers or {}})
        if "vendas-de-derivados" in url:
            return _response(url, _sales_page().encode("utf-8"), "text/html")
        if "producao-de-petroleo" in url:
            return _response(url, _production_page().encode("utf-8"), "text/html")
        if "vendas-combustiveis" in url:
            content = (
                "ANO;MÊS;GRANDE REGIÃO;UNIDADE DA FEDERAÇÃO;PRODUTO;VENDAS\n"
                "2024;Janeiro;Sudeste;São Paulo;GASOLINA C;100,5\n"
            ).encode("latin1")
            return _response(url, content, "text/csv")
        if any(
            token in url
            for token in [
                "producao-petroleo.csv",
                "producao-lgn.csv",
                "producao-gn.csv",
                "reinjecao-gn.csv",
                "queima-perda-gn.csv",
                "consumo-proprio-gn.csv",
                "gn-disponivel.csv",
            ]
        ):
            content = (
                "ANO;MÊS;GRANDE REGIÃO;UNIDADE DA FEDERAÇÃO;PRODUTO;LOCALIZAÇÃO;PRODUÇÃO\n"
                "2024;Fevereiro;Sudeste;Rio de Janeiro;Petróleo;Mar;10,5\n"
            ).encode("latin1")
            return _response(url, content, "text/csv")
        content = (
            "Regiao - Sigla;Estado - Sigla;Municipio;Revenda;CNPJ da Revenda;"
            "Nome da Rua;Numero Rua;Complemento;Bairro;Cep;Produto;Data da Coleta;"
            "Valor de Venda;Valor de Compra;Unidade de Medida;Bandeira\n"
            "SE;SP;Sao Paulo;Posto A;00.000.000/0001-00;Rua A;10;;Centro;"
            "01000-000;GASOLINA C;05/01/2024;5,10;4,90;R$ / litro;BRANCA\n"
        ).encode("latin1")
        return _response(url, content, "text/csv")


def test_anp_pipeline_mocked_fuel_price_raw_to_bronze_to_silver_incremental(
    repo_root, tmp_path, monkeypatch
):
    shutil.copytree(repo_root / "configs", tmp_path / "configs")
    normalize_calls = []
    original_normalize = anp_ingest.normalize_anp_to_silver

    def spy_normalize(dataset_id, bronze):
        normalize_calls.append((dataset_id, bronze["resource_family"].unique().to_list()))
        return original_normalize(dataset_id, bronze)

    monkeypatch.setattr(anp_ingest, "normalize_anp_to_silver", spy_normalize)

    status = run_anp_ingest(
        repo_root=tmp_path,
        dataset_id="anp_fuel_prices_weekly",
        start=date(2024, 1, 1),
        end=date(2024, 1, 31),
        client=MockANPPipelineClient(),
    )

    assert status == {"downloads": 3, "bronze_rows": 3, "silver_rows": 3}
    assert len(normalize_calls) == 3
    assert normalize_calls == [
        ("anp_fuel_prices_weekly", ["diesel_gnv_monthly_2023_2025"]),
        ("anp_fuel_prices_weekly", ["ethanol_gasoline_monthly_2023_2025"]),
        ("anp_fuel_prices_weekly", ["glp_monthly_2023_2025"]),
    ]
    assert (
        tmp_path
        / "data"
        / "bronze"
        / "anp"
        / "anp_fuel_prices_weekly"
        / "year=2024"
        / "data.parquet"
    ).exists()
    silver = pl.read_parquet(
        tmp_path
        / "data"
        / "silver"
        / "anp_fuel_prices_weekly"
        / "year=2024"
        / "data.parquet"
    )
    assert silver.height == 3
    assert silver["sale_price"].to_list() == [5.1, 5.1, 5.1]


def test_anp_pipeline_january_2026_uses_2026_monthly_resource_urls(repo_root, tmp_path):
    shutil.copytree(repo_root / "configs", tmp_path / "configs")
    client = MockANPPipelineClient()

    status = run_anp_ingest(
        repo_root=tmp_path,
        dataset_id="anp_fuel_prices_weekly",
        start=date(2026, 1, 1),
        end=date(2026, 1, 31),
        client=client,
    )

    assert status["downloads"] == 3
    assert [request["url"].split("/")[-1] for request in client.requests] == [
        "01-dados-abertos-precos-diesel-gnv.csv",
        "01-dados-abertos-precos-gasolina-etanol.csv",
        "01-dados-abertos-precos-glp.csv",
    ]


def test_anp_pipeline_mocked_sales_raw_to_bronze_to_silver(repo_root, tmp_path):
    shutil.copytree(repo_root / "configs", tmp_path / "configs")

    status = run_anp_ingest(
        repo_root=tmp_path,
        dataset_id="anp_fuel_sales_monthly",
        start=date(2024, 1, 1),
        end=date(2024, 1, 31),
        client=MockANPPipelineClient(),
    )

    assert status == {"downloads": 1, "bronze_rows": 1, "silver_rows": 1}
    silver = pl.read_parquet(
        tmp_path
        / "data"
        / "silver"
        / "anp_fuel_sales_monthly"
        / "year=2024"
        / "data.parquet"
    )
    assert silver["ref_date"].to_list() == [date(2024, 1, 31)]
    assert silver["sales_volume_m3"].to_list() == [100.5]


def test_anp_pipeline_mocked_production_multi_resource_raw_to_bronze_to_silver(
    repo_root, tmp_path
):
    shutil.copytree(repo_root / "configs", tmp_path / "configs")

    status = run_anp_ingest(
        repo_root=tmp_path,
        dataset_id="anp_oil_gas_production_monthly",
        start=date(2024, 2, 1),
        end=date(2024, 2, 29),
        client=MockANPPipelineClient(),
    )

    assert status == {"downloads": 7, "bronze_rows": 7, "silver_rows": 7}
    silver = pl.read_parquet(
        tmp_path
        / "data"
        / "silver"
        / "anp_oil_gas_production_monthly"
        / "year=2024"
        / "data.parquet"
    )
    assert silver["metric_type"].n_unique() == 7
    assert {"m3", "mil_m3"} <= set(silver["unit"].to_list())


def test_anp_pipeline_rerun_is_idempotent_for_silver_primary_keys(repo_root, tmp_path):
    shutil.copytree(repo_root / "configs", tmp_path / "configs")

    for _ in range(2):
        run_anp_ingest(
            repo_root=tmp_path,
            dataset_id="anp_fuel_sales_monthly",
            start=date(2024, 1, 1),
            end=date(2024, 1, 31),
            client=MockANPPipelineClient(),
        )

    silver = pl.read_parquet(
        tmp_path
        / "data"
        / "silver"
        / "anp_fuel_sales_monthly"
        / "year=2024"
        / "data.parquet"
    )
    assert silver.height == 1
    assert silver.group_by(["ref_date", "state", "product"]).len().height == 1


def test_anp_pipeline_fuel_price_rerun_is_idempotent_for_observation_id(repo_root, tmp_path):
    shutil.copytree(repo_root / "configs", tmp_path / "configs")

    for _ in range(2):
        run_anp_ingest(
            repo_root=tmp_path,
            dataset_id="anp_fuel_prices_weekly",
            start=date(2024, 1, 1),
            end=date(2024, 1, 31),
            client=MockANPPipelineClient(),
        )

    silver = pl.read_parquet(
        tmp_path
        / "data"
        / "silver"
        / "anp_fuel_prices_weekly"
        / "year=2024"
        / "data.parquet"
    )
    assert silver.height == 3
    assert silver.group_by(["observation_id"]).len().height == 3


def test_anp_pipeline_source_map_only_failure_writes_no_data(repo_root, tmp_path):
    shutil.copytree(repo_root / "configs", tmp_path / "configs")

    with pytest.raises(ANPDatasetNotLiveError):
        run_anp_ingest(
            repo_root=tmp_path,
            dataset_id="anp_downstream_movements",
            client=MockANPPipelineClient(),
        )

    assert not (tmp_path / "data").exists()


def _response(url: str, content: bytes, content_type: str) -> HttpResponse:
    return HttpResponse(
        url=url,
        status_code=200,
        headers={"content-type": content_type},
        content=content,
    )


def _sales_page() -> str:
    return """
    <a href="/anp/vendas-combustiveis-m3-1990-2026.csv">
      Vendas de derivados petróleo e etanol (metros cúbicos) 1990-2026
    </a>
    """


def _production_page() -> str:
    return """
    <a href="/anp/producao-petroleo.csv">Produção de petróleo (metros cúbicos)</a>
    <a href="/anp/producao-lgn.csv">Produção de LGN (metros cúbicos)</a>
    <a href="/anp/producao-gn.csv">Produção de gás natural (mil metros cúbicos)</a>
    <a href="/anp/reinjecao-gn.csv">Reinjeção de gás natural (mil metros cúbicos)</a>
    <a href="/anp/queima-perda-gn.csv">Queima e perda de gás natural</a>
    <a href="/anp/consumo-proprio-gn.csv">Consumo próprio de gás natural na E&P</a>
    <a href="/anp/gn-disponivel.csv">Gás natural disponível</a>
    """
