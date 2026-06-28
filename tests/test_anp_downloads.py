from __future__ import annotations

import json
import shutil
from datetime import UTC, date, datetime

import pytest

from bralpha.infra.http import HttpResponse
from bralpha.ingestion.anp.downloads import ANPDatasetNotLiveError, download_anp_dataset


class MockANPClient:
    def __init__(self, *, status_code: int = 200) -> None:
        self.status_code = status_code
        self.requests = []

    def get_bytes(self, url, params=None, headers=None):
        self.requests.append({"url": url, "params": params or {}, "headers": headers or {}})
        if "vendas-de-derivados" in url:
            content = _sales_page().encode("utf-8")
            return _response(url, content, content_type="text/html")
        if "producao-de-petroleo" in url:
            content = _production_page().encode("utf-8")
            return _response(url, content, content_type="text/html")
        content = (
            "Regiao - Sigla;Estado - Sigla;Municipio;Produto;Data da Coleta;"
            "Valor de Venda\nSE;SP;Sao Paulo;GASOLINA C;05/01/2024;5,10\n"
        ).encode("latin1")
        return _response(
            url,
            content,
            status_code=self.status_code,
            content_type="text/csv",
        )


def test_anp_fuel_price_downloads_write_raw_files_and_manifest_only(repo_root, tmp_path):
    shutil.copytree(repo_root / "configs", tmp_path / "configs")
    client = MockANPClient()

    results = download_anp_dataset(
        tmp_path,
        "anp_fuel_prices_weekly",
        start=date(2023, 1, 1),
        end=date(2023, 1, 31),
        client=client,
        downloaded_at=datetime(2024, 2, 5, 12, tzinfo=UTC),
    )

    assert len(results) == 3
    assert [request["url"].split("/")[-1] for request in client.requests] == [
        "01-dados-abertos-precos-diesel-gnv.csv",
        "01-dados-abertos-precos-etanol-gasolina.csv",
        "01-dados-abertos-precos-glp.csv",
    ]
    assert all(result.raw_path is not None for result in results)
    assert not (tmp_path / "data" / "bronze").exists()
    manifest = tmp_path / "data" / "manifests" / "anp" / "downloads.jsonl"
    records = [json.loads(line) for line in manifest.read_text().splitlines()]
    assert [record["request_params"]["resource_family"] for record in records] == [
        "diesel_gnv_monthly",
        "ethanol_gasoline_monthly",
        "glp_monthly",
    ]
    assert all(record["success"] is True for record in records)


def test_anp_sales_download_discovers_page_link_and_resource(repo_root, tmp_path):
    shutil.copytree(repo_root / "configs", tmp_path / "configs")
    client = MockANPClient()

    results = download_anp_dataset(
        tmp_path,
        "anp_fuel_sales_monthly",
        client=client,
        downloaded_at=datetime(2024, 2, 5, 12, tzinfo=UTC),
    )

    assert len(results) == 1
    assert "vendas-de-derivados-de-petroleo-e-biocombustiveis" in client.requests[0]["url"]
    assert client.requests[1]["url"].endswith("vendas-combustiveis-m3-1990-2026.csv")
    assert results[0].raw_path is not None
    assert not (tmp_path / "data" / "bronze").exists()


def test_anp_production_download_discovers_all_resource_links(repo_root, tmp_path):
    shutil.copytree(repo_root / "configs", tmp_path / "configs")
    client = MockANPClient()

    results = download_anp_dataset(
        tmp_path,
        "anp_oil_gas_production_monthly",
        client=client,
        downloaded_at=datetime(2024, 2, 5, 12, tzinfo=UTC),
    )

    assert len(results) == 7
    manifest = tmp_path / "data" / "manifests" / "anp" / "downloads.jsonl"
    records = [json.loads(line) for line in manifest.read_text().splitlines()]
    assert records[0]["request_params"]["resource_family"] == "petroleum_production"
    assert records[-1]["request_params"]["resource_family"] == "natural_gas_available"


def test_anp_http_failure_writes_failure_manifest_without_raw(repo_root, tmp_path):
    shutil.copytree(repo_root / "configs", tmp_path / "configs")
    client = MockANPClient(status_code=503)

    results = download_anp_dataset(
        tmp_path,
        "anp_fuel_prices_weekly",
        start=date(2023, 1, 1),
        end=date(2023, 1, 31),
        client=client,
        downloaded_at=datetime(2024, 2, 5, 12, tzinfo=UTC),
    )

    assert all(result.raw_path is None for result in results)
    assert not (tmp_path / "data" / "raw").exists()
    manifest = tmp_path / "data" / "manifests" / "anp" / "downloads.jsonl"
    records = [json.loads(line) for line in manifest.read_text().splitlines()]
    assert all(record["success"] is False for record in records)
    assert all(record["raw_path"] is None for record in records)


def test_anp_source_map_only_dataset_fails_without_data_writes(repo_root, tmp_path):
    shutil.copytree(repo_root / "configs", tmp_path / "configs")

    with pytest.raises(ANPDatasetNotLiveError, match="not live"):
        download_anp_dataset(tmp_path, "anp_downstream_movements", client=MockANPClient())

    assert not (tmp_path / "data").exists()


def test_anp_fuel_price_download_requires_dates(repo_root, tmp_path):
    shutil.copytree(repo_root / "configs", tmp_path / "configs")

    with pytest.raises(ValueError, match="requires start and end"):
        download_anp_dataset(tmp_path, "anp_fuel_prices_weekly", client=MockANPClient())

    assert not (tmp_path / "data").exists()


def _response(url: str, content: bytes, *, status_code: int = 200, content_type: str):
    return HttpResponse(
        url=url,
        status_code=status_code,
        headers={"content-type": content_type},
        content=content,
    )


def _sales_page() -> str:
    return """
    <a href="metadados.pdf">Metadados - Vendas de derivados petróleo e etanol</a>
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
