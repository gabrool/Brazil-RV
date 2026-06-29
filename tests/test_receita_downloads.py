from __future__ import annotations

import json
import shutil
from datetime import UTC, date, datetime

import pytest

from bralpha.infra.http import HttpResponse
from bralpha.ingestion.receita.downloads import ReceitaDatasetNotLiveError, download_receita_dataset


class MockReceitaClient:
    def __init__(self, *, resource_status: int = 200) -> None:
        self.resource_status = resource_status
        self.requests = []

    def get_bytes(self, url, params=None, headers=None):
        self.requests.append({"url": url, "params": params or {}, "headers": headers or {}})
        if "conjuntos-dados" in url:
            return _response(
                url,
                """
                <a href="/dados/arrecadacao.pdf">Arrecadação PDF</a>
                <a href="/dados/resultado-arrecadacao.csv">Resultado da arrecadação CSV</a>
                """.encode(),
                "text/html",
            )
        return _response(
            url,
            b"ANO;MES;CATEGORIA;CODIGO;DESCRICAO;VALOR\n2024;1;IR;001;IRPJ;10,5\n",
            "text/csv",
            status_code=self.resource_status,
        )


def test_receita_download_writes_raw_resource_and_manifest_only(repo_root, tmp_path):
    shutil.copytree(repo_root / "configs", tmp_path / "configs")
    client = MockReceitaClient()

    results = download_receita_dataset(
        tmp_path,
        "receita_tax_collection_monthly",
        start=date(2024, 1, 1),
        end=date(2024, 1, 31),
        client=client,
        downloaded_at=datetime(2024, 3, 8, 12, tzinfo=UTC),
    )

    assert len(results) == 1
    assert results[0].raw_path is not None
    assert [request["url"] for request in client.requests] == [
        "https://dados.gov.br/dados/conjuntos-dados/resultado-da-arrecadacao",
        "https://dados.gov.br/dados/resultado-arrecadacao.csv",
    ]
    assert not (tmp_path / "data" / "bronze").exists()
    manifest = tmp_path / "data" / "manifests" / "receita" / "downloads.jsonl"
    records = [json.loads(line) for line in manifest.read_text().splitlines()]
    assert records[0]["success"] is True
    assert records[0]["request_params"]["resource_family"] == "tax_collection_monthly"


def test_receita_http_failure_writes_failure_manifest_without_raw(repo_root, tmp_path):
    shutil.copytree(repo_root / "configs", tmp_path / "configs")
    client = MockReceitaClient(resource_status=503)

    results = download_receita_dataset(
        tmp_path,
        "receita_tax_collection_monthly",
        start=date(2024, 1, 1),
        end=date(2024, 1, 31),
        client=client,
        downloaded_at=datetime(2024, 3, 8, 12, tzinfo=UTC),
    )

    assert results[0].raw_path is None
    assert not (tmp_path / "data" / "raw").exists()
    manifest = tmp_path / "data" / "manifests" / "receita" / "downloads.jsonl"
    records = [json.loads(line) for line in manifest.read_text().splitlines()]
    assert records[0]["success"] is False
    assert records[0]["raw_path"] is None
    assert records[0]["http_status"] == 503


def test_receita_source_map_only_dataset_fails_without_data_writes(repo_root, tmp_path):
    shutil.copytree(repo_root / "configs", tmp_path / "configs")

    with pytest.raises(ReceitaDatasetNotLiveError, match="not live"):
        download_receita_dataset(
            tmp_path,
            "receita_tax_expenditures_annual",
            client=MockReceitaClient(),
        )

    assert not (tmp_path / "data").exists()


def test_receita_downloader_does_not_parse(repo_root, tmp_path):
    shutil.copytree(repo_root / "configs", tmp_path / "configs")

    download_receita_dataset(
        tmp_path,
        "receita_tax_collection_monthly",
        start=date(2024, 1, 1),
        end=date(2024, 1, 31),
        client=MockReceitaClient(),
        downloaded_at=datetime(2024, 3, 8, 12, tzinfo=UTC),
    )

    assert not (tmp_path / "data" / "bronze").exists()
    assert not (tmp_path / "data" / "silver").exists()


def _response(url: str, content: bytes, content_type: str, *, status_code: int = 200):
    return HttpResponse(
        url=url,
        status_code=status_code,
        headers={"content-type": content_type},
        content=content,
    )
