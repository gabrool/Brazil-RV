from __future__ import annotations

import pytest

from bralpha.infra.config import load_receita_dataset_registry
from bralpha.ingestion.receita.resources import (
    ReceitaUnsupportedResourceError,
    receita_collection_resources,
)


def test_receita_resource_discovery_selects_csv_and_ignores_pdf(repo_root):
    dataset = load_receita_dataset_registry(repo_root).get("receita_tax_collection_monthly")
    html = """
    <a href="/reports/arrecadacao.pdf">Arrecadação - relatório PDF</a>
    <a href="/dados/resultado-arrecadacao-historica.csv">
      Resultado da arrecadação das receitas federais - série histórica CSV
    </a>
    """

    resources = receita_collection_resources(dataset, html)

    assert len(resources) == 1
    assert resources[0].url.endswith("/dados/resultado-arrecadacao-historica.csv")
    assert resources[0].filename == "resultado-arrecadacao-historica.csv"
    assert resources[0].resource_family == "tax_collection_monthly"


def test_receita_resource_discovery_selects_xlsx_when_csv_absent(repo_root):
    dataset = load_receita_dataset_registry(repo_root).get("receita_tax_collection_monthly")
    html = """
    <a href="/reports/arrecadacao.pdf">Arrecadacao PDF</a>
    <a href="/dados/receitas-federais.xlsx">Receitas Federais XLSX</a>
    """

    resources = receita_collection_resources(dataset, html)

    assert resources[0].url.endswith("/dados/receitas-federais.xlsx")


def test_receita_resource_discovery_rejects_pdf_only_live_page(repo_root):
    dataset = load_receita_dataset_registry(repo_root).get("receita_tax_collection_monthly")
    html = '<a href="/reports/arrecadacao.pdf">Arrecadação das receitas federais</a>'

    with pytest.raises(ReceitaUnsupportedResourceError, match="only PDF"):
        receita_collection_resources(dataset, html)


def test_receita_resource_discovery_matches_unaccented_text(repo_root):
    dataset = load_receita_dataset_registry(repo_root).get("receita_tax_collection_monthly")
    html = '<a href="/dados/arrecadacao.txt">Resultado da arrecadacao TXT</a>'

    resources = receita_collection_resources(dataset, html)

    assert resources[0].url.endswith("/dados/arrecadacao.txt")


def test_receita_resource_discovery_does_not_follow_external_pages(repo_root):
    dataset = load_receita_dataset_registry(repo_root).get("receita_tax_collection_monthly")
    html = """
    <a href="https://example.com/intermediate">Resultado da arrecadação</a>
    <a href="/dados/resultado.csv">Resultado da arrecadação CSV</a>
    """

    resources = receita_collection_resources(dataset, html)

    assert resources[0].url == "https://dados.gov.br/dados/resultado.csv"
