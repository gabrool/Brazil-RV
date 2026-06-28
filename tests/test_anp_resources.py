from __future__ import annotations

from datetime import date

import pytest

from bralpha.infra.config import load_anp_dataset_registry
from bralpha.ingestion.anp.resources import (
    anp_fuel_price_resources,
    anp_multi_page_resources,
    anp_single_page_resource,
)


def test_anp_price_pre_2023_generates_legacy_semester_resources(repo_root):
    dataset = load_anp_dataset_registry(repo_root).get("anp_fuel_prices_weekly")

    resources = anp_fuel_price_resources(dataset, date(2022, 5, 1), date(2022, 8, 1))

    assert [(item.resource_family, item.year, item.semester) for item in resources] == [
        ("automotive_legacy_semester", 2022, 1),
        ("glp_legacy_semester", 2022, 1),
        ("automotive_legacy_semester", 2022, 2),
        ("glp_legacy_semester", 2022, 2),
    ]
    assert [item.filename for item in resources] == [
        "ca-2022-01.zip",
        "glp-2022-01.csv",
        "ca-2022-02.zip",
        "glp-2022-02.csv",
    ]


def test_anp_price_2023_onward_generates_monthly_families(repo_root):
    dataset = load_anp_dataset_registry(repo_root).get("anp_fuel_prices_weekly")

    resources = anp_fuel_price_resources(dataset, date(2023, 1, 1), date(2023, 2, 28))

    assert len(resources) == 6
    assert [item.filename for item in resources[:3]] == [
        "precos-diesel-gnv-01.csv",
        "01-dados-abertos-precos-gasolina-etanol.csv",
        "01-dados-abertos-precos-glp.csv",
    ]
    assert {item.resource_family for item in resources} == {
        "diesel_gnv_monthly",
        "ethanol_gasoline_monthly",
        "glp_monthly",
    }
    assert [item.month for item in resources] == [1, 1, 1, 2, 2, 2]
    assert all("quatro" not in item.url.lower() for item in resources)


def test_anp_price_2022_2023_boundary_uses_non_overlapping_split(repo_root):
    dataset = load_anp_dataset_registry(repo_root).get("anp_fuel_prices_weekly")

    resources = anp_fuel_price_resources(dataset, date(2022, 12, 1), date(2023, 1, 31))

    assert [(item.resource_family, item.year, item.month, item.semester) for item in resources] == [
        ("automotive_legacy_semester", 2022, None, 2),
        ("glp_legacy_semester", 2022, None, 2),
        ("diesel_gnv_monthly", 2023, 1, None),
        ("ethanol_gasoline_monthly", 2023, 1, None),
        ("glp_monthly", 2023, 1, None),
    ]


def test_anp_price_inverted_window_raises(repo_root):
    dataset = load_anp_dataset_registry(repo_root).get("anp_fuel_prices_weekly")

    with pytest.raises(ValueError, match="start <= end"):
        anp_fuel_price_resources(dataset, date(2024, 2, 1), date(2024, 1, 1))


def test_anp_sales_page_html_resolves_current_csv_link(repo_root):
    dataset = load_anp_dataset_registry(repo_root).get("anp_fuel_sales_monthly")
    html = """
    <a href="metadados.pdf">Metadados - Vendas de derivados petróleo e etanol</a>
    <a href="/arquivos/vendas-combustiveis-m3-1990-2026.csv">
      Vendas de derivados petróleo e etanol (metros cúbicos) 1990-2026
    </a>
    """

    resources = anp_single_page_resource(dataset, html)

    assert len(resources) == 1
    assert resources[0].url.endswith("/arquivos/vendas-combustiveis-m3-1990-2026.csv")
    assert resources[0].resource_family == "anp_fuel_sales_monthly"


def test_anp_production_page_html_resolves_all_configured_links(repo_root):
    dataset = load_anp_dataset_registry(repo_root).get("anp_oil_gas_production_monthly")
    html = """
    <a href="meta-petroleo.pdf">Metadados - Produção de petróleo</a>
    <a href="/ppgn/producao-petroleo.csv">Produção de petróleo (metros cúbicos)</a>
    <a href="/ppgn/producao-lgn.csv">Produção de LGN (metros cúbicos)</a>
    <a href="/ppgn/producao-gn.csv">Produção de gás natural (mil metros cúbicos)</a>
    <a href="/ppgn/reinjecao-gn.csv">Reinjeção de gás natural (mil metros cúbicos)</a>
    <a href="/ppgn/queima-perda-gn.csv">Queima e perda de gás natural</a>
    <a href="/ppgn/consumo-proprio-gn.csv">Consumo próprio de gás natural na E&P</a>
    <a href="/ppgn/gn-disponivel.csv">Gás natural disponível</a>
    """

    resources = anp_multi_page_resources(dataset, html)

    assert [item.resource_family for item in resources] == [
        "petroleum_production",
        "lgn_production",
        "natural_gas_production",
        "natural_gas_reinjection",
        "natural_gas_flaring_losses",
        "natural_gas_own_consumption",
        "natural_gas_available",
    ]
    assert all(item.url.endswith(".csv") for item in resources)
