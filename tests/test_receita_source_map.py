from __future__ import annotations

import yaml

from bralpha.infra.config import load_receita_dataset_registry


def test_receita_dataset_and_source_configs_load(repo_root):
    registry = load_receita_dataset_registry(repo_root)
    source_config = yaml.safe_load((repo_root / "configs" / "sources" / "receita.yaml").read_text())

    assert registry.source == "receita"
    assert registry.get("receita_tax_collection_monthly").source_map_status == "live_download"
    assert source_config["source"] == "receita"
    assert "Receita Federal" in source_config["portal"]["name"]


def test_receita_docs_list_every_configured_dataset(repo_root):
    registry = load_receita_dataset_registry(repo_root)
    docs = (repo_root / "docs" / "RECEITA_SOURCE_MAP.md").read_text(encoding="utf-8")

    for dataset in registry.datasets:
        assert dataset.dataset_id in docs


def test_receita_live_dataset_has_official_page_and_discovery_rules(repo_root):
    dataset = load_receita_dataset_registry(repo_root).get("receita_tax_collection_monthly")
    discovery = dataset.resource_discovery

    assert dataset.source_page_url == "https://dados.gov.br/dados/conjuntos-dados/resultado-da-arrecadacao"
    assert discovery["source_page_url"] == dataset.source_page_url
    assert (
        discovery["metadata_api_url"]
        == "https://dados.gov.br/dados/api/publico/conjuntos-dados/resultado-da-arrecadacao"
    )
    assert discovery["mode"] == "official_metadata_api_then_static_html"
    assert dataset.source_urls[0].url_template.startswith("https://dados.gov.br/")
    assert "arrecadação" in dataset.link_text_contains_any
    assert ".csv" in discovery["accepted_extensions"]
    assert ".pdf" in discovery["rejected_extensions"]
    forbidden_tokens = ("TO_BE_FILLED", "placeholder", "example.com")
    assert not any(token in str(dataset.model_extra) for token in forbidden_tokens)


def test_receita_deferred_datasets_have_no_fake_endpoints(repo_root):
    registry = load_receita_dataset_registry(repo_root)

    deferred = [
        dataset
        for dataset in registry.datasets
        if dataset.dataset_id != "receita_tax_collection_monthly"
    ]
    assert deferred
    assert all(not dataset.source_urls for dataset in deferred)
    assert all("source_map_only" in str(dataset.source_map_status) for dataset in deferred)
    assert registry.get("receita_tax_collection_by_state_monthly").priority == "P1"
