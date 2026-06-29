from __future__ import annotations

import yaml

from bralpha.infra.config import load_tesouro_dataset_registry

TESOURO_DATASETS = {
    "tesouro_direto_prices_rates",
    "tesouro_direto_sales",
    "tesouro_direto_redemptions",
    "tesouro_direto_stock",
    "tesouro_dpf_stock",
    "tesouro_rtn_series",
    "tesouro_dpf_emissions_redemptions",
    "tesouro_direto_operations",
    "tesouro_direto_investors",
    "tesouro_capag_states",
    "tesouro_capag_municipalities",
    "tesouro_auction_calendar_results",
}


def test_tesouro_dataset_registry_loads_source_map(repo_root):
    registry = load_tesouro_dataset_registry(repo_root)
    datasets = {dataset.dataset_id: dataset for dataset in registry.datasets}

    assert set(datasets) == TESOURO_DATASETS
    assert registry.raw_storage.manifest_path == "data/manifests/tesouro/downloads.jsonl"
    assert {
        dataset.dataset_id
        for dataset in registry.datasets
        if dataset.source_map_status == "live_download"
    } == {
        "tesouro_direto_prices_rates",
        "tesouro_direto_sales",
        "tesouro_direto_redemptions",
        "tesouro_direto_stock",
        "tesouro_dpf_stock",
    }
    assert datasets["tesouro_rtn_series"].source_map_status == "live_download_if_api_verified"
    assert datasets["tesouro_rtn_series"].source_urls == []
    assert datasets["tesouro_direto_sales"].model_extra["availability_policy"] == (
        "tesouro_direto_sales_official_2bd"
    )
    assert datasets["tesouro_direto_redemptions"].model_extra["availability_policy"] == (
        "tesouro_direto_redemptions_conservative_2bd"
    )
    assert datasets["tesouro_capag_states"].source_map_status == "source_map_only_p1"
    assert datasets["tesouro_capag_states"].source_urls == []


def test_tesouro_sources_config_and_docs_cover_all_datasets(repo_root):
    sources = yaml.safe_load((repo_root / "configs" / "sources" / "tesouro.yaml").read_text())
    pages = {page["dataset_id"] for page in sources["official_pages"]}
    docs = (repo_root / "docs" / "TESOURO_SOURCE_MAP.md").read_text(encoding="utf-8")

    assert sources["portal"]["api_base_url"].endswith("/ckan/api/3/action")
    assert TESOURO_DATASETS - pages == {"tesouro_auction_calendar_results"}
    for dataset_id in TESOURO_DATASETS:
        assert dataset_id in docs
