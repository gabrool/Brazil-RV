from __future__ import annotations

import yaml

from bralpha.infra.config import load_anp_dataset_registry


def test_anp_dataset_and_source_configs_load(repo_root):
    registry = load_anp_dataset_registry(repo_root)
    sources = yaml.safe_load((repo_root / "configs" / "sources" / "anp.yaml").read_text())

    assert registry.source == "anp"
    assert sources["source"] == "anp"
    assert registry.get("anp_fuel_prices_weekly").source_map_status == "live_download"


def test_anp_docs_list_every_configured_dataset(repo_root):
    registry = load_anp_dataset_registry(repo_root)
    doc = (repo_root / "docs" / "ANP_SOURCE_MAP.md").read_text(encoding="utf-8")

    for dataset in registry.datasets:
        assert dataset.dataset_id in doc


def test_anp_live_and_deferred_dataset_scope(repo_root):
    registry = load_anp_dataset_registry(repo_root)
    live_ids = {
        dataset.dataset_id
        for dataset in registry.datasets
        if dataset.source_map_status == "live_download"
    }

    assert live_ids == {
        "anp_fuel_prices_weekly",
        "anp_fuel_sales_monthly",
        "anp_oil_gas_production_monthly",
    }
    assert registry.get("anp_fuel_prices_weekly").priority == "P0"
    assert registry.get("anp_oil_gas_production_monthly").priority == "P1"

    deferred = [
        dataset
        for dataset in registry.datasets
        if dataset.source_map_status != "live_download"
    ]
    assert deferred
    assert all(not dataset.source_urls for dataset in deferred)


def test_anp_no_fake_endpoints_for_source_map_only_datasets(repo_root):
    registry = load_anp_dataset_registry(repo_root)

    for dataset in registry.datasets:
        if dataset.source_map_status != "live_download":
            assert dataset.source_urls == []
            assert dataset.model_extra["source_map_status"].startswith("source_map_only")
