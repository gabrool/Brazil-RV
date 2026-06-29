from __future__ import annotations

import yaml

from bralpha.infra.config import load_ons_dataset_registry

LIVE_DATASETS = {
    "ons_ear_subsystem_daily",
    "ons_ena_subsystem_daily",
    "ons_load_daily",
    "ons_cmo_weekly",
    "ons_energy_balance_subsystem",
    "ons_interchange_subsystem_hourly",
}

DEFERRED_DATASETS = {
    "ons_ear_reservoir_daily",
    "ons_ear_ree_daily",
    "ons_ear_basin_daily",
    "ons_ena_reservoir_daily",
    "ons_ena_ree_daily",
    "ons_ena_basin_daily",
    "ons_reservoir_hydrology_daily",
    "ons_reservoir_hydrology_hourly",
    "ons_generation_by_plant_hourly",
    "ons_constrained_off_wind",
    "ons_constrained_off_solar",
    "ons_thermal_dispatch_reason",
    "ons_installed_generation_capacity",
    "ons_generation_capacity_factor_wind_solar",
    "ons_reliability_indicators",
    "ons_transmission_assets",
}


def test_ons_dataset_registry_loads_live_and_deferred_datasets(repo_root):
    registry = load_ons_dataset_registry(repo_root)
    dataset_ids = {dataset.dataset_id for dataset in registry.datasets}

    assert registry.source == "ons"
    assert registry.raw_storage.manifest_path == "data/manifests/ons/downloads.jsonl"
    assert dataset_ids == LIVE_DATASETS | DEFERRED_DATASETS
    for dataset_id in LIVE_DATASETS:
        dataset = registry.get(dataset_id)
        extra = dataset.model_extra or {}
        assert dataset.source_map_status == "live_download"
        assert "ons-aws-prod-opendata" in extra["direct_url_template"]
        assert extra["filename_template"].endswith("_{year}.csv")
        assert extra["availability_policy"] == "ons_source_last_modified_snapshot"
    for dataset_id in DEFERRED_DATASETS:
        dataset = registry.get(dataset_id)
        assert dataset.source_map_status != "live_download"
        assert dataset.source_urls == []


def test_ons_source_map_docs_list_every_dataset(repo_root):
    text = (repo_root / "docs" / "ONS_SOURCE_MAP.md").read_text(encoding="utf-8")
    registry = load_ons_dataset_registry(repo_root)

    for dataset in registry.datasets:
        assert dataset.dataset_id in text
    assert "ons_conservative_next_business_day" in text
    assert "CC-BY" in text
    assert "no daily aggregation" in text.lower()


def test_ons_sources_config_documents_official_pages_and_dictionary_urls(repo_root):
    data = yaml.safe_load(
        (repo_root / "configs" / "sources" / "ons.yaml").read_text(encoding="utf-8")
    )
    page_ids = {page["dataset_id"] for page in data["official_pages"]}

    assert page_ids == LIVE_DATASETS
    assert data["portal"]["url"] == "https://dados.ons.org.br/"
    assert data["data_dictionaries"]["interchange_subsystem_hourly"].endswith(
        "DicionarioDados_Intercambio_Nacional.json"
    )
    assert "no_browser_automation" in data["endpoint_policy"]["live_download_requires"]
