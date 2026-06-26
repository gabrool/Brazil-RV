from __future__ import annotations

import yaml

from bralpha.infra.config import load_ibge_dataset_registry
from bralpha.ingestion.ibge.sidra import load_sidra_series_config, select_sidra_series

IBGE_DATASETS = {
    "ibge_sidra_series",
    "ibge_release_calendar",
    "ibge_products_metadata",
    "ibge_news_releases_metadata",
    "ibge_errata_revisions_metadata",
}


def test_ibge_dataset_config_loads_all_requested_datasets(repo_root):
    registry = load_ibge_dataset_registry(repo_root)

    assert {dataset.dataset_id for dataset in registry.datasets} == IBGE_DATASETS
    assert registry.raw_storage.manifest_path == "data/manifests/ibge/downloads.jsonl"
    assert registry.get("ibge_sidra_series").source_map_status == "live_download"


def test_ibge_sidra_series_config_loads_and_defaults_to_p0(repo_root):
    series = load_sidra_series_config(repo_root)

    assert any(item.dataset_slug == "ipca" and item.aggregate_id == 7060 for item in series)
    assert any(item.priority == "P1" and item.aggregate_id == 6903 for item in series)
    assert {item.priority for item in select_sidra_series(series)} == {"P0"}
    assert {item.priority for item in select_sidra_series(series, priority=["P1"])} == {"P1"}


def test_ibge_source_map_lists_all_requested_datasets(repo_root):
    text = (repo_root / "docs" / "IBGE_SOURCE_MAP.md").read_text(encoding="utf-8")

    for dataset_id in IBGE_DATASETS:
        assert f"`{dataset_id}`" in text


def test_ibge_errata_has_no_fake_live_downloader(repo_root):
    registry = load_ibge_dataset_registry(repo_root)
    dataset = registry.get("ibge_errata_revisions_metadata")

    assert dataset.source_map_status == "not_implemented_pending_url"
    assert dataset.source_urls == []
    assert "Source-map candidate" in dataset.notes


def test_ibge_series_yaml_is_explicit(repo_root):
    data = yaml.safe_load((repo_root / "configs" / "series" / "ibge_sidra.yaml").read_text())

    for row in data["series"]:
        assert row["dataset_slug"]
        assert row["aggregate_id"]
        assert row["locations"] == "N1[all]"
        assert row["period_selector"] == "date_range"
        assert row["release_calendar_product_id_status"]
        if row["release_calendar_product_id"] is not None and row["model_usable"]:
            assert row["release_calendar_product_id_status"] == "verified"
        if row["release_calendar_product_id_status"] == "needs_verification":
            assert row["model_usable"] is False
