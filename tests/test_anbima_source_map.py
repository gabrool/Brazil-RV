from __future__ import annotations

import yaml

from bralpha.infra.config import load_anbima_dataset_registry

ANBIMA_DATASETS = {
    "anbima_sovereign_secondary_market",
    "anbima_sovereign_yield_curves",
    "anbima_vna",
    "anbima_fixed_income_indices",
    "anbima_inflation_projections",
    "anbima_debenture_secondary_market",
    "anbima_credit_curves",
    "anbima_fund_industry_statistics",
    "anbima_fund_flows",
}


def test_anbima_dataset_config_loads_all_requested_datasets(repo_root):
    registry = load_anbima_dataset_registry(repo_root)

    assert {dataset.dataset_id for dataset in registry.datasets} == ANBIMA_DATASETS
    assert registry.raw_storage.manifest_path == "data/manifests/anbima/downloads.jsonl"
    assert registry.get("anbima_sovereign_secondary_market").source_map_status == (
        "not_implemented_pending_endpoint"
    )
    assert registry.get("anbima_fund_industry_statistics").source_map_status == (
        "not_implemented_pending_endpoint"
    )
    assert registry.get("anbima_fund_flows").source_urls == []


def test_anbima_source_metadata_loads_official_pages(repo_root):
    data = yaml.safe_load((repo_root / "configs" / "sources" / "anbima.yaml").read_text())

    page_names = {page["name"] for page in data["official_pages"]}
    assert {
        "taxas_titulos_publicos",
        "indices",
        "dados",
        "projecoes_ipca_igpm",
        "anbima_data",
    }.issubset(page_names)
    assert "stable_direct_url" in data["endpoint_policy"]["live_download_requires"]
    assert "no_browser_automation" in data["endpoint_policy"]["live_download_requires"]
    page_by_name = {page["name"]: page for page in data["official_pages"]}
    assert "anbima_fund_industry_statistics" in page_by_name["dados"]["dataset_ids"]
    assert "anbima_fund_flows" in page_by_name["anbima_data"]["dataset_ids"]


def test_anbima_source_map_lists_all_requested_datasets(repo_root):
    text = (repo_root / "docs" / "ANBIMA_SOURCE_MAP.md").read_text(encoding="utf-8")

    for dataset_id in ANBIMA_DATASETS:
        assert f"`{dataset_id}`" in text


def test_anbima_pending_datasets_have_no_fake_source_urls(repo_root):
    registry = load_anbima_dataset_registry(repo_root)

    for dataset in registry.datasets:
        if dataset.source_map_status == "live_download":
            assert dataset.source_urls
            assert dataset.model_extra.get("endpoint_verified") is True
        else:
            assert dataset.source_map_status == "not_implemented_pending_endpoint"
            assert dataset.source_urls == []


def test_anbima_source_map_records_conditional_live_policy(repo_root):
    text = (repo_root / "docs" / "ANBIMA_SOURCE_MAP.md").read_text(encoding="utf-8")

    assert "A dataset may be marked `live_download` only when" in text
    assert "no login requirement" in text
    assert "no browser automation" in text
