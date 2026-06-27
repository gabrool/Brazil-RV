from __future__ import annotations

import yaml

from bralpha.infra.config import load_cvm_dataset_registry

LIVE_DATASETS = {
    "cvm_fund_daily_reports",
    "cvm_fund_registry_current",
}

RAW_BRONZE_ONLY_DATASETS = {
    "cvm_fund_registry_history",
    "cvm_fund_class_registry",
}

DEFERRED_DATASETS = {
    "cvm_fund_portfolio_cda",
    "cvm_company_ipe_metadata",
    "cvm_fii_reports",
    "cvm_fidc_reports",
    "cvm_company_itr",
    "cvm_company_dfp",
    "cvm_public_offerings",
    "cvm_sanctions_regulatory_events",
}


def test_cvm_dataset_registry_loads_live_and_deferred_datasets(repo_root):
    registry = load_cvm_dataset_registry(repo_root)
    dataset_ids = {dataset.dataset_id for dataset in registry.datasets}

    assert registry.raw_storage.manifest_path == "data/manifests/cvm/downloads.jsonl"
    assert dataset_ids == LIVE_DATASETS | RAW_BRONZE_ONLY_DATASETS | DEFERRED_DATASETS
    daily = registry.get("cvm_fund_daily_reports")
    assert daily.source_map_status == "live_download"
    assert daily.primary_keys == ["ref_date", "fund_id"]
    assert daily.partition_keys == ["year", "month"]
    assert (daily.model_extra or {})["period_routing"] == {
        "historical_annual_through": 2020,
        "monthly_from": "2021-01",
    }
    for dataset_id in LIVE_DATASETS:
        dataset = registry.get(dataset_id)
        assert dataset.source_map_status == "live_download"
        assert dataset.source_urls
        for source_url in dataset.source_urls:
            assert "dados.cvm.gov.br" in str(source_url.url_template)
    for dataset_id in RAW_BRONZE_ONLY_DATASETS:
        dataset = registry.get(dataset_id)
        assert dataset.source_map_status == "raw_bronze_only_pending_normalizer"
        assert dataset.source_urls
        assert dataset.canonical_table.endswith("_raw")
    for dataset_id in DEFERRED_DATASETS:
        dataset = registry.get(dataset_id)
        assert dataset.source_map_status != "live_download"
        assert dataset.source_urls == []


def test_cvm_source_map_docs_list_every_dataset(repo_root):
    text = (repo_root / "docs" / "CVM_SOURCE_MAP.md").read_text(encoding="utf-8")

    for dataset_id in LIVE_DATASETS | RAW_BRONZE_ONLY_DATASETS | DEFERRED_DATASETS:
        assert dataset_id in text
    assert "No fake endpoints" in text
    assert "cvm_fund_daily_conservative_2bd" in text
    assert "INF_DIARIO/DADOS/HIST" in text
    assert "raw_bronze_only_pending_normalizer" in text


def test_cvm_sources_config_documents_official_pages_and_directories(repo_root):
    data = yaml.safe_load(
        (repo_root / "configs" / "sources" / "cvm.yaml").read_text(encoding="utf-8")
    )
    page_ids = {page["dataset_id"] for page in data["official_pages"]}

    assert page_ids.issuperset(
        {
            "cvm_fund_daily_reports",
            "cvm_fund_registry_current",
            "cvm_fund_portfolio_cda",
            "cvm_company_ipe_metadata",
            "cvm_company_itr",
            "cvm_company_dfp",
        }
    )
    assert data["direct_directories"]["fund_daily_reports_current"].endswith(
        "/FI/DOC/INF_DIARIO/DADOS/"
    )
    assert data["direct_directories"]["fund_daily_reports_history"].endswith(
        "/FI/DOC/INF_DIARIO/DADOS/HIST/"
    )
    assert "fixture_test" in data["endpoint_policy"]["live_download_requires"]
