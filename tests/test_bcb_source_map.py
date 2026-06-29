from __future__ import annotations

from bralpha.infra.config import load_bcb_dataset_registry

BCB_DATASETS = {
    "bcb_sgs_series",
    "bcb_ptax_exchange_rates",
    "bcb_focus_expectations",
    "bcb_focus_top5_expectations",
    "bcb_focus_top5_reference_dates",
    "bcb_copom_calendar",
    "bcb_copom_decisions",
    "bcb_copom_documents",
    "bcb_monetary_policy_reports",
    "bcb_speeches_press_releases",
}

TEXT_HEAVY_DATASETS = {
    "bcb_copom_documents",
    "bcb_monetary_policy_reports",
    "bcb_speeches_press_releases",
}


def test_bcb_source_map_lists_all_requested_datasets(repo_root):
    text = (repo_root / "docs" / "BCB_SOURCE_MAP.md").read_text(encoding="utf-8")

    for dataset_id in BCB_DATASETS:
        assert f"`{dataset_id}`" in text


def test_bcb_source_map_scopes_sgs_reference_expansion(repo_root):
    text = (repo_root / "docs" / "BCB_SOURCE_MAP.md").read_text(encoding="utf-8")
    raw_to_research = (repo_root / "docs" / "BCB_RAW_TO_RESEARCH_SPINE.md").read_text(
        encoding="utf-8"
    )

    assert "monetary/liquidity metadata" in text
    assert (
        "Model-ready SGS currently includes Selic, IPCA, and daily international reserves"
        in text
    )
    assert "BCB_LIVE_TESTS=1" in text
    assert "sgs_feature_daily" in raw_to_research
    assert "international reserves liquidity" in raw_to_research


def test_bcb_text_heavy_datasets_are_pending_or_raw_only(repo_root):
    registry = load_bcb_dataset_registry(repo_root)

    for dataset_id in TEXT_HEAVY_DATASETS:
        dataset = registry.get(dataset_id)
        assert dataset.source_map_status in {
            "not_implemented_pending_url",
            "raw_only_or_pending_url",
        }
        assert "No NLP" in dataset.notes or "Metadata/source-map only" in dataset.notes


def test_bcb_dataset_config_loads_all_requested_datasets(repo_root):
    registry = load_bcb_dataset_registry(repo_root)

    assert {dataset.dataset_id for dataset in registry.datasets} == BCB_DATASETS
    assert registry.raw_storage.manifest_path == "data/manifests/bcb/downloads.jsonl"
