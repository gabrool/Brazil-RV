from __future__ import annotations

from pathlib import Path

from bralpha.infra.config import load_b3_dataset_registry

SOURCE_MAP_PATH = Path("docs/B3_P1_P2_SOURCE_MAP.md")

VALID_STATUSES = {
    "live_download",
    "manual_source_map",
    "raw_only",
    "not_implemented_pending_url",
}

REQUIRED_REGISTRY_DATASETS = {
    "b3_cotahist_daily",
    "b3_indexes_current_portfolio",
    "b3_indexes_theoretical_portfolio",
    "b3_equities_investor_participation",
    "b3_foreign_investor_movement",
    "b3_daily_bulletin_chapters",
    "b3_isin_database",
    "b3_trading_parameters",
    "b3_fee_schedules",
    "b3_market_data_public_reports",
    "b3_derivatives_reference_prices",
    "b3_product_specs_pages",
}

SOURCE_MAP_ONLY_CANDIDATES = {
    "b3_corporate_actions_public",
    "b3_securities_lending_public",
    "b3_margin_and_risk_parameters",
    "b3_index_methodology_and_divisors",
}


def test_p1_p2_dataset_registry_entries_are_complete(repo_root):
    registry = load_b3_dataset_registry(repo_root)
    dataset_ids = {dataset.dataset_id for dataset in registry.datasets}

    assert dataset_ids >= REQUIRED_REGISTRY_DATASETS
    for dataset_id in REQUIRED_REGISTRY_DATASETS:
        dataset = registry.get(dataset_id)
        assert dataset.priority in {"P1", "P2"}
        assert dataset.free_access is True
        assert dataset.requires_auth is False
        assert dataset.point_in_time_required is True
        assert dataset.raw_format
        assert dataset.license_note
        assert dataset.notes
        assert dataset.model_extra.get("source_map_status") in VALID_STATUSES


def test_url_availability_matches_source_map_status(repo_root):
    registry = load_b3_dataset_registry(repo_root)
    for dataset_id in REQUIRED_REGISTRY_DATASETS:
        dataset = registry.get(dataset_id)
        status = dataset.model_extra.get("source_map_status")
        if status == "not_implemented_pending_url":
            assert dataset.source_urls == []
        elif status in {"live_download", "raw_only"}:
            assert dataset.source_urls


def test_source_map_covers_required_and_candidate_datasets(repo_root):
    rows = _source_map_rows(repo_root / SOURCE_MAP_PATH)
    dataset_ids = {row["dataset_id"] for row in rows}

    assert dataset_ids >= REQUIRED_REGISTRY_DATASETS
    assert dataset_ids >= SOURCE_MAP_ONLY_CANDIDATES
    for row in rows:
        assert row["status"] in VALID_STATUSES
        assert row["source_url_or_page"]
        assert row["canonical_or_silver_output"]
        assert row["known_limitations"]


def _source_map_rows(path: Path) -> list[dict[str, str]]:
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.startswith("| b3_"):
            continue
        cells = [cell.strip() for cell in line.strip("|").split("|")]
        rows.append(
            {
                "dataset_id": cells[0],
                "priority": cells[1],
                "status": cells[2],
                "source_url_or_page": cells[3],
                "raw_format": cells[4],
                "expected_frequency": cells[5],
                "canonical_or_silver_output": cells[6],
                "known_limitations": cells[7],
            }
        )
    return rows
