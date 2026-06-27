from __future__ import annotations

import yaml

from bralpha.infra.config import load_fred_dataset_registry
from bralpha.ingestion.fred.common import load_fred_series_config

REQUIRED_P0_SERIES = {
    "DGS2",
    "DGS5",
    "DGS10",
    "DGS30",
    "DFF",
    "SOFR",
    "DFEDTARU",
    "DFEDTARL",
    "DFII5",
    "DFII10",
    "DFII30",
    "T5YIE",
    "T10YIE",
    "DTWEXBGS",
    "DTWEXAFEGS",
    "DTWEXEMEGS",
    "DEXCHUS",
    "SP500",
    "NASDAQCOM",
    "VIXCLS",
    "BAA10Y",
    "AAA10Y",
    "DBAA",
    "DAAA",
    "DCOILWTICO",
    "DCOILBRENTEU",
}


def test_fred_dataset_registry_loads(repo_root):
    registry = load_fred_dataset_registry(repo_root)
    dataset = registry.get("fred_series_observations")

    assert registry.raw_storage.manifest_path == "data/manifests/fred/downloads.jsonl"
    assert dataset.source_map_status == "live_download"
    assert dataset.primary_keys == ["series_id", "ref_date"]
    assert dataset.partition_keys == ["series_id", "year"]
    assert (dataset.model_extra or {})["api_key_env"] == "FRED_API_KEY"
    assert dataset.first_source_url().url_template == (
        "https://api.stlouisfed.org/fred/series/observations"
    )


def test_fred_series_config_contains_required_series_without_duplicates(repo_root):
    series = load_fred_series_config(repo_root)
    series_ids = [row.series_id for row in series]

    assert len(series_ids) == len(set(series_ids))
    assert REQUIRED_P0_SERIES.issubset(set(series_ids))
    assert {row.series_id for row in series if row.priority == "P0"} == REQUIRED_P0_SERIES
    assert next(row for row in series if row.series_id == "PCOPPUSDM").priority == "P1"
    assert {"EWZ", "EEM", "EMB", "HYG", "LQD"}.isdisjoint(series_ids)


def test_fred_source_map_docs_list_live_dataset_and_series(repo_root):
    text = (repo_root / "docs" / "FRED_SOURCE_MAP.md").read_text(encoding="utf-8")

    assert "fred_series_observations" in text
    for series_id in REQUIRED_P0_SERIES | {"PCOPPUSDM"}:
        assert series_id in text
    assert "FRED_API_KEY" in text
    assert "No fake endpoints" in text


def test_fred_sources_config_documents_endpoint_policy(repo_root):
    path = repo_root / "configs" / "sources" / "fred.yaml"
    data = yaml.safe_load(path.read_text(encoding="utf-8"))

    assert data["api"]["url"] == "https://api.stlouisfed.org/fred/series/observations"
    assert data["api"]["api_key_env"] == "FRED_API_KEY"
    assert "fixture_test" in data["endpoint_policy"]["live_download_requires"]
    assert data["deferred_sources"]["non_fred_market_data"]["status"] == (
        "deferred_pending_dedicated_source"
    )
