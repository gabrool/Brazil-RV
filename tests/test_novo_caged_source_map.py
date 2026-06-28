from __future__ import annotations

import yaml

from bralpha.infra.config import load_novo_caged_dataset_registry


def test_novo_caged_dataset_and_source_configs_load(repo_root):
    registry = load_novo_caged_dataset_registry(repo_root)
    source_config = yaml.safe_load(
        (repo_root / "configs" / "sources" / "novo_caged.yaml").read_text()
    )

    assert registry.source == "novo_caged"
    assert registry.get("novo_caged_movements_monthly").source_map_status == "live_download"
    assert registry.get("novo_caged_release_calendar").source_map_status == "live_download"
    assert source_config["portal"]["name"] == "Ministerio do Trabalho e Emprego / PDET"


def test_novo_caged_source_map_lists_every_configured_dataset(repo_root):
    registry = load_novo_caged_dataset_registry(repo_root)
    doc = (repo_root / "docs" / "NOVO_CAGED_SOURCE_MAP.md").read_text(encoding="utf-8")

    for dataset in registry.datasets:
        assert dataset.dataset_id in doc


def test_novo_caged_live_datasets_have_real_urls_or_templates(repo_root):
    movements = load_novo_caged_dataset_registry(repo_root).get("novo_caged_movements_monthly")
    calendar = load_novo_caged_dataset_registry(repo_root).get("novo_caged_release_calendar")

    family = movements.model_extra["resource_families"][0]
    assert "ftp.mtps.gov.br/pdet/microdados/NOVO%20CAGED" in family["url_template"]
    assert "CAGEDMOV{period}.7z" in family["filename_template"]
    assert calendar.first_source_url().url_template.startswith("https://www.gov.br/")


def test_novo_caged_deferred_datasets_have_no_fake_endpoints(repo_root):
    registry = load_novo_caged_dataset_registry(repo_root)
    live_ids = {"novo_caged_movements_monthly", "novo_caged_release_calendar"}

    for dataset in registry.datasets:
        if dataset.dataset_id in live_ids:
            continue
        assert dataset.source_map_status.startswith("source_map_only")
        assert dataset.source_urls == []
