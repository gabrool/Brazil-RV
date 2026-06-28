from __future__ import annotations

from datetime import date

import pytest

from bralpha.infra.config import load_novo_caged_dataset_registry
from bralpha.ingestion.novo_caged.resources import (
    novo_caged_monthly_resources,
    novo_caged_release_calendar_resource,
)


def test_novo_caged_2020_january_resource(repo_root):
    dataset = load_novo_caged_dataset_registry(repo_root).get("novo_caged_movements_monthly")

    resources = novo_caged_monthly_resources(dataset, date(2020, 1, 1), date(2020, 1, 31))

    assert len(resources) == 1
    assert resources[0].period == "202001"
    assert resources[0].filename == "CAGEDMOV202001.7z"
    assert resources[0].url.endswith("/2020/202001/CAGEDMOV202001.7z")
    assert resources[0].record_kind == "movement"


def test_novo_caged_multi_month_resources_are_chronological(repo_root):
    dataset = load_novo_caged_dataset_registry(repo_root).get("novo_caged_movements_monthly")

    resources = novo_caged_monthly_resources(dataset, date(2024, 1, 15), date(2024, 3, 1))

    assert [resource.period for resource in resources] == ["202401", "202402", "202403"]


def test_novo_caged_2019_only_window_returns_no_resources(repo_root):
    dataset = load_novo_caged_dataset_registry(repo_root).get("novo_caged_movements_monthly")

    resources = novo_caged_monthly_resources(dataset, date(2019, 1, 1), date(2019, 12, 31))

    assert resources == []


def test_novo_caged_crossing_2020_clips_to_first_live_month(repo_root):
    dataset = load_novo_caged_dataset_registry(repo_root).get("novo_caged_movements_monthly")

    resources = novo_caged_monthly_resources(dataset, date(2019, 12, 1), date(2020, 2, 29))

    assert [resource.period for resource in resources] == ["202001", "202002"]


def test_novo_caged_inverted_window_raises(repo_root):
    dataset = load_novo_caged_dataset_registry(repo_root).get("novo_caged_movements_monthly")

    with pytest.raises(ValueError, match="start <= end"):
        novo_caged_monthly_resources(dataset, date(2024, 2, 1), date(2024, 1, 1))


def test_novo_caged_release_calendar_resource(repo_root):
    dataset = load_novo_caged_dataset_registry(repo_root).get("novo_caged_release_calendar")

    resources = novo_caged_release_calendar_resource(dataset)

    assert len(resources) == 1
    assert resources[0].filename == "novo_caged_release_calendar.html"
    assert "calendario-de-divulgacao-do-novo-caged" in resources[0].url
