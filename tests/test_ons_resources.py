from __future__ import annotations

from datetime import date

import pytest

from bralpha.infra.config import load_ons_dataset_registry
from bralpha.ingestion.ons.resources import ons_annual_resources


def test_ons_annual_resources_single_year_template_rendering(repo_root):
    dataset = load_ons_dataset_registry(repo_root).get("ons_ear_subsystem_daily")

    resources = ons_annual_resources(dataset, start=date(2024, 6, 1), end=date(2024, 6, 30))

    assert len(resources) == 1
    assert resources[0].year == 2024
    assert resources[0].resource_name == "EAR_Diario_por_Subsistema-2024"
    assert resources[0].filename == "EAR_DIARIO_SUBSISTEMA_2024.csv"
    assert resources[0].url.endswith("/EAR_DIARIO_SUBSISTEMA_2024.csv")


def test_ons_annual_resources_multi_year_window(repo_root):
    dataset = load_ons_dataset_registry(repo_root).get("ons_load_daily")

    resources = ons_annual_resources(dataset, start=date(2023, 12, 31), end=date(2025, 1, 1))

    assert [resource.year for resource in resources] == [2023, 2024, 2025]
    assert [resource.filename for resource in resources] == [
        "CARGA_ENERGIA_2023.csv",
        "CARGA_ENERGIA_2024.csv",
        "CARGA_ENERGIA_2025.csv",
    ]


def test_ons_annual_resources_reject_inverted_windows(repo_root):
    dataset = load_ons_dataset_registry(repo_root).get("ons_cmo_weekly")

    with pytest.raises(ValueError, match="start <= end"):
        ons_annual_resources(dataset, start=date(2025, 1, 1), end=date(2024, 1, 1))
