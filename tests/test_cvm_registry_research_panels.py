from __future__ import annotations

from datetime import date

import polars as pl

from bralpha.derived.cvm.fund_reports import (
    build_fund_daily_observation,
    build_fund_group_observation,
)
from bralpha.derived.cvm.registry import build_fund_registry_current_reference


def test_fund_registry_current_reference_preserves_current_metadata_only():
    panel = build_fund_registry_current_reference(
        pl.DataFrame(
            [
                _registry_row(
                    fund_id="fund-a",
                    fund_type="FI",
                    fund_name="Older Fund",
                    snapshot_date=date(2024, 1, 1),
                ),
                _registry_row(
                    fund_id="fund-a",
                    fund_type="FI",
                    fund_name="Current Fund",
                    snapshot_date=date(2024, 2, 1),
                ),
            ]
        )
    )

    assert panel.height == 1
    row = panel.row(0, named=True)
    assert row["fund_id"] == "fund-a"
    assert row["fund_name"] == "Current Fund"
    assert row["admin_name"] == "Admin Teste"
    assert "ref_date" not in panel.columns
    assert "available_date" not in panel.columns


def test_registry_reference_is_not_joined_into_historical_group_panels():
    registry = build_fund_registry_current_reference(
        pl.DataFrame(
            [
                _registry_row(
                    fund_id="fund-a",
                    fund_type="Registry Type",
                    fund_name="Current Fund",
                    snapshot_date=date(2024, 2, 1),
                )
            ]
        )
    )
    observations = build_fund_daily_observation(
        pl.DataFrame(
            [
                {
                    "ref_date": date(2024, 1, 2),
                    "available_date": date(2024, 1, 4),
                    "availability_policy": "cvm_fund_daily_conservative_2bd",
                    "fund_id": "fund-a",
                    "fund_type": "Daily Type",
                    "portfolio_value": 100.0,
                    "nav": 90.0,
                    "quota_value": 1.0,
                    "subscriptions": 5.0,
                    "redemptions": 1.0,
                    "shareholder_count": 10,
                    "raw_vl_total": "100.0",
                    "raw_vl_patrim_liq": "90.0",
                    "raw_vl_quota": "1.0",
                    "raw_captc_dia": "5.0",
                    "raw_resg_dia": "1.0",
                    "raw_nr_cotst": "10",
                    "source": "cvm",
                    "source_dataset": "cvm_fund_daily_reports",
                    "download_timestamp_utc": None,
                    "raw_path": "raw.zip",
                    "sha256": "abc",
                    "source_version": "v0",
                }
            ]
        )
    )

    groups = build_fund_group_observation(
        observations,
        group_by=["fund_type"],
        max_groups=100,
    )

    assert registry["fund_type"].item() == "Registry Type"
    assert groups["group_value"].to_list() == ["daily_type"]
    assert "fund_name" not in groups.columns
    assert "class_name" not in groups.columns


def _registry_row(
    *,
    fund_id: str,
    fund_type: str,
    fund_name: str,
    snapshot_date: date,
) -> dict[str, object]:
    return {
        "fund_id": fund_id,
        "fund_type": fund_type,
        "fund_name": fund_name,
        "cvm_code": "001",
        "registration_date": date(2020, 1, 1),
        "constitution_date": date(2020, 1, 1),
        "cancellation_date": None,
        "status": "EM FUNCIONAMENTO NORMAL",
        "status_start_date": date(2020, 1, 1),
        "activity_start_date": date(2020, 1, 2),
        "class_name": "Classe",
        "class_start_date": date(2020, 1, 2),
        "benchmark_or_return_target": None,
        "condominium_type": "Aberto",
        "is_fund_of_funds": "N",
        "is_exclusive": "N",
        "is_long_term_tax": "S",
        "public_target": "Geral",
        "admin_id": "11",
        "admin_name": "Admin Teste",
        "manager_id": "22",
        "manager_name": "Gestor Teste",
        "custodian_id": "33",
        "custodian_name": "Custodiante Teste",
        "auditor_id": "44",
        "auditor_name": "Auditor Teste",
        "controller_id": "55",
        "controller_name": "Controlador Teste",
        "snapshot_date": snapshot_date,
        "source": "cvm",
        "source_dataset": "cvm_fund_registry_current",
        "download_timestamp_utc": None,
        "raw_path": "cad_fi.csv",
        "sha256": "abc",
        "source_version": "v0",
    }
