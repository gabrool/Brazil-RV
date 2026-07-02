from __future__ import annotations

from datetime import date

import polars as pl
import pytest

from bralpha.derived.novo_caged.movements import (
    build_movement_group_observation,
    build_movement_record_observation,
)


def test_movement_record_observation_preserves_rows_and_missingness_flags():
    panel = build_movement_record_observation(_movement_silver())

    assert panel.height == 4
    assert panel["movement_record_id"].to_list() == ["jan-1", "jan-2", "jan-3", "feb-1"]
    assert panel.filter(pl.col("movement_record_id") == "jan-2")["has_wage"].to_list() == [False]
    assert panel.filter(pl.col("movement_record_id") == "jan-3")[
        "has_contract_hours"
    ].to_list() == [False]


def test_movement_group_observation_uses_calendar_availability_and_aggregates():
    records = build_movement_record_observation(_movement_silver())
    calendar = _release_reference()

    panel = build_movement_group_observation(
        records,
        release_calendar=calendar,
        prefer_official_calendar=True,
        group_by=["all", "region", "state", "cnae_section"],
        cross_by=["movement_sign"],
        max_groups=50,
    )
    all_sign_one = panel.filter(
        (pl.col("ref_date") == date(2024, 1, 31))
        & (pl.col("group_type") == "all")
        & (pl.col("group_value") == "all")
        & (pl.col("movement_sign") == "1")
    ).to_dicts()[0]

    assert all_sign_one["movement_count"] == 2
    assert all_sign_one["wage_mean"] == 1000.0
    assert all_sign_one["contract_hours_mean"] == 42.0
    assert all_sign_one["wage_count"] == 1
    assert all_sign_one["contract_hours_count"] == 2
    assert all_sign_one["silver_available_date"] == date(2024, 3, 12)
    assert all_sign_one["calendar_available_date"] == date(2024, 3, 4)
    assert all_sign_one["available_date"] == date(2024, 3, 4)
    assert all_sign_one["availability_source"] == "official_calendar"
    assert all_sign_one["feature_id"] == "novo_caged_movement|all|all|1"
    assert panel.group_by(["ref_date", "group_type", "group_value", "movement_sign"]).len().filter(
        pl.col("len") > 1
    ).is_empty()


def test_movement_group_observation_requires_official_calendar_for_model_availability():
    records = build_movement_record_observation(_movement_silver())

    panel = build_movement_group_observation(
        records,
        release_calendar=None,
        prefer_official_calendar=True,
        group_by=["all"],
        cross_by=["movement_sign"],
        max_groups=50,
    )
    row = panel.filter(
        (pl.col("ref_date") == date(2024, 1, 31)) & (pl.col("movement_sign") == "1")
    ).to_dicts()[0]

    assert row["available_date"] is None
    assert row["calendar_available_date"] is None
    assert row["availability_source"] == "missing_official_calendar"
    assert row["availability_policy"] == "novo_caged_official_release_calendar_required"


def test_movement_group_observation_normalizes_group_values_and_sign_tokens():
    records = build_movement_record_observation(_movement_silver())

    panel = build_movement_group_observation(
        records,
        release_calendar=None,
        prefer_official_calendar=True,
        group_by=["state"],
        cross_by=["movement_sign"],
        max_groups=50,
    )

    assert {
        "novo_caged_movement|state|sp|1",
        "novo_caged_movement|state|am|minus_1",
    } <= set(panel["feature_id"].to_list())


def test_movement_group_observation_max_groups_guard():
    records = build_movement_record_observation(_movement_silver())

    with pytest.raises(ValueError, match="exceeds max_groups"):
        build_movement_group_observation(
            records,
            release_calendar=None,
            prefer_official_calendar=True,
            group_by=["all", "region", "state", "cnae_section"],
            cross_by=["movement_sign"],
            max_groups=1,
        )


def _movement_silver() -> pl.DataFrame:
    rows = [
        _row(
            "jan-1",
            date(2024, 1, 31),
            date(2024, 3, 10),
            "Sudeste",
            "SP",
            "G",
            "1",
            1000.0,
            40.0,
        ),
        _row("jan-2", date(2024, 1, 31), date(2024, 3, 12), "Sudeste", "SP", "G", "1", None, 44.0),
        _row("jan-3", date(2024, 1, 31), date(2024, 3, 12), "Norte", "AM", "A", "-1", 2000.0, None),
        _row("feb-1", date(2024, 2, 29), date(2024, 4, 3), "Sudeste", "SP", "G", "1", None, 40.0),
    ]
    return pl.DataFrame(rows)


def _row(
    row_id: str,
    ref_date: date,
    available_date: date,
    region: str,
    state: str,
    cnae_section: str,
    movement_sign: str,
    wage: float | None,
    hours: float | None,
) -> dict[str, object]:
    return {
        "movement_record_id": row_id,
        "ref_date": ref_date,
        "available_date": available_date,
        "availability_policy": "novo_caged_conservative_next_month_end_plus_2bd_reference_only",
        "availability_basis": "conservative_heuristic",
        "revision_policy": "current_snapshot_reference_only",
        "model_usable": False,
        "non_model_usable_reason": "novo_caged_movement_requires_official_release_calendar",
        "competence": f"{ref_date.year}{ref_date.month:02d}",
        "year": ref_date.year,
        "month": ref_date.month,
        "record_kind": "movement",
        "region": region,
        "state": state,
        "municipality_code": "0000000",
        "cnae_section": cnae_section,
        "cnae_subclass": "0000000",
        "occupation_code": "000000",
        "movement_type_code": "10",
        "movement_sign": movement_sign,
        "employment_category": "101",
        "education_degree": "7",
        "age": 30,
        "sex": "1",
        "race_color": "2",
        "disability_type": "0",
        "employer_type": "0",
        "establishment_type": "1",
        "establishment_size_jan": "5",
        "contract_hours": hours,
        "wage": wage,
        "wage_unit": "1",
        "is_apprentice": False,
        "is_intermittent": False,
        "is_part_time": False,
        "source_system": "eSocial",
        "raw_competenciamov": f"{ref_date.year}{ref_date.month:02d}",
        "raw_saldomovimentacao": movement_sign,
        "raw_tipomovimentacao": "10",
        "raw_salario": None if wage is None else str(wage),
        "raw_valorsalariofixo": None,
        "source": "novo_caged",
        "source_dataset": "novo_caged_movements_monthly",
        "download_timestamp_utc": None,
        "raw_path": "raw.7z",
        "sha256": "abc",
        "source_version": "v0",
    }


def _release_reference() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "ref_date": [date(2024, 1, 31)],
            "release_date": [date(2024, 3, 1)],
            "available_date": [date(2024, 3, 4)],
            "availability_policy": ["novo_caged_official_release_calendar"],
            "release_year": [2024],
            "competence_label": ["janeiro de 2024"],
            "source_version": ["v0"],
        }
    )
