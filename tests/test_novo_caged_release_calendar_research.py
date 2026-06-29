from __future__ import annotations

from datetime import UTC, date, datetime

import polars as pl

from bralpha.derived.novo_caged.movements import (
    build_movement_group_observation,
    build_movement_record_observation,
)
from bralpha.derived.novo_caged.release_calendar import build_release_calendar_reference


def test_release_calendar_reference_preserves_official_fields_and_unique_pk():
    silver = pl.DataFrame(
        {
            "ref_date": [date(2024, 1, 31), date(2024, 5, 31)],
            "release_date": [date(2024, 3, 1), date(2024, 6, 28)],
            "available_date": [date(2024, 3, 4), date(2024, 7, 1)],
            "availability_policy": [
                "novo_caged_official_release_calendar",
                "novo_caged_official_release_calendar",
            ],
            "release_year": [2024, 2024],
            "competence_label": ["janeiro de 2024", "maio de 2024"],
            "source": ["novo_caged", "novo_caged"],
            "source_dataset": ["novo_caged_release_calendar", "novo_caged_release_calendar"],
            "download_timestamp_utc": [None, None],
            "raw_path": ["calendar.html", "calendar.html"],
            "sha256": ["abc", "abc"],
            "source_version": ["v0", "v0"],
        }
    )

    panel = build_release_calendar_reference(silver)

    assert panel.columns == [
        "ref_date",
        "release_date",
        "available_date",
        "availability_policy",
        "availability_basis",
        "revision_policy",
        "source_publication_datetime_utc",
        "source_last_modified_utc",
        "first_seen_timestamp_utc",
        "vintage_id",
        "revision_sequence",
        "model_usable",
        "model_usable_reason",
        "release_year",
        "competence_label",
        "source_version",
    ]
    assert panel.group_by(["ref_date"]).len().height == 2
    assert panel["competence_label"].to_list() == ["janeiro de 2024", "maio de 2024"]


def test_release_calendar_reference_is_used_by_movement_group_availability():
    movements = pl.DataFrame(
        [
            {
                "movement_record_id": "r1",
                "ref_date": date(2024, 1, 31),
                "available_date": date(2024, 3, 15),
                "availability_policy": "novo_caged_conservative_next_month_end_plus_2bd",
                "competence": "202401",
                "year": 2024,
                "month": 1,
                "record_kind": "movement",
                "region": "Sudeste",
                "state": "SP",
                "municipality_code": "3550308",
                "cnae_section": "G",
                "cnae_subclass": "4711302",
                "occupation_code": "411005",
                "movement_type_code": "10",
                "movement_sign": "1",
                "employment_category": "101",
                "education_degree": "7",
                "age": 32,
                "sex": "1",
                "race_color": "2",
                "disability_type": "0",
                "employer_type": "0",
                "establishment_type": "1",
                "establishment_size_jan": "5",
                "contract_hours": 44.0,
                "wage": 2500.0,
                "wage_unit": "1",
                "is_apprentice": False,
                "is_intermittent": False,
                "is_part_time": False,
                "source_system": "eSocial",
                "first_seen_timestamp_utc": datetime(2024, 3, 1, 12, tzinfo=UTC),
                "source_version": "v0",
            }
        ]
    )
    calendar = pl.DataFrame(
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

    panel = build_movement_group_observation(
        build_movement_record_observation(movements),
        release_calendar=calendar,
        prefer_official_calendar=True,
        group_by=["all"],
        cross_by=["movement_sign"],
        max_groups=10,
    )

    row = panel.to_dicts()[0]
    assert row["calendar_available_date"] == date(2024, 3, 4)
    assert row["silver_available_date"] == date(2024, 3, 15)
    assert row["available_date"] == date(2024, 3, 4)
    assert row["availability_source"] == "official_calendar_plus_snapshot"
    assert row["model_usable"] is True
