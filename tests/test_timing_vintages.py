from __future__ import annotations

from datetime import UTC, date, datetime

import polars as pl
import pytest

from bralpha.timing.vintages import (
    AVAILABILITY_CURRENT_SNAPSHOT_NO_VINTAGE,
    AVAILABILITY_EXACT_SOURCE_TIMESTAMP,
    AVAILABILITY_FIRST_SEEN_DOWNLOAD_TIMESTAMP,
    AVAILABILITY_FRED_VINTAGE_REQUEST,
    AVAILABILITY_OFFICIAL_RELEASE_CALENDAR,
    AVAILABILITY_SOURCE_DATE_ONLY,
    AVAILABILITY_UNKNOWN,
    REVISION_CURRENT_SNAPSHOT_REFERENCE_ONLY,
    REVISION_REVISED_USE_FIRST_SEEN,
    REVISION_REVISED_USE_VINTAGES,
    REVISION_UNREVISED,
    assert_pit_audit_columns,
    assert_pit_model_ready_panel,
    availability_basis_from_metadata,
    available_date_from_first_seen,
    make_vintage_id,
    missing_pit_audit_columns,
    model_usable_from_revision_policy,
    pit_audit_violations,
)


def test_make_vintage_id_is_stable_and_content_sensitive():
    first = make_vintage_id(
        source="FRED",
        dataset_id="fred_series_observations",
        resource_id="DGS10",
        publication_timestamp=date(2024, 1, 2),
        first_seen_timestamp_utc=datetime(2024, 1, 3, 12, tzinfo=UTC),
        content_hash="abc",
    )
    second = make_vintage_id(
        source="fred",
        dataset_id="fred_series_observations",
        resource_id="DGS10",
        publication_timestamp=date(2024, 1, 2),
        first_seen_timestamp_utc=datetime(2024, 1, 3, 12, tzinfo=UTC),
        content_hash="abc",
    )
    changed = make_vintage_id(
        source="fred",
        dataset_id="fred_series_observations",
        resource_id="DGS10",
        publication_timestamp=date(2024, 1, 2),
        first_seen_timestamp_utc=datetime(2024, 1, 3, 12, tzinfo=UTC),
        content_hash="def",
    )

    assert first == second
    assert first.startswith("fred:fred_series_observations:")
    assert changed != first


@pytest.mark.parametrize(
    ("kwargs", "expected"),
    [
        (
            {"source_publication_datetime_utc": datetime(2024, 1, 2, 13, tzinfo=UTC)},
            AVAILABILITY_EXACT_SOURCE_TIMESTAMP,
        ),
        ({"source_publication_date": date(2024, 1, 2)}, AVAILABILITY_SOURCE_DATE_ONLY),
        ({"vintage_date": date(2024, 1, 3)}, AVAILABILITY_SOURCE_DATE_ONLY),
        (
            {"first_seen_timestamp_utc": datetime(2024, 1, 4, 12, tzinfo=UTC)},
            AVAILABILITY_FIRST_SEEN_DOWNLOAD_TIMESTAMP,
        ),
        ({"is_current_snapshot": True}, AVAILABILITY_CURRENT_SNAPSHOT_NO_VINTAGE),
        ({}, AVAILABILITY_UNKNOWN),
    ],
)
def test_availability_basis_classification(kwargs, expected):
    assert availability_basis_from_metadata(**kwargs) == expected


def test_model_usable_policy_blocks_current_snapshots_without_pit_lineage():
    assert model_usable_from_revision_policy(
        configured_model_usable=True,
        revision_policy=REVISION_UNREVISED,
    )
    assert not model_usable_from_revision_policy(
        configured_model_usable=False,
        revision_policy=REVISION_UNREVISED,
    )
    assert not model_usable_from_revision_policy(
        configured_model_usable=True,
        revision_policy=REVISION_CURRENT_SNAPSHOT_REFERENCE_ONLY,
    )
    assert not model_usable_from_revision_policy(
        configured_model_usable=True,
        revision_policy=REVISION_REVISED_USE_VINTAGES,
        vintage_id="fred:series:abc",
        availability_basis=AVAILABILITY_CURRENT_SNAPSHOT_NO_VINTAGE,
    )
    assert model_usable_from_revision_policy(
        configured_model_usable=True,
        revision_policy=REVISION_REVISED_USE_VINTAGES,
        vintage_id="fred:series:abc",
        availability_basis=AVAILABILITY_FRED_VINTAGE_REQUEST,
    )
    assert not model_usable_from_revision_policy(
        configured_model_usable=True,
        revision_policy=REVISION_REVISED_USE_VINTAGES,
        vintage_id="fred:series:abc",
    )
    assert not model_usable_from_revision_policy(
        configured_model_usable=True,
        revision_policy=REVISION_REVISED_USE_VINTAGES,
        vintage_id="fred:series:abc",
        availability_basis=AVAILABILITY_SOURCE_DATE_ONLY,
    )
    assert not model_usable_from_revision_policy(
        configured_model_usable=True,
        revision_policy=REVISION_REVISED_USE_VINTAGES,
        vintage_id="ibge:sidra:abc",
        availability_basis=AVAILABILITY_EXACT_SOURCE_TIMESTAMP,
    )
    assert model_usable_from_revision_policy(
        configured_model_usable=True,
        revision_policy=REVISION_REVISED_USE_VINTAGES,
        vintage_id="ibge:sidra:abc",
        availability_basis=AVAILABILITY_EXACT_SOURCE_TIMESTAMP,
        model_usable_without_vintage=True,
    )
    assert model_usable_from_revision_policy(
        configured_model_usable=True,
        revision_policy=REVISION_REVISED_USE_VINTAGES,
        vintage_id="ibge:sidra:abc",
        availability_basis=AVAILABILITY_OFFICIAL_RELEASE_CALENDAR,
        model_usable_without_vintage=True,
    )


def test_first_seen_utc_is_converted_to_brazil_cutoff_date():
    assert available_date_from_first_seen(datetime(2024, 1, 2, 21, 0, tzinfo=UTC)) == date(
        2024,
        1,
        2,
    )
    assert available_date_from_first_seen(datetime(2024, 1, 2, 22, 0, tzinfo=UTC)) == date(
        2024,
        1,
        3,
    )
    assert not model_usable_from_revision_policy(
        configured_model_usable=True,
        revision_policy=REVISION_REVISED_USE_FIRST_SEEN,
        vintage_id="cvm:registry:abc",
    )
    assert model_usable_from_revision_policy(
        configured_model_usable=True,
        revision_policy=REVISION_REVISED_USE_FIRST_SEEN,
        vintage_id="cvm:registry:abc",
        first_seen_timestamp_utc=datetime(2024, 1, 5, 14, tzinfo=UTC),
    )


def test_pit_audit_column_helpers_flag_missing_lineage_fields():
    columns = [
        "available_date",
        "availability_basis",
        "revision_policy",
        "vintage_id",
        "model_usable",
    ]

    assert missing_pit_audit_columns(columns) == []
    assert missing_pit_audit_columns(columns, require_first_seen=True) == [
        "first_seen_timestamp_utc"
    ]
    with pytest.raises(ValueError, match="first_seen_timestamp_utc"):
        assert_pit_audit_columns(columns, require_first_seen=True)


def test_pit_audit_flags_row_level_violations():
    frame = pl.DataFrame(
        [
            {
                "ref_date": date(2024, 1, 2),
                "available_date": date(2024, 1, 3),
                "observation_available_date": date(2024, 1, 2),
                "availability_basis": AVAILABILITY_SOURCE_DATE_ONLY,
                "model_usable": True,
            },
            {
                "ref_date": date(2024, 1, 3),
                "available_date": date(2024, 1, 3),
                "observation_available_date": date(2024, 1, 4),
                "availability_basis": AVAILABILITY_CURRENT_SNAPSHOT_NO_VINTAGE,
                "model_usable": False,
            },
        ]
    )

    violations = pit_audit_violations(frame)

    assert "available_date_after_ref_date:1" in violations
    assert "observation_available_date_after_ref_date:1" in violations
    assert "non_model_usable_rows:1" in violations
    assert "current_snapshot_rows:1" in violations
    with pytest.raises(ValueError, match="PIT audit violations"):
        assert_pit_model_ready_panel(frame)
