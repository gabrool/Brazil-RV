from __future__ import annotations

from collections.abc import Iterable
from datetime import UTC, date, datetime, time
from hashlib import sha256
from typing import Any
from zoneinfo import ZoneInfo

import polars as pl

from bralpha.timing.availability import (
    DEFAULT_DECISION_CUTOFF_TIME,
    DEFAULT_TIMING_TIMEZONE,
    usable_date_from_available_datetime,
    usable_date_from_date_only,
)

AVAILABILITY_EXACT_SOURCE_TIMESTAMP = "exact_source_timestamp"
AVAILABILITY_SOURCE_DATE_ONLY = "source_date_only"
AVAILABILITY_FRED_VINTAGE_REQUEST = "fred_vintage_request"
AVAILABILITY_OFFICIAL_RELEASE_CALENDAR = "official_release_calendar"
AVAILABILITY_FIRST_SEEN_DOWNLOAD_TIMESTAMP = "first_seen_download_timestamp"
AVAILABILITY_CURRENT_SNAPSHOT_NO_VINTAGE = "current_snapshot_no_vintage"
AVAILABILITY_UNKNOWN = "unknown"
REVISION_UNREVISED = "unrevised"
REVISION_REVISED_USE_VINTAGES = "revised_use_vintages"
REVISION_REVISED_USE_FIRST_SEEN = "revised_use_first_seen_snapshots"
REVISION_CURRENT_SNAPSHOT_REFERENCE_ONLY = "current_snapshot_reference_only"
PIT_AUDIT_COLUMNS = [
    "available_date",
    "availability_basis",
    "revision_policy",
    "vintage_id",
    "model_usable",
]


def make_vintage_id(
    *,
    source: str,
    dataset_id: str,
    resource_id: str,
    publication_timestamp: date | datetime | str | None,
    first_seen_timestamp_utc: date | datetime | str | None = None,
    content_hash: str | None = None,
) -> str:
    parts = [
        source.strip().lower(),
        dataset_id.strip(),
        resource_id.strip(),
        _stable_text(publication_timestamp),
        _stable_text(first_seen_timestamp_utc),
        content_hash or "",
    ]
    digest = sha256("|".join(parts).encode("utf-8")).hexdigest()[:16]
    return f"{parts[0]}:{parts[1]}:{digest}"


def available_date_from_vintage_date(vintage_date: date | None) -> date | None:
    if vintage_date is None:
        return None
    return usable_date_from_date_only(vintage_date)


def available_date_from_first_seen(
    first_seen_timestamp_utc: datetime | None,
    *,
    cutoff_time: time = DEFAULT_DECISION_CUTOFF_TIME,
) -> date | None:
    if first_seen_timestamp_utc is None:
        return None
    timestamp = first_seen_timestamp_utc
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=UTC)
    timestamp = timestamp.astimezone(ZoneInfo(DEFAULT_TIMING_TIMEZONE))
    return usable_date_from_available_datetime(
        timestamp,
        cutoff_time=cutoff_time,
    )


def availability_basis_from_metadata(
    *,
    source_publication_datetime_utc: datetime | str | None = None,
    source_publication_date: date | str | None = None,
    vintage_date: date | str | None = None,
    first_seen_timestamp_utc: datetime | str | None = None,
    is_current_snapshot: bool = False,
) -> str:
    if source_publication_datetime_utc is not None:
        return AVAILABILITY_EXACT_SOURCE_TIMESTAMP
    if source_publication_date is not None or vintage_date is not None:
        return AVAILABILITY_SOURCE_DATE_ONLY
    if first_seen_timestamp_utc is not None:
        return AVAILABILITY_FIRST_SEEN_DOWNLOAD_TIMESTAMP
    if is_current_snapshot:
        return AVAILABILITY_CURRENT_SNAPSHOT_NO_VINTAGE
    return AVAILABILITY_UNKNOWN


def model_usable_from_revision_policy(
    *,
    configured_model_usable: bool,
    revision_policy: str,
    vintage_id: str | None = None,
    availability_basis: str | None = None,
    has_vintage_history: bool = False,
    first_seen_timestamp_utc: date | datetime | str | None = None,
    model_usable_without_vintage: bool = False,
) -> bool:
    if not configured_model_usable:
        return False
    if revision_policy in {REVISION_UNREVISED, "official_lag_no_revisions"}:
        return True
    if revision_policy == REVISION_REVISED_USE_VINTAGES:
        if availability_basis == AVAILABILITY_CURRENT_SNAPSHOT_NO_VINTAGE:
            return False
        if availability_basis == AVAILABILITY_FRED_VINTAGE_REQUEST:
            return bool(vintage_id)
        if availability_basis in {
            AVAILABILITY_EXACT_SOURCE_TIMESTAMP,
            AVAILABILITY_OFFICIAL_RELEASE_CALENDAR,
        }:
            return bool(vintage_id and model_usable_without_vintage)
        return bool(vintage_id and has_vintage_history)
    if revision_policy == REVISION_REVISED_USE_FIRST_SEEN:
        return bool(vintage_id and first_seen_timestamp_utc)
    if revision_policy == REVISION_CURRENT_SNAPSHOT_REFERENCE_ONLY:
        return False
    return False


def required_pit_audit_columns(*, require_first_seen: bool = False) -> list[str]:
    columns = list(PIT_AUDIT_COLUMNS)
    if require_first_seen:
        columns.append("first_seen_timestamp_utc")
    return columns


def missing_pit_audit_columns(
    columns: Iterable[str],
    *,
    require_first_seen: bool = False,
) -> list[str]:
    available = set(columns)
    return [
        column
        for column in required_pit_audit_columns(require_first_seen=require_first_seen)
        if column not in available
    ]


def assert_pit_audit_columns(
    columns: Iterable[str],
    *,
    require_first_seen: bool = False,
) -> None:
    missing = missing_pit_audit_columns(columns, require_first_seen=require_first_seen)
    if missing:
        raise ValueError(f"missing PIT audit columns: {', '.join(missing)}")


def pit_audit_violations(
    frame: pl.DataFrame,
    *,
    allow_current_snapshot: bool = False,
    require_model_usable: bool = True,
) -> list[str]:
    violations: list[str] = []
    columns = set(frame.columns)
    if {"available_date", "ref_date"} <= columns:
        late = frame.filter(
            pl.col("available_date").is_not_null()
            & (pl.col("available_date") > pl.col("ref_date"))
        )
        if not late.is_empty():
            violations.append(f"available_date_after_ref_date:{late.height}")
    if {"observation_available_date", "ref_date"} <= columns:
        late_observation = frame.filter(
            pl.col("observation_available_date").is_not_null()
            & (pl.col("observation_available_date") > pl.col("ref_date"))
        )
        if not late_observation.is_empty():
            violations.append(
                f"observation_available_date_after_ref_date:{late_observation.height}"
            )
    if require_model_usable and "model_usable" in columns:
        unusable = frame.filter(pl.col("model_usable").fill_null(False).not_())
        if not unusable.is_empty():
            violations.append(f"non_model_usable_rows:{unusable.height}")
    if not allow_current_snapshot and "availability_basis" in columns:
        snapshots = frame.filter(
            pl.col("availability_basis") == AVAILABILITY_CURRENT_SNAPSHOT_NO_VINTAGE
        )
        if not snapshots.is_empty():
            violations.append(f"current_snapshot_rows:{snapshots.height}")
    return violations


def assert_pit_model_ready_panel(
    frame: pl.DataFrame,
    *,
    allow_current_snapshot: bool = False,
    require_model_usable: bool = True,
) -> None:
    violations = pit_audit_violations(
        frame,
        allow_current_snapshot=allow_current_snapshot,
        require_model_usable=require_model_usable,
    )
    if violations:
        raise ValueError(f"PIT audit violations: {', '.join(violations)}")


def _stable_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return str(value)
