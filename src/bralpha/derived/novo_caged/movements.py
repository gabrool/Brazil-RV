from __future__ import annotations

from datetime import date

import polars as pl

from bralpha.derived.novo_caged.pit import (
    ensure_novo_caged_pit_columns,
    novo_caged_pit_aggregations,
)
from bralpha.derived.novo_caged.quality import validate_panel
from bralpha.derived.novo_caged.schemas import (
    NOVO_CAGED_MOVEMENT_GROUP_OBSERVATION_COLUMNS,
    NOVO_CAGED_MOVEMENT_RECORD_OBSERVATION_COLUMNS,
    PANEL_PRIMARY_KEYS,
)
from bralpha.parsing.common import normalize_column_name
from bralpha.timing.vintages import (
    AVAILABILITY_CONSERVATIVE_HEURISTIC,
    AVAILABILITY_CURRENT_SNAPSHOT_NO_VINTAGE,
    REVISION_CURRENT_SNAPSHOT_REFERENCE_ONLY,
    REVISION_REVISED_USE_FIRST_SEEN,
    available_date_from_first_seen,
    available_date_from_source_datetime,
)

_GROUP_BY_COLUMNS = {"all", "region", "state", "cnae_section"}
_CROSS_BY_COLUMNS = {"movement_sign"}
NOVO_CAGED_CALENDAR_SNAPSHOT_POLICY = (
    "novo_caged_official_calendar_plus_snapshot_first_seen"
)
NOVO_CAGED_UNMATCHED_CALENDAR_POLICY = (
    "novo_caged_unmatched_release_calendar_reference_only"
)
NOVO_CAGED_MISSING_SNAPSHOT_POLICY = "novo_caged_missing_snapshot_reference_only"
NOVO_CAGED_CALENDAR_SNAPSHOT_BASIS = (
    "official_release_calendar+first_seen_download_timestamp"
)


def build_movement_record_observation(
    silver: pl.DataFrame,
    *,
    start: date | None = None,
    end: date | None = None,
) -> pl.DataFrame:
    if silver.is_empty():
        return _empty(NOVO_CAGED_MOVEMENT_RECORD_OBSERVATION_COLUMNS)

    frame = ensure_novo_caged_pit_columns(silver)
    if start is not None:
        frame = frame.filter(pl.col("ref_date") >= start)
    if end is not None:
        frame = frame.filter(pl.col("ref_date") <= end)
    if frame.is_empty():
        return _empty(NOVO_CAGED_MOVEMENT_RECORD_OBSERVATION_COLUMNS)

    panel = (
        frame.with_columns(
            has_wage=pl.col("wage").is_not_null(),
            has_contract_hours=pl.col("contract_hours").is_not_null(),
        )
        .select(NOVO_CAGED_MOVEMENT_RECORD_OBSERVATION_COLUMNS)
        .sort(["ref_date", "movement_record_id"])
        .unique(
            subset=PANEL_PRIMARY_KEYS["movement_record_observation"],
            keep="last",
            maintain_order=True,
        )
    )
    validate_panel(
        panel,
        required_columns=NOVO_CAGED_MOVEMENT_RECORD_OBSERVATION_COLUMNS,
        primary_keys=PANEL_PRIMARY_KEYS["movement_record_observation"],
    )
    return panel


def build_movement_group_observation(
    movement_records: pl.DataFrame,
    *,
    release_calendar: pl.DataFrame | None = None,
    prefer_official_calendar: bool,
    group_by: list[str],
    cross_by: list[str],
    max_groups: int,
    start: date | None = None,
    end: date | None = None,
) -> pl.DataFrame:
    _validate_groups(group_by)
    _validate_cross_by(cross_by)
    if movement_records.is_empty():
        return _empty(NOVO_CAGED_MOVEMENT_GROUP_OBSERVATION_COLUMNS)

    frame = ensure_novo_caged_pit_columns(movement_records)
    if start is not None:
        frame = frame.filter(pl.col("ref_date") >= start)
    if end is not None:
        frame = frame.filter(pl.col("ref_date") <= end)
    if frame.is_empty():
        return _empty(NOVO_CAGED_MOVEMENT_GROUP_OBSERVATION_COLUMNS)

    parts = [_aggregate_group(frame, group_type) for group_type in group_by]
    panel = (
        pl.concat(parts, how="diagonal_relaxed")
        if parts
        else _empty(NOVO_CAGED_MOVEMENT_GROUP_OBSERVATION_COLUMNS)
    )
    if panel.is_empty():
        return panel

    panel = _join_release_calendar(
        panel,
        release_calendar=release_calendar,
        prefer_official_calendar=prefer_official_calendar,
    )
    feature_count = panel.select(["group_type", "group_value", "movement_sign"]).unique().height
    if feature_count > max_groups:
        raise ValueError(
            f"Novo CAGED movement group count {feature_count} exceeds max_groups={max_groups}"
        )

    panel = panel.select(NOVO_CAGED_MOVEMENT_GROUP_OBSERVATION_COLUMNS).sort(
        ["ref_date", "group_type", "group_value", "movement_sign"]
    )
    validate_panel(
        panel,
        required_columns=NOVO_CAGED_MOVEMENT_GROUP_OBSERVATION_COLUMNS,
        primary_keys=PANEL_PRIMARY_KEYS["movement_group_observation"],
    )
    return panel


def novo_caged_movement_feature_id(
    group_type: object,
    group_value: object,
    movement_sign: object,
) -> str:
    return (
        "novo_caged_movement|"
        f"{_token(group_type)}|{_token(group_value)}|{_movement_sign_token(movement_sign)}"
    )


def _aggregate_group(frame: pl.DataFrame, group_type: str) -> pl.DataFrame:
    working = frame.with_columns(
        group_type=pl.lit(group_type),
        group_value=(
            pl.lit("all")
            if group_type == "all"
            else pl.col(group_type).map_elements(_token, return_dtype=pl.Utf8)
        ),
        movement_sign=pl.col("movement_sign").map_elements(
            _movement_sign_value,
            return_dtype=pl.Utf8,
        ),
    )
    return (
        working.group_by(["ref_date", "group_type", "group_value", "movement_sign", "vintage_id"])
        .agg(
            [
                pl.col("available_date").max().alias("silver_available_date"),
                pl.col("availability_policy")
                .drop_nulls()
                .first()
                .alias("silver_availability_policy"),
                *novo_caged_pit_aggregations(),
                pl.len().cast(pl.Int64).alias("movement_count"),
                pl.col("wage").mean().alias("wage_mean"),
                pl.col("contract_hours").mean().alias("contract_hours_mean"),
                pl.col("wage").is_not_null().sum().cast(pl.Int64).alias("wage_count"),
                pl.col("contract_hours")
                .is_not_null()
                .sum()
                .cast(pl.Int64)
                .alias("contract_hours_count"),
                pl.col("source_version").drop_nulls().first(),
            ]
        )
        .with_columns(
            feature_id=pl.struct(["group_type", "group_value", "movement_sign"]).map_elements(
                lambda row: novo_caged_movement_feature_id(
                    row["group_type"],
                    row["group_value"],
                    row["movement_sign"],
                ),
                return_dtype=pl.Utf8,
            )
        )
    )


def _join_release_calendar(
    panel: pl.DataFrame,
    *,
    release_calendar: pl.DataFrame | None,
    prefer_official_calendar: bool,
) -> pl.DataFrame:
    if release_calendar is None or release_calendar.is_empty():
        calendar = pl.DataFrame(
            {
                "ref_date": [],
                "calendar_release_date": [],
                "calendar_available_date": [],
                "calendar_availability_policy": [],
            },
            schema={
                "ref_date": pl.Date,
                "calendar_release_date": pl.Date,
                "calendar_available_date": pl.Date,
                "calendar_availability_policy": pl.Utf8,
            },
        )
    else:
        calendar = release_calendar.select(
            [
                "ref_date",
                pl.col("release_date").alias("calendar_release_date"),
                pl.col("available_date").alias("calendar_available_date"),
                pl.col("availability_policy").alias("calendar_availability_policy"),
            ]
        ).unique(subset=["ref_date"], keep="last", maintain_order=True)

    return (
        panel.join(calendar, on="ref_date", how="left")
        .with_columns(
            pl.struct(
                ["source_publication_datetime_utc", "first_seen_timestamp_utc"]
            ).map_elements(_snapshot_available_date, return_dtype=pl.Date).alias(
                "snapshot_available_date"
            )
        )
        .with_columns(
            _chosen_available_date(prefer_official_calendar).alias("available_date"),
            _chosen_availability_source(prefer_official_calendar).alias(
                "availability_source"
            ),
            _chosen_availability_policy(prefer_official_calendar).alias(
                "availability_policy"
            ),
            _chosen_availability_basis(prefer_official_calendar).alias(
                "availability_basis"
            ),
            _chosen_revision_policy(prefer_official_calendar).alias("revision_policy"),
            pl.col("calendar_release_date").alias("release_date"),
            _chosen_model_usable(prefer_official_calendar).alias("model_usable"),
            _chosen_model_usable_reason(prefer_official_calendar).alias(
                "model_usable_reason"
            ),
        )
        .drop(
            [
                "silver_availability_policy",
                "calendar_availability_policy",
                "calendar_release_date",
                "silver_model_usable",
                "silver_model_usable_reason",
            ]
        )
    )


def _chosen_available_date(prefer_official_calendar: bool) -> pl.Expr:
    if not prefer_official_calendar:
        return pl.col("silver_available_date")
    gated_available = pl.max_horizontal("calendar_available_date", "snapshot_available_date")
    return (
        pl.when(
            pl.col("calendar_available_date").is_not_null()
            & pl.col("snapshot_available_date").is_not_null()
        )
        .then(gated_available)
        .otherwise(pl.col("silver_available_date"))
    )


def _chosen_availability_source(prefer_official_calendar: bool) -> pl.Expr:
    if not prefer_official_calendar:
        return pl.lit("conservative_fallback")
    return (
        pl.when(
            pl.col("calendar_available_date").is_not_null()
            & pl.col("snapshot_available_date").is_not_null()
        )
        .then(pl.lit("official_calendar_plus_snapshot"))
        .when(pl.col("calendar_available_date").is_not_null())
        .then(pl.lit("official_calendar_missing_snapshot"))
        .otherwise(pl.lit("conservative_fallback"))
    )


def _chosen_availability_policy(prefer_official_calendar: bool) -> pl.Expr:
    if not prefer_official_calendar:
        return pl.col("silver_availability_policy")
    return (
        pl.when(
            pl.col("calendar_available_date").is_not_null()
            & pl.col("snapshot_available_date").is_not_null()
        )
        .then(pl.lit(NOVO_CAGED_CALENDAR_SNAPSHOT_POLICY))
        .when(pl.col("calendar_available_date").is_not_null())
        .then(pl.lit(NOVO_CAGED_MISSING_SNAPSHOT_POLICY))
        .otherwise(pl.lit(NOVO_CAGED_UNMATCHED_CALENDAR_POLICY))
    )


def _chosen_availability_basis(prefer_official_calendar: bool) -> pl.Expr:
    if not prefer_official_calendar:
        return pl.lit(AVAILABILITY_CONSERVATIVE_HEURISTIC)
    return (
        pl.when(
            pl.col("calendar_available_date").is_not_null()
            & pl.col("snapshot_available_date").is_not_null()
        )
        .then(pl.lit(NOVO_CAGED_CALENDAR_SNAPSHOT_BASIS))
        .when(pl.col("calendar_available_date").is_not_null())
        .then(pl.lit(AVAILABILITY_CURRENT_SNAPSHOT_NO_VINTAGE))
        .otherwise(pl.lit(AVAILABILITY_CONSERVATIVE_HEURISTIC))
    )


def _chosen_revision_policy(prefer_official_calendar: bool) -> pl.Expr:
    if not prefer_official_calendar:
        return pl.lit(REVISION_CURRENT_SNAPSHOT_REFERENCE_ONLY)
    return (
        pl.when(
            pl.col("calendar_available_date").is_not_null()
            & pl.col("snapshot_available_date").is_not_null()
        )
        .then(pl.lit(REVISION_REVISED_USE_FIRST_SEEN))
        .otherwise(pl.lit(REVISION_CURRENT_SNAPSHOT_REFERENCE_ONLY))
    )


def _chosen_model_usable(prefer_official_calendar: bool) -> pl.Expr:
    if not prefer_official_calendar:
        return pl.lit(False)
    return (
        pl.col("calendar_available_date").is_not_null()
        & pl.col("snapshot_available_date").is_not_null()
        & pl.col("vintage_id").is_not_null()
    )


def _chosen_model_usable_reason(prefer_official_calendar: bool) -> pl.Expr:
    if not prefer_official_calendar:
        return pl.col("silver_availability_policy")
    return _chosen_availability_policy(prefer_official_calendar)


def _snapshot_available_date(values: dict[str, object]) -> date | None:
    source_publication = values.get("source_publication_datetime_utc")
    if source_publication is not None:
        return available_date_from_source_datetime(source_publication)
    first_seen = values.get("first_seen_timestamp_utc")
    return available_date_from_first_seen(first_seen)


def _validate_groups(group_by: list[str]) -> None:
    unknown = sorted(set(group_by) - _GROUP_BY_COLUMNS)
    if unknown:
        raise ValueError(f"Unsupported Novo CAGED movement grouping(s): {unknown}")


def _validate_cross_by(cross_by: list[str]) -> None:
    unknown = sorted(set(cross_by) - _CROSS_BY_COLUMNS)
    if unknown:
        raise ValueError(f"Unsupported Novo CAGED movement cross dimension(s): {unknown}")


def _token(value: object) -> str:
    if value is None:
        return "unknown"
    token = normalize_column_name(str(value).strip())
    return token or "unknown"


def _movement_sign_value(value: object) -> str:
    if value is None:
        return "unknown"
    text = str(value).strip()
    return text or "unknown"


def _movement_sign_token(value: object) -> str:
    text = _movement_sign_value(value)
    if text.startswith("-"):
        return f"minus_{normalize_column_name(text[1:]) or 'unknown'}"
    if text.startswith("+"):
        return f"plus_{normalize_column_name(text[1:]) or 'unknown'}"
    return _token(text)


def _empty(columns: list[str]) -> pl.DataFrame:
    return pl.DataFrame(schema={column: pl.Null for column in columns})
