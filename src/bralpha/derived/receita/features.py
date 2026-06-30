from __future__ import annotations

from collections import defaultdict
from datetime import date
from math import log1p
from typing import Any

import polars as pl

from bralpha.derived.feature_utils import (
    as_date,
    in_output_window,
    join_versions,
    max_date,
    optional_float,
    safe_pct_change,
    safe_ratio,
)
from bralpha.derived.pit_metadata import copy_pit_metadata, max_available_date, merge_pit_metadata
from bralpha.derived.receita.quality import validate_asof_panel
from bralpha.derived.receita.schemas import PANEL_PRIMARY_KEYS, RECEITA_FEATURE_DAILY_COLUMNS

SOURCE_FAMILY = "receita_feature"


def build_receita_feature_daily(
    state_asof_daily: pl.DataFrame,
    *,
    inflation_feature_daily: pl.DataFrame | None = None,
    start: date | None = None,
    end: date | None = None,
) -> pl.DataFrame:
    snapshots = _snapshots(state_asof_daily)
    histories = _histories(snapshots)
    totals = _totals_by_ref_date(snapshots)
    inflation = _inflation_by_ref_date(inflation_feature_daily)
    rows: list[dict[str, Any]] = []
    for (ref_date, feature_id), snapshot in sorted(snapshots.items()):
        if not in_output_window(ref_date, start, end):
            continue
        history = histories[feature_id]
        position = _history_position(history, snapshot)
        rows.extend(
            _feature_rows(
                snapshot,
                history,
                position,
                total=totals.get((ref_date, _total_key(snapshot["feature_id"]))),
                ipca_12m=inflation.get(ref_date),
            )
        )
    frame = _frame(rows)
    validate_asof_panel(
        frame,
        required_columns=RECEITA_FEATURE_DAILY_COLUMNS,
        primary_keys=PANEL_PRIMARY_KEYS["feature_daily"],
    )
    return frame


def _feature_rows(
    snapshot: dict[str, Any],
    history: list[dict[str, Any]],
    position: int,
    *,
    total: float | None,
    ipca_12m: float | None,
) -> list[dict[str, Any]]:
    feature_id = f"receita:{snapshot['feature_id']}"
    value = snapshot.get("collection_amount_brl")
    lag_12 = _history_snapshot(history, position, 12)
    rolling_3 = _rolling_values(history, position, 3)
    rolling_12 = _rolling_values(history, position, 12)
    yoy = safe_pct_change(value, lag_12.get("collection_amount_brl") if lag_12 else None)
    return [
        _row(
            snapshot,
            feature_id,
            "collection_signed_log",
            _signed_log1p(value),
            "signed_log_brl",
        ),
        _row(
            snapshot,
            feature_id,
            "collection_yoy_pct",
            yoy,
            "percent",
            extra=[lag_12],
        ),
        _row(
            snapshot,
            feature_id,
            "collection_3obs_sum_signed_log",
            _signed_log1p(sum(rolling_3)) if len(rolling_3) == 3 else None,
            "signed_log_brl",
        ),
        _row(
            snapshot,
            feature_id,
            "collection_12obs_sum_signed_log",
            _signed_log1p(sum(rolling_12)) if len(rolling_12) == 12 else None,
            "signed_log_brl",
        ),
        _row(
            snapshot,
            feature_id,
            "category_share_pct",
            _scale(safe_ratio(value, total), 100.0),
            "percent",
        ),
        _row(
            snapshot,
            feature_id,
            "real_collection_yoy_pct",
            _sub(yoy, ipca_12m),
            "percent",
            extra=[lag_12],
        ),
    ]


def _snapshots(frame: pl.DataFrame) -> dict[tuple[date, str], dict[str, Any]]:
    snapshots: dict[tuple[date, str], dict[str, Any]] = {}
    for row in frame.to_dicts():
        if row.get("source_family") != "receita_tax_collection" or not row.get(
            "is_available", True
        ):
            continue
        if row.get("value_name") != "collection_amount_brl":
            continue
        ref_date = as_date(row["ref_date"])
        feature_id = str(row["feature_id"])
        snapshots[(ref_date, feature_id)] = {
            "ref_date": ref_date,
            "available_date": max_available_date(ref_date, row),
            "feature_id": feature_id,
            "collection_amount_brl": optional_float(row.get("value")),
            "observation_ref_date": as_date(row["observation_ref_date"]),
            "observation_available_date": as_date(row["observation_available_date"]),
            "staleness_days": row.get("staleness_days"),
            "source_version": row.get("source_version") or "v0",
            **copy_pit_metadata(row),
        }
    return snapshots


def _histories(
    snapshots: dict[tuple[date, str], dict[str, Any]],
) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, dict[date, dict[str, Any]]] = defaultdict(dict)
    for (_, feature_id), snapshot in snapshots.items():
        grouped[feature_id][snapshot["observation_ref_date"]] = snapshot
    return {
        feature_id: sorted(items.values(), key=lambda item: item["observation_ref_date"])
        for feature_id, items in grouped.items()
    }


def _totals_by_ref_date(
    snapshots: dict[tuple[date, str], dict[str, Any]],
) -> dict[tuple[date, str], float]:
    explicit: dict[tuple[date, str], list[float]] = defaultdict(list)
    fallback: dict[tuple[date, str], list[float]] = defaultdict(list)
    for (ref_date, _), snapshot in snapshots.items():
        value = snapshot.get("collection_amount_brl")
        if value is not None:
            key = (ref_date, _total_key(snapshot["feature_id"]))
            if _is_total_feature(snapshot["feature_id"]):
                explicit[key].append(value)
            else:
                fallback[key].append(value)
    keys = set(explicit) | set(fallback)
    return {
        key: sum(explicit[key] if explicit.get(key) else fallback[key])
        for key in keys
        if explicit.get(key) or fallback.get(key)
    }


def _inflation_by_ref_date(frame: pl.DataFrame | None) -> dict[date, float]:
    if frame is None or frame.is_empty():
        return {}
    values: dict[date, float] = {}
    for row in frame.to_dicts():
        value_name = str(row.get("value_name") or "")
        feature_id = str(row.get("feature_id") or "")
        if (
            value_name in {"ipca_12m_sum_pct", "trailing_12obs_sum_pct"}
            and "ipca" in feature_id.lower()
        ):
            value = optional_float(row.get("value"))
            if value is not None:
                values[as_date(row["ref_date"])] = value
    return values


def _row(
    snapshot: dict[str, Any],
    feature_id: str,
    value_name: str,
    value: float | None,
    unit: str,
    *,
    extra: list[dict[str, Any] | None] | None = None,
) -> dict[str, Any]:
    contributors = [snapshot, *[item for item in extra or [] if item is not None]]
    return {
        "ref_date": snapshot["ref_date"],
        "available_date": max_available_date(snapshot["ref_date"], *contributors),
        "source_family": SOURCE_FAMILY,
        "feature_id": feature_id,
        "value_name": value_name,
        "value": value,
        "unit": unit,
        "observation_ref_date": max_date(*(item["observation_ref_date"] for item in contributors)),
        "observation_available_date": max_date(
            *(item["observation_available_date"] for item in contributors)
        ),
        "is_available": value is not None,
        "staleness_days": _max_int(*(item.get("staleness_days") for item in contributors)),
        "source_version": join_versions(*(item.get("source_version") for item in contributors)),
        **merge_pit_metadata(*contributors),
    }


def _history_position(history: list[dict[str, Any]], snapshot: dict[str, Any]) -> int:
    for index, item in enumerate(history):
        if item["observation_ref_date"] == snapshot["observation_ref_date"]:
            return index
    return len(history) - 1


def _history_snapshot(
    history: list[dict[str, Any]],
    position: int,
    lag: int,
) -> dict[str, Any] | None:
    lag_position = position - lag
    if lag_position < 0:
        return None
    return history[lag_position]


def _rolling_values(history: list[dict[str, Any]], position: int, window: int) -> list[float]:
    values = [
        optional_float(item.get("collection_amount_brl"))
        for item in history[max(0, position - window + 1) : position + 1]
    ]
    return [value for value in values if value is not None]


def _signed_log1p(value: float | None) -> float | None:
    if value is None:
        return None
    sign = -1.0 if value < 0 else 1.0
    return sign * log1p(abs(value))


def _total_key(feature_id: str) -> str:
    parts = feature_id.split("|")
    if len(parts) >= 2:
        return "|".join(parts[:2])
    return feature_id


def _is_total_feature(feature_id: str) -> bool:
    tokens = {part.strip().lower() for part in feature_id.replace(":", "|").split("|")}
    return bool(tokens & {"total", "total_collection", "receita_total", "arrecadacao_total"})


def _sub(left: float | None, right: float | None) -> float | None:
    if left is None or right is None:
        return None
    return left - right


def _scale(value: float | None, multiplier: float) -> float | None:
    if value is None:
        return None
    return value * multiplier


def _max_int(*values: Any) -> int:
    ints = [int(value) for value in values if value is not None]
    return max(ints) if ints else 0


def _frame(rows: list[dict[str, Any]]) -> pl.DataFrame:
    if not rows:
        return pl.DataFrame(schema={column: pl.Null for column in RECEITA_FEATURE_DAILY_COLUMNS})
    return (
        pl.DataFrame(rows)
        .select(RECEITA_FEATURE_DAILY_COLUMNS)
        .unique(subset=PANEL_PRIMARY_KEYS["feature_daily"], keep="last")
        .sort(["ref_date", "feature_id", "value_name"])
    )
