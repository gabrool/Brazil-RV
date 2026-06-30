from __future__ import annotations

from collections import defaultdict
from datetime import date
from math import log, log1p
from typing import Any

import polars as pl

from bralpha.derived.feature_utils import as_date, in_output_window
from bralpha.derived.ibge.quality import validate_asof_panel
from bralpha.derived.ibge.schemas import IBGE_SIDRA_FEATURE_DAILY_COLUMNS, PANEL_PRIMARY_KEYS

SOURCE_FAMILY = "ibge_sidra_feature"


def build_sidra_feature_daily(
    sidra_asof_daily: pl.DataFrame,
    *,
    start: date | None = None,
    end: date | None = None,
) -> pl.DataFrame:
    rows = _normalized_rows(sidra_asof_daily)
    observations_by_feature = _unique_observations(rows)
    output: list[dict[str, Any]] = []
    for row in rows:
        if not in_output_window(row["ref_date"], start, end):
            continue
        history = observations_by_feature[row["feature_id"]]
        position = _history_position(history, row)
        output.extend(_feature_rows(row, history, position))
    if not output:
        return _empty()
    frame = (
        pl.DataFrame(output)
        .select(IBGE_SIDRA_FEATURE_DAILY_COLUMNS)
        .unique(subset=PANEL_PRIMARY_KEYS["sidra_feature_daily"], keep="last")
        .sort(["ref_date", "feature_id", "value_name"])
    )
    validate_asof_panel(
        frame,
        required_columns=IBGE_SIDRA_FEATURE_DAILY_COLUMNS,
        primary_keys=PANEL_PRIMARY_KEYS["sidra_feature_daily"],
    )
    return frame


def _feature_rows(
    row: dict[str, Any],
    history: list[dict[str, Any]],
    position: int,
) -> list[dict[str, Any]]:
    slug = str(row.get("dataset_slug") or "").lower()
    feature_id = f"ibge_sidra_feature:{row['feature_id']}"
    value = _float(row.get("value"))
    yoy_lag = 4 if _is_quarterly(row) else 12
    if any(token in slug for token in ["ipca", "ipca15", "inpc"]):
        return _inflation_rows(row, history, position, feature_id, value)
    if "gdp_volume_change" in slug:
        return _percent_change_rows(row, history, position, feature_id, value, yoy_lag=4)
    if "gdp_current_values" in slug:
        return _signed_level_rows(row, history, position, feature_id, value, yoy_lag=4)
    rate_tokens = ["unemployment_rate", "participation_rate", "underutilization_rate"]
    if any(token in slug for token in rate_tokens):
        return _percent_change_rows(row, history, position, feature_id, value, yoy_lag=yoy_lag)
    if "pnad" in slug:
        return _positive_level_rows(row, history, position, feature_id, value, yoy_lag=yoy_lag)
    return _positive_level_rows(row, history, position, feature_id, value, yoy_lag=yoy_lag)


def _inflation_rows(
    row: dict[str, Any],
    history: list[dict[str, Any]],
    position: int,
    feature_id: str,
    value: float | None,
) -> list[dict[str, Any]]:
    values = [_float(item.get("value")) for item in history[: position + 1]]
    last_3 = [item for item in values[-3:] if item is not None]
    last_12 = [item for item in values[-12:] if item is not None]
    return [
        _row(row, feature_id, "monthly_pct", value, "percent"),
        _row(
            row,
            feature_id,
            "change_1obs_pp",
            _obs_diff(history, position, 1),
            "percentage_points",
        ),
        _row(
            row,
            feature_id,
            "trailing_3obs_sum_pct",
            sum(last_3) if len(last_3) == 3 else None,
            "percent",
        ),
        _row(
            row,
            feature_id,
            "trailing_12obs_sum_pct",
            sum(last_12) if len(last_12) == 12 else None,
            "percent",
        ),
        _row(
            row,
            feature_id,
            "trailing_3obs_ann_pct",
            ((1.0 + sum(last_3) / 100.0) ** 4 - 1.0) * 100.0 if len(last_3) == 3 else None,
            "percent_annualized",
        ),
    ]


def _positive_level_rows(
    row: dict[str, Any],
    history: list[dict[str, Any]],
    position: int,
    feature_id: str,
    value: float | None,
    *,
    yoy_lag: int,
) -> list[dict[str, Any]]:
    current_log = _log_positive(value)
    lag1 = _history_value(history, position, 1)
    lag3 = _history_value(history, position, 3)
    lag_yoy = _history_value(history, position, yoy_lag)
    return [
        _row(row, feature_id, "log_level", current_log, "log_level"),
        _row(row, feature_id, "log_change_1obs", _log_diff(value, lag1), "log_change"),
        _row(row, feature_id, "log_change_3obs", _log_diff(value, lag3), "log_change"),
        _row(row, feature_id, "yoy_log_change", _log_diff(value, lag_yoy), "log_change"),
    ]


def _signed_level_rows(
    row: dict[str, Any],
    history: list[dict[str, Any]],
    position: int,
    feature_id: str,
    value: float | None,
    *,
    yoy_lag: int,
) -> list[dict[str, Any]]:
    lag1 = _history_value(history, position, 1)
    lag_yoy = _history_value(history, position, yoy_lag)
    return [
        _row(row, feature_id, "signed_log_level", _signed_log1p(value), "signed_log_level"),
        _row(
            row,
            feature_id,
            "signed_log_change_1obs",
            _signed_log_diff(value, lag1),
            "signed_log_change",
        ),
        _row(
            row,
            feature_id,
            "yoy_signed_log_change",
            _signed_log_diff(value, lag_yoy),
            "signed_log_change",
        ),
    ]


def _percent_change_rows(
    row: dict[str, Any],
    history: list[dict[str, Any]],
    position: int,
    feature_id: str,
    value: float | None,
    *,
    yoy_lag: int,
) -> list[dict[str, Any]]:
    return [
        _row(row, feature_id, "level_pct", value, "percent"),
        _row(
            row,
            feature_id,
            "change_1obs_pp",
            _obs_diff(history, position, 1),
            "percentage_points",
        ),
        _row(
            row,
            feature_id,
            "yoy_change_pp",
            _obs_diff(history, position, yoy_lag),
            "percentage_points",
        ),
    ]


def _row(
    base: dict[str, Any],
    feature_id: str,
    value_name: str,
    value: float | None,
    unit: str,
) -> dict[str, Any]:
    return {
        "ref_date": base["ref_date"],
        "available_date": base.get("available_date") or base["ref_date"],
        "source_family": SOURCE_FAMILY,
        "feature_id": feature_id,
        "value_name": value_name,
        "value": value,
        "unit": unit,
        "observation_ref_date": base.get("observation_ref_date"),
        "observation_available_date": base.get("observation_available_date"),
        "availability_policy": base.get("availability_policy"),
        "availability_basis": base.get("availability_basis"),
        "revision_policy": base.get("revision_policy"),
        "vintage_id": base.get("vintage_id"),
        "first_seen_timestamp_utc": base.get("first_seen_timestamp_utc"),
        "source_publication_datetime_utc": base.get("source_publication_datetime_utc"),
        "is_available": base.get("is_available", True),
        "has_value": value is not None,
        "staleness_days": base.get("staleness_days"),
        "value_status": "ok" if value is not None else "insufficient_history",
        "source_version": base.get("source_version") or "v0",
    }


def _normalized_rows(frame: pl.DataFrame) -> list[dict[str, Any]]:
    rows = []
    for row in frame.to_dicts():
        if not row.get("is_available", True):
            continue
        rows.append(
            {
                **row,
                "ref_date": as_date(row["ref_date"]),
                "available_date": as_date(row.get("available_date") or row["ref_date"]),
                "observation_ref_date": as_date(
                    row.get("observation_ref_date") or row["ref_date"]
                ),
                "observation_available_date": as_date(
                    row.get("observation_available_date") or row["ref_date"]
                ),
                "source_version": row.get("source_version") or "v0",
            }
        )
    return rows


def _unique_observations(rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, dict[date, dict[str, Any]]] = defaultdict(dict)
    for row in rows:
        grouped[row["feature_id"]][row["observation_ref_date"]] = row
    return {
        feature_id: sorted(items.values(), key=lambda item: item["observation_ref_date"])
        for feature_id, items in grouped.items()
    }


def _history_position(history: list[dict[str, Any]], row: dict[str, Any]) -> int:
    for index, item in enumerate(history):
        if item["observation_ref_date"] == row["observation_ref_date"]:
            return index
    return len(history) - 1


def _history_value(history: list[dict[str, Any]], position: int, lag: int) -> float | None:
    lag_position = position - lag
    if lag_position < 0:
        return None
    return _float(history[lag_position].get("value"))


def _obs_diff(history: list[dict[str, Any]], position: int, lag: int) -> float | None:
    current = _float(history[position].get("value"))
    lagged = _history_value(history, position, lag)
    if current is None or lagged is None:
        return None
    return current - lagged


def _log_positive(value: float | None) -> float | None:
    if value is None or value <= 0:
        return None
    return log(value)


def _log_diff(current: float | None, previous: float | None) -> float | None:
    current_log = _log_positive(current)
    previous_log = _log_positive(previous)
    if current_log is None or previous_log is None:
        return None
    return current_log - previous_log


def _signed_log1p(value: float | None) -> float | None:
    if value is None:
        return None
    sign = -1.0 if value < 0 else 1.0
    return sign * log1p(abs(value))


def _signed_log_diff(current: float | None, previous: float | None) -> float | None:
    current_log = _signed_log1p(current)
    previous_log = _signed_log1p(previous)
    if current_log is None or previous_log is None:
        return None
    return current_log - previous_log


def _is_quarterly(row: dict[str, Any]) -> bool:
    frequency = str(row.get("frequency") or "").lower()
    slug = str(row.get("dataset_slug") or "").lower()
    return "quarter" in frequency or "quarter" in slug or "gdp" in slug


def _float(value: Any) -> float | None:
    if value is None:
        return None
    return float(value)


def _empty() -> pl.DataFrame:
    return pl.DataFrame(schema={column: pl.Null for column in IBGE_SIDRA_FEATURE_DAILY_COLUMNS})
