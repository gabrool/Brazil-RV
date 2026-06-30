from __future__ import annotations

from collections import defaultdict
from datetime import date
from math import log1p
from statistics import mean, stdev
from typing import Any

import polars as pl

from bralpha.derived.feature_utils import (
    as_date,
    diff,
    in_output_window,
    join_versions,
    max_date,
    optional_float,
    safe_log,
    safe_log1p,
    safe_log_return,
    safe_ratio,
)
from bralpha.derived.ons.quality import validate_asof_panel
from bralpha.derived.ons.schemas import ONS_POWER_FEATURE_DAILY_COLUMNS, PANEL_PRIMARY_KEYS
from bralpha.derived.pit_metadata import copy_pit_metadata, max_available_date, merge_pit_metadata

SOURCE_FAMILY = "ons_power_feature"


def build_ons_power_feature_daily(
    state_asof_daily: pl.DataFrame,
    *,
    start: date | None = None,
    end: date | None = None,
) -> pl.DataFrame:
    snapshots = _snapshots(state_asof_daily)
    histories = _histories(snapshots)
    rows: list[dict[str, Any]] = []
    for (_, feature_id), series in sorted(snapshots.items()):
        history = histories[feature_id]
        for snapshot in sorted(series, key=lambda item: item["ref_date"]):
            if not in_output_window(snapshot["ref_date"], start, end):
                continue
            position = _history_position(history, snapshot)
            rows.extend(_feature_rows(snapshot, history, position))
    frame = _frame(rows)
    validate_asof_panel(
        frame,
        required_columns=ONS_POWER_FEATURE_DAILY_COLUMNS,
        primary_keys=PANEL_PRIMARY_KEYS["power_feature_daily"],
    )
    return frame


def _feature_rows(
    snapshot: dict[str, Any],
    history: list[dict[str, Any]],
    position: int,
) -> list[dict[str, Any]]:
    source_family = snapshot["source_family"]
    feature_id = f"ons_power:{snapshot['feature_id']}"
    if source_family == "ons_ear_subsystem":
        return _ear_rows(snapshot, history, position, feature_id)
    if source_family == "ons_ena_subsystem":
        return _ena_rows(snapshot, history, position, feature_id)
    if source_family == "ons_cmo_weekly":
        return _cmo_rows(snapshot, history, position, feature_id)
    if source_family == "ons_interchange_daily":
        return _interchange_rows(snapshot, feature_id)
    if source_family in {"ons_load_daily", "ons_energy_balance_daily"}:
        return _load_balance_rows(snapshot, history, position, feature_id)
    return []


def _ear_rows(
    snapshot: dict[str, Any],
    history: list[dict[str, Any]],
    position: int,
    feature_id: str,
) -> list[dict[str, Any]]:
    percent = _value(snapshot, "stored_energy_percent")
    lag_5 = _history_snapshot(history, position, 5)
    lag_21 = _history_snapshot(history, position, 21)
    return [
        _row(snapshot, feature_id, "stored_energy_percent_level", percent, "percent"),
        _row(
            snapshot,
            feature_id,
            "stored_energy_percent_change_5bd",
            diff(percent, _value(lag_5, "stored_energy_percent")),
            "percentage_points",
            extra=[lag_5],
        ),
        _row(
            snapshot,
            feature_id,
            "stored_energy_percent_change_21bd",
            diff(percent, _value(lag_21, "stored_energy_percent")),
            "percentage_points",
            extra=[lag_21],
        ),
        _row(
            snapshot,
            feature_id,
            "stored_energy_percent_seasonal_z",
            _seasonal_z(history, position, "stored_energy_percent"),
            "z_score",
        ),
        _row(
            snapshot,
            feature_id,
            "stored_energy_mwmes_log",
            safe_log(_value(snapshot, "stored_energy_mwmes")),
            "log_mwmed",
        ),
    ]


def _ena_rows(
    snapshot: dict[str, Any],
    history: list[dict[str, Any]],
    position: int,
    feature_id: str,
) -> list[dict[str, Any]]:
    value = _value(snapshot, "ena_value")
    lag_5 = _history_snapshot(history, position, 5)
    if not _is_percent_mlt(snapshot, "ena_value"):
        return [
            _row(snapshot, feature_id, "ena_mwmed_log", safe_log(value), "log_mwmed"),
            _row(
                snapshot,
                feature_id,
                "ena_mwmed_log_change_5bd",
                safe_log_return(value, _value(lag_5, "ena_value")),
                "log_return",
                extra=[lag_5],
            ),
        ]
    return [
        _row(snapshot, feature_id, "ena_percent_mlt_level", value, "percent_mlt"),
        _row(
            snapshot,
            feature_id,
            "ena_percent_mlt_change_5bd",
            diff(value, _value(lag_5, "ena_value")),
            "percentage_points",
            extra=[lag_5],
        ),
        _row(
            snapshot,
            feature_id,
            "ena_percent_mlt_seasonal_z",
            _seasonal_z(history, position, "ena_value"),
            "z_score",
        ),
    ]


def _cmo_rows(
    snapshot: dict[str, Any],
    history: list[dict[str, Any]],
    position: int,
    feature_id: str,
) -> list[dict[str, Any]]:
    cmo = _value(snapshot, "cmo_brl_mwh")
    lag_1 = _history_snapshot(history, position, 1)
    return [
        _row(snapshot, feature_id, "cmo_log1p", safe_log1p(cmo), "log_brl_mwh"),
        _row(
            snapshot,
            feature_id,
            "cmo_log_change_1obs",
            safe_log_return(cmo, _value(lag_1, "cmo_brl_mwh")),
            "log_return",
            extra=[lag_1],
        ),
    ]


def _load_balance_rows(
    snapshot: dict[str, Any],
    history: list[dict[str, Any]],
    position: int,
    feature_id: str,
) -> list[dict[str, Any]]:
    load = _value(snapshot, "load_mwmed")
    total_generation = _total_generation(snapshot)
    trailing_load = _trailing_mean(history, position, "load_mwmed", 21)
    rows = [
        _row(snapshot, feature_id, "load_log", safe_log(load), "log_mwmed"),
        _row(
            snapshot,
            feature_id,
            "load_deviation_21bd_pct",
            _pct_spread(load, trailing_load),
            "percent",
        ),
    ]
    if snapshot["source_family"] == "ons_energy_balance_daily":
        rows.extend(
            [
                _row(
                    snapshot,
                    feature_id,
                    "hydro_generation_share_pct",
                    _share_pct(_value(snapshot, "hydro_generation_mwmed"), total_generation),
                    "percent",
                ),
                _row(
                    snapshot,
                    feature_id,
                    "thermal_generation_share_pct",
                    _share_pct(_value(snapshot, "thermal_generation_mwmed"), total_generation),
                    "percent",
                ),
                _row(
                    snapshot,
                    feature_id,
                    "wind_generation_share_pct",
                    _share_pct(_value(snapshot, "wind_generation_mwmed"), total_generation),
                    "percent",
                ),
                _row(
                    snapshot,
                    feature_id,
                    "solar_generation_share_pct",
                    _share_pct(_value(snapshot, "solar_generation_mwmed"), total_generation),
                    "percent",
                ),
                _row(
                    snapshot,
                    feature_id,
                    "interchange_to_load_pct",
                    _scale(safe_ratio(_value(snapshot, "interchange_mwmed"), load), 100.0),
                    "percent",
                ),
                _row(
                    snapshot,
                    feature_id,
                    "hour_count_log1p",
                    safe_log1p(_value(snapshot, "hour_count")),
                    "log_count",
                ),
            ]
        )
    return rows


def _interchange_rows(snapshot: dict[str, Any], feature_id: str) -> list[dict[str, Any]]:
    return [
        _row(
            snapshot,
            feature_id,
            "interchange_mwmed_signed_log",
            _signed_log1p(_value(snapshot, "interchange_mwmed")),
            "signed_log_mwmed",
        ),
        _row(
            snapshot,
            feature_id,
            "programmed_interchange_mwmed_signed_log",
            _signed_log1p(_value(snapshot, "programmed_interchange_mwmed")),
            "signed_log_mwmed",
        ),
        _row(
            snapshot,
            feature_id,
            "hour_count_log1p",
            safe_log1p(_value(snapshot, "hour_count")),
            "log_count",
        ),
    ]


def _snapshots(frame: pl.DataFrame) -> dict[tuple[date, str], list[dict[str, Any]]]:
    grouped: dict[tuple[date, str], dict[str, Any]] = defaultdict(dict)
    values: dict[tuple[date, str], dict[str, float | None]] = defaultdict(dict)
    units: dict[tuple[date, str], dict[str, str | None]] = defaultdict(dict)
    for row in frame.to_dicts():
        if not row.get("is_available", True):
            continue
        ref_date = as_date(row["ref_date"])
        feature_id = str(row["feature_id"])
        key = (ref_date, feature_id)
        grouped[key] = {
            **grouped[key],
            "ref_date": ref_date,
            "available_date": max_available_date(ref_date, grouped.get(key), row),
            "source_family": row["source_family"],
            "feature_id": feature_id,
            "observation_ref_date": as_date(row["observation_ref_date"]),
            "observation_available_date": as_date(row["observation_available_date"]),
            "staleness_days": row.get("staleness_days"),
            "source_version": row.get("source_version") or "v0",
            **merge_pit_metadata(grouped.get(key), copy_pit_metadata(row)),
        }
        value_name = str(row["value_name"])
        values[key][value_name] = optional_float(row.get("value"))
        units[key][value_name] = row.get("unit")

    result: dict[tuple[date, str], list[dict[str, Any]]] = defaultdict(list)
    for key, base in grouped.items():
        result[key].append({**base, "values": values[key], "units": units[key]})
    return result


def _histories(
    snapshots: dict[tuple[date, str], list[dict[str, Any]]],
) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, dict[date, dict[str, Any]]] = defaultdict(dict)
    for (_, feature_id), series in snapshots.items():
        for snapshot in series:
            grouped[feature_id][snapshot["observation_ref_date"]] = snapshot
    return {
        feature_id: sorted(items.values(), key=lambda item: item["observation_ref_date"])
        for feature_id, items in grouped.items()
    }


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


def _seasonal_z(
    history: list[dict[str, Any]],
    position: int,
    value_name: str,
) -> float | None:
    current = _value(history[position], value_name)
    if current is None:
        return None
    month = history[position]["observation_ref_date"].month
    prior = [
        _value(item, value_name)
        for item in history[:position]
        if item["observation_ref_date"].month == month
    ]
    values = [value for value in prior if value is not None]
    if len(values) < 24:
        return None
    sigma = stdev(values)
    if sigma == 0:
        return None
    return (current - mean(values)) / sigma


def _trailing_mean(
    history: list[dict[str, Any]],
    position: int,
    value_name: str,
    window: int,
) -> float | None:
    values = [
        _value(item, value_name)
        for item in history[max(0, position - window + 1) : position + 1]
    ]
    values = [value for value in values if value is not None]
    if len(values) < window:
        return None
    return mean(values)


def _total_generation(snapshot: dict[str, Any]) -> float | None:
    values = [
        _value(snapshot, "hydro_generation_mwmed"),
        _value(snapshot, "thermal_generation_mwmed"),
        _value(snapshot, "wind_generation_mwmed"),
        _value(snapshot, "solar_generation_mwmed"),
        _value(snapshot, "other_generation_mwmed"),
    ]
    if any(value is None for value in values):
        return None
    total = sum(value for value in values if value is not None)
    return total if total > 0 else None


def _share_pct(value: float | None, total: float | None) -> float | None:
    return _scale(safe_ratio(value, total), 100.0)


def _pct_spread(value: float | None, base: float | None) -> float | None:
    ratio = safe_ratio(value, base)
    if ratio is None:
        return None
    return 100.0 * (ratio - 1.0)


def _value(snapshot: dict[str, Any] | None, value_name: str) -> float | None:
    if snapshot is None:
        return None
    return optional_float(snapshot.get("values", {}).get(value_name))


def _is_percent_mlt(snapshot: dict[str, Any], value_name: str) -> bool:
    unit = str(snapshot.get("units", {}).get(value_name) or "").lower()
    return "percent_mlt" in unit or "percent mlt" in unit or "%mlt" in unit or "% mlt" in unit


def _signed_log1p(value: float | None) -> float | None:
    number = optional_float(value)
    if number is None:
        return None
    sign = -1.0 if number < 0 else 1.0
    return sign * log1p(abs(number))


def _scale(value: float | None, multiplier: float) -> float | None:
    if value is None:
        return None
    return value * multiplier


def _max_int(*values: Any) -> int:
    ints = [int(value) for value in values if value is not None]
    return max(ints) if ints else 0


def _frame(rows: list[dict[str, Any]]) -> pl.DataFrame:
    if not rows:
        return pl.DataFrame(schema={column: pl.Null for column in ONS_POWER_FEATURE_DAILY_COLUMNS})
    return (
        pl.DataFrame(rows)
        .select(ONS_POWER_FEATURE_DAILY_COLUMNS)
        .unique(subset=PANEL_PRIMARY_KEYS["power_feature_daily"], keep="last")
        .sort(["ref_date", "feature_id", "value_name"])
    )
