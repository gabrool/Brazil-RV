from __future__ import annotations

from collections import defaultdict
from datetime import date
from typing import Any

import polars as pl

from bralpha.derived.b3.quality import validate_target_panel
from bralpha.derived.b3.schemas import PANEL_PRIMARY_KEYS, TARGETS_DAILY_COLUMNS

GENERIC_QUOTE_TARGET_TYPES = {"quote_diff", "quote_pct_change"}
DI_CURVE_TARGET_TYPES = {"rate_bp_change", "discount_factor_log_change"}
TARGET_TYPES_BY_ASSET_FAMILY = {
    "futures": GENERIC_QUOTE_TARGET_TYPES,
    "index": GENERIC_QUOTE_TARGET_TYPES,
    "di_curve": DI_CURVE_TARGET_TYPES,
}
SUPPORTED_TARGET_TYPES = GENERIC_QUOTE_TARGET_TYPES | DI_CURVE_TARGET_TYPES


def build_targets_daily(
    *,
    continuous_futures_daily: pl.DataFrame | None = None,
    di_curve_grid_daily: pl.DataFrame | None = None,
    index_daily: pl.DataFrame | None = None,
    horizons: list[int],
    target_types: list[str],
    start: date | None = None,
    end: date | None = None,
) -> pl.DataFrame:
    observations = []
    observations.extend(
        _observations(
            continuous_futures_daily,
            id_col="continuous_id",
            value_col="settlement",
            asset_family="futures",
        )
    )
    observations.extend(
        _observations(
            di_curve_grid_daily,
            id_col="curve_target_id",
            value_col="curve_value",
            asset_family="di_curve",
            secondary_value_col="log_discount_factor",
        )
    )
    observations.extend(
        _observations(index_daily, id_col="index_id", value_col="close", asset_family="index")
    )

    rows = []
    by_target: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for observation in observations:
        by_target[observation["target_id"]].append(observation)
    for target_id, series in by_target.items():
        series.sort(key=lambda row: row["ref_date"])
        for position, current in enumerate(series):
            if start is not None and current["ref_date"] < start:
                continue
            if end is not None and current["ref_date"] > end:
                continue
            for horizon in horizons:
                future_index = position + horizon
                if future_index >= len(series):
                    continue
                future = series[future_index]
                for target_type in _target_types_for_asset_family(
                    target_types,
                    current["asset_family"],
                ):
                    target_value = _target_value(
                        target_type,
                        current["value"],
                        future["value"],
                        current.get("secondary_value"),
                        future.get("secondary_value"),
                    )
                    if target_value is None:
                        continue
                    rows.append(
                        {
                            "ref_date": current["ref_date"],
                            "label_available_date": future["available_date"],
                            "target_id": target_id,
                            "asset_family": current["asset_family"],
                            "horizon": horizon,
                            "target_type": target_type,
                            "target_start_date": current["ref_date"],
                            "target_end_date": future["ref_date"],
                            "target_value": target_value,
                            "source_version": _join_versions(
                                current.get("source_version"),
                                future.get("source_version"),
                            ),
                        }
                    )
    frame = _frame(rows)
    validate_target_panel(
        frame,
        required_columns=TARGETS_DAILY_COLUMNS,
        primary_keys=PANEL_PRIMARY_KEYS["targets_daily"],
    )
    return frame


def _observations(
    frame: pl.DataFrame | None,
    *,
    id_col: str,
    value_col: str,
    asset_family: str,
    secondary_value_col: str | None = None,
) -> list[dict[str, Any]]:
    if frame is None or frame.is_empty():
        return []
    rows = []
    for row in frame.to_dicts():
        target_id = _target_id(row, id_col)
        value = row.get(value_col)
        if target_id is None or value is None:
            continue
        rows.append(
            {
                "ref_date": _as_date(row["ref_date"]),
                "available_date": _as_date(row.get("available_date")),
                "target_id": target_id,
                "asset_family": asset_family,
                "value": value,
                "secondary_value": (
                    row.get(secondary_value_col) if secondary_value_col is not None else None
                ),
                "source_version": row.get("source_version") or "v0",
            }
        )
    return rows


def _target_types_for_asset_family(
    target_types: list[str],
    asset_family: str,
) -> list[str]:
    unknown = sorted(set(target_types) - SUPPORTED_TARGET_TYPES)
    if unknown:
        raise ValueError(f"Unsupported target_type(s): {unknown}")
    allowed = TARGET_TYPES_BY_ASSET_FAMILY.get(asset_family, set())
    return [target_type for target_type in target_types if target_type in allowed]


def _target_id(row: dict[str, Any], id_col: str) -> str | None:
    if id_col == "curve_target_id":
        curve_id = row.get("curve_id")
        tenor = row.get("tenor_business_days") or row.get("tenor_days")
        return f"{curve_id}_{tenor}BD" if curve_id and tenor is not None else None
    value = row.get(id_col)
    return str(value) if value is not None else None


def _target_value(
    target_type: str,
    start_value: float | None,
    end_value: float | None,
    start_secondary_value: float | None = None,
    end_secondary_value: float | None = None,
) -> float | None:
    if target_type == "discount_factor_log_change":
        if start_secondary_value is None or end_secondary_value is None:
            return None
        return end_secondary_value - start_secondary_value
    if target_type == "quote_diff":
        if start_value is None or end_value is None:
            return None
        return end_value - start_value
    if target_type == "quote_pct_change":
        if start_value is None or end_value is None:
            return None
        if start_value == 0:
            return None
        return (end_value / start_value) - 1
    if target_type == "rate_bp_change":
        if start_value is None or end_value is None:
            return None
        return (end_value - start_value) * 10_000.0
    raise ValueError(f"Unsupported target_type: {target_type}")


def _join_versions(left: Any, right: Any) -> str:
    versions = sorted({str(value) for value in [left, right] if value})
    return "|".join(versions) if versions else "v0"


def _frame(rows: list[dict[str, Any]]) -> pl.DataFrame:
    if not rows:
        return pl.DataFrame(schema={column: pl.Null for column in TARGETS_DAILY_COLUMNS})
    return pl.DataFrame(rows).select(TARGETS_DAILY_COLUMNS)


def _as_date(value: Any) -> date:
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])
