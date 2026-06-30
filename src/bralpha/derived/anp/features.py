from __future__ import annotations

from collections import defaultdict
from datetime import date
from typing import Any

import polars as pl

from bralpha.derived.anp.quality import validate_asof_panel
from bralpha.derived.anp.schemas import ANP_FUEL_FEATURE_DAILY_COLUMNS, PANEL_PRIMARY_KEYS
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
from bralpha.derived.pit_metadata import copy_pit_metadata, max_available_date, merge_pit_metadata

SOURCE_FAMILY = "anp_fuel_feature"


def build_anp_fuel_feature_daily(
    state_asof_daily: pl.DataFrame,
    *,
    start: date | None = None,
    end: date | None = None,
) -> pl.DataFrame:
    snapshots = _snapshots(state_asof_daily)
    histories = _histories(snapshots)
    rows: list[dict[str, Any]] = []
    for key, series in sorted(snapshots.items()):
        history = histories[key[1]]
        for snapshot in sorted(series, key=lambda item: item["ref_date"]):
            if not in_output_window(snapshot["ref_date"], start, end):
                continue
            position = _history_position(history, snapshot)
            rows.extend(_snapshot_feature_rows(snapshot, history, position))
    rows.extend(_cross_product_rows(snapshots, start=start, end=end))
    frame = _frame(rows)
    validate_asof_panel(
        frame,
        required_columns=ANP_FUEL_FEATURE_DAILY_COLUMNS,
        primary_keys=PANEL_PRIMARY_KEYS["fuel_feature_daily"],
    )
    return frame


def _snapshot_feature_rows(
    snapshot: dict[str, Any],
    history: list[dict[str, Any]],
    position: int,
) -> list[dict[str, Any]]:
    source_family = snapshot["source_family"]
    feature_id = f"anp_fuel:{snapshot['feature_id']}"
    if source_family == "anp_fuel_price":
        return _fuel_price_rows(snapshot, history, position, feature_id)
    if source_family == "anp_fuel_sales":
        return _volume_rows(
            snapshot,
            history,
            position,
            feature_id,
            value_column="sales_volume_m3",
            prefix="sales_volume",
        )
    if source_family == "anp_oil_gas":
        return _volume_rows(
            snapshot,
            history,
            position,
            feature_id,
            value_column="metric_value",
            prefix="production",
        )
    return []


def _fuel_price_rows(
    snapshot: dict[str, Any],
    history: list[dict[str, Any]],
    position: int,
    feature_id: str,
) -> list[dict[str, Any]]:
    sale_price = _value(snapshot, "sale_price")
    purchase_price = _value(snapshot, "purchase_price")
    lag_1 = _history_snapshot(history, position, 1)
    lag_4 = _history_snapshot(history, position, 4)
    return [
        _row(snapshot, feature_id, "sale_price_log", safe_log(sale_price), "log_price"),
        _row(
            snapshot,
            feature_id,
            "purchase_price_log",
            safe_log(purchase_price),
            "log_price",
        ),
        _row(
            snapshot,
            feature_id,
            "sale_price_log_change_1obs",
            safe_log_return(sale_price, _value(lag_1, "sale_price")),
            "log_return",
            extra=[lag_1],
        ),
        _row(
            snapshot,
            feature_id,
            "sale_price_log_change_4obs",
            safe_log_return(sale_price, _value(lag_4, "sale_price")),
            "log_return",
            extra=[lag_4],
        ),
        _row(
            snapshot,
            feature_id,
            "sale_purchase_spread_pct",
            _scale(safe_ratio(diff(sale_price, purchase_price), sale_price), 100.0),
            "percent",
        ),
        _row(
            snapshot,
            feature_id,
            "station_count_log1p",
            safe_log1p(_value(snapshot, "station_count")),
            "log_count",
        ),
    ]


def _volume_rows(
    snapshot: dict[str, Any],
    history: list[dict[str, Any]],
    position: int,
    feature_id: str,
    *,
    value_column: str,
    prefix: str,
) -> list[dict[str, Any]]:
    value = _value(snapshot, value_column)
    lag_12 = _history_snapshot(history, position, 12)
    rolling_3 = _history_values(history, position, value_column, 3)
    return [
        _row(snapshot, feature_id, f"{prefix}_log", safe_log(value), "log_volume"),
        _row(
            snapshot,
            feature_id,
            f"{prefix}_yoy_log_change",
            safe_log_return(value, _value(lag_12, value_column)),
            "log_return",
            extra=[lag_12],
        ),
        _row(
            snapshot,
            feature_id,
            f"{prefix}_3obs_sum_log",
            safe_log(sum(rolling_3)) if len(rolling_3) == 3 else None,
            "log_volume",
        ),
        _row(
            snapshot,
            feature_id,
            "state_count_log1p",
            safe_log1p(_value(snapshot, "state_count")),
            "log_count",
        ),
    ]


def _cross_product_rows(
    snapshots: dict[tuple[date, str], list[dict[str, Any]]],
    *,
    start: date | None,
    end: date | None,
) -> list[dict[str, Any]]:
    by_ref_and_geo: dict[tuple[date, str], dict[str, dict[str, Any]]] = defaultdict(dict)
    for (ref_date, _), series in snapshots.items():
        if not in_output_window(ref_date, start, end):
            continue
        current = series[-1]
        if current["source_family"] != "anp_fuel_price":
            continue
        parsed = _parse_price_feature_id(current["feature_id"])
        if parsed is None:
            continue
        geo_key, product = parsed
        by_ref_and_geo[(ref_date, geo_key)][product] = current

    rows: list[dict[str, Any]] = []
    for (ref_date, geo_key), products in sorted(by_ref_and_geo.items()):
        ethanol = _find_product(products, "etanol")
        gasoline = _find_product(products, "gasolina")
        diesel = _find_product(products, "diesel")
        feature_id = f"anp_fuel:{geo_key}:cross_product"
        rows.append(
            _combined_row(
                ref_date,
                feature_id,
                "ethanol_gasoline_parity",
                safe_ratio(_value(ethanol, "sale_price"), _value(gasoline, "sale_price")),
                "ratio",
                [ethanol, gasoline],
            )
        )
        rows.append(
            _combined_row(
                ref_date,
                feature_id,
                "diesel_gasoline_spread_pct",
                _pct_spread(_value(diesel, "sale_price"), _value(gasoline, "sale_price")),
                "percent",
                [diesel, gasoline],
            )
        )
    return rows


def _snapshots(frame: pl.DataFrame) -> dict[tuple[date, str], list[dict[str, Any]]]:
    grouped: dict[tuple[date, str], dict[str, Any]] = defaultdict(dict)
    values: dict[tuple[date, str], dict[str, float | None]] = defaultdict(dict)
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
        values[key][str(row["value_name"])] = optional_float(row.get("value"))
    result: dict[tuple[date, str], list[dict[str, Any]]] = defaultdict(list)
    for key, base in grouped.items():
        result[key].append({**base, "values": values[key]})
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


def _combined_row(
    ref_date: date,
    feature_id: str,
    value_name: str,
    value: float | None,
    unit: str,
    contributors: list[dict[str, Any] | None],
) -> dict[str, Any]:
    present = [item for item in contributors if item is not None]
    return {
        "ref_date": ref_date,
        "available_date": max_available_date(ref_date, *present),
        "source_family": SOURCE_FAMILY,
        "feature_id": feature_id,
        "value_name": value_name,
        "value": value,
        "unit": unit,
        "observation_ref_date": max_date(*(item["observation_ref_date"] for item in present)),
        "observation_available_date": max_date(
            *(item["observation_available_date"] for item in present)
        ),
        "is_available": value is not None,
        "staleness_days": _max_int(*(item.get("staleness_days") for item in present)),
        "source_version": join_versions(*(item.get("source_version") for item in present)),
        **merge_pit_metadata(*present),
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


def _history_values(
    history: list[dict[str, Any]],
    position: int,
    value_name: str,
    window: int,
) -> list[float]:
    values = [
        _value(item, value_name)
        for item in history[max(0, position - window + 1) : position + 1]
    ]
    return [value for value in values if value is not None]


def _value(snapshot: dict[str, Any] | None, value_name: str) -> float | None:
    if snapshot is None:
        return None
    return optional_float(snapshot.get("values", {}).get(value_name))


def _parse_price_feature_id(feature_id: str) -> tuple[str, str] | None:
    parts = feature_id.split("|")
    if len(parts) != 4 or parts[0] != "anp_fuel_price":
        return None
    return "|".join(parts[1:3]), parts[3]


def _find_product(
    products: dict[str, dict[str, Any]],
    token: str,
) -> dict[str, Any] | None:
    for product, snapshot in products.items():
        if token in product:
            return snapshot
    return None


def _pct_spread(current: float | None, base: float | None) -> float | None:
    ratio = safe_ratio(current, base)
    if ratio is None:
        return None
    return 100.0 * (ratio - 1.0)


def _scale(value: float | None, multiplier: float) -> float | None:
    if value is None:
        return None
    return value * multiplier


def _max_int(*values: Any) -> int:
    ints = [int(value) for value in values if value is not None]
    return max(ints) if ints else 0


def _frame(rows: list[dict[str, Any]]) -> pl.DataFrame:
    if not rows:
        return pl.DataFrame(schema={column: pl.Null for column in ANP_FUEL_FEATURE_DAILY_COLUMNS})
    return (
        pl.DataFrame(rows)
        .select(ANP_FUEL_FEATURE_DAILY_COLUMNS)
        .unique(subset=PANEL_PRIMARY_KEYS["fuel_feature_daily"], keep="last")
        .sort(["ref_date", "feature_id", "value_name"])
    )
