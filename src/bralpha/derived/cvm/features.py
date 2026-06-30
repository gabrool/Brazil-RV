from __future__ import annotations

from collections import defaultdict
from datetime import date
from typing import Any

import polars as pl

from bralpha.derived.cvm.quality import validate_asof_panel
from bralpha.derived.cvm.schemas import CVM_FUND_FEATURE_DAILY_COLUMNS, PANEL_PRIMARY_KEYS
from bralpha.derived.feature_utils import (
    as_date,
    in_output_window,
    join_versions,
    max_date,
    optional_float,
    safe_log1p,
    safe_pct_change,
    safe_ratio,
)
from bralpha.derived.pit_metadata import copy_pit_metadata, max_available_date, merge_pit_metadata

SOURCE_FAMILY = "cvm_fund_feature"


def build_cvm_fund_feature_daily(
    *,
    fund_flows_daily: pl.DataFrame | None = None,
    fund_state_asof_daily: pl.DataFrame | None = None,
    start: date | None = None,
    end: date | None = None,
) -> pl.DataFrame:
    snapshots = _snapshots(
        fund_flows_daily=fund_flows_daily,
        fund_state_asof_daily=fund_state_asof_daily,
    )
    histories = _histories(snapshots)
    rows: list[dict[str, Any]] = []
    for (ref_date, feature_id), snapshot in sorted(snapshots.items()):
        if not in_output_window(ref_date, start, end):
            continue
        history = histories[feature_id]
        position = _history_position(history, snapshot)
        rows.extend(_feature_rows(snapshot, history, position))
    frame = _frame(rows)
    validate_asof_panel(
        frame,
        required_columns=CVM_FUND_FEATURE_DAILY_COLUMNS,
        primary_keys=PANEL_PRIMARY_KEYS["fund_feature_daily"],
    )
    return frame


def _feature_rows(
    snapshot: dict[str, Any],
    history: list[dict[str, Any]],
    position: int,
) -> list[dict[str, Any]]:
    feature_id = f"cvm_fund:{snapshot['feature_id']}"
    subscriptions = _value(snapshot, "subscriptions")
    redemptions = _value(snapshot, "redemptions")
    nav = _value(snapshot, "nav")
    portfolio_value = _value(snapshot, "portfolio_value")
    shareholder_count = _value(snapshot, "shareholder_count")
    fund_count = _value(snapshot, "fund_count")
    net_flow = _sub(subscriptions, redemptions)
    gross_flow = _sum_if_any(subscriptions, redemptions)
    lag_21 = _history_snapshot(history, position, 21)
    return [
        _row(snapshot, feature_id, "subscriptions_log", safe_log1p(subscriptions), "log_brl"),
        _row(snapshot, feature_id, "redemptions_log", safe_log1p(redemptions), "log_brl"),
        _row(snapshot, feature_id, "net_flow_brl", net_flow, "BRL"),
        _row(snapshot, feature_id, "gross_flow_brl", gross_flow, "BRL"),
        _row(
            snapshot,
            feature_id,
            "net_flow_to_nav_pct",
            _scale(safe_ratio(net_flow, nav), 100.0),
            "percent",
        ),
        _row(
            snapshot,
            feature_id,
            "gross_flow_to_nav_pct",
            _scale(safe_ratio(gross_flow, nav), 100.0),
            "percent",
        ),
        _row(
            snapshot,
            feature_id,
            "redemption_share_pct",
            _scale(safe_ratio(redemptions, gross_flow), 100.0),
            "percent",
        ),
        _row(snapshot, feature_id, "nav_log", safe_log1p(nav), "log_brl"),
        _row(snapshot, feature_id, "portfolio_value_log", safe_log1p(portfolio_value), "log_brl"),
        _row(
            snapshot,
            feature_id,
            "shareholder_count_log1p",
            safe_log1p(shareholder_count),
            "log_count",
        ),
        _row(snapshot, feature_id, "fund_count_log1p", safe_log1p(fund_count), "log_count"),
        _row(
            snapshot,
            feature_id,
            "shareholder_count_change_21bd_pct",
            safe_pct_change(shareholder_count, _value(lag_21, "shareholder_count")),
            "percent",
            extra=[lag_21],
        ),
    ]


def _snapshots(
    *,
    fund_flows_daily: pl.DataFrame | None,
    fund_state_asof_daily: pl.DataFrame | None,
) -> dict[tuple[date, str], dict[str, Any]]:
    snapshots: dict[tuple[date, str], dict[str, Any]] = {}
    if fund_flows_daily is not None and not fund_flows_daily.is_empty():
        for row in fund_flows_daily.to_dicts():
            ref_date = as_date(row["ref_date"])
            key = (ref_date, str(row["feature_id"]))
            snapshots[key] = _merge_snapshot(
                snapshots.get(key),
                row,
                value_names=[
                    "subscriptions",
                    "redemptions",
                    "subscriptions_count",
                    "redemptions_count",
                    "fund_count",
                ],
            )
    if fund_state_asof_daily is not None and not fund_state_asof_daily.is_empty():
        for row in fund_state_asof_daily.to_dicts():
            if not row.get("is_available", True):
                continue
            ref_date = as_date(row["ref_date"])
            key = (ref_date, str(row["feature_id"]))
            snapshots[key] = _merge_snapshot(
                snapshots.get(key),
                row,
                value_names=[
                    "portfolio_value",
                    "nav",
                    "shareholder_count",
                    "fund_count",
                    "portfolio_value_count",
                    "nav_count",
                    "shareholder_count_count",
                ],
            )
    return snapshots


def _merge_snapshot(
    existing: dict[str, Any] | None,
    row: dict[str, Any],
    *,
    value_names: list[str],
) -> dict[str, Any]:
    ref_date = as_date(row["ref_date"])
    base = existing or {
        "ref_date": ref_date,
        "available_date": max_available_date(ref_date, row),
        "feature_id": str(row["feature_id"]),
        "values": {},
        "observation_ref_date": as_date(row.get("observation_ref_date") or row["ref_date"]),
        "observation_available_date": as_date(
            row.get("observation_available_date") or row.get("available_date") or row["ref_date"]
        ),
        "staleness_days": row.get("staleness_days"),
        "source_version": row.get("source_version") or "v0",
        **copy_pit_metadata(row),
    }
    base["available_date"] = max_available_date(ref_date, base, row)
    base["observation_ref_date"] = max_date(
        base.get("observation_ref_date"),
        row.get("observation_ref_date") or row["ref_date"],
    )
    base["observation_available_date"] = max_date(
        base.get("observation_available_date"),
        row.get("observation_available_date") or row.get("available_date") or row["ref_date"],
    )
    base["staleness_days"] = _max_int(base.get("staleness_days"), row.get("staleness_days"))
    base["source_version"] = join_versions(base.get("source_version"), row.get("source_version"))
    base.update(merge_pit_metadata(base, copy_pit_metadata(row)))
    for value_name in value_names:
        if value_name in row:
            base["values"][value_name] = optional_float(row.get(value_name))
    return base


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


def _value(snapshot: dict[str, Any] | None, value_name: str) -> float | None:
    if snapshot is None:
        return None
    return optional_float(snapshot.get("values", {}).get(value_name))


def _sub(left: float | None, right: float | None) -> float | None:
    if left is None or right is None:
        return None
    return left - right


def _sum_if_any(*values: float | None) -> float | None:
    present = [value for value in values if value is not None]
    if not present:
        return None
    return sum(present)


def _scale(value: float | None, multiplier: float) -> float | None:
    if value is None:
        return None
    return value * multiplier


def _max_int(*values: Any) -> int:
    ints = [int(value) for value in values if value is not None]
    return max(ints) if ints else 0


def _frame(rows: list[dict[str, Any]]) -> pl.DataFrame:
    if not rows:
        return pl.DataFrame(schema={column: pl.Null for column in CVM_FUND_FEATURE_DAILY_COLUMNS})
    return (
        pl.DataFrame(rows)
        .select(CVM_FUND_FEATURE_DAILY_COLUMNS)
        .unique(subset=PANEL_PRIMARY_KEYS["fund_feature_daily"], keep="last")
        .sort(["ref_date", "feature_id", "value_name"])
    )
