from __future__ import annotations

from collections import defaultdict
from datetime import date
from typing import Any

import polars as pl

from bralpha.derived.feature_utils import (
    as_date,
    in_output_window,
    join_versions,
    max_date,
    optional_float,
    safe_log,
    safe_log_return,
    safe_ratio,
)
from bralpha.derived.novo_caged.quality import validate_asof_panel
from bralpha.derived.novo_caged.schemas import NOVO_CAGED_FEATURE_DAILY_COLUMNS, PANEL_PRIMARY_KEYS
from bralpha.derived.pit_metadata import copy_pit_metadata, max_available_date, merge_pit_metadata

SOURCE_FAMILY = "novo_caged_feature"


def build_novo_caged_feature_daily(
    state_asof_daily: pl.DataFrame,
    *,
    start: date | None = None,
    end: date | None = None,
) -> pl.DataFrame:
    signed = _signed_snapshots(state_asof_daily)
    combined = _combined_snapshots(signed)
    histories = _histories(combined)
    diffusion = _diffusion_by_ref_date(combined)
    rows: list[dict[str, Any]] = []
    for (ref_date, feature_id), snapshot in sorted(combined.items()):
        if not in_output_window(ref_date, start, end):
            continue
        history = histories[feature_id]
        position = _history_position(history, snapshot)
        rows.extend(_feature_rows(snapshot, history, position))
        rows.extend(_diffusion_rows(snapshot, diffusion.get(ref_date, {})))
    frame = _frame(rows)
    validate_asof_panel(
        frame,
        required_columns=NOVO_CAGED_FEATURE_DAILY_COLUMNS,
        primary_keys=PANEL_PRIMARY_KEYS["feature_daily"],
    )
    return frame


def _feature_rows(
    snapshot: dict[str, Any],
    history: list[dict[str, Any]],
    position: int,
) -> list[dict[str, Any]]:
    feature_id = f"novo_caged:{snapshot['base_feature_id']}"
    admissions = snapshot["admissions_count"]
    dismissals = snapshot["dismissals_count"]
    net_jobs = _sub(admissions, dismissals)
    gross_jobs = _sum_if_any(admissions, dismissals)
    wage_mean = snapshot.get("wage_mean")
    lag_12 = _history_snapshot(history, position, 12)
    return [
        _row(snapshot, feature_id, "admissions_count", admissions, "jobs"),
        _row(snapshot, feature_id, "dismissals_count", dismissals, "jobs"),
        _row(snapshot, feature_id, "net_jobs", net_jobs, "jobs"),
        _row(snapshot, feature_id, "gross_jobs", gross_jobs, "jobs"),
        _row(
            snapshot,
            feature_id,
            "admission_dismissal_ratio",
            safe_ratio(admissions, dismissals),
            "ratio",
        ),
        _row(
            snapshot,
            feature_id,
            "net_jobs_yoy_change",
            _sub(net_jobs, _value(lag_12, "net_jobs")),
            "jobs",
            extra=[lag_12],
        ),
        _row(
            snapshot,
            feature_id,
            "net_jobs_to_gross_pct",
            _scale(safe_ratio(net_jobs, gross_jobs), 100.0),
            "percent",
        ),
        _row(snapshot, feature_id, "wage_mean_log", safe_log(wage_mean), "log_brl"),
        _row(
            snapshot,
            feature_id,
            "wage_mean_yoy_log_change",
            safe_log_return(wage_mean, _value(lag_12, "wage_mean")),
            "log_return",
            extra=[lag_12],
        ),
        _row(
            snapshot,
            feature_id,
            "contract_hours_mean",
            snapshot.get("contract_hours_mean"),
            "hours",
        ),
    ]


def _diffusion_rows(
    snapshot: dict[str, Any],
    diffusion: dict[str, float],
) -> list[dict[str, Any]]:
    feature_id = f"novo_caged:{snapshot['base_feature_id']}"
    return [
        _row(
            snapshot,
            feature_id,
            "state_positive_diffusion_share_pct",
            diffusion.get("state"),
            "percent",
        ),
        _row(
            snapshot,
            feature_id,
            "sector_positive_diffusion_share_pct",
            diffusion.get("cnae_section"),
            "percent",
        ),
    ]


def _signed_snapshots(
    frame: pl.DataFrame,
) -> dict[tuple[date, str], dict[str, dict[str, Any]]]:
    signed: dict[tuple[date, str], dict[str, dict[str, Any]]] = defaultdict(dict)
    for row in frame.to_dicts():
        if row.get("source_family") != "novo_caged_movements" or not row.get(
            "is_available", True
        ):
            continue
        parsed = _parse_feature_id(str(row["feature_id"]))
        if parsed is None:
            continue
        base_feature_id, sign = parsed
        ref_date = as_date(row["ref_date"])
        sign_bucket = signed[(ref_date, base_feature_id)].setdefault(
            sign,
            {
                "ref_date": ref_date,
                "available_date": max_available_date(ref_date, row),
                "base_feature_id": base_feature_id,
                "group_type": base_feature_id.split("|")[1]
                if "|" in base_feature_id
                else "unknown",
                "observation_ref_date": as_date(row["observation_ref_date"]),
                "observation_available_date": as_date(row["observation_available_date"]),
                "staleness_days": row.get("staleness_days"),
                "source_version": row.get("source_version") or "v0",
                "values": {},
                **copy_pit_metadata(row),
            },
        )
        sign_bucket["available_date"] = max_available_date(ref_date, sign_bucket, row)
        sign_bucket["values"][str(row["value_name"])] = optional_float(row.get("value"))
        sign_bucket["observation_ref_date"] = max_date(
            sign_bucket["observation_ref_date"],
            row["observation_ref_date"],
        )
        sign_bucket["observation_available_date"] = max_date(
            sign_bucket["observation_available_date"],
            row["observation_available_date"],
        )
        sign_bucket["staleness_days"] = _max_int(
            sign_bucket.get("staleness_days"),
            row.get("staleness_days"),
        )
        sign_bucket["source_version"] = join_versions(
            sign_bucket.get("source_version"),
            row.get("source_version"),
        )
        sign_bucket.update(merge_pit_metadata(sign_bucket, copy_pit_metadata(row)))
    return signed


def _combined_snapshots(
    signed: dict[tuple[date, str], dict[str, dict[str, Any]]],
) -> dict[tuple[date, str], dict[str, Any]]:
    combined: dict[tuple[date, str], dict[str, Any]] = {}
    for (ref_date, base_feature_id), signs in signed.items():
        admission = _pick_sign(signs, positive=True)
        dismissal = _pick_sign(signs, positive=False)
        contributors = [item for item in [admission, dismissal] if item is not None]
        admissions = _snapshot_value(admission, "movement_count")
        dismissals = _snapshot_value(dismissal, "movement_count")
        net_jobs = _sub(admissions, dismissals)
        snapshot = {
            "ref_date": ref_date,
            "available_date": max_available_date(ref_date, *contributors),
            "base_feature_id": base_feature_id,
            "group_type": _group_type(base_feature_id),
            "admissions_count": admissions,
            "dismissals_count": dismissals,
            "net_jobs": net_jobs,
            "gross_jobs": _sum_if_any(admissions, dismissals),
            "wage_mean": _weighted_mean(admission, dismissal, "wage_mean", "wage_count"),
            "contract_hours_mean": _weighted_mean(
                admission,
                dismissal,
                "contract_hours_mean",
                "contract_hours_count",
            ),
            "observation_ref_date": max_date(
                *(item["observation_ref_date"] for item in contributors)
            ),
            "observation_available_date": max_date(
                *(item["observation_available_date"] for item in contributors)
            ),
            "staleness_days": _max_int(*(item.get("staleness_days") for item in contributors)),
            "source_version": join_versions(*(item.get("source_version") for item in contributors)),
            **merge_pit_metadata(*contributors),
            "values": {},
        }
        snapshot["values"] = {
            "net_jobs": snapshot["net_jobs"],
            "wage_mean": snapshot["wage_mean"],
        }
        combined[(ref_date, base_feature_id)] = snapshot
    return combined


def _diffusion_by_ref_date(
    combined: dict[tuple[date, str], dict[str, Any]],
) -> dict[date, dict[str, float]]:
    grouped: dict[date, dict[str, list[float]]] = defaultdict(lambda: defaultdict(list))
    for (ref_date, _), snapshot in combined.items():
        group_type = snapshot["group_type"]
        if group_type in {"state", "cnae_section"} and snapshot.get("net_jobs") is not None:
            grouped[ref_date][group_type].append(snapshot["net_jobs"])
    output: dict[date, dict[str, float]] = {}
    for ref_date, groups in grouped.items():
        output[ref_date] = {}
        for group_type, values in groups.items():
            if values:
                output[ref_date][group_type] = 100.0 * sum(value > 0 for value in values) / len(
                    values
                )
    return output


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


def _parse_feature_id(feature_id: str) -> tuple[str, str] | None:
    parts = feature_id.split("|")
    if len(parts) != 4 or parts[0] != "novo_caged_movement":
        return None
    return "|".join(parts[:3]), parts[3]


def _pick_sign(signs: dict[str, dict[str, Any]], *, positive: bool) -> dict[str, Any] | None:
    prefixes = ("plus", "admiss", "1") if positive else ("minus", "deslig", "-1")
    for sign, snapshot in signs.items():
        if sign.startswith(prefixes):
            return snapshot
    return None


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


def _snapshot_value(snapshot: dict[str, Any] | None, value_name: str) -> float | None:
    if snapshot is None:
        return None
    return optional_float(snapshot.get("values", {}).get(value_name))


def _value(snapshot: dict[str, Any] | None, value_name: str) -> float | None:
    if snapshot is None:
        return None
    if value_name in snapshot:
        return optional_float(snapshot.get(value_name))
    return _snapshot_value(snapshot, value_name)


def _weighted_mean(
    admission: dict[str, Any] | None,
    dismissal: dict[str, Any] | None,
    value_name: str,
    count_name: str,
) -> float | None:
    pairs = []
    for snapshot in [admission, dismissal]:
        value = _snapshot_value(snapshot, value_name)
        count = _snapshot_value(snapshot, count_name)
        if value is not None and count is not None and count > 0:
            pairs.append((value, count))
    if not pairs:
        return None
    total_count = sum(count for _, count in pairs)
    return sum(value * count for value, count in pairs) / total_count


def _group_type(base_feature_id: str) -> str:
    parts = base_feature_id.split("|")
    return parts[1] if len(parts) > 1 else "unknown"


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
        return pl.DataFrame(schema={column: pl.Null for column in NOVO_CAGED_FEATURE_DAILY_COLUMNS})
    return (
        pl.DataFrame(rows)
        .select(NOVO_CAGED_FEATURE_DAILY_COLUMNS)
        .unique(subset=PANEL_PRIMARY_KEYS["feature_daily"], keep="last")
        .sort(["ref_date", "feature_id", "value_name"])
    )
