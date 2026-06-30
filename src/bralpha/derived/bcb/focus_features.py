from __future__ import annotations

from collections import defaultdict
from datetime import date
from math import log1p
from typing import Any

import polars as pl

from bralpha.derived.bcb.quality import validate_asof_panel
from bralpha.derived.bcb.schemas import BCB_FOCUS_FEATURE_DAILY_COLUMNS, PANEL_PRIMARY_KEYS
from bralpha.derived.feature_utils import as_date, diff, in_output_window, join_versions, max_date

SOURCE_FAMILY = "bcb_focus_feature"
REVISION_LAGS = [1, 5, 21]


def build_focus_feature_daily(
    focus_expectation_asof_daily: pl.DataFrame,
    *,
    start: date | None = None,
    end: date | None = None,
) -> pl.DataFrame:
    rows = _normalized_rows(focus_expectation_asof_daily)
    by_key: dict[str, list[dict[str, Any]]] = defaultdict(list)
    by_match: dict[tuple[date, tuple[Any, ...]], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_key[str(row["expectation_key"])].append(row)
        by_match[(row["ref_date"], _match_key(row))].append(row)

    output: list[dict[str, Any]] = []
    for key_rows in by_key.values():
        key_rows.sort(key=lambda item: item["ref_date"])
        output.extend(_series_features(key_rows, start=start, end=end))
    for (ref_date, _key), match_rows in sorted(by_match.items()):
        if in_output_window(ref_date, start, end):
            output.extend(_top5_spread_rows(match_rows))
    if not output:
        return _empty()
    frame = (
        pl.DataFrame(output)
        .select(BCB_FOCUS_FEATURE_DAILY_COLUMNS)
        .unique(subset=PANEL_PRIMARY_KEYS["focus_feature_daily"], keep="last")
        .sort(["ref_date", "feature_id", "value_name"])
    )
    validate_asof_panel(
        frame,
        required_columns=BCB_FOCUS_FEATURE_DAILY_COLUMNS,
        primary_keys=PANEL_PRIMARY_KEYS["focus_feature_daily"],
    )
    return frame


def _series_features(
    rows: list[dict[str, Any]],
    *,
    start: date | None,
    end: date | None,
) -> list[dict[str, Any]]:
    output = []
    for position, row in enumerate(rows):
        if not in_output_window(row["ref_date"], start, end):
            continue
        feature_id = f"bcb_focus:{row['expectation_key']}"
        output.extend(
            [
                _row(row, feature_id, "median_level", _float(row.get("median")), None),
                _row(row, feature_id, "mean_level", _float(row.get("mean")), None),
                _row(
                    row,
                    feature_id,
                    "std_dev_log1p",
                    _log1p(row.get("std_dev")),
                    "log_value",
                ),
                _row(
                    row,
                    feature_id,
                    "respondents_log1p",
                    _log1p(row.get("respondents")),
                    "log_count",
                ),
                _row(
                    row,
                    feature_id,
                    "dispersion_to_abs_median",
                    _dispersion_ratio(row),
                    "ratio",
                ),
            ]
        )
        for lag in REVISION_LAGS:
            lagged = _lag(rows, position, lag)
            output.append(
                _row(
                    row,
                    feature_id,
                    f"median_revision_{lag}bd",
                    None if lagged is None else diff(row.get("median"), lagged.get("median")),
                    None,
                    extra_rows=[lagged],
                )
            )
            output.append(
                _row(
                    row,
                    feature_id,
                    f"mean_revision_{lag}bd",
                    None if lagged is None else diff(row.get("mean"), lagged.get("mean")),
                    None,
                    extra_rows=[lagged],
                )
            )
    return output


def _top5_spread_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    general = [row for row in rows if not bool(row.get("is_top5"))]
    top5 = [row for row in rows if bool(row.get("is_top5"))]
    if not general or not top5:
        return []
    general_row = general[-1]
    top5_row = top5[-1]
    feature_id = f"bcb_focus:top5_minus_general:{_match_key_text(top5_row)}"
    base = _combined_base([top5_row, general_row])
    return [
        _row(
            base,
            feature_id,
            "top5_minus_general_median",
            diff(top5_row.get("median"), general_row.get("median")),
            None,
            extra_rows=[top5_row, general_row],
        ),
        _row(
            base,
            feature_id,
            "top5_minus_general_mean",
            diff(top5_row.get("mean"), general_row.get("mean")),
            None,
            extra_rows=[top5_row, general_row],
        ),
    ]


def _row(
    base: dict[str, Any],
    feature_id: str,
    value_name: str,
    value: float | None,
    unit: str | None,
    *,
    extra_rows: list[dict[str, Any] | None] | None = None,
) -> dict[str, Any]:
    extra_rows = extra_rows or []
    rows = [base, *[row for row in extra_rows if row is not None]]
    return {
        "ref_date": base["ref_date"],
        "available_date": base.get("available_date") or base["ref_date"],
        "source_family": SOURCE_FAMILY,
        "feature_id": feature_id,
        "value_name": value_name,
        "value": value,
        "unit": unit,
        "observation_ref_date": max_date(*(row.get("observation_ref_date") for row in rows)),
        "observation_available_date": max_date(
            *(row.get("observation_available_date") for row in rows)
        ),
        "availability_policy": None,
        "availability_basis": None,
        "revision_policy": "unrevised",
        "model_usable": True,
        "is_available": True,
        "staleness_days": _max_int(*(row.get("staleness_days") for row in rows)),
        "source_version": join_versions(*(row.get("source_version") for row in rows)),
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
                "observation_ref_date": row.get("observation_ref_date") or row["ref_date"],
                "observation_available_date": row.get("observation_available_date")
                or row.get("available_date")
                or row["ref_date"],
                "source_version": row.get("source_version") or "v0",
            }
        )
    return rows


def _match_key(row: dict[str, Any]) -> tuple[Any, ...]:
    return (
        row.get("endpoint"),
        row.get("indicator"),
        row.get("indicator_detail"),
        row.get("reference_period"),
        row.get("meeting"),
        row.get("calculation_type"),
        row.get("base_calculation"),
    )


def _match_key_text(row: dict[str, Any]) -> str:
    return "|".join(str(value).lower().replace(" ", "_") for value in _match_key(row))


def _combined_base(rows: list[dict[str, Any]]) -> dict[str, Any]:
    base = dict(rows[0])
    base["observation_ref_date"] = max_date(*(row.get("observation_ref_date") for row in rows))
    base["observation_available_date"] = max_date(
        *(row.get("observation_available_date") for row in rows)
    )
    base["source_version"] = join_versions(*(row.get("source_version") for row in rows))
    base["staleness_days"] = _max_int(*(row.get("staleness_days") for row in rows))
    return base


def _dispersion_ratio(row: dict[str, Any]) -> float | None:
    std = _float(row.get("std_dev"))
    median = _float(row.get("median"))
    if std is None or median is None:
        return None
    return std / max(abs(median), 1.0)


def _lag(rows: list[dict[str, Any]], position: int, lag: int) -> dict[str, Any] | None:
    lag_position = position - lag
    if lag_position < 0:
        return None
    return rows[lag_position]


def _float(value: Any) -> float | None:
    if value is None:
        return None
    return float(value)


def _log1p(value: Any) -> float | None:
    number = _float(value)
    if number is None or number < 0:
        return None
    return log1p(number)


def _max_int(*values: Any) -> int | None:
    ints = [int(value) for value in values if value is not None]
    if not ints:
        return None
    return max(ints)


def _empty() -> pl.DataFrame:
    return pl.DataFrame(schema={column: pl.Null for column in BCB_FOCUS_FEATURE_DAILY_COLUMNS})
