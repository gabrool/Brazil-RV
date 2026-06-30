from __future__ import annotations

from collections import defaultdict
from datetime import date
from math import log1p
from typing import Any

import polars as pl

from bralpha.derived.feature_utils import (
    as_date,
    diff,
    in_output_window,
    join_versions,
    max_date,
    optional_float,
    realized_vol_ann,
    safe_log,
    safe_log_return,
)
from bralpha.derived.fred.quality import validate_asof_panel
from bralpha.derived.fred.schemas import FRED_FEATURE_DAILY_COLUMNS, PANEL_PRIMARY_KEYS

RATE_SERIES = {
    "DGS2",
    "DGS5",
    "DGS10",
    "DGS30",
    "DFF",
    "SOFR",
    "DFEDTARU",
    "DFEDTARL",
    "DFII5",
    "DFII10",
    "DFII30",
    "T5YIE",
    "T10YIE",
    "BAA10Y",
    "AAA10Y",
    "DBAA",
    "DAAA",
}
MARKET_SERIES = {
    "DTWEXBGS",
    "DTWEXAFEGS",
    "DTWEXEMEGS",
    "DEXCHUS",
    "SP500",
    "NASDAQCOM",
    "DCOILWTICO",
    "DCOILBRENTEU",
    "PCOPPUSDM",
}
OIL_SERIES = {"DCOILWTICO", "DCOILBRENTEU"}
VIX_SERIES = "VIXCLS"
CHANGE_LAGS = [1, 5, 21]
VOL_WINDOWS = [21, 63]


def build_fred_rate_feature_daily(
    asof_daily: pl.DataFrame,
    *,
    start: date | None = None,
    end: date | None = None,
) -> pl.DataFrame:
    rows = _normalized_rows(asof_daily)
    by_series = _rows_by_series(rows, RATE_SERIES)
    output: list[dict[str, Any]] = []
    for series_rows in by_series.values():
        output.extend(_rate_series_features(series_rows, start=start, end=end))
    output.extend(_curve_features(rows, start=start, end=end))
    return _validated_frame(output, "rate_feature_daily")


def build_fred_market_feature_daily(
    asof_daily: pl.DataFrame,
    *,
    start: date | None = None,
    end: date | None = None,
) -> pl.DataFrame:
    rows = _normalized_rows(asof_daily)
    by_series = _rows_by_series(rows, MARKET_SERIES | {VIX_SERIES})
    output: list[dict[str, Any]] = []
    for series_id, series_rows in by_series.items():
        if series_id == VIX_SERIES:
            output.extend(_vix_features(series_rows, start=start, end=end))
        elif series_id in OIL_SERIES:
            output.extend(_oil_series_features(series_rows, start=start, end=end))
        else:
            output.extend(_market_series_features(series_rows, start=start, end=end))
    return _validated_frame(output, "market_feature_daily")


def _rate_series_features(
    series_rows: list[dict[str, Any]],
    *,
    start: date | None,
    end: date | None,
) -> list[dict[str, Any]]:
    output = []
    for position, row in enumerate(series_rows):
        if not in_output_window(row["ref_date"], start, end):
            continue
        feature_id = f"fred_rate:{row['series_id'].lower()}"
        output.append(_row(row, "fred_rate_feature", feature_id, "level_bp", _bp(row), "bp"))
        for lag in CHANGE_LAGS:
            lagged = _lag(series_rows, position, lag)
            output.append(
                _row(
                    row,
                    "fred_rate_feature",
                    feature_id,
                    f"change_{lag}bd_bp",
                    _bp_diff(row, lagged),
                    "bp",
                    extra_rows=[lagged],
                )
            )
    return output


def _curve_features(
    rows: list[dict[str, Any]],
    *,
    start: date | None,
    end: date | None,
) -> list[dict[str, Any]]:
    by_date: dict[date, dict[str, dict[str, Any]]] = defaultdict(dict)
    for row in rows:
        if row["series_id"] in RATE_SERIES:
            by_date[row["ref_date"]][row["series_id"]] = row

    output = []
    fed_mid_history: list[tuple[dict[str, Any], float | None]] = []
    for ref_date in sorted(by_date):
        same_day = by_date[ref_date]
        fed_mid_base = _combined_base([same_day.get("DFEDTARU"), same_day.get("DFEDTARL")])
        fed_mid = _average_bp(same_day.get("DFEDTARU"), same_day.get("DFEDTARL"))
        if fed_mid_base is not None:
            fed_mid_history.append((fed_mid_base, fed_mid))
        if not in_output_window(ref_date, start, end):
            continue
        feature_id = "fred_rate:curve"
        output.extend(
            [
                _curve_row(same_day, feature_id, "ust_slope_2y_10y_bp", "DGS10", "DGS2"),
                _curve_row(same_day, feature_id, "ust_slope_2y_5y_bp", "DGS5", "DGS2"),
                _curve_row(same_day, feature_id, "ust_slope_5y_30y_bp", "DGS30", "DGS5"),
                _single_bp_row(same_day, feature_id, "real_rate_5y_bp", "DFII5"),
                _single_bp_row(same_day, feature_id, "real_rate_10y_bp", "DFII10"),
                _single_bp_row(same_day, feature_id, "breakeven_5y_bp", "T5YIE"),
                _single_bp_row(same_day, feature_id, "breakeven_10y_bp", "T10YIE"),
                _single_bp_row(same_day, feature_id, "credit_spread_baa10y_bp", "BAA10Y"),
                _single_bp_row(same_day, feature_id, "credit_spread_aaa10y_bp", "AAA10Y"),
                _curve_row(same_day, feature_id, "credit_spread_baa_aaa_bp", "DBAA", "DAAA"),
            ]
        )
        if fed_mid_base is not None:
            output.append(
                _row(
                    fed_mid_base,
                    "fred_rate_feature",
                    feature_id,
                    "fed_target_mid_bp",
                    fed_mid,
                    "bp",
                )
            )
            position = len(fed_mid_history) - 1
            for lag in CHANGE_LAGS:
                lagged = fed_mid_history[position - lag] if position >= lag else None
                output.append(
                    _row(
                        fed_mid_base,
                        "fred_rate_feature",
                        feature_id,
                        f"fed_target_mid_change_{lag}bd_bp",
                        None if lagged is None or fed_mid is None else fed_mid - lagged[1],
                        "bp",
                        extra_rows=[lagged[0] if lagged is not None else None],
                    )
                )
    return output


def _market_series_features(
    series_rows: list[dict[str, Any]],
    *,
    start: date | None,
    end: date | None,
) -> list[dict[str, Any]]:
    output = []
    one_day_returns: list[float | None] = []
    for position, row in enumerate(series_rows):
        previous = _lag(series_rows, position, 1)
        one_day_returns.append(
            safe_log_return(row.get("value"), previous.get("value") if previous else None)
        )
        if not in_output_window(row["ref_date"], start, end):
            continue
        feature_id = f"fred_market:{row['series_id'].lower()}"
        output.append(
            _row(
                row,
                "fred_market_feature",
                feature_id,
                "log_level",
                safe_log(row.get("value")),
                "log_level",
            )
        )
        for lag in CHANGE_LAGS:
            lagged = _lag(series_rows, position, lag)
            output.append(
                _row(
                    row,
                    "fred_market_feature",
                    feature_id,
                    f"log_return_{lag}bd",
                    safe_log_return(row.get("value"), lagged.get("value") if lagged else None),
                    "log_return",
                    extra_rows=[lagged],
                )
            )
        for window in VOL_WINDOWS:
            output.append(
                _row(
                    row,
                    "fred_market_feature",
                    feature_id,
                    f"realized_vol_{window}bd_ann",
                    realized_vol_ann(one_day_returns, window),
                    "annualized_log_vol",
                )
            )
    return output


def _oil_series_features(
    series_rows: list[dict[str, Any]],
    *,
    start: date | None,
    end: date | None,
) -> list[dict[str, Any]]:
    output = []
    one_day_changes: list[float | None] = []
    for position, row in enumerate(series_rows):
        previous = _lag(series_rows, position, 1)
        one_day_changes.append(
            _signed_log_change(row.get("value"), previous.get("value") if previous else None)
        )
        if not in_output_window(row["ref_date"], start, end):
            continue
        feature_id = f"fred_market:{row['series_id'].lower()}"
        output.append(
            _row(
                row,
                "fred_market_feature",
                feature_id,
                "signed_log_level",
                _signed_log_level(row.get("value")),
                "signed_log_level",
            )
        )
        for lag in CHANGE_LAGS:
            lagged = _lag(series_rows, position, lag)
            output.append(
                _row(
                    row,
                    "fred_market_feature",
                    feature_id,
                    f"signed_log_change_{lag}bd",
                    _signed_log_change(
                        row.get("value"),
                        lagged.get("value") if lagged else None,
                    ),
                    "signed_log_change",
                    extra_rows=[lagged],
                )
            )
        for window in VOL_WINDOWS:
            output.append(
                _row(
                    row,
                    "fred_market_feature",
                    feature_id,
                    f"realized_vol_{window}bd_ann",
                    realized_vol_ann(one_day_changes, window),
                    "annualized_signed_log_vol",
                )
            )
    return output


def _vix_features(
    series_rows: list[dict[str, Any]],
    *,
    start: date | None,
    end: date | None,
) -> list[dict[str, Any]]:
    output = []
    for position, row in enumerate(series_rows):
        if not in_output_window(row["ref_date"], start, end):
            continue
        feature_id = "fred_market:vixcls"
        value = optional_float(row.get("value"))
        output.append(
            _row(
                row,
                "fred_market_feature",
                feature_id,
                "log1p_level",
                None if value is None or value < 0 else log1p(value),
                "log1p_index",
            )
        )
        for lag in CHANGE_LAGS:
            lagged = _lag(series_rows, position, lag)
            output.append(
                _row(
                    row,
                    "fred_market_feature",
                    feature_id,
                    f"change_{lag}bd",
                    diff(row.get("value"), lagged.get("value") if lagged else None),
                    "index_points",
                    extra_rows=[lagged],
                )
            )
    return output


def _normalized_rows(frame: pl.DataFrame) -> list[dict[str, Any]]:
    rows = []
    for row in frame.to_dicts():
        has_value = row.get("has_value", row.get("value") is not None)
        if not row.get("is_available", True) or not has_value:
            continue
        rows.append(
            {
                **row,
                "ref_date": as_date(row["ref_date"]),
                "available_date": as_date(row.get("available_date") or row["ref_date"]),
                "series_id": str(row.get("series_id") or "").upper(),
                "source_version": row.get("source_version") or "v0",
            }
        )
    return sorted(rows, key=lambda item: (item["series_id"], item["ref_date"]))


def _rows_by_series(
    rows: list[dict[str, Any]],
    series_ids: set[str],
) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        if row["series_id"] in series_ids:
            grouped[row["series_id"]].append(row)
    return grouped


def _row(
    base: dict[str, Any],
    source_family: str,
    feature_id: str,
    value_name: str,
    value: float | None,
    unit: str,
    *,
    extra_rows: list[dict[str, Any] | None] | None = None,
) -> dict[str, Any]:
    extra_rows = extra_rows or []
    rows = [base, *[row for row in extra_rows if row is not None]]
    return {
        "ref_date": base["ref_date"],
        "available_date": base["ref_date"],
        "source_family": source_family,
        "feature_id": feature_id,
        "value_name": value_name,
        "value": value,
        "unit": unit,
        "observation_ref_date": max_date(*(row.get("observation_ref_date") for row in rows)),
        "vintage_date": max_date(*(row.get("vintage_date") for row in rows)),
        "vintage_id": join_versions(*(row.get("vintage_id") for row in rows)),
        "observation_available_date": max_date(
            *(row.get("observation_available_date") for row in rows)
        ),
        "availability_basis": _join_text(*(row.get("availability_basis") for row in rows)),
        "vintage_policy": _join_text(*(row.get("vintage_policy") for row in rows)),
        "vintage_request_mode": _join_text(*(row.get("vintage_request_mode") for row in rows)),
        "revision_policy": _join_text(*(row.get("revision_policy") for row in rows)),
        "first_seen_timestamp_utc": _join_text(
            *(row.get("first_seen_timestamp_utc") for row in rows)
        ),
        "is_available": True,
        "has_value": value is not None,
        "staleness_days": _max_int(*(row.get("staleness_days") for row in rows)),
        "value_status": "ok" if value is not None else "missing_input",
        "source_version": join_versions(*(row.get("source_version") for row in rows)),
    }


def _curve_row(
    same_day: dict[str, dict[str, Any]],
    feature_id: str,
    value_name: str,
    right: str,
    left: str,
) -> dict[str, Any]:
    right_row = same_day.get(right)
    left_row = same_day.get(left)
    base = _combined_base([right_row, left_row])
    value = None if right_row is None or left_row is None else _bp_diff(right_row, left_row)
    return _row(
        base or _placeholder_base(same_day),
        "fred_rate_feature",
        feature_id,
        value_name,
        value,
        "bp",
    )


def _single_bp_row(
    same_day: dict[str, dict[str, Any]],
    feature_id: str,
    value_name: str,
    series_id: str,
) -> dict[str, Any]:
    row = same_day.get(series_id)
    return _row(
        row or _placeholder_base(same_day),
        "fred_rate_feature",
        feature_id,
        value_name,
        _bp(row),
        "bp",
    )


def _combined_base(rows: list[dict[str, Any] | None]) -> dict[str, Any] | None:
    present = [row for row in rows if row is not None]
    if not present:
        return None
    base = dict(present[0])
    base["observation_ref_date"] = max_date(*(row.get("observation_ref_date") for row in present))
    base["observation_available_date"] = max_date(
        *(row.get("observation_available_date") for row in present)
    )
    base["vintage_date"] = max_date(*(row.get("vintage_date") for row in present))
    base["vintage_id"] = join_versions(*(row.get("vintage_id") for row in present))
    base["source_version"] = join_versions(*(row.get("source_version") for row in present))
    base["staleness_days"] = _max_int(*(row.get("staleness_days") for row in present))
    return base


def _placeholder_base(same_day: dict[str, dict[str, Any]]) -> dict[str, Any]:
    if same_day:
        return next(iter(same_day.values()))
    raise ValueError("same_day must contain at least one row")


def _bp(row: dict[str, Any] | None) -> float | None:
    if row is None:
        return None
    value = optional_float(row.get("value"))
    if value is None:
        return None
    return value * 100.0


def _bp_diff(row: dict[str, Any], lagged: dict[str, Any] | None) -> float | None:
    value = _bp(row)
    previous = _bp(lagged)
    if value is None or previous is None:
        return None
    return value - previous


def _signed_log_level(value: Any) -> float | None:
    number = optional_float(value)
    if number is None:
        return None
    sign = -1.0 if number < 0 else 1.0
    return sign * log1p(abs(number))


def _signed_log_change(current: Any, previous: Any) -> float | None:
    current_level = _signed_log_level(current)
    previous_level = _signed_log_level(previous)
    if current_level is None or previous_level is None:
        return None
    return current_level - previous_level


def _average_bp(left: dict[str, Any] | None, right: dict[str, Any] | None) -> float | None:
    left_value = _bp(left)
    right_value = _bp(right)
    if left_value is None or right_value is None:
        return None
    return (left_value + right_value) / 2.0


def _lag(rows: list[dict[str, Any]], position: int, lag: int) -> dict[str, Any] | None:
    lag_position = position - lag
    if lag_position < 0:
        return None
    return rows[lag_position]


def _join_text(*values: Any) -> str | None:
    unique = sorted({str(value) for value in values if value is not None and str(value).strip()})
    return "|".join(unique) if unique else None


def _max_int(*values: Any) -> int | None:
    ints = [int(value) for value in values if value is not None]
    if not ints:
        return None
    return max(ints)


def _validated_frame(rows: list[dict[str, Any]], panel: str) -> pl.DataFrame:
    if not rows:
        return pl.DataFrame(schema={column: pl.Null for column in FRED_FEATURE_DAILY_COLUMNS})
    frame = (
        pl.DataFrame(rows)
        .select(FRED_FEATURE_DAILY_COLUMNS)
        .unique(subset=PANEL_PRIMARY_KEYS[panel], keep="last")
        .sort(["ref_date", "source_family", "feature_id", "value_name"])
    )
    validate_asof_panel(
        frame,
        required_columns=FRED_FEATURE_DAILY_COLUMNS,
        primary_keys=PANEL_PRIMARY_KEYS[panel],
    )
    return frame
