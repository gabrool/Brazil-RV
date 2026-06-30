from __future__ import annotations

from collections import defaultdict
from datetime import date
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
    safe_pct_change,
    safe_ratio,
)
from bralpha.derived.tesouro.quality import validate_asof_panel
from bralpha.derived.tesouro.schemas import PANEL_PRIMARY_KEYS, TESOURO_FEATURE_DAILY_COLUMNS

SOURCE_FAMILY = "tesouro_feature"
CHANGE_LAGS = [1, 5, 21]


def build_tesouro_feature_daily(
    *,
    direto_prices_rates_asof_daily: pl.DataFrame | None = None,
    direto_flows_daily: pl.DataFrame | None = None,
    direto_stock_asof_daily: pl.DataFrame | None = None,
    dpf_stock_asof_daily: pl.DataFrame | None = None,
    start: date | None = None,
    end: date | None = None,
) -> pl.DataFrame:
    rows: list[dict[str, Any]] = []
    if direto_prices_rates_asof_daily is not None and not direto_prices_rates_asof_daily.is_empty():
        rows.extend(_prices_features(direto_prices_rates_asof_daily, start=start, end=end))
    if direto_flows_daily is not None and not direto_flows_daily.is_empty():
        rows.extend(
            _flow_features(
                direto_flows_daily,
                direto_stock_asof_daily,
                start=start,
                end=end,
            )
        )
    if direto_stock_asof_daily is not None and not direto_stock_asof_daily.is_empty():
        rows.extend(_direto_stock_features(direto_stock_asof_daily, start=start, end=end))
    if dpf_stock_asof_daily is not None and not dpf_stock_asof_daily.is_empty():
        rows.extend(_dpf_stock_features(dpf_stock_asof_daily, start=start, end=end))
    if not rows:
        return _empty()
    frame = (
        pl.DataFrame(rows)
        .select(TESOURO_FEATURE_DAILY_COLUMNS)
        .unique(subset=PANEL_PRIMARY_KEYS["feature_daily"], keep="last")
        .sort(["ref_date", "feature_id", "value_name"])
    )
    validate_asof_panel(
        frame,
        required_columns=TESOURO_FEATURE_DAILY_COLUMNS,
        primary_keys=PANEL_PRIMARY_KEYS["feature_daily"],
    )
    return frame


def _prices_features(
    frame: pl.DataFrame,
    *,
    start: date | None,
    end: date | None,
) -> list[dict[str, Any]]:
    grouped = _group_by_feature(frame)
    rows: list[dict[str, Any]] = []
    for series_rows in grouped.values():
        series_rows.sort(key=lambda item: item["ref_date"])
        for position, row in enumerate(series_rows):
            if not in_output_window(row["ref_date"], start, end):
                continue
            feature_id = f"tesouro_price:{row['feature_id']}"
            mid_rate = _mid(row.get("buy_rate"), row.get("sell_rate"))
            mid_price = _mid(row.get("buy_price"), row.get("sell_price"))
            rows.extend(
                [
                    _row(
                        row,
                        feature_id,
                        "mid_rate_bp",
                        None if mid_rate is None else mid_rate * 100.0,
                        "bp",
                    ),
                    _row(
                        row,
                        feature_id,
                        "rate_spread_bp",
                        _rate_spread_bp(row.get("buy_rate"), row.get("sell_rate")),
                        "bp",
                    ),
                    _row(
                        row,
                        feature_id,
                        "price_bid_ask_spread_pct",
                        _price_spread_pct(row.get("buy_price"), row.get("sell_price")),
                        "percent",
                    ),
                    _row(row, feature_id, "mid_price", mid_price, "brl"),
                    _row(row, feature_id, "log_mid_price", safe_log(mid_price), "log_brl"),
                ]
            )
            for lag in CHANGE_LAGS:
                lagged = _lag(series_rows, position, lag)
                mid_rate_change = (
                    None
                    if lagged is None
                    else diff(mid_rate, _mid(lagged.get("buy_rate"), lagged.get("sell_rate")))
                )
                rows.append(
                    _row(
                        row,
                        feature_id,
                        f"mid_rate_change_{lag}bd_bp",
                        None if mid_rate_change is None else mid_rate_change * 100.0,
                        "bp",
                        extra_rows=[lagged],
                    )
                )
                rows.append(
                    _row(
                        row,
                        feature_id,
                        f"price_log_return_{lag}bd",
                        safe_log_return(
                            mid_price,
                            _mid(lagged.get("buy_price"), lagged.get("sell_price"))
                            if lagged is not None
                            else None,
                        ),
                        "log_return",
                        extra_rows=[lagged],
                    )
                )
    return rows


def _flow_features(
    flows: pl.DataFrame,
    stock: pl.DataFrame | None,
    *,
    start: date | None,
    end: date | None,
) -> list[dict[str, Any]]:
    stock_by_key = {}
    if stock is not None and not stock.is_empty():
        for row in _normalize_rows(stock):
            stock_by_key[_security_key(row)] = row

    grouped: dict[tuple[date, str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in _normalize_rows(flows):
        grouped[
            (row["ref_date"], str(row.get("security_name")), str(row.get("maturity_date")))
        ].append(row)

    rows: list[dict[str, Any]] = []
    for key, flow_rows in grouped.items():
        ref_date = key[0]
        if not in_output_window(ref_date, start, end):
            continue
        sales = [row for row in flow_rows if str(row.get("flow_type")).lower() == "sale"]
        redemptions = [
            row for row in flow_rows if str(row.get("flow_type")).lower() != "sale"
        ]
        sale_value = sum(optional_float(row.get("value")) or 0.0 for row in sales)
        redemption_value = sum(optional_float(row.get("value")) or 0.0 for row in redemptions)
        sale_quantity = sum(optional_float(row.get("quantity")) or 0.0 for row in sales)
        redemption_quantity = sum(optional_float(row.get("quantity")) or 0.0 for row in redemptions)
        investor_count = sum(optional_float(row.get("investor_count")) or 0.0 for row in flow_rows)
        gross_value = sale_value + redemption_value
        net_value = sale_value - redemption_value
        flow_to_stock = None
        base = _combined_base(flow_rows)
        feature_id = f"tesouro_flow:{base['feature_id']}"
        stock_row = stock_by_key.get(_security_key(base))
        if stock_row is not None:
            flow_to_stock_ratio = safe_ratio(net_value, stock_row.get("stock_value"))
            flow_to_stock = None if flow_to_stock_ratio is None else 100.0 * flow_to_stock_ratio
        rows.extend(
            [
                _row(base, feature_id, "sales_value", sale_value, "brl"),
                _row(base, feature_id, "redemptions_value", redemption_value, "brl"),
                _row(base, feature_id, "net_flow_value", net_value, "brl"),
                _row(base, feature_id, "gross_flow_value", gross_value, "brl"),
                _row(base, feature_id, "sales_quantity", sale_quantity, "count"),
                _row(base, feature_id, "redemptions_quantity", redemption_quantity, "count"),
                _row(
                    base,
                    feature_id,
                    "net_quantity",
                    sale_quantity - redemption_quantity,
                    "count",
                ),
                _row(base, feature_id, "investor_count", investor_count, "count"),
                _row(
                    base,
                    feature_id,
                    "redemption_share_pct",
                    _scale(safe_ratio(redemption_value, gross_value), 100.0),
                    "percent",
                ),
                _row(
                    _combined_base([base, stock_row]) if stock_row is not None else base,
                    feature_id,
                    "net_flow_to_stock_value_pct",
                    flow_to_stock,
                    "percent",
                    extra_rows=[stock_row],
                ),
            ]
        )
    return rows


def _direto_stock_features(
    frame: pl.DataFrame,
    *,
    start: date | None,
    end: date | None,
) -> list[dict[str, Any]]:
    rows = []
    for series_rows in _group_by_feature(frame).values():
        series_rows.sort(key=lambda item: item["ref_date"])
        for position, row in enumerate(series_rows):
            if not in_output_window(row["ref_date"], start, end):
                continue
            lag_21 = _lag(series_rows, position, 21)
            feature_id = f"tesouro_stock:{row['feature_id']}"
            rows.extend(
                [
                    _row(
                        row,
                        feature_id,
                        "stock_value_log",
                        safe_log(row.get("stock_value")),
                        "log_brl",
                    ),
                    _row(
                        row,
                        feature_id,
                        "stock_value_change_21bd_pct",
                        safe_pct_change(
                            row.get("stock_value"),
                            lag_21.get("stock_value") if lag_21 is not None else None,
                        ),
                        "percent",
                        extra_rows=[lag_21],
                    ),
                    _row(
                        row,
                        feature_id,
                        "quantity_log1p",
                        safe_log1p(row.get("quantity")),
                        "log_count",
                    ),
                    _row(
                        row,
                        feature_id,
                        "investor_count_log1p",
                        safe_log1p(row.get("investor_count")),
                        "log_count",
                    ),
                ]
            )
    return rows


def _dpf_stock_features(
    frame: pl.DataFrame,
    *,
    start: date | None,
    end: date | None,
) -> list[dict[str, Any]]:
    rows = []
    for series_rows in _group_by_feature(frame).values():
        series_rows.sort(key=lambda item: item["ref_date"])
        for position, row in enumerate(series_rows):
            if not in_output_window(row["ref_date"], start, end):
                continue
            lag_21 = _lag(series_rows, position, 21)
            feature_id = f"tesouro_dpf_stock:{row['feature_id']}"
            rows.extend(
                [
                    _row(
                        row,
                        feature_id,
                        "stock_value_log",
                        safe_log(row.get("stock_value")),
                        "log_brl",
                    ),
                    _row(
                        row,
                        feature_id,
                        "stock_value_change_21bd_pct",
                        safe_pct_change(
                            row.get("stock_value"),
                            lag_21.get("stock_value") if lag_21 is not None else None,
                        ),
                        "percent",
                        extra_rows=[lag_21],
                    ),
                ]
            )
    return rows


def _row(
    base: dict[str, Any],
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
        "availability_policy": _join_text(*(row.get("availability_policy") for row in rows)),
        "availability_basis": _join_text(*(row.get("availability_basis") for row in rows)),
        "is_available": True,
        "staleness_days": _max_int(*(row.get("staleness_days") for row in rows)),
        "source_version": join_versions(*(row.get("source_version") for row in rows)),
    }


def _normalize_rows(frame: pl.DataFrame) -> list[dict[str, Any]]:
    rows = []
    for row in frame.to_dicts():
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


def _group_by_feature(frame: pl.DataFrame) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in _normalize_rows(frame):
        grouped[str(row.get("feature_id"))].append(row)
    return grouped


def _mid(left: Any, right: Any) -> float | None:
    left_value = optional_float(left)
    right_value = optional_float(right)
    if left_value is None and right_value is None:
        return None
    if left_value is None:
        return right_value
    if right_value is None:
        return left_value
    return (left_value + right_value) / 2.0


def _rate_spread_bp(buy_rate: Any, sell_rate: Any) -> float | None:
    value = diff(sell_rate, buy_rate)
    if value is None:
        return None
    return value * 100.0


def _price_spread_pct(buy_price: Any, sell_price: Any) -> float | None:
    mid_price = _mid(buy_price, sell_price)
    value = diff(sell_price, buy_price)
    return _scale(safe_ratio(value, mid_price), 100.0)


def _scale(value: float | None, multiplier: float) -> float | None:
    if value is None:
        return None
    return value * multiplier


def _lag(rows: list[dict[str, Any]], position: int, lag: int) -> dict[str, Any] | None:
    lag_position = position - lag
    if lag_position < 0:
        return None
    return rows[lag_position]


def _combined_base(rows: list[dict[str, Any] | None]) -> dict[str, Any]:
    present = [row for row in rows if row is not None]
    base = dict(present[0])
    base["available_date"] = max_date(*(row.get("available_date") for row in present))
    base["observation_ref_date"] = max_date(*(row.get("observation_ref_date") for row in present))
    base["observation_available_date"] = max_date(
        *(row.get("observation_available_date") for row in present)
    )
    base["source_version"] = join_versions(*(row.get("source_version") for row in present))
    base["staleness_days"] = _max_int(*(row.get("staleness_days") for row in present))
    return base


def _security_key(row: dict[str, Any]) -> tuple[str, str]:
    return (str(row.get("security_name")), str(row.get("maturity_date")))


def _join_text(*values: Any) -> str | None:
    unique = sorted({str(value) for value in values if value is not None and str(value).strip()})
    return "|".join(unique) if unique else None


def _max_int(*values: Any) -> int | None:
    ints = [int(value) for value in values if value is not None]
    if not ints:
        return None
    return max(ints)


def _empty() -> pl.DataFrame:
    return pl.DataFrame(schema={column: pl.Null for column in TESOURO_FEATURE_DAILY_COLUMNS})
