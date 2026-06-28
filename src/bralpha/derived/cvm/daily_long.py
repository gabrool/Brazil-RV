from __future__ import annotations

import polars as pl

from bralpha.derived.cvm.quality import validate_asof_panel
from bralpha.derived.cvm.schemas import CVM_DAILY_LONG_COLUMNS, PANEL_PRIMARY_KEYS

_FLOW_VALUE_COLUMNS = [
    "subscriptions",
    "redemptions",
    "subscriptions_count",
    "redemptions_count",
    "fund_count",
]

_STATE_VALUE_COLUMNS = [
    "portfolio_value",
    "nav",
    "shareholder_count",
    "portfolio_value_count",
    "nav_count",
    "shareholder_count_count",
    "fund_count",
]


def build_cvm_daily_long(
    *,
    fund_flows_daily: pl.DataFrame | None = None,
    fund_state_asof_daily: pl.DataFrame | None = None,
    include_fund_flows: bool,
    include_fund_state: bool,
) -> pl.DataFrame:
    parts: list[pl.DataFrame] = []
    if include_fund_flows and fund_flows_daily is not None and not fund_flows_daily.is_empty():
        parts.extend(_long_rows(fund_flows_daily, "cvm_fund_flows", _FLOW_VALUE_COLUMNS))
    if (
        include_fund_state
        and fund_state_asof_daily is not None
        and not fund_state_asof_daily.is_empty()
    ):
        parts.extend(_long_rows(fund_state_asof_daily, "cvm_fund_state", _STATE_VALUE_COLUMNS))

    if not parts:
        return _empty()

    frame = (
        pl.concat(parts, how="diagonal_relaxed")
        .filter(pl.col("value").is_not_null())
        .select(CVM_DAILY_LONG_COLUMNS)
        .sort(["ref_date", "source_family", "feature_id", "value_name", "observation_ref_date"])
        .unique(subset=PANEL_PRIMARY_KEYS["daily_long"], keep="last", maintain_order=True)
    )
    validate_asof_panel(
        frame,
        required_columns=CVM_DAILY_LONG_COLUMNS,
        primary_keys=PANEL_PRIMARY_KEYS["daily_long"],
    )
    return frame


def _long_rows(
    frame: pl.DataFrame,
    source_family: str,
    value_columns: list[str],
) -> list[pl.DataFrame]:
    rows: list[pl.DataFrame] = []
    frame = _ensure_daily_long_columns(frame)
    for column in value_columns:
        rows.append(
            frame.with_columns(
                source_family=pl.lit(source_family),
                value_name=pl.lit(column),
                value=pl.col(column).cast(pl.Float64),
                unit=pl.lit(_unit(column)),
            ).select(CVM_DAILY_LONG_COLUMNS)
        )
    return rows


def _ensure_daily_long_columns(frame: pl.DataFrame) -> pl.DataFrame:
    work = frame
    if "is_available" not in work.columns:
        work = work.with_columns(is_available=pl.lit(True))
    if "staleness_days" not in work.columns:
        work = work.with_columns(staleness_days=pl.lit(0, dtype=pl.Int64))
    return work


def _unit(column: str) -> str:
    if column in {"portfolio_value", "nav", "subscriptions", "redemptions"}:
        return "BRL"
    if column == "shareholder_count":
        return "shareholders"
    if column == "fund_count":
        return "funds"
    if column.endswith("_count"):
        return "observations"
    return "value"


def _empty() -> pl.DataFrame:
    return pl.DataFrame(schema={column: pl.Null for column in CVM_DAILY_LONG_COLUMNS})
