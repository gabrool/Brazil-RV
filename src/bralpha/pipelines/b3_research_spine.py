from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path

import polars as pl

from bralpha.derived.b3.continuous_futures import build_continuous_futures_daily
from bralpha.derived.b3.di_curve import build_di_curve_contract_daily, build_di_curve_grid_daily
from bralpha.derived.b3.di_features import build_di_curve_feature_daily
from bralpha.derived.b3.futures_contract_panel import build_futures_contract_daily
from bralpha.derived.b3.futures_features import build_futures_feature_daily
from bralpha.derived.b3.index_features import (
    build_index_composition_feature_daily,
    build_index_feature_daily,
)
from bralpha.derived.b3.io import (
    ResearchInputMissingError,
    read_gold_panel,
    read_silver_dataset,
    write_gold_panel,
)
from bralpha.derived.b3.listed_market import (
    build_index_composition_daily,
    build_index_daily,
    build_listed_market_daily,
)
from bralpha.derived.b3.schemas import PANEL_PRIMARY_KEYS
from bralpha.derived.b3.targets import build_targets_daily
from bralpha.derived.feature_utils import feature_warmup_start
from bralpha.infra.config import load_b3_research_config, load_paths_config, resolve_project_paths
from bralpha.modeling.config import load_model_dataset_config

PANEL_ORDER = [
    "futures_contract_daily",
    "continuous_futures_daily",
    "futures_feature_daily",
    "di_curve_contract_daily",
    "di_curve_grid_daily",
    "di_feature_daily",
    "listed_market_daily",
    "index_daily",
    "index_feature_daily",
    "index_composition_daily",
    "index_composition_feature_daily",
    "targets_daily",
]


def run_b3_research_spine(
    *,
    repo_root: Path,
    start: date,
    end: date,
    panels: list[str] | None = None,
) -> dict[str, str]:
    requested = PANEL_ORDER if panels is None else panels
    unknown = sorted(set(requested) - set(PANEL_ORDER))
    if unknown:
        raise ValueError(f"Unknown B3 research panel(s): {unknown}")

    explicit = panels is not None
    paths = resolve_project_paths(repo_root, load_paths_config(repo_root))
    config = load_b3_research_config(repo_root).b3_research
    model_config = load_model_dataset_config(repo_root)
    warmup_start = feature_warmup_start(start, model_config.feature_warmup_business_days)
    built: dict[str, pl.DataFrame] = {}
    status: dict[str, str] = {}

    for panel in PANEL_ORDER:
        if panel not in requested:
            continue
        try:
            frame = _build_panel(
                panel,
                paths,
                config,
                built,
                start,
                end,
                warmup_start=warmup_start,
                required=explicit,
            )
        except ResearchInputMissingError as exc:
            if explicit:
                raise
            status[panel] = f"skipped: {exc}"
            print(f"{panel}: {status[panel]}")
            continue
        if frame is None:
            status[panel] = "skipped: inputs absent"
            print(f"{panel}: {status[panel]}")
            continue
        write_gold_panel(
            frame,
            paths,
            panel=panel,
            primary_keys=PANEL_PRIMARY_KEYS[panel],
            ref_date_col="ref_date",
        )
        built[panel] = frame
        status[panel] = f"written: {frame.height} rows"
        if (
            panel == "di_curve_grid_daily"
            and frame.height
            and not frame["has_curve_value"].fill_null(False).any()
        ):
            status[panel] += "; no curve values available because maturity data was missing"
        print(f"{panel}: {status[panel]}")
    return status


def _build_panel(
    panel: str,
    paths,
    config,
    built: dict[str, pl.DataFrame],
    start: date,
    end: date,
    *,
    warmup_start: date,
    required: bool,
) -> pl.DataFrame | None:
    if panel == "futures_contract_daily":
        settlements = _silver(paths, "b3_futures_settlements", start, end, required=required)
        if settlements is None:
            return None
        return build_futures_contract_daily(
            settlements=settlements,
            open_interest=_silver(paths, "b3_derivatives_open_interest", start, end),
            trade_summary=_silver(paths, "b3_derivatives_trade_summary", start, end),
            contract_master=read_silver_dataset(paths, "b3_futures_contract_master"),
            holiday_calendar=_holiday_calendar(paths),
            start=start,
            end=end,
        )
    if panel == "continuous_futures_daily":
        contracts = _dependency(
            paths,
            built,
            "futures_contract_daily",
            start,
            end,
            required=required,
        )
        if contracts is None:
            return None
        continuous = config.continuous_futures
        roots = [*config.roots.primary, *config.roots.secondary]
        return build_continuous_futures_daily(
            contracts,
            roots=roots,
            max_front_rank=continuous.max_front_rank,
            min_days_to_maturity=continuous.min_days_to_maturity,
            prefer_liquidity_when_available=continuous.prefer_liquidity_when_available,
            roll_policy=continuous.roll_policy,
            start=start,
            end=end,
        )
    if panel == "futures_feature_daily":
        continuous = _feature_dependency(
            paths,
            built,
            "continuous_futures_daily",
            warmup_start,
            end,
            required=required,
        )
        if continuous is None:
            return None
        return build_futures_feature_daily(continuous, start=start, end=end)
    if panel == "di_curve_contract_daily":
        contracts = _dependency(
            paths,
            built,
            "futures_contract_daily",
            start,
            end,
            required=required,
        )
        if contracts is None:
            return None
        return build_di_curve_contract_daily(
            contracts,
            source_roots=config.di_curve.source_roots,
            start=start,
            end=end,
        )
    if panel == "di_curve_grid_daily":
        curve_contracts = _dependency(
            paths,
            built,
            "di_curve_contract_daily",
            start,
            end,
            required=required,
        )
        if curve_contracts is None:
            return None
        return build_di_curve_grid_daily(
            curve_contracts,
            tenor_business_days=config.di_curve.configured_tenor_business_days,
            interpolation_method=config.di_curve.interpolation,
            start=start,
            end=end,
        )
    if panel == "di_feature_daily":
        grid = _feature_dependency(
            paths,
            built,
            "di_curve_grid_daily",
            warmup_start,
            end,
            required=required,
        )
        if grid is None:
            return None
        return build_di_curve_feature_daily(grid, start=start, end=end)
    if panel == "listed_market_daily":
        yearly = _silver(paths, "b3_cotahist_yearly", start, end)
        daily = _silver(paths, "b3_cotahist_daily", start, end)
        if yearly is None and daily is None:
            if required:
                raise ResearchInputMissingError("Missing COTAHIST input for listed_market_daily")
            return None
        return build_listed_market_daily(
            cotahist_yearly=yearly,
            cotahist_daily=daily,
            traded_securities=read_silver_dataset(paths, "b3_traded_securities"),
            isin_database=_silver(paths, "b3_isin_database", start, end),
            start=start,
            end=end,
        )
    if panel == "index_daily":
        indexes = _silver(paths, "b3_indexes_historical_data", start, end, required=required)
        if indexes is None:
            return None
        return build_index_daily(indexes, start=start, end=end)
    if panel == "index_feature_daily":
        indexes = _feature_dependency(
            paths,
            built,
            "index_daily",
            warmup_start,
            end,
            required=required,
        )
        if indexes is None:
            return None
        return build_index_feature_daily(indexes, start=start, end=end)
    if panel == "index_composition_daily":
        composition = _silver(paths, "b3_indexes_composition", start, end)
        current = _silver(paths, "b3_indexes_current_portfolio", start, end)
        theoretical = _silver(paths, "b3_indexes_theoretical_portfolio", start, end)
        if composition is None and current is None and theoretical is None:
            if required:
                raise ResearchInputMissingError("Missing index composition inputs")
            return None
        return build_index_composition_daily(
            indexes_composition=composition,
            indexes_current_portfolio=current,
            indexes_theoretical_portfolio=theoretical,
            start=start,
            end=end,
        )
    if panel == "index_composition_feature_daily":
        composition = _feature_dependency(
            paths,
            built,
            "index_composition_daily",
            warmup_start,
            end,
            required=required,
        )
        if composition is None:
            return None
        return build_index_composition_feature_daily(composition, start=start, end=end)
    if panel == "targets_daily":
        continuous = _dependency(paths, built, "continuous_futures_daily", start, end)
        grid = _dependency(paths, built, "di_curve_grid_daily", start, end)
        indexes = _dependency(paths, built, "index_daily", start, end)
        if continuous is None and grid is None and indexes is None:
            if required:
                raise ResearchInputMissingError("Missing target source panels")
            return None
        return build_targets_daily(
            continuous_futures_daily=continuous,
            di_curve_grid_daily=grid,
            index_daily=indexes,
            horizons=config.targets.horizons,
            target_types=config.targets.target_types,
            start=start,
            end=end,
        )
    raise ValueError(f"Unknown panel: {panel}")


def _silver(paths, dataset_id: str, start: date, end: date, *, required: bool = False):
    return read_silver_dataset(paths, dataset_id, required=required, start=start, end=end)


def _holiday_calendar(paths) -> pl.DataFrame | None:
    calendar = read_silver_dataset(paths, "b3_holiday_calendar")
    if calendar is not None:
        return calendar
    return read_silver_dataset(paths, "reference_calendar")


def _dependency(
    paths,
    built: dict[str, pl.DataFrame],
    panel: str,
    start: date,
    end: date,
    *,
    required: bool = False,
) -> pl.DataFrame | None:
    if panel in built:
        return built[panel]
    return read_gold_panel(paths, panel, required=required, start=start, end=end)


def _feature_dependency(
    paths,
    built: dict[str, pl.DataFrame],
    panel: str,
    start: date,
    end: date,
    *,
    required: bool = False,
) -> pl.DataFrame | None:
    frames = []
    history = read_gold_panel(paths, panel, start=start, end=end)
    if history is not None:
        frames.append(history)
    if panel in built:
        frames.append(built[panel])
    if not frames:
        if required:
            raise ResearchInputMissingError(f"Missing feature dependency panel: {panel}")
        return None
    if len(frames) == 1:
        return frames[0]
    return (
        pl.concat(frames, how="diagonal_relaxed")
        .unique(subset=PANEL_PRIMARY_KEYS[panel], keep="last", maintain_order=True)
        .sort("ref_date")
    )


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--panel", choices=PANEL_ORDER, action="append", dest="panels")
    args = parser.parse_args(argv)
    run_b3_research_spine(
        repo_root=Path(args.repo_root).resolve(),
        start=date.fromisoformat(args.start),
        end=date.fromisoformat(args.end),
        panels=args.panels,
    )


if __name__ == "__main__":
    main()
