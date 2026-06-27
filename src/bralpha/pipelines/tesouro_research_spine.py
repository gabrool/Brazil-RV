from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path

import polars as pl

from bralpha.derived.tesouro.daily_long import build_daily_long
from bralpha.derived.tesouro.flows import build_direto_flows_daily
from bralpha.derived.tesouro.io import (
    TesouroResearchInputMissingError,
    read_gold_panel,
    read_silver_dataset,
    write_gold_panel,
)
from bralpha.derived.tesouro.prices_rates import (
    build_direto_prices_rates_asof_daily,
    build_direto_prices_rates_observation,
)
from bralpha.derived.tesouro.schemas import PANEL_PRIMARY_KEYS
from bralpha.derived.tesouro.stock import (
    build_direto_stock_asof_daily,
    build_direto_stock_observation,
    build_dpf_stock_asof_daily,
    build_dpf_stock_observation,
)
from bralpha.infra.config import (
    load_paths_config,
    load_tesouro_research_config,
    resolve_project_paths,
)

PANEL_ORDER = [
    "direto_prices_rates_observation",
    "direto_prices_rates_asof_daily",
    "direto_flows_daily",
    "direto_stock_observation",
    "direto_stock_asof_daily",
    "dpf_stock_observation",
    "dpf_stock_asof_daily",
    "daily_long",
]


def run_tesouro_research_spine(
    *,
    repo_root: Path,
    start: date,
    end: date,
    panels: list[str] | None = None,
) -> dict[str, str]:
    requested = PANEL_ORDER if panels is None else panels
    unknown = sorted(set(requested) - set(PANEL_ORDER))
    if unknown:
        raise ValueError(f"Unknown Tesouro research panel(s): {unknown}")

    explicit = panels is not None
    paths = resolve_project_paths(repo_root, load_paths_config(repo_root))
    config = load_tesouro_research_config(repo_root).tesouro_research
    built: dict[str, pl.DataFrame] = {}
    status: dict[str, str] = {}

    for panel in PANEL_ORDER:
        if panel not in requested:
            continue
        try:
            frame = _build_panel(panel, paths, config, built, start, end, required=explicit)
        except TesouroResearchInputMissingError as exc:
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
        )
        built[panel] = frame
        status[panel] = f"written: {frame.height} rows"
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
    required: bool,
) -> pl.DataFrame | None:
    if panel == "direto_prices_rates_observation":
        silver = read_silver_dataset(
            paths,
            "tesouro_direto_prices_rates",
            required=required,
            start=start,
            end=end,
        )
        if silver is None:
            return None
        return build_direto_prices_rates_observation(silver, start=start, end=end)
    if panel == "direto_prices_rates_asof_daily":
        observations = _prices_rates_history(paths, config, end, required=required)
        if observations is None:
            return None
        return build_direto_prices_rates_asof_daily(
            observations,
            start=start,
            end=end,
            max_dense_securities=config.prices_rates.max_dense_securities,
        )
    if panel == "direto_flows_daily":
        sales = read_silver_dataset(paths, "tesouro_direto_sales", start=None, end=end)
        redemptions = read_silver_dataset(
            paths,
            "tesouro_direto_redemptions",
            start=None,
            end=end,
        )
        if sales is None and redemptions is None:
            if required:
                raise TesouroResearchInputMissingError("Missing Tesouro Direto flow silver inputs")
            return None
        return build_direto_flows_daily(
            sales=sales,
            redemptions=redemptions,
            include_sales=config.flows.include_sales,
            include_redemptions=config.flows.include_redemptions,
            start=start,
            end=end,
        )
    if panel == "direto_stock_observation":
        silver = read_silver_dataset(
            paths,
            "tesouro_direto_stock",
            required=required,
            start=start,
            end=end,
        )
        if silver is None:
            return None
        return build_direto_stock_observation(silver, start=start, end=end)
    if panel == "direto_stock_asof_daily":
        observations = _direto_stock_history(paths, end, required=required)
        if observations is None:
            return None
        return build_direto_stock_asof_daily(
            observations,
            start=start,
            end=end,
            max_dense_keys=config.stock.max_dense_keys,
        )
    if panel == "dpf_stock_observation":
        silver = read_silver_dataset(
            paths,
            "tesouro_dpf_stock",
            required=required,
            start=start,
            end=end,
        )
        if silver is None:
            return None
        return build_dpf_stock_observation(silver, start=start, end=end)
    if panel == "dpf_stock_asof_daily":
        observations = _dpf_stock_history(paths, end, required=required)
        if observations is None:
            return None
        return build_dpf_stock_asof_daily(
            observations,
            start=start,
            end=end,
            max_dense_keys=config.stock.max_dense_keys,
        )
    if panel == "daily_long":
        prices = _dependency(paths, built, "direto_prices_rates_asof_daily", start, end)
        flows = _dependency(paths, built, "direto_flows_daily", start, end)
        direto_stock = _dependency(paths, built, "direto_stock_asof_daily", start, end)
        dpf_stock = _dependency(paths, built, "dpf_stock_asof_daily", start, end)
        if prices is None and flows is None and direto_stock is None and dpf_stock is None:
            if required:
                raise TesouroResearchInputMissingError("Missing daily_long source panels")
            return None
        return build_daily_long(
            direto_prices_rates_asof_daily=prices,
            direto_flows_daily=flows,
            direto_stock_asof_daily=direto_stock,
            dpf_stock_asof_daily=dpf_stock,
            include_prices_rates=config.daily_long.include_prices_rates,
            include_flows=config.daily_long.include_flows,
            include_stock=config.daily_long.include_stock,
        )
    raise ValueError(f"Unknown panel: {panel}")


def _prices_rates_history(paths, config, end: date, *, required: bool) -> pl.DataFrame | None:
    silver = read_silver_dataset(paths, "tesouro_direto_prices_rates", start=None, end=end)
    if silver is not None:
        return build_direto_prices_rates_observation(silver, start=None, end=end)
    gold = read_gold_panel(paths, "direto_prices_rates_observation", start=None, end=end)
    if gold is None and required:
        raise TesouroResearchInputMissingError(
            "Missing prices/rates history from tesouro_direto_prices_rates or gold observation"
        )
    return gold


def _direto_stock_history(paths, end: date, *, required: bool) -> pl.DataFrame | None:
    silver = read_silver_dataset(paths, "tesouro_direto_stock", start=None, end=end)
    if silver is not None:
        return build_direto_stock_observation(silver, start=None, end=end)
    gold = read_gold_panel(paths, "direto_stock_observation", start=None, end=end)
    if gold is None and required:
        raise TesouroResearchInputMissingError(
            "Missing Tesouro Direto stock history from silver or gold observation"
        )
    return gold


def _dpf_stock_history(paths, end: date, *, required: bool) -> pl.DataFrame | None:
    silver = read_silver_dataset(paths, "tesouro_dpf_stock", start=None, end=end)
    if silver is not None:
        return build_dpf_stock_observation(silver, start=None, end=end)
    gold = read_gold_panel(paths, "dpf_stock_observation", start=None, end=end)
    if gold is None and required:
        raise TesouroResearchInputMissingError(
            "Missing DPF stock history from silver or gold observation"
        )
    return gold


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


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--panel", choices=PANEL_ORDER, action="append", dest="panels")
    args = parser.parse_args(argv)
    run_tesouro_research_spine(
        repo_root=Path(args.repo_root).resolve(),
        start=date.fromisoformat(args.start),
        end=date.fromisoformat(args.end),
        panels=args.panels,
    )


if __name__ == "__main__":
    main()
