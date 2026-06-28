from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path

import polars as pl

from bralpha.derived.anp.daily_long import build_anp_daily_long, build_anp_state_asof_daily
from bralpha.derived.anp.fuel_prices import (
    build_fuel_price_group_observation,
    build_fuel_price_station_observation,
)
from bralpha.derived.anp.fuel_sales import (
    build_fuel_sales_group_observation,
    build_fuel_sales_observation,
)
from bralpha.derived.anp.io import (
    ANPResearchInputMissingError,
    read_gold_panel,
    read_silver_dataset,
    write_gold_panel,
)
from bralpha.derived.anp.oil_gas import (
    build_oil_gas_group_observation,
    build_oil_gas_production_observation,
)
from bralpha.derived.anp.schemas import PANEL_PRIMARY_KEYS
from bralpha.infra.config import (
    load_anp_research_config,
    load_paths_config,
    resolve_project_paths,
)

PANEL_ORDER = [
    "fuel_price_station_observation",
    "fuel_price_group_observation",
    "fuel_sales_observation",
    "fuel_sales_group_observation",
    "oil_gas_production_observation",
    "oil_gas_group_observation",
    "state_asof_daily",
    "daily_long",
]


def run_anp_research_spine(
    *,
    repo_root: Path,
    start: date,
    end: date,
    panels: list[str] | None = None,
) -> dict[str, str]:
    requested = PANEL_ORDER if panels is None else panels
    unknown = sorted(set(requested) - set(PANEL_ORDER))
    if unknown:
        raise ValueError(f"Unknown ANP research panel(s): {unknown}")

    explicit = panels is not None
    paths = resolve_project_paths(repo_root, load_paths_config(repo_root))
    config = load_anp_research_config(repo_root).anp_research
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
                required=explicit,
            )
        except ANPResearchInputMissingError as exc:
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
    if panel == "fuel_price_station_observation":
        if not config.fuel_prices.include_station_observation:
            return None
        silver = read_silver_dataset(
            paths,
            "anp_fuel_prices_weekly",
            required=required,
            start=start,
            end=end,
        )
        if silver is None:
            return None
        return build_fuel_price_station_observation(silver, start=start, end=end)

    if panel == "fuel_price_group_observation":
        if not config.fuel_prices.include_group_observation:
            return None
        stations = _fuel_price_station_dependency(paths, built, start, end, required=required)
        if stations is None:
            return None
        return build_fuel_price_group_observation(
            stations,
            group_by=config.fuel_prices.group_by,
            max_groups=config.fuel_prices.max_groups,
            start=start,
            end=end,
        )

    if panel == "fuel_sales_observation":
        if not config.fuel_sales.include_observation:
            return None
        silver = read_silver_dataset(
            paths,
            "anp_fuel_sales_monthly",
            required=required,
            start=start,
            end=end,
        )
        if silver is None:
            return None
        return build_fuel_sales_observation(silver, start=start, end=end)

    if panel == "fuel_sales_group_observation":
        if not config.fuel_sales.include_group_observation:
            return None
        observations = _fuel_sales_observation_dependency(
            paths, built, start, end, required=required
        )
        if observations is None:
            return None
        return build_fuel_sales_group_observation(
            observations,
            group_by=config.fuel_sales.group_by,
            max_groups=config.fuel_sales.max_groups,
            start=start,
            end=end,
        )

    if panel == "oil_gas_production_observation":
        if not config.oil_gas.include_observation:
            return None
        silver = read_silver_dataset(
            paths,
            "anp_oil_gas_production_monthly",
            required=required,
            start=start,
            end=end,
        )
        if silver is None:
            return None
        return build_oil_gas_production_observation(silver, start=start, end=end)

    if panel == "oil_gas_group_observation":
        if not config.oil_gas.include_group_observation:
            return None
        observations = _oil_gas_observation_dependency(
            paths, built, start, end, required=required
        )
        if observations is None:
            return None
        return build_oil_gas_group_observation(
            observations,
            group_by=config.oil_gas.group_by,
            max_groups=config.oil_gas.max_groups,
            start=start,
            end=end,
        )

    if panel == "state_asof_daily":
        if not config.asof.include_state_asof_daily:
            return None
        fuel_prices = _fuel_price_group_history(paths, config, end)
        fuel_sales = _fuel_sales_group_history(paths, config, end)
        oil_gas = _oil_gas_group_history(paths, config, end)
        if fuel_prices is None and fuel_sales is None and oil_gas is None:
            if required:
                raise ANPResearchInputMissingError("Missing ANP group observations for state")
            return None
        return build_anp_state_asof_daily(
            fuel_prices=fuel_prices,
            fuel_sales=fuel_sales,
            oil_gas=oil_gas,
            start=start,
            end=end,
            max_features=config.asof.max_features,
        )

    if panel == "daily_long":
        state = _dependency(paths, built, "state_asof_daily", start, end, required=required)
        if state is None:
            return None
        return build_anp_daily_long(
            state_asof_daily=state,
            include_fuel_prices=config.daily_long.include_fuel_prices,
            include_fuel_sales=config.daily_long.include_fuel_sales,
            include_oil_gas=config.daily_long.include_oil_gas,
        )

    raise ValueError(f"Unknown panel: {panel}")


def _fuel_price_station_dependency(
    paths,
    built: dict[str, pl.DataFrame],
    start: date | None,
    end: date | None,
    *,
    required: bool,
) -> pl.DataFrame | None:
    if "fuel_price_station_observation" in built:
        return built["fuel_price_station_observation"]
    gold = read_gold_panel(paths, "fuel_price_station_observation", start=start, end=end)
    if gold is not None:
        return gold
    silver = read_silver_dataset(
        paths,
        "anp_fuel_prices_weekly",
        required=required,
        start=start,
        end=end,
    )
    if silver is None:
        return None
    return build_fuel_price_station_observation(silver, start=start, end=end)


def _fuel_sales_observation_dependency(
    paths,
    built: dict[str, pl.DataFrame],
    start: date | None,
    end: date | None,
    *,
    required: bool,
) -> pl.DataFrame | None:
    if "fuel_sales_observation" in built:
        return built["fuel_sales_observation"]
    gold = read_gold_panel(paths, "fuel_sales_observation", start=start, end=end)
    if gold is not None:
        return gold
    silver = read_silver_dataset(
        paths,
        "anp_fuel_sales_monthly",
        required=required,
        start=start,
        end=end,
    )
    if silver is None:
        return None
    return build_fuel_sales_observation(silver, start=start, end=end)


def _oil_gas_observation_dependency(
    paths,
    built: dict[str, pl.DataFrame],
    start: date | None,
    end: date | None,
    *,
    required: bool,
) -> pl.DataFrame | None:
    if "oil_gas_production_observation" in built:
        return built["oil_gas_production_observation"]
    gold = read_gold_panel(paths, "oil_gas_production_observation", start=start, end=end)
    if gold is not None:
        return gold
    silver = read_silver_dataset(
        paths,
        "anp_oil_gas_production_monthly",
        required=required,
        start=start,
        end=end,
    )
    if silver is None:
        return None
    return build_oil_gas_production_observation(silver, start=start, end=end)


def _fuel_price_group_history(paths, config, end: date) -> pl.DataFrame | None:
    silver = read_silver_dataset(paths, "anp_fuel_prices_weekly", start=None, end=end)
    if silver is not None:
        stations = build_fuel_price_station_observation(silver, start=None, end=end)
        return build_fuel_price_group_observation(
            stations,
            group_by=config.fuel_prices.group_by,
            max_groups=config.fuel_prices.max_groups,
            start=None,
            end=end,
        )
    return read_gold_panel(paths, "fuel_price_group_observation", start=None, end=end)


def _fuel_sales_group_history(paths, config, end: date) -> pl.DataFrame | None:
    silver = read_silver_dataset(paths, "anp_fuel_sales_monthly", start=None, end=end)
    if silver is not None:
        observations = build_fuel_sales_observation(silver, start=None, end=end)
        return build_fuel_sales_group_observation(
            observations,
            group_by=config.fuel_sales.group_by,
            max_groups=config.fuel_sales.max_groups,
            start=None,
            end=end,
        )
    return read_gold_panel(paths, "fuel_sales_group_observation", start=None, end=end)


def _oil_gas_group_history(paths, config, end: date) -> pl.DataFrame | None:
    silver = read_silver_dataset(paths, "anp_oil_gas_production_monthly", start=None, end=end)
    if silver is not None:
        observations = build_oil_gas_production_observation(silver, start=None, end=end)
        return build_oil_gas_group_observation(
            observations,
            group_by=config.oil_gas.group_by,
            max_groups=config.oil_gas.max_groups,
            start=None,
            end=end,
        )
    return read_gold_panel(paths, "oil_gas_group_observation", start=None, end=end)


def _dependency(
    paths,
    built: dict[str, pl.DataFrame],
    panel: str,
    start: date,
    end: date,
    *,
    required: bool,
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
    run_anp_research_spine(
        repo_root=Path(args.repo_root).resolve(),
        start=date.fromisoformat(args.start),
        end=date.fromisoformat(args.end),
        panels=args.panels,
    )


if __name__ == "__main__":
    main()
