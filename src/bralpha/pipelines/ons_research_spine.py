from __future__ import annotations

import argparse
from collections.abc import Callable
from datetime import date
from pathlib import Path

import polars as pl

from bralpha.derived.ons.daily_long import build_ons_daily_long, build_ons_state_asof_daily
from bralpha.derived.ons.hourly_daily import (
    build_energy_balance_daily_observation,
    build_interchange_daily_observation,
)
from bralpha.derived.ons.hydro import (
    build_ear_subsystem_observation,
    build_ena_subsystem_observation,
)
from bralpha.derived.ons.io import (
    ONSResearchInputMissingError,
    read_gold_panel,
    read_silver_dataset,
    write_gold_panel,
)
from bralpha.derived.ons.load_cmo import (
    build_cmo_weekly_observation,
    build_load_daily_observation,
)
from bralpha.derived.ons.schemas import PANEL_PRIMARY_KEYS
from bralpha.infra.config import (
    load_ons_research_config,
    load_paths_config,
    resolve_project_paths,
)

PANEL_ORDER = [
    "ear_subsystem_observation",
    "ena_subsystem_observation",
    "load_daily_observation",
    "cmo_weekly_observation",
    "energy_balance_daily_observation",
    "interchange_daily_observation",
    "state_asof_daily",
    "daily_long",
]


def run_ons_research_spine(
    *,
    repo_root: Path,
    start: date,
    end: date,
    panels: list[str] | None = None,
) -> dict[str, str]:
    requested = PANEL_ORDER if panels is None else panels
    unknown = sorted(set(requested) - set(PANEL_ORDER))
    if unknown:
        raise ValueError(f"Unknown ONS research panel(s): {unknown}")

    explicit = panels is not None
    paths = resolve_project_paths(repo_root, load_paths_config(repo_root))
    config = load_ons_research_config(repo_root).ons_research
    built: dict[str, pl.DataFrame] = {}
    status: dict[str, str] = {}

    for panel in PANEL_ORDER:
        if panel not in requested:
            continue
        try:
            frame = _build_panel(panel, paths, config, built, start, end, required=explicit)
        except ONSResearchInputMissingError as exc:
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
    if panel == "ear_subsystem_observation":
        if not config.hydro.include_ear_subsystem:
            return None
        silver = read_silver_dataset(
            paths,
            "ons_ear_subsystem_daily",
            required=required,
            start=start,
            end=end,
        )
        if silver is None:
            return None
        return build_ear_subsystem_observation(silver, start=start, end=end)
    if panel == "ena_subsystem_observation":
        if not config.hydro.include_ena_subsystem:
            return None
        silver = read_silver_dataset(
            paths,
            "ons_ena_subsystem_daily",
            required=required,
            start=start,
            end=end,
        )
        if silver is None:
            return None
        return build_ena_subsystem_observation(silver, start=start, end=end)
    if panel == "load_daily_observation":
        if not config.load_cmo.include_load_daily:
            return None
        silver = read_silver_dataset(
            paths,
            "ons_load_daily",
            required=required,
            start=start,
            end=end,
        )
        if silver is None:
            return None
        return build_load_daily_observation(silver, start=start, end=end)
    if panel == "cmo_weekly_observation":
        if not config.load_cmo.include_cmo_weekly:
            return None
        silver = read_silver_dataset(
            paths,
            "ons_cmo_weekly",
            required=required,
            start=start,
            end=end,
        )
        if silver is None:
            return None
        return build_cmo_weekly_observation(silver, start=start, end=end)
    if panel == "energy_balance_daily_observation":
        if not config.hourly_daily.include_energy_balance_daily:
            return None
        _require_daily_mean(config.hourly_daily.aggregation)
        silver = read_silver_dataset(
            paths,
            "ons_energy_balance_subsystem",
            required=required,
            start=start,
            end=end,
        )
        if silver is None:
            return None
        return build_energy_balance_daily_observation(
            silver,
            start=start,
            end=end,
            min_hour_count=config.hourly_daily.min_hour_count,
        )
    if panel == "interchange_daily_observation":
        if not config.hourly_daily.include_interchange_daily:
            return None
        _require_daily_mean(config.hourly_daily.aggregation)
        silver = read_silver_dataset(
            paths,
            "ons_interchange_subsystem_hourly",
            required=required,
            start=start,
            end=end,
        )
        if silver is None:
            return None
        return build_interchange_daily_observation(
            silver,
            start=start,
            end=end,
            min_hour_count=config.hourly_daily.min_hour_count,
        )
    if panel == "state_asof_daily":
        if not config.asof.include_state_asof_daily:
            return None
        histories = _observation_histories(paths, config, end, required=required)
        if all(frame is None for frame in histories.values()):
            if required:
                raise ONSResearchInputMissingError("Missing ONS observation history inputs")
            return None
        return build_ons_state_asof_daily(
            ear=histories["ear"],
            ena=histories["ena"],
            load=histories["load"],
            cmo=histories["cmo"],
            energy_balance=histories["energy_balance"],
            interchange=histories["interchange"],
            start=start,
            end=end,
            max_features=config.asof.max_features,
        )
    if panel == "daily_long":
        asof = _dependency(paths, built, "state_asof_daily", start, end, required=required)
        if asof is None:
            return None
        return build_ons_daily_long(
            state_asof_daily=asof,
            include_hydro=config.daily_long.include_hydro,
            include_load_cmo=config.daily_long.include_load_cmo,
            include_energy_balance=config.daily_long.include_energy_balance,
            include_interchange=config.daily_long.include_interchange,
        )
    raise ValueError(f"Unknown panel: {panel}")


def _observation_histories(
    paths,
    config,
    end: date,
    *,
    required: bool,
) -> dict[str, pl.DataFrame | None]:
    return {
        "ear": _history(
            paths,
            panel="ear_subsystem_observation",
            dataset_id="ons_ear_subsystem_daily",
            builder=build_ear_subsystem_observation,
            enabled=config.hydro.include_ear_subsystem,
            end=end,
            required=required,
        ),
        "ena": _history(
            paths,
            panel="ena_subsystem_observation",
            dataset_id="ons_ena_subsystem_daily",
            builder=build_ena_subsystem_observation,
            enabled=config.hydro.include_ena_subsystem,
            end=end,
            required=required,
        ),
        "load": _history(
            paths,
            panel="load_daily_observation",
            dataset_id="ons_load_daily",
            builder=build_load_daily_observation,
            enabled=config.load_cmo.include_load_daily,
            end=end,
            required=required,
        ),
        "cmo": _history(
            paths,
            panel="cmo_weekly_observation",
            dataset_id="ons_cmo_weekly",
            builder=build_cmo_weekly_observation,
            enabled=config.load_cmo.include_cmo_weekly,
            end=end,
            required=required,
        ),
        "energy_balance": _history(
            paths,
            panel="energy_balance_daily_observation",
            dataset_id="ons_energy_balance_subsystem",
            builder=lambda silver, *, start, end: build_energy_balance_daily_observation(
                silver,
                start=start,
                end=end,
                min_hour_count=config.hourly_daily.min_hour_count,
            ),
            enabled=config.hourly_daily.include_energy_balance_daily,
            end=end,
            required=required,
        ),
        "interchange": _history(
            paths,
            panel="interchange_daily_observation",
            dataset_id="ons_interchange_subsystem_hourly",
            builder=lambda silver, *, start, end: build_interchange_daily_observation(
                silver,
                start=start,
                end=end,
                min_hour_count=config.hourly_daily.min_hour_count,
            ),
            enabled=config.hourly_daily.include_interchange_daily,
            end=end,
            required=required,
        ),
    }


def _history(
    paths,
    *,
    panel: str,
    dataset_id: str,
    builder: Callable[..., pl.DataFrame],
    enabled: bool,
    end: date,
    required: bool,
) -> pl.DataFrame | None:
    if not enabled:
        return None
    silver = read_silver_dataset(paths, dataset_id, start=None, end=end)
    if silver is not None:
        return builder(silver, start=None, end=end)
    gold = read_gold_panel(paths, panel, start=None, end=end)
    if gold is None and required:
        raise ONSResearchInputMissingError(
            f"Missing ONS history from {dataset_id} or gold {panel}"
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


def _require_daily_mean(aggregation: str) -> None:
    if aggregation != "daily_mean":
        raise ValueError(f"Unsupported ONS hourly_daily aggregation: {aggregation}")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--panel", choices=PANEL_ORDER, action="append", dest="panels")
    args = parser.parse_args(argv)
    run_ons_research_spine(
        repo_root=Path(args.repo_root).resolve(),
        start=date.fromisoformat(args.start),
        end=date.fromisoformat(args.end),
        panels=args.panels,
    )


if __name__ == "__main__":
    main()
