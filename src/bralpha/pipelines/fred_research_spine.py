from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path

import polars as pl

from bralpha.derived.fred.daily_long import build_fred_daily_long
from bralpha.derived.fred.io import (
    FredResearchInputMissingError,
    read_gold_panel,
    read_silver_dataset,
    write_gold_panel,
)
from bralpha.derived.fred.observations import build_fred_asof_daily, build_fred_observation
from bralpha.derived.fred.reference import build_fred_series_reference
from bralpha.derived.fred.schemas import PANEL_PRIMARY_KEYS
from bralpha.infra.config import (
    load_fred_research_config,
    load_paths_config,
    resolve_project_paths,
)
from bralpha.ingestion.fred.common import load_fred_series_config

PANEL_ORDER = ["observation", "asof_daily", "series_reference", "daily_long"]


def run_fred_research_spine(
    *,
    repo_root: Path,
    start: date,
    end: date,
    panels: list[str] | None = None,
) -> dict[str, str]:
    requested = PANEL_ORDER if panels is None else panels
    unknown = sorted(set(requested) - set(PANEL_ORDER))
    if unknown:
        raise ValueError(f"Unknown FRED research panel(s): {unknown}")

    explicit = panels is not None
    paths = resolve_project_paths(repo_root, load_paths_config(repo_root))
    config = load_fred_research_config(repo_root).fred_research
    series_config = load_fred_series_config(repo_root)
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
                series_config,
                built,
                start,
                end,
                required=explicit,
            )
        except FredResearchInputMissingError as exc:
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
            ref_date_col=None if panel == "series_reference" else "ref_date",
        )
        built[panel] = frame
        status[panel] = f"written: {frame.height} rows"
        print(f"{panel}: {status[panel]}")
    return status


def _build_panel(
    panel: str,
    paths,
    config,
    series_config,
    built: dict[str, pl.DataFrame],
    start: date,
    end: date,
    *,
    required: bool,
) -> pl.DataFrame | None:
    if panel == "observation":
        silver = read_silver_dataset(
            paths,
            "fred_series_observations",
            required=required,
            start=start,
            end=end,
        )
        if silver is None:
            return None
        return build_fred_observation(
            silver,
            series_config=series_config,
            include_model_usable_only=config.observations.include_model_usable_only,
            include_priorities=config.observations.include_priorities,
            start=start,
            end=end,
        )
    if panel == "asof_daily":
        observations = _observation_history(paths, config, series_config, end, required=required)
        if observations is None:
            return None
        return build_fred_asof_daily(
            observations,
            start=start,
            end=end,
            max_dense_series=config.observations.max_dense_series,
        )
    if panel == "series_reference":
        if not config.references.include_series_reference:
            return None
        return build_fred_series_reference(series_config)
    if panel == "daily_long":
        asof = _dependency(paths, built, "asof_daily", start, end, required=required)
        if asof is None:
            return None
        return build_fred_daily_long(
            asof_daily=asof,
            include_observations=config.daily_long.include_observations,
        )
    raise ValueError(f"Unknown panel: {panel}")


def _observation_history(paths, config, series_config, end: date, *, required: bool):
    silver = read_silver_dataset(paths, "fred_series_observations", start=None, end=end)
    if silver is not None:
        return build_fred_observation(
            silver,
            series_config=series_config,
            include_model_usable_only=config.observations.include_model_usable_only,
            include_priorities=config.observations.include_priorities,
            start=None,
            end=end,
        )
    gold = read_gold_panel(paths, "observation", start=None, end=end)
    if gold is None and required:
        raise FredResearchInputMissingError(
            "Missing FRED observation history from fred_series_observations or gold observation"
        )
    return gold


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
    run_fred_research_spine(
        repo_root=Path(args.repo_root).resolve(),
        start=date.fromisoformat(args.start),
        end=date.fromisoformat(args.end),
        panels=args.panels,
    )


if __name__ == "__main__":
    main()
