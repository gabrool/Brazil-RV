from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path

import polars as pl

from bralpha.derived.ibge.daily_long import build_daily_long
from bralpha.derived.ibge.io import (
    IBGEResearchInputMissingError,
    read_gold_panel,
    read_silver_dataset,
    write_gold_panel,
)
from bralpha.derived.ibge.reference import (
    build_news_release_metadata,
    build_products_reference,
    build_release_calendar_reference,
)
from bralpha.derived.ibge.schemas import PANEL_PRIMARY_KEYS
from bralpha.derived.ibge.sidra import build_sidra_asof_daily, build_sidra_observation
from bralpha.infra.config import (
    load_ibge_research_config,
    load_paths_config,
    resolve_project_paths,
)
from bralpha.ingestion.ibge.sidra import load_sidra_series_config

PANEL_ORDER = [
    "sidra_observation",
    "sidra_asof_daily",
    "release_calendar_reference",
    "products_reference",
    "news_release_metadata",
    "daily_long",
]


def run_ibge_research_spine(
    *,
    repo_root: Path,
    start: date,
    end: date,
    panels: list[str] | None = None,
) -> dict[str, str]:
    requested = PANEL_ORDER if panels is None else panels
    unknown = sorted(set(requested) - set(PANEL_ORDER))
    if unknown:
        raise ValueError(f"Unknown IBGE research panel(s): {unknown}")

    explicit = panels is not None
    paths = resolve_project_paths(repo_root, load_paths_config(repo_root))
    config = load_ibge_research_config(repo_root).ibge_research
    series_config = load_sidra_series_config(repo_root)
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
        except IBGEResearchInputMissingError as exc:
            if explicit:
                raise
            status[panel] = f"skipped: {exc}"
            print(f"{panel}: {status[panel]}")
            continue
        if frame is None:
            status[panel] = "skipped: inputs absent"
            print(f"{panel}: {status[panel]}")
            continue

        ref_date_col = _panel_ref_date_col(panel)
        write_gold_panel(
            frame,
            paths,
            panel=panel,
            primary_keys=PANEL_PRIMARY_KEYS[panel],
            ref_date_col=ref_date_col,
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
    if panel == "sidra_observation":
        silver = read_silver_dataset(
            paths,
            "ibge_sidra_series",
            required=required,
            start=start,
            end=end,
        )
        if silver is None:
            return None
        return build_sidra_observation(
            silver,
            series_config=series_config,
            include_model_usable_only=config.sidra.include_model_usable_only,
            include_priorities=config.sidra.include_priorities,
            selected_dataset_slugs=config.sidra.selected_dataset_slugs,
            start=start,
            end=end,
        )
    if panel == "sidra_asof_daily":
        observations = _sidra_observation_history(
            paths,
            config,
            series_config,
            end,
            required=required,
        )
        if observations is None:
            return None
        return build_sidra_asof_daily(
            observations,
            start=start,
            end=end,
            max_dense_features=config.sidra.max_dense_features,
        )
    if panel == "release_calendar_reference":
        if not config.references.include_release_calendar:
            return None
        silver = read_silver_dataset(
            paths,
            "ibge_release_calendar",
            required=required,
            start=start,
            end=end,
            date_col="release_date",
        )
        return None if silver is None else build_release_calendar_reference(silver)
    if panel == "products_reference":
        if not config.references.include_products:
            return None
        silver = read_silver_dataset(
            paths,
            "ibge_products_metadata",
            required=required,
            date_col=None,
        )
        return None if silver is None else build_products_reference(silver)
    if panel == "news_release_metadata":
        if not config.references.include_news_metadata:
            return None
        silver = read_silver_dataset(
            paths,
            "ibge_news_releases_metadata",
            required=required,
            start=start,
            end=end,
            date_col="published_date",
        )
        return None if silver is None else build_news_release_metadata(silver)
    if panel == "daily_long":
        sidra = _dependency(paths, built, "sidra_asof_daily", start, end, required=required)
        if sidra is None:
            return None
        return build_daily_long(
            sidra_asof_daily=sidra,
            include_sidra=config.daily_long.include_sidra,
        )
    raise ValueError(f"Unknown panel: {panel}")


def _sidra_observation_history(
    paths,
    config,
    series_config,
    end: date,
    *,
    required: bool,
) -> pl.DataFrame | None:
    silver = read_silver_dataset(paths, "ibge_sidra_series", start=None, end=end)
    if silver is not None:
        return build_sidra_observation(
            silver,
            series_config=series_config,
            include_model_usable_only=config.sidra.include_model_usable_only,
            include_priorities=config.sidra.include_priorities,
            selected_dataset_slugs=config.sidra.selected_dataset_slugs,
            start=None,
            end=end,
        )
    gold = read_gold_panel(paths, "sidra_observation", start=None, end=end)
    if gold is None and required:
        raise IBGEResearchInputMissingError(
            "Missing SIDRA observation history from ibge_sidra_series or sidra_observation"
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


def _panel_ref_date_col(panel: str) -> str | None:
    if panel == "release_calendar_reference":
        return "release_date"
    if panel == "products_reference":
        return None
    if panel == "news_release_metadata":
        return "published_date"
    return "ref_date"


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--panel", choices=PANEL_ORDER, action="append", dest="panels")
    args = parser.parse_args(argv)
    run_ibge_research_spine(
        repo_root=Path(args.repo_root).resolve(),
        start=date.fromisoformat(args.start),
        end=date.fromisoformat(args.end),
        panels=args.panels,
    )


if __name__ == "__main__":
    main()
