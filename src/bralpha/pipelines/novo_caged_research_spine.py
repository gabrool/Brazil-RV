from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path

import polars as pl

from bralpha.derived.novo_caged.daily_long import (
    build_novo_caged_daily_long,
    build_novo_caged_state_asof_daily,
)
from bralpha.derived.novo_caged.io import (
    NovoCagedResearchInputMissingError,
    read_gold_panel,
    read_silver_dataset,
    write_gold_panel,
)
from bralpha.derived.novo_caged.movements import (
    build_movement_group_observation,
    build_movement_record_observation,
)
from bralpha.derived.novo_caged.release_calendar import build_release_calendar_reference
from bralpha.derived.novo_caged.schemas import PANEL_PRIMARY_KEYS
from bralpha.infra.config import (
    load_novo_caged_research_config,
    load_paths_config,
    resolve_project_paths,
)

PANEL_ORDER = [
    "movement_record_observation",
    "release_calendar_reference",
    "movement_group_observation",
    "state_asof_daily",
    "daily_long",
]


def run_novo_caged_research_spine(
    *,
    repo_root: Path,
    start: date,
    end: date,
    panels: list[str] | None = None,
) -> dict[str, str]:
    requested = PANEL_ORDER if panels is None else panels
    unknown = sorted(set(requested) - set(PANEL_ORDER))
    if unknown:
        raise ValueError(f"Unknown Novo CAGED research panel(s): {unknown}")

    explicit = panels is not None
    paths = resolve_project_paths(repo_root, load_paths_config(repo_root))
    config = load_novo_caged_research_config(repo_root).novo_caged_research
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
        except NovoCagedResearchInputMissingError as exc:
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
    if panel == "movement_record_observation":
        if not config.movements.include_record_observation:
            return None
        silver = read_silver_dataset(
            paths,
            "novo_caged_movements_monthly",
            required=required,
            start=start,
            end=end,
        )
        if silver is None:
            return None
        return build_movement_record_observation(silver, start=start, end=end)

    if panel == "release_calendar_reference":
        if not config.release_calendar.include_reference:
            return None
        silver = read_silver_dataset(
            paths,
            "novo_caged_release_calendar",
            required=required,
            start=start,
            end=end,
        )
        if silver is None:
            return None
        return build_release_calendar_reference(silver, start=start, end=end)

    if panel == "movement_group_observation":
        if not config.movements.include_group_observation:
            return None
        records = _movement_record_dependency(paths, built, start, end, required=required)
        if records is None:
            return None
        release_reference = _release_reference_dependency(
            paths,
            built,
            start,
            end,
            required=False,
        )
        return build_movement_group_observation(
            records,
            release_calendar=release_reference,
            prefer_official_calendar=config.release_calendar.prefer_official_calendar,
            group_by=config.movements.group_by,
            cross_by=config.movements.cross_by,
            max_groups=config.movements.max_groups,
            start=start,
            end=end,
        )

    if panel == "state_asof_daily":
        if not config.asof.include_state_asof_daily:
            return None
        movement_groups = _movement_group_history(paths, config, end)
        if movement_groups is None:
            if required:
                raise NovoCagedResearchInputMissingError(
                    "Missing Novo CAGED movement groups for state_asof_daily"
                )
            return None
        return build_novo_caged_state_asof_daily(
            movement_groups=movement_groups,
            start=start,
            end=end,
            include_movement_counts=config.daily_long.include_movement_counts,
            include_wage_hours=config.daily_long.include_wage_hours,
            max_features=config.asof.max_features,
        )

    if panel == "daily_long":
        state = _dependency(paths, built, "state_asof_daily", start, end, required=required)
        if state is None:
            return None
        return build_novo_caged_daily_long(state_asof_daily=state)

    raise ValueError(f"Unknown panel: {panel}")


def _movement_record_dependency(
    paths,
    built: dict[str, pl.DataFrame],
    start: date | None,
    end: date | None,
    *,
    required: bool,
) -> pl.DataFrame | None:
    if "movement_record_observation" in built:
        return built["movement_record_observation"]
    gold = read_gold_panel(paths, "movement_record_observation", start=start, end=end)
    if gold is not None:
        return gold
    silver = read_silver_dataset(
        paths,
        "novo_caged_movements_monthly",
        required=required,
        start=start,
        end=end,
    )
    if silver is None:
        return None
    return build_movement_record_observation(silver, start=start, end=end)


def _release_reference_dependency(
    paths,
    built: dict[str, pl.DataFrame],
    start: date | None,
    end: date | None,
    *,
    required: bool,
) -> pl.DataFrame | None:
    if "release_calendar_reference" in built:
        return built["release_calendar_reference"]
    gold = read_gold_panel(paths, "release_calendar_reference", start=start, end=end)
    if gold is not None:
        return gold
    silver = read_silver_dataset(
        paths,
        "novo_caged_release_calendar",
        required=required,
        start=start,
        end=end,
    )
    if silver is None:
        return None
    return build_release_calendar_reference(silver, start=start, end=end)


def _movement_group_history(paths, config, end: date) -> pl.DataFrame | None:
    silver = read_silver_dataset(
        paths,
        "novo_caged_movements_monthly",
        start=None,
        end=end,
    )
    if silver is not None:
        records = build_movement_record_observation(silver, start=None, end=end)
        release_reference = _release_reference_history(paths, end)
        return build_movement_group_observation(
            records,
            release_calendar=release_reference,
            prefer_official_calendar=config.release_calendar.prefer_official_calendar,
            group_by=config.movements.group_by,
            cross_by=config.movements.cross_by,
            max_groups=config.movements.max_groups,
            start=None,
            end=end,
        )
    return read_gold_panel(paths, "movement_group_observation", start=None, end=end)


def _release_reference_history(paths, end: date) -> pl.DataFrame | None:
    gold = read_gold_panel(paths, "release_calendar_reference", start=None, end=end)
    if gold is not None:
        return gold
    silver = read_silver_dataset(
        paths,
        "novo_caged_release_calendar",
        start=None,
        end=end,
    )
    if silver is None:
        return None
    return build_release_calendar_reference(silver, start=None, end=end)


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
    run_novo_caged_research_spine(
        repo_root=Path(args.repo_root).resolve(),
        start=date.fromisoformat(args.start),
        end=date.fromisoformat(args.end),
        panels=args.panels,
    )


if __name__ == "__main__":
    main()
