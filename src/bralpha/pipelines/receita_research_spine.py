from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path

import polars as pl

from bralpha.derived.feature_utils import feature_warmup_start
from bralpha.derived.receita.daily_long import (
    build_receita_daily_long,
    build_receita_state_asof_daily,
)
from bralpha.derived.receita.features import build_receita_feature_daily
from bralpha.derived.receita.io import (
    ReceitaResearchInputMissingError,
    read_gold_panel,
    read_silver_dataset,
    write_gold_panel,
)
from bralpha.derived.receita.schemas import PANEL_PRIMARY_KEYS
from bralpha.derived.receita.tax_collection import (
    build_tax_collection_feature_observation,
    build_tax_collection_observation,
)
from bralpha.infra.config import (
    load_paths_config,
    load_receita_research_config,
    resolve_project_paths,
)
from bralpha.modeling.config import load_model_dataset_config

PANEL_ORDER = [
    "tax_collection_observation",
    "tax_collection_feature_observation",
    "state_asof_daily",
    "feature_daily",
    "daily_long",
]


def run_receita_research_spine(
    *,
    repo_root: Path,
    start: date,
    end: date,
    panels: list[str] | None = None,
) -> dict[str, str]:
    requested = PANEL_ORDER if panels is None else panels
    unknown = sorted(set(requested) - set(PANEL_ORDER))
    if unknown:
        raise ValueError(f"Unknown Receita research panel(s): {unknown}")

    explicit = panels is not None
    paths = resolve_project_paths(repo_root, load_paths_config(repo_root))
    config = load_receita_research_config(repo_root).receita_research
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
        except ReceitaResearchInputMissingError as exc:
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
    warmup_start: date,
    required: bool,
) -> pl.DataFrame | None:
    if panel == "tax_collection_observation":
        if not config.tax_collection.include_observation:
            return None
        silver = read_silver_dataset(
            paths,
            "receita_tax_collection_monthly",
            required=required,
            start=start,
            end=end,
        )
        if silver is None:
            return None
        return build_tax_collection_observation(silver, start=start, end=end)

    if panel == "tax_collection_feature_observation":
        if not config.tax_collection.include_feature_observation:
            return None
        observations = _tax_collection_observation_dependency(
            paths,
            built,
            start,
            end,
            required=required,
        )
        if observations is None:
            return None
        return build_tax_collection_feature_observation(
            observations,
            max_features=config.tax_collection.max_features,
            start=start,
            end=end,
        )

    if panel == "state_asof_daily":
        if not config.asof.include_state_asof_daily:
            return None
        feature_history = _tax_collection_feature_history(paths, config, end)
        if feature_history is None:
            if required:
                raise ReceitaResearchInputMissingError(
                    "Missing Receita feature observations for state_asof_daily"
                )
            return None
        return build_receita_state_asof_daily(
            feature_observations=feature_history,
            start=start,
            end=end,
            max_features=config.asof.max_features,
        )

    if panel == "feature_daily":
        state = _state_asof_history(paths, config, warmup_start, end, required=required)
        if state is None:
            return None
        return build_receita_feature_daily(state, start=start, end=end)

    if panel == "daily_long":
        state = _dependency(paths, built, "state_asof_daily", start, end, required=required)
        features = _dependency(paths, built, "feature_daily", start, end, required=False)
        if state is None and features is None:
            return None
        return build_receita_daily_long(
            state_asof_daily=state,
            feature_daily=features,
            include_tax_collection=config.daily_long.include_tax_collection,
        )

    raise ValueError(f"Unknown panel: {panel}")


def _tax_collection_observation_dependency(
    paths,
    built: dict[str, pl.DataFrame],
    start: date | None,
    end: date | None,
    *,
    required: bool,
) -> pl.DataFrame | None:
    if "tax_collection_observation" in built:
        return built["tax_collection_observation"]
    gold = read_gold_panel(paths, "tax_collection_observation", start=start, end=end)
    if gold is not None:
        return gold
    silver = read_silver_dataset(
        paths,
        "receita_tax_collection_monthly",
        required=required,
        start=start,
        end=end,
    )
    if silver is None:
        return None
    return build_tax_collection_observation(silver, start=start, end=end)


def _tax_collection_feature_history(paths, config, end: date) -> pl.DataFrame | None:
    silver = read_silver_dataset(
        paths,
        "receita_tax_collection_monthly",
        start=None,
        end=end,
    )
    if silver is not None:
        observations = build_tax_collection_observation(silver, start=None, end=end)
        return build_tax_collection_feature_observation(
            observations,
            max_features=config.tax_collection.max_features,
            start=None,
            end=end,
        )
    return read_gold_panel(paths, "tax_collection_feature_observation", start=None, end=end)


def _state_asof_history(
    paths,
    config,
    start: date,
    end: date,
    *,
    required: bool,
) -> pl.DataFrame | None:
    if not config.asof.include_state_asof_daily:
        return None
    feature_history = _tax_collection_feature_history(paths, config, end)
    if feature_history is None:
        if required:
            raise ReceitaResearchInputMissingError(
                "Missing Receita feature observations for feature_daily"
            )
        return None
    return build_receita_state_asof_daily(
        feature_observations=feature_history,
        start=start,
        end=end,
        max_features=config.asof.max_features,
    )


def _dependency(
    paths,
    built: dict[str, pl.DataFrame],
    panel: str,
    start: date | None,
    end: date | None,
    *,
    required: bool,
) -> pl.DataFrame | None:
    if panel in built:
        return built[panel]
    return read_gold_panel(paths, panel, required=required, start=start, end=end)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build Receita raw-to-research panels")
    parser.add_argument("--repo-root", type=Path, default=Path("."))
    parser.add_argument("--start", type=date.fromisoformat, required=True)
    parser.add_argument("--end", type=date.fromisoformat, required=True)
    parser.add_argument("--panel", action="append", choices=PANEL_ORDER)
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    run_receita_research_spine(
        repo_root=args.repo_root,
        start=args.start,
        end=args.end,
        panels=args.panel,
    )


if __name__ == "__main__":
    main()
