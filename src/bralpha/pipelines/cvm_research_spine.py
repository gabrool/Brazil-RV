from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path

import polars as pl

from bralpha.derived.cvm.daily_long import build_cvm_daily_long
from bralpha.derived.cvm.features import build_cvm_fund_feature_daily
from bralpha.derived.cvm.fund_reports import (
    build_fund_daily_observation,
    build_fund_flows_daily,
    build_fund_group_observation,
    build_fund_state_asof_daily,
)
from bralpha.derived.cvm.io import (
    CVMResearchInputMissingError,
    read_gold_panel,
    read_silver_dataset,
    write_gold_panel,
)
from bralpha.derived.cvm.registry import build_fund_registry_current_reference
from bralpha.derived.cvm.schemas import PANEL_PRIMARY_KEYS
from bralpha.derived.feature_utils import feature_warmup_start
from bralpha.infra.config import (
    load_cvm_research_config,
    load_paths_config,
    resolve_project_paths,
)
from bralpha.modeling.config import load_model_dataset_config

PANEL_ORDER = [
    "fund_daily_observation",
    "fund_group_observation",
    "fund_flows_daily",
    "fund_state_asof_daily",
    "fund_feature_daily",
    "fund_registry_current_reference",
    "daily_long",
]


def run_cvm_research_spine(
    *,
    repo_root: Path,
    start: date,
    end: date,
    panels: list[str] | None = None,
) -> dict[str, str]:
    requested = PANEL_ORDER if panels is None else panels
    unknown = sorted(set(requested) - set(PANEL_ORDER))
    if unknown:
        raise ValueError(f"Unknown CVM research panel(s): {unknown}")

    explicit = panels is not None
    paths = resolve_project_paths(repo_root, load_paths_config(repo_root))
    config = load_cvm_research_config(repo_root).cvm_research
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
        except CVMResearchInputMissingError as exc:
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
            ref_date_col=None if panel == "fund_registry_current_reference" else "ref_date",
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
    fund_reports = config.fund_reports
    if panel == "fund_daily_observation":
        if not fund_reports.include_per_fund_observation:
            return None
        silver = read_silver_dataset(
            paths,
            "cvm_fund_daily_reports",
            required=required,
            start=start,
            end=end,
        )
        if silver is None:
            return None
        return build_fund_daily_observation(silver, start=start, end=end)

    if panel == "fund_group_observation":
        if not fund_reports.include_group_observation:
            return None
        observations = _daily_observation_dependency(paths, built, start, end, required=required)
        if observations is None:
            return None
        return build_fund_group_observation(
            observations,
            group_by=fund_reports.group_by,
            max_groups=fund_reports.max_groups,
            start=start,
            end=end,
        )

    if panel == "fund_flows_daily":
        if not fund_reports.include_flow_daily:
            return None
        groups = _group_observation_history(paths, config, end, required=required)
        if groups is None:
            return None
        return build_fund_flows_daily(groups, start=start, end=end)

    if panel == "fund_state_asof_daily":
        if not fund_reports.include_state_asof_daily:
            return None
        groups = _group_observation_history(paths, config, end, required=required)
        if groups is None:
            return None
        return build_fund_state_asof_daily(
            groups,
            start=start,
            end=end,
            max_groups=fund_reports.max_groups,
        )

    if panel == "fund_feature_daily":
        groups = _group_observation_history(paths, config, end, required=required)
        if groups is None:
            return None
        flows = build_fund_flows_daily(groups, start=warmup_start, end=end)
        state = build_fund_state_asof_daily(
            groups,
            start=warmup_start,
            end=end,
            max_groups=fund_reports.max_groups,
        )
        return build_cvm_fund_feature_daily(
            fund_flows_daily=flows,
            fund_state_asof_daily=state,
            start=start,
            end=end,
        )

    if panel == "fund_registry_current_reference":
        if not config.registry.include_current_reference:
            return None
        silver = read_silver_dataset(
            paths,
            "cvm_fund_registry_current",
            required=required,
            date_col=None,
        )
        if silver is None:
            return None
        return build_fund_registry_current_reference(silver)

    if panel == "daily_long":
        flows = _dependency(paths, built, "fund_flows_daily", start, end, required=False)
        state = _dependency(paths, built, "fund_state_asof_daily", start, end, required=False)
        features = _dependency(paths, built, "fund_feature_daily", start, end, required=False)
        if flows is None and state is None and features is None:
            if required:
                raise CVMResearchInputMissingError("Missing CVM flow/state panels for daily_long")
            return None
        return build_cvm_daily_long(
            fund_flows_daily=flows,
            fund_state_asof_daily=state,
            fund_feature_daily=features,
            include_fund_flows=config.daily_long.include_fund_flows,
            include_fund_state=config.daily_long.include_fund_state,
        )

    raise ValueError(f"Unknown panel: {panel}")


def _daily_observation_dependency(
    paths,
    built: dict[str, pl.DataFrame],
    start: date | None,
    end: date | None,
    *,
    required: bool,
) -> pl.DataFrame | None:
    if "fund_daily_observation" in built:
        return built["fund_daily_observation"]
    gold = read_gold_panel(
        paths,
        "fund_daily_observation",
        start=start,
        end=end,
    )
    if gold is not None:
        return gold
    silver = read_silver_dataset(
        paths,
        "cvm_fund_daily_reports",
        required=required,
        start=start,
        end=end,
    )
    if silver is None:
        return None
    return build_fund_daily_observation(silver, start=start, end=end)


def _group_observation_history(
    paths,
    config,
    end: date,
    *,
    required: bool,
) -> pl.DataFrame | None:
    silver = read_silver_dataset(
        paths,
        "cvm_fund_daily_reports",
        start=None,
        end=end,
    )
    if silver is not None:
        daily = build_fund_daily_observation(silver, start=None, end=end)
        return build_fund_group_observation(
            daily,
            group_by=config.fund_reports.group_by,
            max_groups=config.fund_reports.max_groups,
            start=None,
            end=end,
        )
    gold = read_gold_panel(paths, "fund_group_observation", start=None, end=end)
    if gold is None and required:
        raise CVMResearchInputMissingError(
            "Missing CVM group observation history from cvm_fund_daily_reports or gold"
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
    run_cvm_research_spine(
        repo_root=Path(args.repo_root).resolve(),
        start=date.fromisoformat(args.start),
        end=date.fromisoformat(args.end),
        panels=args.panels,
    )


if __name__ == "__main__":
    main()
