from __future__ import annotations

import argparse
from datetime import date, timedelta
from pathlib import Path

import polars as pl

from bralpha.derived.bcb.daily_long import build_daily_long
from bralpha.derived.bcb.focus import (
    build_focus_expectation_asof_daily,
    build_focus_expectation_observation_daily,
    build_focus_reference_dates,
)
from bralpha.derived.bcb.io import (
    BCBResearchInputMissingError,
    read_gold_panel,
    read_silver_dataset,
    write_gold_panel,
)
from bralpha.derived.bcb.ptax import build_ptax_selected_daily
from bralpha.derived.bcb.schemas import PANEL_PRIMARY_KEYS
from bralpha.derived.bcb.sgs import build_sgs_asof_daily, build_sgs_observation_daily
from bralpha.derived.bcb.sgs_features import build_sgs_feature_daily
from bralpha.domain.b3_calendar import previous_business_day
from bralpha.infra.config import load_bcb_research_config, load_paths_config, resolve_project_paths

SGS_FEATURE_WARMUP_BUSINESS_DAYS = 252
SGS_FEATURE_WARMUP_CALENDAR_DAYS = 425

PANEL_ORDER = [
    "sgs_observation_daily",
    "sgs_asof_daily",
    "sgs_feature_daily",
    "ptax_selected_daily",
    "focus_expectation_observation_daily",
    "focus_expectation_asof_daily",
    "focus_reference_dates",
    "daily_long",
]


def run_bcb_research_spine(
    *,
    repo_root: Path,
    start: date,
    end: date,
    panels: list[str] | None = None,
) -> dict[str, str]:
    requested = PANEL_ORDER if panels is None else panels
    unknown = sorted(set(requested) - set(PANEL_ORDER))
    if unknown:
        raise ValueError(f"Unknown BCB research panel(s): {unknown}")

    explicit = panels is not None
    paths = resolve_project_paths(repo_root, load_paths_config(repo_root))
    config = load_bcb_research_config(repo_root).bcb_research
    built: dict[str, pl.DataFrame] = {}
    status: dict[str, str] = {}

    for panel in PANEL_ORDER:
        if panel not in requested:
            continue
        try:
            frame = _build_panel(panel, paths, config, built, start, end, required=explicit)
        except BCBResearchInputMissingError as exc:
            if explicit:
                raise
            status[panel] = f"skipped: {exc}"
            print(f"{panel}: {status[panel]}")
            continue
        if frame is None:
            status[panel] = "skipped: inputs absent"
            print(f"{panel}: {status[panel]}")
            continue

        ref_date_col = "reference_date" if panel == "focus_reference_dates" else "ref_date"
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
    built: dict[str, pl.DataFrame],
    start: date,
    end: date,
    *,
    required: bool,
) -> pl.DataFrame | None:
    if panel == "sgs_observation_daily":
        sgs = read_silver_dataset(paths, "bcb_sgs_series", required=required, start=start, end=end)
        if sgs is None:
            return None
        return build_sgs_observation_daily(
            sgs,
            include_model_usable_only=config.sgs.include_model_usable_only,
            start=start,
            end=end,
        )
    if panel == "sgs_asof_daily":
        observations = _sgs_observation_history(paths, config, end, required=required)
        if observations is None:
            return None
        return build_sgs_asof_daily(observations, start=start, end=end)
    if panel == "sgs_feature_daily":
        asof = _sgs_feature_asof_history(paths, config, start, end, required=required)
        if asof is None:
            return None
        return build_sgs_feature_daily(asof, start=start, end=end)
    if panel == "ptax_selected_daily":
        ptax = read_silver_dataset(
            paths,
            "bcb_ptax_exchange_rates",
            required=required,
            start=start,
            end=end,
        )
        if ptax is None:
            return None
        return build_ptax_selected_daily(
            ptax,
            currencies=config.ptax.currencies,
            use_selected_bulletin_only=config.ptax.use_selected_bulletin_only,
            start=start,
            end=end,
        )
    if panel == "focus_expectation_observation_daily":
        general = read_silver_dataset(
            paths,
            "bcb_focus_expectations",
            start=start,
            end=end,
        )
        top5 = read_silver_dataset(
            paths,
            "bcb_focus_top5_expectations",
            start=start,
            end=end,
        )
        if general is None and top5 is None:
            if required:
                raise BCBResearchInputMissingError("Missing Focus expectation silver inputs")
            return None
        return build_focus_expectation_observation_daily(
            general=general,
            top5=top5,
            availability_note=config.focus.availability_note,
            include_general=config.focus.include_general_expectations,
            include_top5=config.focus.include_top5_expectations,
            start=start,
            end=end,
        )
    if panel == "focus_expectation_asof_daily":
        observations = _focus_observation_history(paths, config, end, required=required)
        if observations is None:
            return None
        return build_focus_expectation_asof_daily(
            observations,
            selected_indicators=config.focus.selected_indicators,
            max_dense_keys=config.focus.max_dense_keys,
            start=start,
            end=end,
        )
    if panel == "focus_reference_dates":
        refs = read_silver_dataset(
            paths,
            "bcb_focus_top5_reference_dates",
            required=required,
            start=start,
            end=end,
            date_col="reference_date",
        )
        if refs is None:
            return None
        return build_focus_reference_dates(refs)
    if panel == "daily_long":
        sgs = _dependency(paths, built, "sgs_asof_daily", start, end)
        sgs_features = _dependency(paths, built, "sgs_feature_daily", start, end)
        ptax = _dependency(paths, built, "ptax_selected_daily", start, end)
        focus = _dependency(paths, built, "focus_expectation_asof_daily", start, end)
        if sgs is None and sgs_features is None and ptax is None and focus is None:
            if required:
                raise BCBResearchInputMissingError("Missing daily_long source panels")
            return None
        return build_daily_long(
            sgs_asof_daily=sgs,
            sgs_feature_daily=sgs_features,
            ptax_selected_daily=ptax,
            focus_expectation_asof_daily=focus,
            include_sgs=config.daily_long.include_sgs,
            include_ptax=config.daily_long.include_ptax,
            include_focus=config.daily_long.include_focus,
        )
    raise ValueError(f"Unknown panel: {panel}")


def _sgs_observation_history(paths, config, end: date, *, required: bool) -> pl.DataFrame | None:
    silver = read_silver_dataset(paths, "bcb_sgs_series", start=None, end=end)
    if silver is not None:
        return build_sgs_observation_daily(
            silver,
            include_model_usable_only=config.sgs.include_model_usable_only,
            start=None,
            end=end,
        )
    gold = read_gold_panel(paths, "sgs_observation_daily", start=None, end=end)
    if gold is None and required:
        raise BCBResearchInputMissingError(
            "Missing SGS observation history from bcb_sgs_series or sgs_observation_daily"
        )
    return gold


def _sgs_feature_asof_history(
    paths,
    config,
    start: date,
    end: date,
    *,
    required: bool,
) -> pl.DataFrame | None:
    warmup_start = _sgs_feature_warmup_start(start)
    observations = _sgs_observation_history(paths, config, end, required=False)
    if observations is not None:
        return build_sgs_asof_daily(observations, start=warmup_start, end=end)

    asof = read_gold_panel(paths, "sgs_asof_daily", start=warmup_start, end=end)
    if asof is None and required:
        raise BCBResearchInputMissingError(
            "Missing SGS history from bcb_sgs_series, sgs_observation_daily, or sgs_asof_daily"
        )
    return asof


def _sgs_feature_warmup_start(start: date) -> date:
    candidate = start
    for _ in range(SGS_FEATURE_WARMUP_BUSINESS_DAYS):
        candidate = previous_business_day(candidate)
    monthly_candidate = start - timedelta(days=SGS_FEATURE_WARMUP_CALENDAR_DAYS)
    return min(candidate, monthly_candidate)


def _focus_observation_history(paths, config, end: date, *, required: bool) -> pl.DataFrame | None:
    general = read_silver_dataset(
        paths,
        "bcb_focus_expectations",
        start=None,
        end=end,
    )
    top5 = read_silver_dataset(
        paths,
        "bcb_focus_top5_expectations",
        start=None,
        end=end,
    )
    if general is not None or top5 is not None:
        return build_focus_expectation_observation_daily(
            general=general,
            top5=top5,
            availability_note=config.focus.availability_note,
            include_general=config.focus.include_general_expectations,
            include_top5=config.focus.include_top5_expectations,
            start=None,
            end=end,
        )
    gold = read_gold_panel(paths, "focus_expectation_observation_daily", start=None, end=end)
    if gold is None and required:
        raise BCBResearchInputMissingError(
            "Missing Focus expectation observation history from silver or gold"
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
    run_bcb_research_spine(
        repo_root=Path(args.repo_root).resolve(),
        start=date.fromisoformat(args.start),
        end=date.fromisoformat(args.end),
        panels=args.panels,
    )


if __name__ == "__main__":
    main()
