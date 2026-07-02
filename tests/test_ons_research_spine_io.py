from __future__ import annotations

import shutil
from datetime import date

import polars as pl
import pytest

from bralpha.derived.ons.io import (
    ONSResearchInputMissingError,
    gold_panel_root,
    read_silver_dataset,
    write_gold_panel,
)
from bralpha.infra.config import (
    load_ons_research_config,
    load_paths_config,
    resolve_project_paths,
)
from bralpha.pipelines.ons_research_spine import run_ons_research_spine


def test_ons_research_config_loads_and_gold_root_is_scoped(repo_root):
    config = load_ons_research_config(repo_root).ons_research
    paths = resolve_project_paths(repo_root, load_paths_config(repo_root))

    assert config.calendar.default == "b3_trading_calendar"
    assert config.hourly_daily.aggregation == "daily_mean"
    assert config.asof.max_features == 10000
    assert gold_panel_root(paths, "daily_long") == paths.gold / "ons" / "daily_long"


def test_ons_silver_reads_prune_year_partitions(repo_root, tmp_path):
    shutil.copytree(repo_root / "configs", tmp_path / "configs")
    paths = resolve_project_paths(tmp_path, load_paths_config(tmp_path))
    _write_silver(paths.silver / "ons_load_daily", 2023, date(2023, 12, 29), 1.0)
    _write_silver(paths.silver / "ons_load_daily", 2024, date(2024, 1, 2), 2.0)

    frame = read_silver_dataset(
        paths,
        "ons_load_daily",
        start=date(2024, 1, 1),
        end=date(2024, 1, 31),
    )

    assert frame is not None
    assert frame["ref_date"].to_list() == [date(2024, 1, 2)]
    assert frame["load_mwmed"].to_list() == [2.0]


def test_ons_gold_writes_use_exact_primary_key_upsert(repo_root, tmp_path):
    shutil.copytree(repo_root / "configs", tmp_path / "configs")
    paths = resolve_project_paths(tmp_path, load_paths_config(tmp_path))
    first = pl.DataFrame({"ref_date": [date(2024, 1, 2)], "feature_id": ["a"], "value": [1.0]})
    second = pl.DataFrame({"ref_date": [date(2024, 1, 2)], "feature_id": ["a"], "value": [2.0]})

    write_gold_panel(first, paths, panel="test_panel", primary_keys=["ref_date", "feature_id"])
    write_gold_panel(second, paths, panel="test_panel", primary_keys=["ref_date", "feature_id"])

    written = pl.read_parquet(paths.gold / "ons" / "test_panel" / "year=2024" / "data.parquet")
    assert written.height == 1
    assert written["value"].to_list() == [2.0]


def test_ons_pipeline_full_run_skips_missing_inputs(repo_root, tmp_path):
    shutil.copytree(repo_root / "configs", tmp_path / "configs")

    status = run_ons_research_spine(
        repo_root=tmp_path,
        start=date(2024, 1, 2),
        end=date(2024, 1, 5),
    )

    assert set(status) == {
        "ear_subsystem_observation",
        "ena_subsystem_observation",
        "load_daily_observation",
        "cmo_weekly_observation",
        "energy_balance_daily_observation",
        "interchange_daily_observation",
        "state_asof_daily",
        "daily_long",
    }
    assert all(value.startswith("skipped:") for value in status.values())
    assert not (tmp_path / "data" / "gold" / "ons").exists()


def test_ons_pipeline_explicit_missing_input_raises(repo_root, tmp_path):
    shutil.copytree(repo_root / "configs", tmp_path / "configs")

    with pytest.raises(ONSResearchInputMissingError):
        run_ons_research_spine(
            repo_root=tmp_path,
            start=date(2024, 1, 2),
            end=date(2024, 1, 5),
            panels=["ear_subsystem_observation"],
        )


def test_ons_pipeline_does_not_mutate_silver_inputs(repo_root, tmp_path):
    shutil.copytree(repo_root / "configs", tmp_path / "configs")
    paths = resolve_project_paths(tmp_path, load_paths_config(tmp_path))
    silver_path = _write_silver(paths.silver / "ons_load_daily", 2024, date(2024, 1, 2), 2.0)
    before = silver_path.read_bytes()

    run_ons_research_spine(
        repo_root=tmp_path,
        start=date(2024, 1, 2),
        end=date(2024, 1, 2),
        panels=["load_daily_observation"],
    )

    assert silver_path.read_bytes() == before
    assert (paths.gold / "ons" / "load_daily_observation" / "year=2024" / "data.parquet").exists()


def _write_silver(root, year: int, ref_date: date, value: float):
    path = root / f"year={year}" / "data.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "ref_date": [ref_date],
            "available_date": [date(2024, 1, 3) if year == 2024 else date(2024, 1, 1)],
            "availability_policy": ["ons_conservative_next_business_day"],
            "subsystem_id": ["SE"],
            "subsystem": ["Sudeste"],
            "load_mwmed": [value],
            "unit": ["MWmed"],
            "methodology_note": ["test_methodology"],
            "source_version": ["v0"],
        }
    ).write_parquet(path)
    return path
