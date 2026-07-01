from __future__ import annotations

import shutil
from datetime import date

import polars as pl
import pytest

from bralpha.derived.anp.io import (
    ANPResearchInputMissingError,
    gold_panel_root,
    read_silver_dataset,
    write_gold_panel,
)
from bralpha.infra.config import (
    load_anp_research_config,
    load_paths_config,
    resolve_project_paths,
)
from bralpha.pipelines.anp_research_spine import run_anp_research_spine


def test_anp_research_config_loads_and_gold_root_is_scoped(repo_root):
    config = load_anp_research_config(repo_root).anp_research
    paths = resolve_project_paths(repo_root, load_paths_config(repo_root))

    assert config.calendar.default == "b3_trading_calendar"
    assert config.fuel_prices.group_by == ["all", "region", "state"]
    assert config.asof.max_features == 20000
    assert gold_panel_root(paths, "daily_long") == paths.gold / "anp" / "daily_long"


def test_anp_silver_reads_prune_year_partitions(repo_root, tmp_path):
    shutil.copytree(repo_root / "configs", tmp_path / "configs")
    paths = resolve_project_paths(tmp_path, load_paths_config(tmp_path))
    _write_sales_silver(paths.silver / "anp_fuel_sales_monthly", 2023, date(2023, 12, 31), 1.0)
    _write_sales_silver(paths.silver / "anp_fuel_sales_monthly", 2024, date(2024, 1, 31), 2.0)

    frame = read_silver_dataset(
        paths,
        "anp_fuel_sales_monthly",
        start=date(2024, 1, 1),
        end=date(2024, 1, 31),
    )

    assert frame is not None
    assert frame["ref_date"].to_list() == [date(2024, 1, 31)]
    assert frame["sales_volume_m3"].to_list() == [2.0]


def test_anp_gold_writes_use_exact_primary_key_upsert(repo_root, tmp_path):
    shutil.copytree(repo_root / "configs", tmp_path / "configs")
    paths = resolve_project_paths(tmp_path, load_paths_config(tmp_path))
    first = pl.DataFrame({"ref_date": [date(2024, 1, 2)], "feature_id": ["a"], "value": [1.0]})
    second = pl.DataFrame({"ref_date": [date(2024, 1, 2)], "feature_id": ["a"], "value": [2.0]})

    write_gold_panel(first, paths, panel="test_panel", primary_keys=["ref_date", "feature_id"])
    write_gold_panel(second, paths, panel="test_panel", primary_keys=["ref_date", "feature_id"])

    written = pl.read_parquet(paths.gold / "anp" / "test_panel" / "year=2024" / "data.parquet")
    assert written.height == 1
    assert written["value"].to_list() == [2.0]


def test_anp_pipeline_full_run_skips_missing_inputs(repo_root, tmp_path):
    shutil.copytree(repo_root / "configs", tmp_path / "configs")

    status = run_anp_research_spine(
        repo_root=tmp_path,
        start=date(2024, 1, 2),
        end=date(2024, 1, 5),
    )

    assert set(status) == {
        "fuel_price_station_observation",
        "fuel_price_group_observation",
        "fuel_sales_observation",
        "fuel_sales_group_observation",
        "oil_gas_production_observation",
        "oil_gas_group_observation",
        "state_asof_daily",
        "daily_long",
    }
    assert all(value.startswith("skipped:") for value in status.values())
    assert not (tmp_path / "data" / "gold" / "anp").exists()


def test_anp_pipeline_explicit_missing_input_raises(repo_root, tmp_path):
    shutil.copytree(repo_root / "configs", tmp_path / "configs")

    with pytest.raises(ANPResearchInputMissingError):
        run_anp_research_spine(
            repo_root=tmp_path,
            start=date(2024, 1, 2),
            end=date(2024, 1, 5),
            panels=["fuel_sales_observation"],
        )


def test_anp_pipeline_does_not_mutate_silver_inputs(repo_root, tmp_path):
    shutil.copytree(repo_root / "configs", tmp_path / "configs")
    paths = resolve_project_paths(tmp_path, load_paths_config(tmp_path))
    silver_path = _write_sales_silver(
        paths.silver / "anp_fuel_sales_monthly",
        2024,
        date(2024, 1, 31),
        100.0,
    )
    before = silver_path.read_bytes()

    run_anp_research_spine(
        repo_root=tmp_path,
        start=date(2024, 1, 1),
        end=date(2024, 1, 31),
        panels=["fuel_sales_observation"],
    )

    assert silver_path.read_bytes() == before
    assert (paths.gold / "anp" / "fuel_sales_observation" / "year=2024" / "data.parquet").exists()


def _write_sales_silver(root, year: int, ref_date: date, value: float):
    path = root / f"year={year}" / "data.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "ref_date": [ref_date],
            "available_date": [date(2024, 2, 1)],
            "availability_policy": ["anp_monthly_next_month_end_next_business_day"],
            "year": [ref_date.year],
            "month": [ref_date.month],
            "region": ["Sudeste"],
            "state": ["SP"],
            "product": ["GASOLINA C"],
            "sales_volume_m3": [value],
            "unit": ["m3"],
            "source_version": ["v0"],
        }
    ).write_parquet(path)
    return path
