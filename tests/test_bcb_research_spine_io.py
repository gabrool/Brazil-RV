from __future__ import annotations

import shutil
from datetime import date, timedelta
from math import isclose
from pathlib import Path

import polars as pl
import pytest

import bralpha.derived.bcb.io as io_module
from bralpha.derived.bcb.io import gold_panel_root, write_gold_panel
from bralpha.domain.b3_calendar import business_days
from bralpha.infra.config import (
    load_bcb_research_config,
    load_paths_config,
    resolve_project_paths,
)
from bralpha.parsing.common import write_source_partitioned
from bralpha.pipelines.bcb_research_spine import run_bcb_research_spine


def test_bcb_research_config_loads(repo_root):
    config = load_bcb_research_config(repo_root).bcb_research

    assert config.calendar.default == "b3_trading_calendar"
    assert config.ptax.currencies[:2] == ["USD", "EUR"]
    assert (
        config.focus.availability_note
        == "bcb_focus_data_field_is_official_weekly_publication_date_"
        "same_day_eod_if_b3_business_day"
    )


def test_bcb_gold_output_path_stays_under_data_gold_bcb(repo_root, tmp_path):
    paths = resolve_project_paths(tmp_path, load_paths_config(repo_root))
    frame = pl.DataFrame(
        [{"ref_date": date(2024, 1, 2), "series_id": 11, "value": 10.0}]
    )

    written = write_gold_panel(
        frame,
        paths,
        panel="sgs_observation_daily",
        primary_keys=["series_id", "ref_date"],
    )

    expected_root = tmp_path / "data" / "gold" / "bcb" / "sgs_observation_daily"
    assert gold_panel_root(paths, "sgs_observation_daily") == expected_root
    assert written[0].is_relative_to(expected_root)
    assert not (tmp_path / "data" / "silver" / "sgs_observation_daily").exists()


def test_bcb_missing_inputs_skip_full_pipeline_but_selected_panel_raises(repo_root, tmp_path):
    shutil.copytree(repo_root / "configs", tmp_path / "configs")

    status = run_bcb_research_spine(
        repo_root=tmp_path,
        start=date(2024, 1, 1),
        end=date(2024, 1, 31),
    )

    assert status["sgs_observation_daily"].startswith("skipped:")
    with pytest.raises(FileNotFoundError, match="bcb_sgs_series"):
        run_bcb_research_spine(
            repo_root=tmp_path,
            start=date(2024, 1, 1),
            end=date(2024, 1, 31),
            panels=["sgs_observation_daily"],
        )


def test_bcb_partitioned_parquet_read_prunes_unrelated_years(tmp_path, monkeypatch):
    root = tmp_path / "silver" / "bcb_sgs_series"
    _write_partition(root, 2023, 1.0)
    _write_partition(root, 2024, 2.0)
    scanned, globbed = _track_scan_and_glob(monkeypatch)

    frame = io_module.read_parquet_root(
        root,
        start=date(2024, 1, 1),
        end=date(2024, 12, 31),
    )

    assert frame["value"].to_list() == [2.0]
    assert any("year=2024" in path for path in scanned)
    assert not any("year=2023" in path for path in scanned)
    assert (root / "year=2024", "**/*.parquet") in globbed
    assert (root / "year=2023", "**/*.parquet") not in globbed


def test_bcb_nested_year_partitioned_parquet_read_prunes_unrelated_years(
    tmp_path,
    monkeypatch,
):
    root = tmp_path / "silver" / "bcb_sgs_series"
    _write_nested_partition(root, "a", 2023, 1.0)
    _write_nested_partition(root, "a", 2024, 2.0)
    _write_nested_partition(root, "b", 2024, 3.0)
    scanned, globbed = _track_scan_and_glob(monkeypatch)

    frame = io_module.read_parquet_root(
        root,
        start=date(2024, 1, 1),
        end=date(2024, 12, 31),
    )

    assert frame.sort("value")["value"].to_list() == [2.0, 3.0]
    assert any("year=2024" in path for path in scanned)
    assert not any("year=2023" in path for path in scanned)
    parquet_globs = [path for path, pattern in globbed if pattern == "**/*.parquet"]
    assert any(path.name == "year=2024" for path in parquet_globs)
    assert not any(path.name == "year=2023" for path in parquet_globs)


def test_bcb_pipeline_writes_gold_outputs_from_silver(repo_root, tmp_path):
    shutil.copytree(repo_root / "configs", tmp_path / "configs")
    write_source_partitioned(
        pl.DataFrame(
            [
                {
                    "ref_date": date(2024, 1, 1),
                    "available_date": date(2024, 1, 2),
                    "series_id": 11,
                    "series_slug": "selic_over",
                    "series_name": "Selic",
                    "category": "rates",
                    "frequency": "daily",
                    "value": 10.0,
                    "unit": "percent_annualized",
                    "availability_policy": "next_business_day",
                    "model_usable": True,
                    "source_version": "v0",
                }
            ]
        ),
        tmp_path / "data" / "silver" / "bcb_sgs_series",
        primary_keys=["series_id", "ref_date"],
    )

    status = run_bcb_research_spine(
        repo_root=tmp_path,
        start=date(2024, 1, 1),
        end=date(2024, 1, 5),
        panels=["sgs_observation_daily", "sgs_asof_daily", "sgs_feature_daily"],
    )

    assert status["sgs_observation_daily"] == "written: 1 rows"
    assert status["sgs_asof_daily"] == "written: 4 rows"
    assert status["sgs_feature_daily"] == "written: 4 rows"
    assert (tmp_path / "data" / "gold" / "bcb" / "sgs_observation_daily").exists()
    assert (tmp_path / "data" / "gold" / "bcb" / "sgs_asof_daily").exists()
    assert (tmp_path / "data" / "gold" / "bcb" / "sgs_feature_daily").exists()


def test_bcb_pipeline_sgs_asof_uses_pre_window_silver_history(repo_root, tmp_path):
    shutil.copytree(repo_root / "configs", tmp_path / "configs")
    write_source_partitioned(
        pl.DataFrame(
            [
                _sgs_silver_row(
                    ref_date=date(2023, 12, 29),
                    available_date=date(2024, 1, 2),
                    value=10.0,
                )
            ]
        ),
        tmp_path / "data" / "silver" / "bcb_sgs_series",
        primary_keys=["series_id", "ref_date"],
    )

    status = run_bcb_research_spine(
        repo_root=tmp_path,
        start=date(2024, 1, 2),
        end=date(2024, 1, 5),
        panels=["sgs_asof_daily"],
    )
    asof = io_module.read_parquet_root(
        tmp_path / "data" / "gold" / "bcb" / "sgs_asof_daily"
    ).sort("ref_date")

    assert status["sgs_asof_daily"] == "written: 4 rows"
    assert asof["ref_date"].to_list() == [
        date(2024, 1, 2),
        date(2024, 1, 3),
        date(2024, 1, 4),
        date(2024, 1, 5),
    ]
    assert asof["value"].to_list() == [10.0, 10.0, 10.0, 10.0]
    assert asof["observation_ref_date"].to_list() == [date(2023, 12, 29)] * 4


def test_bcb_pipeline_sgs_features_use_pre_window_history_for_rolling_features(
    repo_root,
    tmp_path,
):
    shutil.copytree(repo_root / "configs", tmp_path / "configs")
    start = date(2024, 12, 2)
    end = date(2024, 12, 3)
    reserve_dates = _business_dates(date(2023, 9, 1), 340)
    start_index = reserve_dates.index(start)
    reserve_levels = [1000.0 + index * 0.1 for index, _ in enumerate(reserve_dates)]
    reserve_levels[start_index - 40] = 2000.0
    rows = [
        _sgs_silver_row(
            ref_date=ref_date,
            available_date=ref_date,
            value=reserve_levels[index],
            series_id=13982,
            series_slug="international_reserves_liquidity",
            category="external_reserves",
            unit="usd_millions",
        )
        for index, ref_date in enumerate(reserve_dates)
    ]
    for month_index, observation_ref_date in enumerate(_monthly_dates(date(2023, 12, 1), 12)):
        rows.append(
            _sgs_silver_row(
                ref_date=observation_ref_date,
                available_date=observation_ref_date + timedelta(days=14),
                value=float(month_index + 1),
                series_id=433,
                series_slug="ipca",
                category="inflation",
                frequency="monthly",
                unit="percent_monthly",
            )
        )
    write_source_partitioned(
        pl.DataFrame(rows),
        tmp_path / "data" / "silver" / "bcb_sgs_series",
        primary_keys=["series_id", "ref_date"],
    )

    status = run_bcb_research_spine(
        repo_root=tmp_path,
        start=start,
        end=end,
        panels=["sgs_asof_daily", "sgs_feature_daily"],
    )
    features = io_module.read_parquet_root(
        tmp_path / "data" / "gold" / "bcb" / "sgs_feature_daily"
    )

    current_reserve = reserve_levels[start_index]
    previous_20bd_reserve = reserve_levels[start_index - 20]

    assert status["sgs_asof_daily"] == "written: 4 rows"
    assert status["sgs_feature_daily"].startswith("written: ")
    assert features["ref_date"].min() == start
    assert features["ref_date"].max() == end
    assert _feature_value(features, start, "bcb_sgs_feature:inflation:ipca_12m_sum_pct") == 78.0
    assert isclose(
        _feature_value(
            features,
            start,
            "bcb_sgs_feature:external_reserves:reserves_pct_change_20bd",
        ),
        (current_reserve / previous_20bd_reserve - 1) * 100,
    )
    assert isclose(
        _feature_value(
            features,
            start,
            "bcb_sgs_feature:external_reserves:reserves_drawdown_from_252bd_high_pct",
        ),
        (current_reserve / 2000.0 - 1) * 100,
    )


def _write_partition(root: Path, year: int, value: float) -> None:
    (root / f"year={year}").mkdir(parents=True)
    pl.DataFrame(
        [{"ref_date": date(year, 1, 2), "series_id": 11, "value": value}]
    ).write_parquet(root / f"year={year}" / "data.parquet")


def _write_nested_partition(root: Path, group: str, year: int, value: float) -> None:
    part_dir = root / f"some_key={group}" / f"year={year}"
    part_dir.mkdir(parents=True)
    pl.DataFrame(
        [{"ref_date": date(year, 1, 2), "series_id": 11, "value": value}]
    ).write_parquet(part_dir / "data.parquet")


def _sgs_silver_row(
    *,
    ref_date: date,
    available_date: date,
    value: float,
    series_id: int = 11,
    series_slug: str = "selic_over",
    category: str = "rates",
    frequency: str = "daily",
    unit: str = "percent_annualized",
) -> dict[str, object]:
    return {
        "ref_date": ref_date,
        "available_date": available_date,
        "series_id": series_id,
        "series_slug": series_slug,
        "series_name": series_slug,
        "category": category,
        "frequency": frequency,
        "value": value,
        "unit": unit,
        "availability_policy": "next_business_day",
        "model_usable": True,
        "source_version": "v0",
    }


def _business_dates(start: date, count: int) -> list[date]:
    return business_days(start, start + timedelta(days=count * 2))[:count]


def _monthly_dates(start: date, count: int) -> list[date]:
    dates = []
    year = start.year
    month = start.month
    for _ in range(count):
        dates.append(date(year, month, 1))
        month += 1
        if month == 13:
            month = 1
            year += 1
    return dates


def _feature_value(features: pl.DataFrame, ref_date: date, feature_id: str) -> float:
    return features.filter(
        (pl.col("ref_date") == ref_date) & (pl.col("feature_id") == feature_id)
    )["value"].item()


def _track_scan_and_glob(monkeypatch) -> tuple[list[str], list[tuple[Path, str]]]:
    scanned: list[str] = []
    globbed: list[tuple[Path, str]] = []
    original_scan_parquet = io_module.pl.scan_parquet
    original_glob = Path.glob

    def tracking_scan(source, *args, **kwargs):
        scanned.extend(source if isinstance(source, list) else [source])
        return original_scan_parquet(source, *args, **kwargs)

    def tracking_glob(self, pattern):
        globbed.append((self, pattern))
        return original_glob(self, pattern)

    monkeypatch.setattr(io_module.pl, "scan_parquet", tracking_scan)
    monkeypatch.setattr(Path, "glob", tracking_glob)
    return scanned, globbed
