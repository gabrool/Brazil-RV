from __future__ import annotations

import shutil
from datetime import date
from pathlib import Path

import polars as pl
import pytest

import bralpha.derived.bcb.io as io_module
from bralpha.derived.bcb.io import gold_panel_root, write_gold_panel
from bralpha.infra.config import (
    load_bcb_research_config,
    load_paths_config,
    resolve_project_paths,
)
from bralpha.parsing.common import write_source_partitioned
from bralpha.pipelines.bcb_research_spine import run_bcb_research_spine


def test_bcb_research_config_loads(repo_root):
    config = load_bcb_research_config(repo_root).bcb_research

    assert config.calendar.default == "business_days_mon_fri"
    assert config.ptax.currencies[:2] == ["USD", "EUR"]
    assert (
        config.focus.availability_note
        == "date_only_next_business_day_until_publication_calendar"
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


def _sgs_silver_row(*, ref_date: date, available_date: date, value: float) -> dict[str, object]:
    return {
        "ref_date": ref_date,
        "available_date": available_date,
        "series_id": 11,
        "series_slug": "selic_over",
        "series_name": "Selic",
        "category": "rates",
        "frequency": "daily",
        "value": value,
        "unit": "percent_annualized",
        "availability_policy": "next_business_day",
        "model_usable": True,
        "source_version": "v0",
    }


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
