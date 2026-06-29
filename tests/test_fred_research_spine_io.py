from __future__ import annotations

import shutil
from datetime import date
from pathlib import Path

import polars as pl
import pytest

import bralpha.derived.fred.io as io_module
from bralpha.derived.fred.io import gold_panel_root, read_parquet_root, write_gold_panel
from bralpha.derived.fred.schemas import PANEL_PRIMARY_KEYS
from bralpha.infra.config import (
    load_fred_research_config,
    load_paths_config,
    resolve_project_paths,
)
from bralpha.ingestion.fred.common import write_partitioned_frame
from bralpha.pipelines.fred_research_spine import run_fred_research_spine


def test_fred_research_config_loads(repo_root):
    config = load_fred_research_config(repo_root).fred_research

    assert config.calendar.default == "business_days_mon_fri"
    assert config.observations.include_model_usable_only is True
    assert config.observations.include_priorities == ["P0", "P1"]
    assert config.observations.max_dense_series == 5000
    assert config.references.include_series_reference is True
    assert config.daily_long.include_observations is True


def test_fred_gold_output_path_stays_under_data_gold_fred(repo_root, tmp_path):
    paths = resolve_project_paths(tmp_path, load_paths_config(repo_root))
    frame = pl.DataFrame(
        [{"ref_date": date(2024, 1, 2), "series_id": "DGS10", "value": 4.0}]
    )

    written = write_gold_panel(
        frame,
        paths,
        panel="observation",
        primary_keys=["series_id", "ref_date"],
    )

    expected_root = tmp_path / "data" / "gold" / "fred" / "observation"
    assert gold_panel_root(paths, "observation") == expected_root
    assert written[0].is_relative_to(expected_root)
    assert not (tmp_path / "data" / "silver" / "observation").exists()


def test_fred_missing_inputs_skip_full_pipeline_but_selected_panel_raises(repo_root, tmp_path):
    shutil.copytree(repo_root / "configs", tmp_path / "configs")

    status = run_fred_research_spine(
        repo_root=tmp_path,
        start=date(2024, 1, 1),
        end=date(2024, 1, 31),
    )

    assert status["observation"].startswith("skipped:")
    assert status["series_reference"].startswith("written:")
    with pytest.raises(FileNotFoundError, match="fred_series_observations"):
        run_fred_research_spine(
            repo_root=tmp_path,
            start=date(2024, 1, 1),
            end=date(2024, 1, 31),
            panels=["observation"],
        )


def test_fred_nested_year_partition_read_prunes_unrelated_years(tmp_path, monkeypatch):
    root = tmp_path / "silver" / "fred_series_observations"
    _write_nested_partition(root, "DGS10", 2023, 3.0)
    _write_nested_partition(root, "DGS10", 2024, 4.0)
    _write_nested_partition(root, "SP500", 2024, 5000.0)
    scanned, globbed = _track_scan_and_glob(monkeypatch)

    frame = io_module.read_parquet_root(
        root,
        start=date(2024, 1, 1),
        end=date(2024, 12, 31),
    )

    assert frame.sort("value")["value"].to_list() == [4.0, 5000.0]
    assert any("year=2024" in path for path in scanned)
    assert not any("year=2023" in path for path in scanned)
    parquet_globs = [path for path, pattern in globbed if pattern == "**/*.parquet"]
    assert any(path.name == "year=2024" for path in parquet_globs)
    assert not any(path.name == "year=2023" for path in parquet_globs)


def test_fred_gold_writes_use_exact_panel_primary_keys(repo_root, tmp_path):
    paths = resolve_project_paths(tmp_path, load_paths_config(repo_root))
    first = _gold_row(value=4.0, source_dataset="first")
    second = _gold_row(value=4.1, source_dataset="second")

    write_gold_panel(
        pl.DataFrame([first]),
        paths,
        panel="observation",
        primary_keys=PANEL_PRIMARY_KEYS["observation"],
    )
    write_gold_panel(
        pl.DataFrame([second]),
        paths,
        panel="observation",
        primary_keys=PANEL_PRIMARY_KEYS["observation"],
    )

    frame = read_parquet_root(gold_panel_root(paths, "observation"))
    assert frame.height == 1
    assert frame["value"].item() == 4.1
    assert frame["source_dataset"].item() == "second"


def test_fred_pipeline_writes_gold_outputs_from_silver(repo_root, tmp_path):
    shutil.copytree(repo_root / "configs", tmp_path / "configs")
    write_partitioned_frame(
        pl.DataFrame(
            [
                _silver_row(
                    series_id="DGS10",
                    ref_date=date(2024, 1, 2),
                    available_date=date(2024, 1, 3),
                    value=4.0,
                    raw_value="4.0",
                    value_status="ok",
                )
            ]
        ),
        tmp_path / "data" / "silver" / "fred_series_observations",
        primary_keys=["series_id", "ref_date"],
        ref_date_col="ref_date",
        partition_cols=["series_id", "year"],
    )

    status = run_fred_research_spine(
        repo_root=tmp_path,
        start=date(2024, 1, 2),
        end=date(2024, 1, 5),
        panels=["observation", "asof_daily", "daily_long"],
    )

    assert status["observation"] == "written: 1 rows"
    assert status["asof_daily"] == "written: 3 rows"
    assert status["daily_long"] == "written: 3 rows"
    assert (tmp_path / "data" / "silver" / "fred_series_observations").exists()
    assert (tmp_path / "data" / "gold" / "fred" / "observation").exists()
    assert (tmp_path / "data" / "gold" / "fred" / "asof_daily").exists()
    assert (tmp_path / "data" / "gold" / "fred" / "daily_long").exists()


def _write_nested_partition(root: Path, series_id: str, year: int, value: float) -> None:
    part_dir = root / f"series_id={series_id}" / f"year={year}"
    part_dir.mkdir(parents=True)
    pl.DataFrame(
        [{"ref_date": date(year, 1, 2), "series_id": series_id, "value": value}]
    ).write_parquet(part_dir / "data.parquet")


def _silver_row(
    *,
    series_id: str,
    ref_date: date,
    available_date: date,
    value: float | None,
    raw_value: str,
    value_status: str,
    model_usable: bool = True,
) -> dict[str, object]:
    return {
        "series_id": series_id,
        "series_name": series_id,
        "category": "treasury_nominal",
        "frequency": "daily",
        "unit": "percent",
        "ref_date": ref_date,
        "available_date": available_date,
        "availability_policy": "date_only_next_business_day",
        "value": value,
        "raw_value": raw_value,
        "value_status": value_status,
        "realtime_start": ref_date,
        "realtime_end": ref_date,
        "model_usable": model_usable,
        "source_version": "v0",
    }


def _gold_row(*, value: float, source_dataset: str) -> dict[str, object]:
    return {
        "series_id": "DGS10",
        "ref_date": date(2024, 1, 2),
        "vintage_id": "fred:fred_series_observations:test",
        "value": value,
        "source_dataset": source_dataset,
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
