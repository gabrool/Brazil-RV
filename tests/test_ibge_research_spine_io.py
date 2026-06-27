from __future__ import annotations

import shutil
from datetime import date, datetime
from pathlib import Path

import polars as pl
import pytest

import bralpha.derived.ibge.io as io_module
from bralpha.derived.ibge.io import gold_panel_root, read_parquet_root, write_gold_panel
from bralpha.infra.config import (
    load_ibge_research_config,
    load_paths_config,
    resolve_project_paths,
)
from bralpha.parsing.common import write_source_partitioned
from bralpha.pipelines.ibge_research_spine import run_ibge_research_spine


def test_ibge_research_config_loads(repo_root):
    config = load_ibge_research_config(repo_root).ibge_research

    assert config.calendar.default == "business_days_mon_fri"
    assert config.sidra.include_model_usable_only is True
    assert config.sidra.selected_dataset_slugs[:3] == ["ipca", "ipca15", "inpc"]
    assert config.sidra.max_dense_features == 20000


def test_ibge_gold_output_path_stays_under_data_gold_ibge(repo_root, tmp_path):
    paths = resolve_project_paths(tmp_path, load_paths_config(repo_root))
    frame = pl.DataFrame(
        [
            {
                "ref_date": date(2024, 2, 9),
                "feature_id": "feature",
                "value": 1.0,
            }
        ]
    )

    written = write_gold_panel(
        frame,
        paths,
        panel="sidra_asof_daily",
        primary_keys=["ref_date", "feature_id"],
    )

    expected_root = tmp_path / "data" / "gold" / "ibge" / "sidra_asof_daily"
    assert gold_panel_root(paths, "sidra_asof_daily") == expected_root
    assert written[0].is_relative_to(expected_root)
    assert not (tmp_path / "data" / "silver" / "sidra_asof_daily").exists()


def test_ibge_missing_inputs_skip_full_pipeline_but_selected_panel_raises(repo_root, tmp_path):
    shutil.copytree(repo_root / "configs", tmp_path / "configs")

    status = run_ibge_research_spine(
        repo_root=tmp_path,
        start=date(2024, 2, 9),
        end=date(2024, 2, 12),
    )

    assert status["sidra_observation"].startswith("skipped:")
    with pytest.raises(FileNotFoundError, match="ibge_sidra_series"):
        run_ibge_research_spine(
            repo_root=tmp_path,
            start=date(2024, 2, 9),
            end=date(2024, 2, 12),
            panels=["sidra_observation"],
        )


def test_ibge_partitioned_parquet_read_prunes_unrelated_years(tmp_path, monkeypatch):
    root = tmp_path / "silver" / "ibge_sidra_series"
    _write_partition(root, 2023, 1.0)
    _write_partition(root, 2024, 2.0)
    scanned, globbed = _track_scan_and_glob(monkeypatch)

    frame = read_parquet_root(
        root,
        start=date(2024, 1, 1),
        end=date(2024, 12, 31),
    )

    assert frame["value"].to_list() == [2.0]
    assert any("year=2024" in path for path in scanned)
    assert not any("year=2023" in path for path in scanned)
    assert (root / "year=2024", "**/*.parquet") in globbed
    assert (root / "year=2023", "**/*.parquet") not in globbed


def test_ibge_nested_year_partitioned_parquet_read_prunes_unrelated_years(
    tmp_path,
    monkeypatch,
):
    root = tmp_path / "silver" / "ibge_sidra_series"
    _write_nested_partition(root, "a", 2023, 1.0)
    _write_nested_partition(root, "a", 2024, 2.0)
    _write_nested_partition(root, "b", 2024, 3.0)
    scanned, globbed = _track_scan_and_glob(monkeypatch)

    frame = read_parquet_root(
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


def test_ibge_gold_writes_use_exact_primary_keys(repo_root, tmp_path):
    paths = resolve_project_paths(tmp_path, load_paths_config(repo_root))
    first = pl.DataFrame([{"product_id": 9256, "product_name": "old"}])
    second = pl.DataFrame([{"product_id": 9256, "product_name": "new"}])

    write_gold_panel(
        first,
        paths,
        panel="products_reference",
        primary_keys=["product_id"],
        ref_date_col=None,
    )
    write_gold_panel(
        second,
        paths,
        panel="products_reference",
        primary_keys=["product_id"],
        ref_date_col=None,
    )
    result = pl.read_parquet(
        tmp_path / "data" / "gold" / "ibge" / "products_reference" / "data.parquet"
    )

    assert result.height == 1
    assert result["product_name"].item() == "new"


def test_ibge_pipeline_writes_sidra_gold_outputs_from_silver(repo_root, tmp_path):
    shutil.copytree(repo_root / "configs", tmp_path / "configs")
    write_source_partitioned(
        pl.DataFrame([_sidra_silver_row()]),
        tmp_path / "data" / "silver" / "ibge_sidra_series",
        primary_keys=[
            "dataset_slug",
            "aggregate_id",
            "variable_id",
            "period_code",
            "geography_id",
            "classification_key",
        ],
    )

    status = run_ibge_research_spine(
        repo_root=tmp_path,
        start=date(2024, 1, 31),
        end=date(2024, 2, 12),
        panels=["sidra_observation", "sidra_asof_daily", "daily_long"],
    )

    assert status["sidra_observation"] == "written: 1 rows"
    assert status["sidra_asof_daily"] == "written: 2 rows"
    assert status["daily_long"] == "written: 2 rows"
    assert (tmp_path / "data" / "gold" / "ibge" / "sidra_observation").exists()
    assert (tmp_path / "data" / "gold" / "ibge" / "sidra_asof_daily").exists()
    assert (tmp_path / "data" / "gold" / "ibge" / "daily_long").exists()


def _write_partition(root: Path, year: int, value: float) -> None:
    (root / f"year={year}").mkdir(parents=True)
    pl.DataFrame(
        [
            {
                "ref_date": date(year, 1, 31),
                "dataset_slug": "ipca",
                "value": value,
            }
        ]
    ).write_parquet(root / f"year={year}" / "data.parquet")


def _write_nested_partition(root: Path, group: str, year: int, value: float) -> None:
    part_dir = root / f"some_key={group}" / f"year={year}"
    part_dir.mkdir(parents=True)
    pl.DataFrame(
        [
            {
                "ref_date": date(year, 1, 31),
                "dataset_slug": group,
                "value": value,
            }
        ]
    ).write_parquet(part_dir / "data.parquet")


def _sidra_silver_row() -> dict[str, object]:
    return {
        "dataset_slug": "ipca",
        "aggregate_id": 7060,
        "variable_id": "63",
        "variable_name": "IPCA monthly variation",
        "unit": "%",
        "period_code": "202401",
        "period_label": "202401",
        "ref_period_start": date(2024, 1, 1),
        "ref_period_end": date(2024, 1, 31),
        "ref_date": date(2024, 1, 31),
        "release_date": date(2024, 2, 9),
        "available_datetime_local": datetime(2024, 2, 9, 9),
        "available_datetime_utc": datetime(2024, 2, 9, 12),
        "available_date": date(2024, 2, 9),
        "availability_policy": "exact_timestamp_cutoff",
        "availability_note": None,
        "model_usable": True,
        "geography_level": "N1",
        "geography_id": "1",
        "geography_name": "Brasil",
        "classification_key": "315=7169",
        "classifications_json": "[]",
        "value": 0.42,
        "raw_value": "0.42",
        "value_status": "ok",
        "source": "ibge",
        "source_dataset": "ibge_sidra_series",
        "download_timestamp_utc": datetime(2024, 2, 9, 12),
        "raw_path": "raw.json",
        "sha256": "abc",
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
