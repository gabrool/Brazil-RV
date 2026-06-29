from __future__ import annotations

import shutil
from datetime import date
from pathlib import Path

import polars as pl
import pytest

import bralpha.derived.b3.io as io_module
from bralpha.derived.b3.io import gold_panel_root, write_gold_panel
from bralpha.infra.config import (
    load_b3_research_config,
    load_paths_config,
    resolve_project_paths,
)
from bralpha.parsing.common import write_source_partitioned
from bralpha.pipelines.b3_research_spine import run_b3_research_spine


def test_b3_research_config_loads(repo_root):
    config = load_b3_research_config(repo_root).b3_research

    assert config.roots.primary == ["DI1", "DOL", "WDO", "IND", "WIN"]
    assert config.continuous_futures.max_front_rank == 3
    assert config.targets.horizons == [1, 5, 20]


def test_gold_output_path_stays_under_data_gold_b3(repo_root, tmp_path):
    paths = resolve_project_paths(tmp_path, load_paths_config(repo_root))
    frame = pl.DataFrame(
        [{"ref_date": date(2024, 1, 2), "contract_id": "DI1_F26", "value": 1.0}]
    )

    written = write_gold_panel(
        frame,
        paths,
        panel="futures_contract_daily",
        primary_keys=["ref_date", "contract_id"],
    )

    expected_root = tmp_path / "data" / "gold" / "b3" / "futures_contract_daily"
    assert gold_panel_root(paths, "futures_contract_daily") == expected_root
    assert written[0].is_relative_to(expected_root)
    assert not (tmp_path / "data" / "silver" / "futures_contract_daily").exists()


def test_gold_writer_upserts_by_declared_primary_key(repo_root, tmp_path):
    paths = resolve_project_paths(tmp_path, load_paths_config(repo_root))
    primary_keys = ["ref_date", "symbol", "market_type"]
    first = pl.DataFrame(
        [
            {
                "ref_date": date(2024, 1, 2),
                "symbol": "PETR4",
                "market_type": "010",
                "source_dataset": "b3_cotahist_yearly",
                "close": 10.0,
            }
        ]
    )
    second = pl.DataFrame(
        [
            {
                "ref_date": date(2024, 1, 2),
                "symbol": "PETR4",
                "market_type": "010",
                "source_dataset": "b3_cotahist_daily",
                "close": 10.5,
            }
        ]
    )

    write_gold_panel(first, paths, panel="listed_market_daily", primary_keys=primary_keys)
    write_gold_panel(second, paths, panel="listed_market_daily", primary_keys=primary_keys)

    frame = io_module.read_parquet_root(gold_panel_root(paths, "listed_market_daily"))
    assert frame.height == 1
    row = frame.row(0, named=True)
    assert row["source_dataset"] == "b3_cotahist_daily"
    assert row["close"] == 10.5


def test_source_writer_keeps_source_dataset_key_augmentation_by_default(tmp_path):
    primary_keys = ["ref_date", "symbol", "market_type"]
    yearly = pl.DataFrame(
        [
            {
                "ref_date": date(2024, 1, 2),
                "symbol": "PETR4",
                "market_type": "010",
                "source_dataset": "b3_cotahist_yearly",
                "close": 10.0,
            }
        ]
    )
    daily = pl.DataFrame(
        [
            {
                "ref_date": date(2024, 1, 2),
                "symbol": "PETR4",
                "market_type": "010",
                "source_dataset": "b3_cotahist_daily",
                "close": 10.5,
            }
        ]
    )
    default_root = tmp_path / "default"
    exact_root = tmp_path / "exact"

    write_source_partitioned(yearly, default_root, primary_keys=primary_keys)
    write_source_partitioned(daily, default_root, primary_keys=primary_keys)
    default_frame = io_module.read_parquet_root(default_root)
    assert default_frame.height == 2
    assert set(default_frame["source_dataset"].to_list()) == {
        "b3_cotahist_daily",
        "b3_cotahist_yearly",
    }

    write_source_partitioned(
        yearly,
        exact_root,
        primary_keys=primary_keys,
        augment_source_dataset_key=False,
    )
    write_source_partitioned(
        daily,
        exact_root,
        primary_keys=primary_keys,
        augment_source_dataset_key=False,
    )
    exact_frame = io_module.read_parquet_root(exact_root)
    assert exact_frame.height == 1
    row = exact_frame.row(0, named=True)
    assert row["source_dataset"] == "b3_cotahist_daily"
    assert row["close"] == 10.5


def test_missing_inputs_skip_full_pipeline_but_selected_panel_raises(repo_root, tmp_path):
    shutil.copytree(repo_root / "configs", tmp_path / "configs")

    status = run_b3_research_spine(
        repo_root=tmp_path,
        start=date(2024, 1, 1),
        end=date(2024, 1, 31),
    )

    assert status["futures_contract_daily"].startswith("skipped:")
    with pytest.raises(FileNotFoundError, match="b3_futures_settlements"):
        run_b3_research_spine(
            repo_root=tmp_path,
            start=date(2024, 1, 1),
            end=date(2024, 1, 31),
            panels=["futures_contract_daily"],
        )


def test_futures_contract_pipeline_uses_holidays_beyond_research_end(repo_root, tmp_path):
    shutil.copytree(repo_root / "configs", tmp_path / "configs")
    paths = resolve_project_paths(tmp_path, load_paths_config(repo_root))
    start = date(2024, 1, 2)
    end = date(2024, 1, 3)
    write_source_partitioned(
        pl.DataFrame(
            [
                {
                    "ref_date": start,
                    "available_date": start,
                    "source_dataset": "b3_futures_settlements",
                    "commodity": "DI1",
                    "maturity_code": "F24",
                    "contract_id": "DI1_F24",
                    "settlement": 98_000.0,
                    "source_version": "settle-v0",
                }
            ]
        ),
        paths.silver / "b3_futures_settlements",
        primary_keys=["ref_date", "contract_id"],
    )
    write_source_partitioned(
        pl.DataFrame(
            [
                {
                    "ref_date": start,
                    "contract_id": "DI1_F24",
                    "maturity_date": date(2024, 1, 5),
                    "source_version": "master-v0",
                }
            ]
        ),
        paths.silver / "b3_futures_contract_master",
        primary_keys=["ref_date", "contract_id"],
    )
    write_source_partitioned(
        pl.DataFrame(
            [
                {
                    "ref_date": date(2024, 1, 4),
                    "calendar_id": "B3",
                    "is_business_day": False,
                    "holiday_name": "Fixture holiday after research end",
                    "source_dataset": "b3_holiday_calendar",
                    "source_version": "cal-v0",
                }
            ]
        ),
        paths.silver / "b3_holiday_calendar",
        primary_keys=["ref_date", "calendar_id"],
    )

    status = run_b3_research_spine(
        repo_root=tmp_path,
        start=start,
        end=end,
        panels=["futures_contract_daily"],
    )

    assert status["futures_contract_daily"] == "written: 1 rows"
    panel = io_module.read_parquet_root(gold_panel_root(paths, "futures_contract_daily"))
    row = panel.row(0, named=True)
    assert row["calendar_source"] == "b3_holiday_calendar"
    assert row["business_days_to_maturity"] == 2


def test_partitioned_parquet_read_prunes_unrelated_years(tmp_path, monkeypatch):
    root = tmp_path / "silver" / "b3_cotahist_yearly"
    _write_partition(root, 2023, "OLD")
    _write_partition(root, 2024, "NEW")
    scanned, globbed = _track_scan_and_glob(monkeypatch)

    frame = io_module.read_parquet_root(
        root,
        start=date(2024, 1, 1),
        end=date(2024, 12, 31),
    )

    assert frame["symbol"].to_list() == ["NEW"]
    assert any("year=2024" in path for path in scanned)
    assert not any("year=2023" in path for path in scanned)
    assert (root, "**/*.parquet") not in globbed
    assert (root / "year=2023", "**/*.parquet") not in globbed
    assert (root / "year=2024", "**/*.parquet") in globbed


def test_nested_year_partitioned_parquet_read_prunes_unrelated_years(
    tmp_path,
    monkeypatch,
):
    root = tmp_path / "silver" / "b3_cotahist_yearly"
    _write_nested_partition(root, "a", 2023, "OLD")
    _write_nested_partition(root, "a", 2024, "NEW_A")
    _write_nested_partition(root, "b", 2024, "NEW_B")
    scanned, globbed = _track_scan_and_glob(monkeypatch)

    frame = io_module.read_parquet_root(
        root,
        start=date(2024, 1, 1),
        end=date(2024, 12, 31),
    )

    assert frame.sort("symbol")["symbol"].to_list() == ["NEW_A", "NEW_B"]
    assert any("year=2024" in path for path in scanned)
    assert not any("year=2023" in path for path in scanned)
    parquet_globs = [path for path, pattern in globbed if pattern == "**/*.parquet"]
    assert any(path.name == "year=2024" for path in parquet_globs)
    assert not any(path.name == "year=2023" for path in parquet_globs)


def test_partitioned_parquet_read_prunes_start_open_range(tmp_path, monkeypatch):
    root = tmp_path / "silver" / "b3_cotahist_yearly"
    _write_partition(root, 2023, "OLD")
    _write_partition(root, 2024, "NEW")
    _write_partition(root, 2025, "FUTURE")
    scanned, _ = _track_scan_and_glob(monkeypatch)

    frame = io_module.read_parquet_root(root, start=date(2024, 1, 1))

    assert frame.sort("ref_date")["symbol"].to_list() == ["NEW", "FUTURE"]
    assert not any("year=2023" in path for path in scanned)
    assert any("year=2024" in path for path in scanned)
    assert any("year=2025" in path for path in scanned)


def test_partitioned_parquet_read_prunes_end_open_range(tmp_path, monkeypatch):
    root = tmp_path / "silver" / "b3_cotahist_yearly"
    _write_partition(root, 2023, "OLD")
    _write_partition(root, 2024, "NEW")
    _write_partition(root, 2025, "FUTURE")
    scanned, _ = _track_scan_and_glob(monkeypatch)

    frame = io_module.read_parquet_root(root, end=date(2024, 12, 31))

    assert frame.sort("ref_date")["symbol"].to_list() == ["OLD", "NEW"]
    assert any("year=2023" in path for path in scanned)
    assert any("year=2024" in path for path in scanned)
    assert not any("year=2025" in path for path in scanned)


def test_nonpartitioned_parquet_read_still_filters_dates(tmp_path):
    root = tmp_path / "silver" / "flat_source"
    root.mkdir(parents=True)
    pl.DataFrame(
        [
            {"ref_date": date(2023, 1, 2), "symbol": "OLD"},
            {"ref_date": date(2024, 1, 2), "symbol": "NEW"},
        ]
    ).write_parquet(root / "data.parquet")

    frame = io_module.read_parquet_root(
        root,
        start=date(2024, 1, 1),
        end=date(2024, 12, 31),
    )

    assert frame["symbol"].to_list() == ["NEW"]


def _write_partition(root: Path, year: int, symbol: str) -> None:
    (root / f"year={year}").mkdir(parents=True)
    pl.DataFrame(
        [{"ref_date": date(year, 1, 2), "symbol": symbol}]
    ).write_parquet(root / f"year={year}" / "data.parquet")


def _write_nested_partition(root: Path, group: str, year: int, symbol: str) -> None:
    part_dir = root / f"some_key={group}" / f"year={year}"
    part_dir.mkdir(parents=True)
    pl.DataFrame(
        [{"ref_date": date(year, 1, 2), "symbol": symbol}]
    ).write_parquet(part_dir / "data.parquet")


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
