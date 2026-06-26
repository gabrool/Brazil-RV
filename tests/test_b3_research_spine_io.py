from __future__ import annotations

import shutil
from datetime import date

import polars as pl
import pytest

import bralpha.derived.b3.io as io_module
from bralpha.derived.b3.io import gold_panel_root, write_gold_panel
from bralpha.infra.config import (
    load_b3_research_config,
    load_paths_config,
    resolve_project_paths,
)
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


def test_partitioned_parquet_read_prunes_unrelated_years(tmp_path, monkeypatch):
    root = tmp_path / "silver" / "b3_cotahist_yearly"
    (root / "year=2023").mkdir(parents=True)
    (root / "year=2024").mkdir(parents=True)
    pl.DataFrame([{"ref_date": date(2023, 1, 2), "symbol": "OLD"}]).write_parquet(
        root / "year=2023" / "data.parquet"
    )
    pl.DataFrame([{"ref_date": date(2024, 1, 2), "symbol": "NEW"}]).write_parquet(
        root / "year=2024" / "data.parquet"
    )
    scanned: list[str] = []
    original_scan_parquet = io_module.pl.scan_parquet

    def tracking_scan(source, *args, **kwargs):
        scanned.extend(source if isinstance(source, list) else [source])
        return original_scan_parquet(source, *args, **kwargs)

    monkeypatch.setattr(io_module.pl, "scan_parquet", tracking_scan)

    frame = io_module.read_parquet_root(
        root,
        start=date(2024, 1, 1),
        end=date(2024, 12, 31),
    )

    assert frame["symbol"].to_list() == ["NEW"]
    assert any("year=2024" in path for path in scanned)
    assert not any("year=2023" in path for path in scanned)


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
