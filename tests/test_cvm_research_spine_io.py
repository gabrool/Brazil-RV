from __future__ import annotations

import shutil
from datetime import date
from pathlib import Path

import polars as pl
import pytest

import bralpha.derived.cvm.io as io_module
from bralpha.derived.cvm.io import gold_panel_root, read_parquet_root, write_gold_panel
from bralpha.derived.cvm.schemas import PANEL_PRIMARY_KEYS
from bralpha.infra.config import (
    load_cvm_research_config,
    load_paths_config,
    resolve_project_paths,
)
from bralpha.ingestion.cvm.common import write_partitioned_frame
from bralpha.pipelines.cvm_research_spine import run_cvm_research_spine


def test_cvm_research_config_loads(repo_root):
    config = load_cvm_research_config(repo_root).cvm_research

    assert config.calendar.default == "b3_trading_calendar"
    assert config.fund_reports.group_by == ["all", "fund_type"]
    assert config.fund_reports.max_groups == 100
    assert config.fund_reports.include_per_fund_observation is True
    assert config.fund_reports.include_group_observation is True
    assert config.fund_reports.include_flow_daily is True
    assert config.fund_reports.include_state_asof_daily is True
    assert config.registry.include_current_reference is True
    assert config.daily_long.include_fund_flows is True
    assert config.daily_long.include_fund_state is True


def test_cvm_gold_output_path_stays_under_data_gold_cvm(repo_root, tmp_path):
    paths = resolve_project_paths(tmp_path, load_paths_config(repo_root))
    frame = pl.DataFrame(
        [{"ref_date": date(2024, 1, 2), "fund_id": "00", "portfolio_value": 1.0}]
    )

    written = write_gold_panel(
        frame,
        paths,
        panel="fund_daily_observation",
        primary_keys=["ref_date", "fund_id"],
    )

    expected_root = tmp_path / "data" / "gold" / "cvm" / "fund_daily_observation"
    assert gold_panel_root(paths, "fund_daily_observation") == expected_root
    assert written[0].is_relative_to(expected_root)
    assert not (tmp_path / "data" / "silver" / "fund_daily_observation").exists()


def test_cvm_nested_year_month_partition_read_prunes_unrelated_years(tmp_path, monkeypatch):
    root = tmp_path / "silver" / "cvm_fund_daily_reports"
    _write_nested_daily_partition(root, 2023, 12, 10.0)
    _write_nested_daily_partition(root, 2024, 1, 20.0)
    _write_nested_daily_partition(root, 2024, 2, 30.0)
    scanned, globbed = _track_scan_and_glob(monkeypatch)

    frame = io_module.read_parquet_root(
        root,
        start=date(2024, 1, 1),
        end=date(2024, 12, 31),
    )

    assert frame.sort("portfolio_value")["portfolio_value"].to_list() == [20.0, 30.0]
    assert any("year=2024" in path for path in scanned)
    assert not any("year=2023" in path for path in scanned)
    parquet_globs = [path for path, pattern in globbed if pattern == "**/*.parquet"]
    assert any(path.name == "year=2024" for path in parquet_globs)
    assert not any(path.name == "year=2023" for path in parquet_globs)


def test_cvm_gold_writes_use_exact_panel_primary_keys(repo_root, tmp_path):
    paths = resolve_project_paths(tmp_path, load_paths_config(repo_root))
    first = _daily_gold_row(portfolio_value=1.0, source_version="first")
    second = _daily_gold_row(portfolio_value=2.0, source_version="second")

    write_gold_panel(
        pl.DataFrame([first]),
        paths,
        panel="fund_daily_observation",
        primary_keys=PANEL_PRIMARY_KEYS["fund_daily_observation"],
    )
    write_gold_panel(
        pl.DataFrame([second]),
        paths,
        panel="fund_daily_observation",
        primary_keys=PANEL_PRIMARY_KEYS["fund_daily_observation"],
    )

    frame = read_parquet_root(gold_panel_root(paths, "fund_daily_observation"))
    assert frame.height == 1
    assert frame["portfolio_value"].item() == 2.0
    assert frame["source_version"].item() == "second"


def test_cvm_missing_inputs_skip_full_pipeline_but_selected_panel_raises(repo_root, tmp_path):
    shutil.copytree(repo_root / "configs", tmp_path / "configs")

    status = run_cvm_research_spine(
        repo_root=tmp_path,
        start=date(2024, 1, 1),
        end=date(2024, 1, 31),
    )

    assert status["fund_daily_observation"].startswith("skipped:")
    assert status["fund_registry_current_reference"].startswith("skipped:")
    with pytest.raises(FileNotFoundError, match="cvm_fund_daily_reports"):
        run_cvm_research_spine(
            repo_root=tmp_path,
            start=date(2024, 1, 1),
            end=date(2024, 1, 31),
            panels=["fund_daily_observation"],
        )


def test_cvm_pipeline_writes_gold_outputs_from_silver_without_mutating_silver(repo_root, tmp_path):
    shutil.copytree(repo_root / "configs", tmp_path / "configs")
    write_partitioned_frame(
        pl.DataFrame(
            [
                _silver_daily_row(
                    fund_id="00.000.000/0001-00",
                    fund_type="FI",
                    ref_date=date(2024, 1, 2),
                    available_date=date(2024, 1, 4),
                    portfolio_value=100.0,
                    nav=90.0,
                    subscriptions=5.0,
                    redemptions=1.0,
                    shareholder_count=10,
                )
            ]
        ),
        tmp_path / "data" / "silver" / "cvm_fund_daily_reports",
        primary_keys=["ref_date", "fund_id"],
        ref_date_col="ref_date",
        partition_cols=["year", "month"],
    )
    silver_before = pl.read_parquet(
        tmp_path
        / "data"
        / "silver"
        / "cvm_fund_daily_reports"
        / "year=2024"
        / "month=1"
        / "data.parquet"
    )

    status = run_cvm_research_spine(
        repo_root=tmp_path,
        start=date(2024, 1, 2),
        end=date(2024, 1, 5),
        panels=[
            "fund_daily_observation",
            "fund_group_observation",
            "fund_flows_daily",
            "fund_state_asof_daily",
            "daily_long",
        ],
    )

    assert status["fund_daily_observation"] == "written: 1 rows"
    assert status["fund_group_observation"] == "written: 2 rows"
    assert status["fund_flows_daily"] == "written: 2 rows"
    assert status["fund_state_asof_daily"] == "written: 4 rows"
    assert status["daily_long"].startswith("written:")
    assert (tmp_path / "data" / "gold" / "cvm" / "daily_long").exists()
    silver_after = pl.read_parquet(
        tmp_path
        / "data"
        / "silver"
        / "cvm_fund_daily_reports"
        / "year=2024"
        / "month=1"
        / "data.parquet"
    )
    assert silver_after.equals(silver_before)


def test_cvm_pipeline_uses_pre_window_history_for_flows_state_and_daily_long(
    repo_root,
    tmp_path,
):
    shutil.copytree(repo_root / "configs", tmp_path / "configs")
    write_partitioned_frame(
        pl.DataFrame(
            [
                _silver_daily_row(
                    fund_id="fund-a",
                    fund_type="FI",
                    ref_date=date(2023, 12, 29),
                    available_date=date(2024, 1, 2),
                    portfolio_value=100.0,
                    nav=90.0,
                    subscriptions=5.0,
                    redemptions=1.0,
                    shareholder_count=10,
                ),
                _silver_daily_row(
                    fund_id="fund-a",
                    fund_type="FI",
                    ref_date=date(2024, 1, 3),
                    available_date=date(2024, 1, 5),
                    portfolio_value=110.0,
                    nav=95.0,
                    subscriptions=7.0,
                    redemptions=2.0,
                    shareholder_count=11,
                ),
            ]
        ),
        tmp_path / "data" / "silver" / "cvm_fund_daily_reports",
        primary_keys=["ref_date", "fund_id"],
        ref_date_col="ref_date",
        partition_cols=["year", "month"],
    )
    paths = resolve_project_paths(tmp_path, load_paths_config(repo_root))

    run_cvm_research_spine(
        repo_root=tmp_path,
        start=date(2024, 1, 1),
        end=date(2024, 1, 5),
        panels=[
            "fund_daily_observation",
            "fund_group_observation",
            "fund_flows_daily",
            "fund_state_asof_daily",
            "daily_long",
        ],
    )

    groups = read_parquet_root(gold_panel_root(paths, "fund_group_observation")).sort(
        ["ref_date", "feature_id"]
    )
    assert date(2023, 12, 29) not in groups["ref_date"].to_list()
    assert set(groups["ref_date"].to_list()) == {date(2024, 1, 3)}

    flows = read_parquet_root(gold_panel_root(paths, "fund_flows_daily")).filter(
        pl.col("feature_id") == "cvm_fund_group|all|all"
    )
    pre_window_flow = flows.filter(
        (pl.col("ref_date") == date(2024, 1, 2))
        & (pl.col("observation_ref_date") == date(2023, 12, 29))
    ).row(0, named=True)
    assert pre_window_flow["observation_available_date"] == date(2024, 1, 2)
    assert pre_window_flow["subscriptions"] == 5.0
    assert pre_window_flow["redemptions"] == 1.0
    assert flows.filter(
        (pl.col("ref_date") == date(2024, 1, 5))
        & (pl.col("observation_ref_date") == date(2024, 1, 3))
    ).height == 1

    state = (
        read_parquet_root(gold_panel_root(paths, "fund_state_asof_daily"))
        .filter(pl.col("feature_id") == "cvm_fund_group|all|all")
        .sort("ref_date")
    )
    assert state["ref_date"].to_list() == [
        date(2024, 1, 2),
        date(2024, 1, 3),
        date(2024, 1, 4),
        date(2024, 1, 5),
    ]
    assert state["portfolio_value"].to_list() == [100.0, 100.0, 100.0, 110.0]
    assert state["staleness_days"].to_list() == [0, 1, 2, 0]

    daily_long = read_parquet_root(gold_panel_root(paths, "daily_long"))
    long_row = daily_long.filter(
        (pl.col("source_family") == "cvm_fund_flows")
        & (pl.col("feature_id") == "cvm_fund_group|all|all")
        & (pl.col("value_name") == "subscriptions")
        & (pl.col("ref_date") == date(2024, 1, 2))
        & (pl.col("observation_ref_date") == date(2023, 12, 29))
    ).row(0, named=True)
    assert long_row["value"] == 5.0


def _write_nested_daily_partition(
    root: Path,
    year: int,
    month: int,
    portfolio_value: float,
) -> None:
    part_dir = root / f"year={year}" / f"month={month}"
    part_dir.mkdir(parents=True)
    pl.DataFrame(
        [
            {
                "ref_date": date(year, month, 2),
                "fund_id": f"{year}-{month}",
                "portfolio_value": portfolio_value,
            }
        ]
    ).write_parquet(part_dir / "data.parquet")


def _silver_daily_row(
    *,
    fund_id: str,
    fund_type: str | None,
    ref_date: date,
    available_date: date,
    portfolio_value: float | None,
    nav: float | None,
    subscriptions: float | None,
    redemptions: float | None,
    shareholder_count: int | None,
) -> dict[str, object]:
    return {
        "ref_date": ref_date,
        "available_date": available_date,
        "availability_policy": "cvm_fund_daily_conservative_2bd",
        "fund_id": fund_id,
        "fund_type": fund_type,
        "portfolio_value": portfolio_value,
        "nav": nav,
        "quota_value": None,
        "subscriptions": subscriptions,
        "redemptions": redemptions,
        "shareholder_count": shareholder_count,
        "raw_vl_total": None if portfolio_value is None else str(portfolio_value),
        "raw_vl_patrim_liq": None if nav is None else str(nav),
        "raw_vl_quota": None,
        "raw_captc_dia": None if subscriptions is None else str(subscriptions),
        "raw_resg_dia": None if redemptions is None else str(redemptions),
        "raw_nr_cotst": None if shareholder_count is None else str(shareholder_count),
        "source": "cvm",
        "source_dataset": "cvm_fund_daily_reports",
        "download_timestamp_utc": None,
        "raw_path": "raw.zip",
        "sha256": "abc",
        "source_version": "v0",
    }


def _daily_gold_row(*, portfolio_value: float, source_version: str) -> dict[str, object]:
    row = _silver_daily_row(
        fund_id="00.000.000/0001-00",
        fund_type="FI",
        ref_date=date(2024, 1, 2),
        available_date=date(2024, 1, 4),
        portfolio_value=portfolio_value,
        nav=90.0,
        subscriptions=5.0,
        redemptions=1.0,
        shareholder_count=10,
    )
    row.update(
        {
            "has_portfolio_value": True,
            "has_nav": True,
            "has_quota_value": False,
            "has_subscriptions": True,
            "has_redemptions": True,
            "has_shareholder_count": True,
            "source_version": source_version,
        }
    )
    return row


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
