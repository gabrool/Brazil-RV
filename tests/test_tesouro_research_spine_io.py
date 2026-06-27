from __future__ import annotations

import shutil
from datetime import date
from pathlib import Path

import polars as pl
import pytest

import bralpha.derived.tesouro.io as io_module
from bralpha.derived.tesouro.io import gold_panel_root, read_parquet_root, write_gold_panel
from bralpha.derived.tesouro.schemas import PANEL_PRIMARY_KEYS
from bralpha.infra.config import (
    load_paths_config,
    load_tesouro_research_config,
    resolve_project_paths,
)
from bralpha.pipelines.tesouro_research_spine import run_tesouro_research_spine


def test_tesouro_research_config_loads(repo_root):
    config = load_tesouro_research_config(repo_root).tesouro_research

    assert config.calendar.default == "business_days_mon_fri"
    assert config.prices_rates.max_dense_securities == 5000
    assert config.flows.include_sales is True
    assert config.flows.include_redemptions is True
    assert config.stock.max_dense_keys == 10000
    assert config.daily_long.include_prices_rates is True
    assert config.daily_long.include_flows is True
    assert config.daily_long.include_stock is True


def test_tesouro_gold_output_path_stays_under_data_gold_tesouro(repo_root, tmp_path):
    paths = resolve_project_paths(tmp_path, load_paths_config(repo_root))
    frame = pl.DataFrame(
        [
            {
                "ref_date": date(2024, 1, 2),
                "security_name": "Tesouro Prefixado",
                "maturity_date": date(2027, 1, 1),
                "value": 10.0,
            }
        ]
    )

    written = write_gold_panel(
        frame,
        paths,
        panel="direto_prices_rates_observation",
        primary_keys=["ref_date", "security_name", "maturity_date"],
    )

    expected_root = tmp_path / "data" / "gold" / "tesouro" / "direto_prices_rates_observation"
    assert gold_panel_root(paths, "direto_prices_rates_observation") == expected_root
    assert written[0].is_relative_to(expected_root)
    assert not (tmp_path / "data" / "silver" / "direto_prices_rates_observation").exists()


def test_tesouro_missing_inputs_skip_full_pipeline_but_selected_panel_raises(repo_root, tmp_path):
    shutil.copytree(repo_root / "configs", tmp_path / "configs")

    status = run_tesouro_research_spine(
        repo_root=tmp_path,
        start=date(2024, 1, 1),
        end=date(2024, 1, 31),
    )

    assert status["direto_prices_rates_observation"].startswith("skipped:")
    with pytest.raises(FileNotFoundError, match="tesouro_direto_prices_rates"):
        run_tesouro_research_spine(
            repo_root=tmp_path,
            start=date(2024, 1, 1),
            end=date(2024, 1, 31),
            panels=["direto_prices_rates_observation"],
        )


def test_tesouro_partitioned_parquet_read_prunes_unrelated_years(tmp_path, monkeypatch):
    root = tmp_path / "silver" / "tesouro_direto_prices_rates"
    _write_partition(root, 2023, 10.0)
    _write_partition(root, 2024, 11.0)
    scanned, globbed = _track_scan_and_glob(monkeypatch)

    frame = io_module.read_parquet_root(
        root,
        start=date(2024, 1, 1),
        end=date(2024, 12, 31),
    )

    assert frame["buy_rate"].to_list() == [11.0]
    assert any("year=2024" in path for path in scanned)
    assert not any("year=2023" in path for path in scanned)
    assert (root / "year=2024", "**/*.parquet") in globbed
    assert (root / "year=2023", "**/*.parquet") not in globbed


def test_tesouro_nested_year_partition_read_prunes_unrelated_years(tmp_path, monkeypatch):
    root = tmp_path / "silver" / "tesouro_direto_redemptions"
    _write_nested_partition(root, "early_repurchase", 2023, 10.0)
    _write_nested_partition(root, "early_repurchase", 2024, 20.0)
    _write_nested_partition(root, "maturity", 2024, 21.0)
    scanned, globbed = _track_scan_and_glob(monkeypatch)

    frame = io_module.read_parquet_root(
        root,
        start=date(2024, 1, 1),
        end=date(2024, 12, 31),
    )

    assert frame.sort("redemption_type")["value"].to_list() == [20.0, 21.0]
    assert any("year=2024" in path for path in scanned)
    assert not any("year=2023" in path for path in scanned)
    parquet_globs = [path for path, pattern in globbed if pattern == "**/*.parquet"]
    assert any(path.name == "year=2024" for path in parquet_globs)
    assert not any(path.name == "year=2023" for path in parquet_globs)


def test_tesouro_gold_writes_use_exact_panel_primary_keys(repo_root, tmp_path):
    paths = resolve_project_paths(tmp_path, load_paths_config(repo_root))
    first = _flow_gold_row(value=10.0, source_dataset="sales_first")
    second = _flow_gold_row(value=11.0, source_dataset="sales_second")

    write_gold_panel(
        pl.DataFrame([first]),
        paths,
        panel="direto_flows_daily",
        primary_keys=PANEL_PRIMARY_KEYS["direto_flows_daily"],
    )
    write_gold_panel(
        pl.DataFrame([second]),
        paths,
        panel="direto_flows_daily",
        primary_keys=PANEL_PRIMARY_KEYS["direto_flows_daily"],
    )

    frame = read_parquet_root(gold_panel_root(paths, "direto_flows_daily"))
    assert frame.height == 1
    assert frame["value"].item() == 11.0
    assert frame["source_dataset"].item() == "sales_second"


def _write_partition(root: Path, year: int, value: float) -> None:
    (root / f"year={year}").mkdir(parents=True)
    pl.DataFrame(
        [{"ref_date": date(year, 1, 2), "security_name": "A", "buy_rate": value}]
    ).write_parquet(root / f"year={year}" / "data.parquet")


def _write_nested_partition(
    root: Path,
    redemption_type: str,
    year: int,
    value: float,
) -> None:
    part_dir = root / f"redemption_type={redemption_type}" / f"year={year}"
    part_dir.mkdir(parents=True)
    pl.DataFrame(
        [
            {
                "ref_date": date(year, 1, 2),
                "redemption_type": redemption_type,
                "value": value,
            }
        ]
    ).write_parquet(part_dir / "data.parquet")


def _flow_gold_row(*, value: float, source_dataset: str) -> dict[str, object]:
    return {
        "ref_date": date(2024, 1, 2),
        "available_date": date(2024, 1, 2),
        "observation_ref_date": date(2024, 1, 1),
        "observation_available_date": date(2024, 1, 2),
        "flow_type": "sale",
        "redemption_type": None,
        "security_name": "Tesouro Selic",
        "security_type": "Tesouro Selic",
        "maturity_date": date(2027, 3, 1),
        "feature_id": "tesouro_direto_flows|sale|null|tesouro_selic|tesouro_selic|2027-03-01",
        "quantity": 1.0,
        "value": value,
        "investor_count": 2,
        "unit": "BRL",
        "source_dataset": source_dataset,
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
