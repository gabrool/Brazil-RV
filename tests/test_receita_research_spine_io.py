from __future__ import annotations

import shutil
from datetime import date

import polars as pl
import pytest

from bralpha.derived.receita.io import (
    ReceitaResearchInputMissingError,
    gold_panel_root,
    read_silver_dataset,
    write_gold_panel,
)
from bralpha.derived.receita.schemas import PANEL_PRIMARY_KEYS
from bralpha.derived.receita.tax_collection import build_tax_collection_observation
from bralpha.infra.config import (
    load_paths_config,
    load_receita_research_config,
    resolve_project_paths,
)
from bralpha.pipelines.receita_research_spine import run_receita_research_spine


def test_receita_research_config_loads_and_gold_root_is_source_specific(repo_root):
    config = load_receita_research_config(repo_root).receita_research
    paths = resolve_project_paths(repo_root, load_paths_config(repo_root))

    assert config.calendar.default == "business_days_mon_fri"
    assert config.tax_collection.max_features == 20000
    assert config.asof.include_state_asof_daily is True
    assert config.daily_long.include_tax_collection is True
    assert gold_panel_root(paths, "daily_long") == paths.gold / "receita" / "daily_long"


def test_receita_read_silver_prunes_year_partitions(repo_root, tmp_path):
    shutil.copytree(repo_root / "configs", tmp_path / "configs")
    paths = resolve_project_paths(tmp_path, load_paths_config(tmp_path))
    silver_root = paths.silver / "receita_tax_collection_monthly"
    _write_silver_partition(silver_root, 2023, [_silver_row(2023)])
    _write_silver_partition(silver_root, 2024, [_silver_row(2024)])

    frame = read_silver_dataset(
        paths,
        "receita_tax_collection_monthly",
        start=date(2024, 1, 1),
        end=date(2024, 12, 31),
    )

    assert frame is not None
    assert frame["year"].to_list() == [2024]


def test_receita_gold_write_upserts_exact_primary_keys(repo_root, tmp_path):
    shutil.copytree(repo_root / "configs", tmp_path / "configs")
    paths = resolve_project_paths(tmp_path, load_paths_config(tmp_path))
    panel = build_tax_collection_observation(pl.DataFrame([_silver_row(2024)]))
    updated = panel.with_columns(collection_amount_brl=pl.lit(22.0))

    write_gold_panel(
        panel,
        paths,
        panel="tax_collection_observation",
        primary_keys=PANEL_PRIMARY_KEYS["tax_collection_observation"],
    )
    write_gold_panel(
        updated,
        paths,
        panel="tax_collection_observation",
        primary_keys=PANEL_PRIMARY_KEYS["tax_collection_observation"],
    )

    stored = pl.read_parquet(
        paths.gold / "receita" / "tax_collection_observation" / "year=2024" / "data.parquet"
    )
    assert stored.height == 1
    assert stored["collection_amount_brl"].to_list() == [22.0]


def test_receita_pipeline_skips_missing_optional_inputs_in_full_run(repo_root, tmp_path):
    shutil.copytree(repo_root / "configs", tmp_path / "configs")

    status = run_receita_research_spine(
        repo_root=tmp_path,
        start=date(2024, 1, 1),
        end=date(2024, 12, 31),
    )

    assert all(value.startswith("skipped:") for value in status.values())
    assert not (tmp_path / "data" / "gold" / "receita").exists()


def test_receita_pipeline_explicit_panel_raises_on_missing_required_input(repo_root, tmp_path):
    shutil.copytree(repo_root / "configs", tmp_path / "configs")

    with pytest.raises(ReceitaResearchInputMissingError, match="receita_tax_collection_monthly"):
        run_receita_research_spine(
            repo_root=tmp_path,
            start=date(2024, 1, 1),
            end=date(2024, 12, 31),
            panels=["tax_collection_observation"],
        )


def test_receita_pipeline_writes_gold_without_mutating_silver(repo_root, tmp_path):
    shutil.copytree(repo_root / "configs", tmp_path / "configs")
    paths = resolve_project_paths(tmp_path, load_paths_config(tmp_path))
    silver_root = paths.silver / "receita_tax_collection_monthly"
    _write_silver_partition(silver_root, 2024, [_silver_row(2024)])
    silver_file = silver_root / "year=2024" / "data.parquet"
    before = silver_file.read_bytes()

    status = run_receita_research_spine(
        repo_root=tmp_path,
        start=date(2024, 1, 1),
        end=date(2024, 12, 31),
        panels=["tax_collection_observation", "tax_collection_feature_observation"],
    )

    assert status["tax_collection_observation"] == "written: 1 rows"
    assert status["tax_collection_feature_observation"] == "written: 1 rows"
    assert silver_file.read_bytes() == before
    assert (paths.gold / "receita" / "tax_collection_observation" / "year=2024").exists()


def _write_silver_partition(root, year: int, rows: list[dict[str, object]]) -> None:
    part = root / f"year={year}"
    part.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(rows).write_parquet(part / "data.parquet")


def _silver_row(year: int) -> dict[str, object]:
    return {
        "ref_date": date(year, 1, 31),
        "available_date": date(year, 3, 7),
        "availability_policy": "receita_monthly_collection_conservative_next_month_end_plus_5bd",
        "year": year,
        "month": 1,
        "collection_scope": "federal_total",
        "revenue_category": "IR",
        "revenue_subcategory": "IRPJ",
        "revenue_code": "001",
        "revenue_key": "001_irpj",
        "revenue_name": "IRPJ",
        "table_kind": "principal",
        "collection_amount_brl": 10.0,
        "unit": "BRL",
        "source_table": "arrecadacao",
        "source": "receita",
        "source_dataset": "receita_tax_collection_monthly",
        "download_timestamp_utc": "2024-03-08T12:00:00Z",
        "raw_path": "data/raw/receita/file.csv",
        "sha256": "abc",
        "source_version": "v0",
    }
