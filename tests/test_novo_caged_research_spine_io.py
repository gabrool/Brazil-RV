from __future__ import annotations

import shutil
from datetime import date

import polars as pl
import pytest

from bralpha.derived.novo_caged.io import (
    NovoCagedResearchInputMissingError,
    gold_panel_root,
    read_silver_dataset,
    write_gold_panel,
)
from bralpha.infra.config import (
    load_novo_caged_research_config,
    load_paths_config,
    resolve_project_paths,
)
from bralpha.pipelines.novo_caged_research_spine import run_novo_caged_research_spine


def test_novo_caged_research_config_loads_and_gold_root_is_source_specific(repo_root):
    config = load_novo_caged_research_config(repo_root).novo_caged_research
    paths = resolve_project_paths(repo_root, load_paths_config(repo_root))

    assert config.movements.group_by == ["all", "region", "state", "cnae_section"]
    assert gold_panel_root(paths, "daily_long") == paths.gold / "novo_caged" / "daily_long"


def test_novo_caged_read_silver_prunes_year_partitions(repo_root, tmp_path):
    shutil.copytree(repo_root / "configs", tmp_path / "configs")
    paths = resolve_project_paths(tmp_path, load_paths_config(tmp_path))
    _write_partition(
        paths.silver / "novo_caged_movements_monthly" / "year=2023",
        _movement_silver([_movement_row("old", date(2023, 12, 31))]),
    )
    _write_partition(
        paths.silver / "novo_caged_movements_monthly" / "year=2024",
        _movement_silver([_movement_row("new", date(2024, 1, 31))]),
    )

    frame = read_silver_dataset(
        paths,
        "novo_caged_movements_monthly",
        start=date(2024, 1, 1),
        end=date(2024, 12, 31),
    )

    assert frame is not None
    assert frame["movement_record_id"].to_list() == ["new"]


def test_novo_caged_gold_writes_use_exact_primary_key_upserts(repo_root, tmp_path):
    shutil.copytree(repo_root / "configs", tmp_path / "configs")
    paths = resolve_project_paths(tmp_path, load_paths_config(tmp_path))
    first = pl.DataFrame(
        {
            "ref_date": [date(2024, 1, 31)],
            "available_date": [date(2024, 3, 4)],
            "feature_id": ["f"],
            "value": [1.0],
        }
    )
    second = first.with_columns(value=pl.lit(2.0))

    write_gold_panel(
        first,
        paths,
        panel="example",
        primary_keys=["ref_date", "feature_id"],
    )
    write_gold_panel(
        second,
        paths,
        panel="example",
        primary_keys=["ref_date", "feature_id"],
    )

    stored = pl.read_parquet(paths.gold / "novo_caged" / "example" / "year=2024" / "data.parquet")
    assert stored.height == 1
    assert stored["value"].to_list() == [2.0]


def test_novo_caged_full_run_skips_missing_optional_inputs(repo_root, tmp_path):
    shutil.copytree(repo_root / "configs", tmp_path / "configs")

    status = run_novo_caged_research_spine(
        repo_root=tmp_path,
        start=date(2024, 1, 1),
        end=date(2024, 1, 31),
    )

    assert status["movement_record_observation"].startswith("skipped:")
    assert status["release_calendar_reference"].startswith("skipped:")
    assert not (tmp_path / "data" / "gold" / "novo_caged").exists()


def test_novo_caged_explicit_missing_panel_raises(repo_root, tmp_path):
    shutil.copytree(repo_root / "configs", tmp_path / "configs")

    with pytest.raises(NovoCagedResearchInputMissingError):
        run_novo_caged_research_spine(
            repo_root=tmp_path,
            start=date(2024, 1, 1),
            end=date(2024, 1, 31),
            panels=["movement_record_observation"],
        )


def test_novo_caged_pipeline_does_not_mutate_silver_inputs(repo_root, tmp_path):
    shutil.copytree(repo_root / "configs", tmp_path / "configs")
    paths = resolve_project_paths(tmp_path, load_paths_config(tmp_path))
    silver_root = paths.silver / "novo_caged_movements_monthly" / "year=2024"
    _write_partition(silver_root, _movement_silver([_movement_row("r1", date(2024, 1, 31))]))
    before = (silver_root / "data.parquet").read_bytes()

    run_novo_caged_research_spine(
        repo_root=tmp_path,
        start=date(2024, 1, 1),
        end=date(2024, 1, 31),
    )

    assert (silver_root / "data.parquet").read_bytes() == before
    assert (paths.gold / "novo_caged" / "movement_record_observation").exists()


def test_novo_caged_asof_history_prefers_pre_window_release_calendar_silver(
    repo_root,
    tmp_path,
):
    shutil.copytree(repo_root / "configs", tmp_path / "configs")
    paths = resolve_project_paths(tmp_path, load_paths_config(tmp_path))
    _write_partition(
        paths.silver / "novo_caged_movements_monthly" / "year=2023",
        _movement_silver(
            [
                _movement_row(
                    "pre-window",
                    date(2023, 12, 31),
                    available_date=date(2024, 2, 2),
                )
            ]
        ),
    )
    _write_partition(
        paths.silver / "novo_caged_movements_monthly" / "year=2024",
        _movement_silver([_movement_row("in-window", date(2024, 1, 31))]),
    )
    _write_partition(
        paths.silver / "novo_caged_release_calendar" / "release_year=2024",
        _release_calendar_silver(
            [
                _release_row(
                    date(2023, 12, 31),
                    release_date=date(2024, 1, 30),
                    available_date=date(2024, 1, 30),
                    competence_label="dezembro de 2023",
                ),
                _release_row(
                    date(2024, 1, 31),
                    release_date=date(2024, 3, 1),
                    available_date=date(2024, 3, 4),
                    competence_label="janeiro de 2024",
                ),
            ]
        ),
    )

    run_novo_caged_research_spine(
        repo_root=tmp_path,
        start=date(2024, 1, 30),
        end=date(2024, 1, 31),
        panels=[
            "release_calendar_reference",
            "movement_group_observation",
            "state_asof_daily",
        ],
    )

    release_reference = pl.read_parquet(
        paths.gold / "novo_caged" / "release_calendar_reference" / "year=2024" / "data.parquet"
    )
    assert release_reference["ref_date"].to_list() == [date(2024, 1, 31)]
    state = pl.read_parquet(
        paths.gold / "novo_caged" / "state_asof_daily" / "year=2024" / "data.parquet"
    )
    row = state.filter(
        (pl.col("ref_date") == date(2024, 1, 30))
        & (pl.col("feature_id") == "novo_caged_movement|all|all|1")
        & (pl.col("value_name") == "movement_count")
    ).to_dicts()[0]

    assert row["observation_ref_date"] == date(2023, 12, 31)
    assert row["observation_available_date"] == date(2024, 1, 30)
    assert row["staleness_days"] == 0
    assert row["value"] == 1.0


def _write_partition(path, frame: pl.DataFrame) -> None:
    path.mkdir(parents=True, exist_ok=True)
    frame.write_parquet(path / "data.parquet")


def _movement_silver(rows: list[dict[str, object]]) -> pl.DataFrame:
    return pl.DataFrame(rows)


def _movement_row(
    row_id: str,
    ref_date: date,
    *,
    available_date: date = date(2024, 3, 4),
) -> dict[str, object]:
    return {
        "movement_record_id": row_id,
        "ref_date": ref_date,
        "available_date": available_date,
        "availability_policy": "novo_caged_conservative_next_month_end_plus_2bd_reference_only",
        "availability_basis": "conservative_heuristic",
        "revision_policy": "current_snapshot_reference_only",
        "model_usable": False,
        "non_model_usable_reason": "novo_caged_movement_requires_official_release_calendar",
        "competence": f"{ref_date.year}{ref_date.month:02d}",
        "year": ref_date.year,
        "month": ref_date.month,
        "record_kind": "movement",
        "region": "Sudeste",
        "state": "SP",
        "municipality_code": "3550308",
        "cnae_section": "G",
        "cnae_subclass": "4711302",
        "occupation_code": "411005",
        "movement_type_code": "10",
        "movement_sign": "1",
        "employment_category": "101",
        "education_degree": "7",
        "age": 32,
        "sex": "1",
        "race_color": "2",
        "disability_type": "0",
        "employer_type": "0",
        "establishment_type": "1",
        "establishment_size_jan": "5",
        "contract_hours": 44.0,
        "wage": 2500.0,
        "wage_unit": "1",
        "is_apprentice": False,
        "is_intermittent": False,
        "is_part_time": False,
        "source_system": "eSocial",
        "raw_competenciamov": f"{ref_date.year}{ref_date.month:02d}",
        "raw_saldomovimentacao": "1",
        "raw_tipomovimentacao": "10",
        "raw_salario": "2500,00",
        "raw_valorsalariofixo": None,
        "source": "novo_caged",
        "source_dataset": "novo_caged_movements_monthly",
        "download_timestamp_utc": None,
        "raw_path": "raw.7z",
        "sha256": "abc",
        "source_version": "v0",
    }


def _release_calendar_silver(rows: list[dict[str, object]]) -> pl.DataFrame:
    return pl.DataFrame(rows)


def _release_row(
    ref_date: date,
    *,
    release_date: date,
    available_date: date,
    competence_label: str,
) -> dict[str, object]:
    return {
        "ref_date": ref_date,
        "release_date": release_date,
        "available_date": available_date,
        "availability_policy": "novo_caged_official_release_calendar",
        "release_year": release_date.year,
        "competence_label": competence_label,
        "source": "novo_caged",
        "source_dataset": "novo_caged_release_calendar",
        "download_timestamp_utc": None,
        "raw_path": "calendar.html",
        "sha256": "def",
        "source_version": "v0",
    }
