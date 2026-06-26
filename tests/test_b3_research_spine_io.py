from __future__ import annotations

import shutil
from datetime import date

import polars as pl
import pytest

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
