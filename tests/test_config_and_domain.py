from __future__ import annotations

from datetime import date

import pytest
from pydantic import ValidationError

from bralpha.domain.b3_calendar import is_business_day, next_business_day, previous_business_day
from bralpha.domain.b3_contracts import build_b3_contract_id
from bralpha.domain.b3_month_codes import MONTH_CODES, parse_b3_maturity_code
from bralpha.domain.instruments import InstrumentRegistry, asset_class_for_root
from bralpha.infra.config import (
    load_b3_dataset_registry,
    load_instruments_config,
    load_paths_config,
    load_project_config,
    resolve_project_paths,
)
from bralpha.metadata.datasets import DatasetRegistry


def test_config_files_load(repo_root):
    project = load_project_config(repo_root)
    paths = load_paths_config(repo_root)
    instruments = load_instruments_config(repo_root)
    registry = load_b3_dataset_registry(repo_root)

    assert project.project.package == "bralpha"
    assert paths.paths.raw.as_posix() == "data/raw"
    assert instruments.horizons == [1, 5, 20]
    assert registry.get("b3_futures_settlements").canonical_table == "market_daily"


def test_duplicate_dataset_ids_fail(repo_root):
    data = load_b3_dataset_registry(repo_root).model_dump(mode="json")
    data["datasets"].append(data["datasets"][0])

    with pytest.raises(ValidationError, match="Duplicate B3 dataset_id"):
        DatasetRegistry.model_validate(data)


def test_missing_required_dataset_fields_fail(repo_root):
    data = load_b3_dataset_registry(repo_root).model_dump(mode="json")
    data["datasets"][0].pop("primary_keys")

    with pytest.raises(ValidationError):
        DatasetRegistry.model_validate(data)


def test_paths_resolve_without_creating_directories(repo_root, tmp_path):
    paths = load_paths_config(repo_root)
    resolved = resolve_project_paths(tmp_path, paths)

    assert resolved.raw == tmp_path / "data" / "raw"
    assert not resolved.raw.exists()


def test_all_month_codes_parse():
    for month_code, month in MONTH_CODES.items():
        maturity = parse_b3_maturity_code(f"{month_code.lower()}26")
        assert maturity.month == month
        assert maturity.year == 2026


def test_invalid_maturity_codes_fail():
    for code in ["A26", "F2", "F2X", "", "FF26"]:
        with pytest.raises(ValueError):
            parse_b3_maturity_code(code)


def test_contract_ids_are_deterministic():
    assert build_b3_contract_id("di1", "f26") == "DI1_F26"
    assert build_b3_contract_id("DOL", "M25") == "DOL_M25"


def test_instrument_roots_map_from_config(repo_root):
    instruments = load_instruments_config(repo_root)
    registry = InstrumentRegistry.from_config(instruments)

    assert asset_class_for_root("DI1", registry) == "rates"
    assert asset_class_for_root("wdo", registry) == "fx"
    assert asset_class_for_root("WIN", registry) == "equity_index"
    assert asset_class_for_root("UNKNOWN", registry) is None


def test_calendar_helpers_handle_weekends_and_holidays():
    holidays = {date(2024, 1, 1)}

    assert not is_business_day(date(2024, 1, 6), holidays)
    assert not is_business_day(date(2024, 1, 1), holidays)
    assert next_business_day(date(2023, 12, 29), holidays) == date(2024, 1, 2)
    assert previous_business_day(date(2024, 1, 2), holidays) == date(2023, 12, 29)
