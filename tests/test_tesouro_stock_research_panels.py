from __future__ import annotations

from datetime import date

import polars as pl

from bralpha.derived.tesouro.schemas import (
    TESOURO_DIRETO_STOCK_ASOF_DAILY_COLUMNS,
    TESOURO_DIRETO_STOCK_OBSERVATION_COLUMNS,
    TESOURO_DPF_STOCK_ASOF_DAILY_COLUMNS,
    TESOURO_DPF_STOCK_OBSERVATION_COLUMNS,
)
from bralpha.derived.tesouro.stock import (
    build_direto_stock_asof_daily,
    build_direto_stock_observation,
    build_dpf_stock_asof_daily,
    build_dpf_stock_observation,
)


def test_direto_stock_observation_preserves_official_stock_values():
    panel = build_direto_stock_observation(pl.DataFrame([_direto_stock_row()]))
    row = panel.row(0, named=True)

    assert panel.columns == TESOURO_DIRETO_STOCK_OBSERVATION_COLUMNS
    assert row["quantity"] == 100.0
    assert row["stock_value"] == 98765.43
    assert row["investor_count"] == 9
    assert row["feature_id"] == (
        "tesouro_direto_stock|tesouro_prefixado|tesouro_prefixado|2027-01-01"
    )


def test_direto_stock_asof_uses_latest_available_monthly_state():
    observations = build_direto_stock_observation(pl.DataFrame([_direto_stock_row()]))

    panel = build_direto_stock_asof_daily(
        observations,
        start=date(2024, 2, 28),
        end=date(2024, 3, 4),
        max_dense_keys=10000,
    ).sort("ref_date")

    assert panel.columns == TESOURO_DIRETO_STOCK_ASOF_DAILY_COLUMNS
    assert panel["ref_date"].to_list() == [date(2024, 3, 1), date(2024, 3, 4)]
    assert panel["observation_ref_date"].to_list() == [date(2024, 1, 31), date(2024, 1, 31)]
    assert panel["staleness_days"].to_list() == [0, 3]
    assert panel["stock_value"].to_list() == [98765.43, 98765.43]


def test_dpf_stock_observation_and_asof_preserve_official_categories():
    observations = build_dpf_stock_observation(pl.DataFrame([_dpf_stock_row()]))
    row = observations.row(0, named=True)

    assert observations.columns == TESOURO_DPF_STOCK_OBSERVATION_COLUMNS
    assert row["debt_category"] == "DPMFi"
    assert row["instrument_type"] == "LFT"
    assert row["indexer"] == "Selic"
    assert row["holder_or_maturity_bucket"] == "0 a 1 ano"
    assert row["stock_value"] == 123456.78
    assert row["feature_id"] == "tesouro_dpf_stock|dpmfi|lft|selic|0_a_1_ano"

    panel = build_dpf_stock_asof_daily(
        observations,
        start=date(2024, 3, 15),
        end=date(2024, 3, 19),
        max_dense_keys=10000,
    ).sort("ref_date")

    assert panel.columns == TESOURO_DPF_STOCK_ASOF_DAILY_COLUMNS
    assert panel["ref_date"].to_list() == [date(2024, 3, 18), date(2024, 3, 19)]
    assert panel["observation_ref_date"].to_list() == [date(2024, 1, 31), date(2024, 1, 31)]
    assert panel["staleness_days"].to_list() == [0, 1]
    assert panel.filter(pl.col("observation_available_date") > pl.col("ref_date")).is_empty()


def test_stock_asof_emits_no_rows_before_first_availability():
    observations = build_direto_stock_observation(pl.DataFrame([_direto_stock_row()]))

    panel = build_direto_stock_asof_daily(
        observations,
        start=date(2024, 2, 28),
        end=date(2024, 2, 29),
        max_dense_keys=10000,
    )

    assert panel.is_empty()


def _direto_stock_row() -> dict[str, object]:
    return {
        "ref_date": date(2024, 1, 31),
        "available_date": date(2024, 3, 1),
        "security_name": "Tesouro Prefixado",
        "security_type": "Tesouro Prefixado",
        "maturity_date": date(2027, 1, 1),
        "quantity": 100.0,
        "stock_value": 98765.43,
        "investor_count": 9,
        "unit": "BRL",
        "source_version": "v0",
    }


def _dpf_stock_row() -> dict[str, object]:
    return {
        "ref_date": date(2024, 1, 31),
        "available_date": date(2024, 3, 18),
        "debt_category": "DPMFi",
        "instrument_type": "LFT",
        "indexer": "Selic",
        "holder_or_maturity_bucket": "0 a 1 ano",
        "stock_value": 123456.78,
        "unit": "BRL",
        "source_version": "v0",
    }
