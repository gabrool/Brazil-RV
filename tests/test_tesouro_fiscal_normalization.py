from __future__ import annotations

from datetime import UTC, date, datetime

import polars as pl

from bralpha.normalization.tesouro_fiscal import (
    TESOURO_DPF_STOCK_COLUMNS,
    normalize_dpf_stock_to_silver,
)


def test_tesouro_dpf_stock_uses_45_day_lag_then_next_business_day():
    silver = normalize_dpf_stock_to_silver(
        pl.DataFrame(
            [
                _bronze_row(
                    raw_data_base="01/2024",
                    raw_categoria_divida="DPMFi",
                    raw_tipo_titulo="LFT",
                    raw_indexador="Selic",
                    raw_prazo="0 a 1 ano",
                    raw_estoque="123456,78",
                )
            ]
        )
    )
    row = silver.row(0, named=True)

    assert silver.columns == TESOURO_DPF_STOCK_COLUMNS
    assert row["ref_date"] == date(2024, 1, 1)
    assert row["available_date"] == date(2024, 2, 16)
    assert row["debt_category"] == "DPMFi"
    assert row["instrument_type"] == "LFT"
    assert row["indexer"] == "Selic"
    assert row["holder_or_maturity_bucket"] == "0 a 1 ano"
    assert row["stock_value"] == 123456.78


def _bronze_row(**fields) -> dict[str, object]:
    return {
        "row_index": 0,
        "resource_name": "fixture",
        "raw_fields_json": "{}",
        "source": "tesouro",
        "source_dataset": "tesouro_fixture",
        "download_timestamp_utc": datetime(2024, 1, 2, 12, tzinfo=UTC),
        "raw_path": "raw.csv",
        "sha256": "abc",
        **fields,
    }
