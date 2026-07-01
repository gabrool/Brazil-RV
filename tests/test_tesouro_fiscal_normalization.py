from __future__ import annotations

from datetime import UTC, date, datetime

import polars as pl

from bralpha.domain.b3_calendar import add_business_days
from bralpha.normalization.tesouro_fiscal import (
    TESOURO_DPF_STOCK_COLUMNS,
    normalize_dpf_stock_to_silver,
)


def test_tesouro_dpf_stock_uses_45_b3_business_day_lag():
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
    expected_ref_date = date(2024, 1, 31)
    assert row["ref_date"] == expected_ref_date
    assert row["available_date"] == add_business_days(expected_ref_date, 45)
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
