from __future__ import annotations

from datetime import date, datetime

import polars as pl
import pytest

from bralpha.normalization.receita_revenue import (
    RECEITA_COLLECTION_AVAILABILITY_POLICY,
    ReceitaNormalizationError,
    normalize_receita_tax_collection_monthly,
)


def test_receita_long_layout_maps_fields_and_conservative_availability():
    silver = normalize_receita_tax_collection_monthly(_long_bronze())

    row = silver.to_dicts()[0]
    assert row["ref_date"] == date(2024, 1, 31)
    assert row["available_date"] == date(2024, 3, 7)
    assert row["availability_policy"] == RECEITA_COLLECTION_AVAILABILITY_POLICY
    assert row["collection_scope"] == "federal_total"
    assert row["revenue_category"] == "Imposto de Renda"
    assert row["revenue_code"] == "001"
    assert row["revenue_name"] == "IRPJ"
    assert row["collection_amount_brl"] == 10.5
    assert row["unit"] == "BRL"
    assert row["download_timestamp_utc"] == datetime(2024, 3, 8, 12)


def test_receita_wide_layout_unpivots_month_columns_to_long_rows():
    silver = normalize_receita_tax_collection_monthly(_wide_bronze())

    assert silver["ref_date"].to_list() == [date(2024, 1, 31), date(2024, 2, 29)]
    assert silver["collection_amount_brl"].to_list() == [10.0, 20.0]
    assert set(silver["table_kind"].to_list()) == {"by_tax"}


def test_receita_missing_code_gets_stable_revenue_key_for_primary_keys():
    silver = normalize_receita_tax_collection_monthly(
        _long_bronze().drop("raw_codigo_receita").with_columns(raw_descricao=pl.lit("COFINS"))
    )

    row = silver.to_dicts()[0]
    assert row["revenue_code"] == "unknown"
    assert "cofins" in row["revenue_key"]


def test_receita_ambiguous_layout_raises_instead_of_guessing():
    bronze = pl.DataFrame(
        {
            "source": ["receita"],
            "source_dataset": ["receita_tax_collection_monthly"],
            "resource_name": ["ambiguous"],
            "resource_family": ["tax_collection_monthly"],
            "row_index": [0],
            "raw_categoria": ["IR"],
        }
    )

    with pytest.raises(ReceitaNormalizationError, match="period/value"):
        normalize_receita_tax_collection_monthly(bronze)


def _long_bronze() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "source": ["receita"],
            "source_dataset": ["receita_tax_collection_monthly"],
            "download_timestamp_utc": [datetime(2024, 3, 8, 12)],
            "raw_path": ["raw.csv"],
            "sha256": ["abc"],
            "resource_name": ["resultado-arrecadacao"],
            "resource_family": ["tax_collection_monthly"],
            "sheet_name": [None],
            "inner_filename": [None],
            "row_index": [0],
            "year": [2024],
            "raw_ano": ["2024"],
            "raw_mes": ["1"],
            "raw_categoria": ["Imposto de Renda"],
            "raw_codigo_receita": ["001"],
            "raw_descricao": ["IRPJ"],
            "raw_valor_arrecadado": ["10,5"],
        }
    )


def _wide_bronze() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "source": ["receita"],
            "source_dataset": ["receita_tax_collection_monthly"],
            "download_timestamp_utc": [datetime(2024, 3, 8, 12)],
            "raw_path": ["raw.csv"],
            "sha256": ["abc"],
            "resource_name": ["arrecadacao_por_receita"],
            "resource_family": ["tax_collection_monthly"],
            "sheet_name": [None],
            "inner_filename": [None],
            "row_index": [0],
            "year": [2024],
            "raw_ano": ["2024"],
            "raw_categoria": ["Receitas Federais"],
            "raw_codigo": ["002"],
            "raw_receita": ["IPI"],
            "raw_janeiro": ["10,0"],
            "raw_fevereiro": ["20,0"],
        }
    )
