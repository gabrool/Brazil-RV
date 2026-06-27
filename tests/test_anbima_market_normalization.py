from __future__ import annotations

from datetime import UTC, date, datetime

import polars as pl

from bralpha.normalization.anbima_market import (
    ANBIMA_FIXED_INCOME_INDEX_COLUMNS,
    ANBIMA_INFLATION_PROJECTION_COLUMNS,
    ANBIMA_SOVEREIGN_SECONDARY_MARKET_COLUMNS,
    ANBIMA_VNA_COLUMNS,
    ANBIMA_YIELD_CURVE_COLUMNS,
    normalize_fixed_income_indices_to_silver,
    normalize_inflation_projections_to_silver,
    normalize_sovereign_secondary_market_to_silver,
    normalize_vna_to_silver,
    normalize_yield_curves_to_silver,
)


def test_anbima_sovereign_secondary_market_preserves_official_values():
    silver = normalize_sovereign_secondary_market_to_silver(
        pl.DataFrame(
            [
                _bronze_row(
                    raw_data_referencia="2024-01-02",
                    raw_codigo_titulo="LTN_202501",
                    raw_tipo_titulo="LTN",
                    raw_nome_titulo="LTN Jan 2025",
                    raw_data_vencimento="2025-01-01",
                    raw_taxa_indicativa="10.5",
                    raw_taxa_compra="10.4",
                    raw_taxa_venda="10.6",
                    raw_pu="950.25",
                    raw_duracao="0.9",
                )
            ]
        )
    )
    row = silver.row(0, named=True)

    assert silver.columns == ANBIMA_SOVEREIGN_SECONDARY_MARKET_COLUMNS
    assert row["ref_date"] == date(2024, 1, 2)
    assert row["available_date"] == date(2024, 1, 3)
    assert row["security_id"] == "LTN_202501"
    assert row["indicative_rate"] == 10.5
    assert row["pu"] == 950.25
    assert row["source_dataset"] == "anbima_fixture"
    assert silver.group_by(["ref_date", "security_id"]).len().height == silver.height


def test_anbima_yield_curve_preserves_curve_points():
    silver = normalize_yield_curves_to_silver(
        pl.DataFrame(
            [
                _bronze_row(
                    raw_data_referencia="2024-01-02",
                    raw_tipo_curva="pre",
                    raw_prazo_dias="252",
                    raw_prazo="1Y",
                    raw_taxa="10.75",
                    raw_unidade="% a.a.",
                )
            ]
        )
    )
    row = silver.row(0, named=True)

    assert silver.columns == ANBIMA_YIELD_CURVE_COLUMNS
    assert row["available_date"] == date(2024, 1, 3)
    assert row["curve_type"] == "pre"
    assert row["tenor_days"] == 252
    assert row["rate"] == 10.75
    assert silver.group_by(["ref_date", "curve_type", "tenor_days"]).len().height == silver.height


def test_anbima_vna_preserves_vna_value():
    silver = normalize_vna_to_silver(
        pl.DataFrame(
            [
                _bronze_row(
                    raw_data_referencia="2024-01-02",
                    raw_tipo_titulo="NTN-B",
                    raw_indexador="IPCA",
                    raw_vna="4350.12",
                    raw_unidade="BRL",
                )
            ]
        )
    )
    row = silver.row(0, named=True)

    assert silver.columns == ANBIMA_VNA_COLUMNS
    assert row["security_type"] == "NTN-B"
    assert row["vna"] == 4350.12
    assert row["available_date"] == date(2024, 1, 3)
    assert silver.group_by(["ref_date", "security_type"]).len().height == silver.height


def test_anbima_fixed_income_index_preserves_official_return_only():
    silver = normalize_fixed_income_indices_to_silver(
        pl.DataFrame(
            [
                _bronze_row(
                    raw_data_referencia="2024-01-02",
                    raw_codigo_indice="IMA-B5",
                    raw_familia_indice="IMA-B",
                    raw_nome_indice="IMA-B 5",
                    raw_valor_indice="8100.4",
                    raw_rentabilidade_dia="0.12",
                    raw_taxa="6.1",
                    raw_valor_mercado="123456.7",
                )
            ]
        )
    )
    row = silver.row(0, named=True)

    assert silver.columns == ANBIMA_FIXED_INCOME_INDEX_COLUMNS
    assert row["index_id"] == "IMA-B5"
    assert row["return_1d_official"] == 0.12
    assert row["yield_rate"] == 6.1
    assert row["available_date"] == date(2024, 1, 3)
    assert silver.group_by(["ref_date", "index_id"]).len().height == silver.height


def test_anbima_inflation_projection_preserves_consensus_projection():
    silver = normalize_inflation_projections_to_silver(
        pl.DataFrame(
            [
                _bronze_row(
                    raw_data_referencia="2024-01-02",
                    raw_indicador="IPCA",
                    raw_periodo_referencia="2024",
                    raw_estatistica="consensus",
                    raw_projecao="4.5",
                    raw_unidade="%",
                )
            ]
        )
    )
    row = silver.row(0, named=True)

    assert silver.columns == ANBIMA_INFLATION_PROJECTION_COLUMNS
    assert row["indicator"] == "IPCA"
    assert row["reference_period"] == "2024"
    assert row["projection_value"] == 4.5
    assert row["available_date"] == date(2024, 1, 3)
    assert (
        silver.group_by(["ref_date", "indicator", "reference_period", "statistic"]).len().height
        == silver.height
    )


def _bronze_row(**fields) -> dict[str, object]:
    return {
        "row_index": 0,
        "raw_fields_json": "{}",
        "source": "anbima",
        "source_dataset": "anbima_fixture",
        "download_timestamp_utc": datetime(2024, 1, 2, 12, tzinfo=UTC),
        "raw_path": "raw.csv",
        "sha256": "abc",
        **fields,
    }
