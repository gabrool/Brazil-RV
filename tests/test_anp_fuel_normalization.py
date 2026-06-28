from __future__ import annotations

from datetime import date, datetime

import polars as pl

from bralpha.normalization.anp_fuels import (
    ANP_MONTHLY_AVAILABILITY_POLICY,
    ANP_WEEKLY_PRICE_AVAILABILITY_POLICY,
    normalize_anp_fuel_prices_weekly,
    normalize_anp_fuel_sales_monthly,
    normalize_anp_oil_gas_production_monthly,
)


def test_anp_fuel_price_normalization_maps_official_fields_and_availability():
    bronze = pl.DataFrame(
        {
            "row_index": [0, 1],
            "raw_regiao_sigla": ["SE", "SE"],
            "raw_estado_sigla": ["SP", "SP"],
            "raw_municipio": ["Sao Paulo", "Sao Paulo"],
            "raw_revenda": ["Posto A", "Posto A"],
            "raw_cnpj_da_revenda": ["00.000.000/0001-00", "00.000.000/0001-00"],
            "raw_nome_da_rua": ["Rua A", "Rua A"],
            "raw_numero_rua": ["10", "10"],
            "raw_complemento": ["", ""],
            "raw_bairro": ["Centro", "Centro"],
            "raw_cep": ["01000-000", "01000-000"],
            "raw_produto": ["GASOLINA C", "GASOLINA C"],
            "raw_data_da_coleta": ["05/01/2024", "01/09/2020"],
            "raw_valor_de_venda": ["5,10", "4,50"],
            "raw_valor_de_compra": ["4,90", ""],
            "raw_unidade_de_medida": ["R$ / litro", "R$ / litro"],
            "raw_bandeira": ["BRANCA", "BRANCA"],
            "resource_family": ["ethanol_gasoline_monthly", "ethanol_gasoline_monthly"],
            "source": ["anp", "anp"],
            "source_dataset": ["anp_fuel_prices_weekly", "anp_fuel_prices_weekly"],
            "download_timestamp_utc": [datetime(2024, 1, 20), datetime(2024, 1, 20)],
            "raw_path": ["prices.csv", "prices.csv"],
            "sha256": ["abc", "abc"],
        }
    )

    silver = normalize_anp_fuel_prices_weekly(bronze)
    silver_again = normalize_anp_fuel_prices_weekly(bronze)

    assert silver["region"].to_list() == ["SE", "SE"]
    assert silver["station_cnpj"].to_list() == ["00.000.000/0001-00", "00.000.000/0001-00"]
    assert silver["sale_price"].to_list() == [5.1, 4.5]
    assert silver["purchase_price"].to_list() == [4.9, None]
    assert silver["available_date"].to_list()[0] == date(2024, 1, 15)
    assert silver["availability_policy"].unique().to_list() == [
        ANP_WEEKLY_PRICE_AVAILABILITY_POLICY
    ]
    assert silver["observation_id"].to_list() == silver_again["observation_id"].to_list()
    assert "spread" not in silver.columns
    assert "price_change" not in silver.columns


def test_anp_fuel_sales_normalization_maps_monthly_volume_without_shares():
    bronze = pl.DataFrame(
        {
            "raw_ano": ["2024"],
            "raw_mes": ["Janeiro"],
            "raw_grande_regiao": ["Sudeste"],
            "raw_unidade_da_federacao": ["São Paulo"],
            "raw_produto": ["GASOLINA C"],
            "raw_vendas": ["1.234,50"],
            "source": ["anp"],
            "source_dataset": ["anp_fuel_sales_monthly"],
            "download_timestamp_utc": [datetime(2024, 3, 1)],
            "raw_path": ["sales.csv"],
            "sha256": ["abc"],
        }
    )

    silver = normalize_anp_fuel_sales_monthly(bronze)

    assert silver["ref_date"].to_list() == [date(2024, 1, 31)]
    assert silver["available_date"].to_list() == [date(2024, 3, 1)]
    assert silver["availability_policy"].to_list() == [ANP_MONTHLY_AVAILABILITY_POLICY]
    assert silver["month"].to_list() == [1]
    assert silver["sales_volume_m3"].to_list() == [1234.5]
    assert silver["unit"].to_list() == ["m3"]
    assert "sales_share" not in silver.columns
    assert "volume_change" not in silver.columns


def test_anp_production_normalization_maps_metric_family_and_units():
    bronze = pl.DataFrame(
        {
            "raw_ano": ["2024", "2024"],
            "raw_mes": ["Fevereiro", "Fevereiro"],
            "raw_grande_regiao": ["Sudeste", "Sudeste"],
            "raw_unidade_da_federacao": ["Rio de Janeiro", "Rio de Janeiro"],
            "raw_produto": ["Petróleo", "Gás Natural"],
            "raw_localizacao": ["Mar", "Mar"],
            "raw_producao": ["10,5", "20,5"],
            "resource_family": ["petroleum_production", "natural_gas_production"],
            "source": ["anp", "anp"],
            "source_dataset": [
                "anp_oil_gas_production_monthly",
                "anp_oil_gas_production_monthly",
            ],
            "download_timestamp_utc": [datetime(2024, 4, 1), datetime(2024, 4, 1)],
            "raw_path": ["petroleo.csv", "gas.csv"],
            "sha256": ["abc", "def"],
        }
    )

    silver = normalize_anp_oil_gas_production_monthly(bronze)

    assert silver["ref_date"].to_list() == [date(2024, 2, 29), date(2024, 2, 29)]
    assert silver["available_date"].to_list() == [date(2024, 4, 1), date(2024, 4, 1)]
    assert silver["metric_type"].to_list() == [
        "petroleum_production",
        "natural_gas_production",
    ]
    assert silver["metric_value"].to_list() == [10.5, 20.5]
    assert silver["unit"].to_list() == ["m3", "mil_m3"]
    assert "boe" not in silver.columns
    assert "flaring_share" not in silver.columns
