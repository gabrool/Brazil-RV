from __future__ import annotations

from datetime import UTC, date, datetime

import polars as pl

from bralpha.domain.b3_calendar import add_business_days
from bralpha.normalization.tesouro_market import (
    TESOURO_DIRETO_PRICES_RATES_COLUMNS,
    TESOURO_DIRETO_REDEMPTIONS_COLUMNS,
    TESOURO_DIRETO_SALES_COLUMNS,
    TESOURO_DIRETO_STOCK_COLUMNS,
    normalize_prices_rates_to_silver,
    normalize_redemptions_to_silver,
    normalize_sales_to_silver,
    normalize_tesouro_direto_stock_to_silver,
)


def test_tesouro_prices_rates_preserve_official_rates_prices():
    silver = normalize_prices_rates_to_silver(
        pl.DataFrame(
            [
                _bronze_row(
                    raw_data_base="02/01/2024",
                    raw_tipo_titulo="Tesouro Prefixado",
                    raw_data_vencimento="01/01/2027",
                    raw_taxa_compra_manha="10,50",
                    raw_taxa_venda_manha="10,62",
                    raw_pu_compra_manha="950,25",
                    raw_pu_venda_manha="949,10",
                )
            ]
        )
    )
    row = silver.row(0, named=True)

    assert silver.columns == TESOURO_DIRETO_PRICES_RATES_COLUMNS
    assert row["ref_date"] == date(2024, 1, 2)
    assert row["available_date"] == date(2024, 1, 3)
    assert row["availability_policy"] == "date_only_next_business_day"
    assert row["security_name"] == "Tesouro Prefixado"
    assert row["security_type"] == "Tesouro Prefixado"
    assert row["maturity_date"] == date(2027, 1, 1)
    assert row["buy_rate"] == 10.5
    assert row["sell_price"] == 949.10


def test_tesouro_sales_preserve_quantity_value_and_investor_count():
    silver = normalize_sales_to_silver(
        pl.DataFrame(
            [
                _bronze_row(
                    raw_data_venda="2024-01-02",
                    raw_tipo_titulo="Tesouro Selic",
                    raw_vencimento_do_titulo="2027-03-01",
                    raw_quantidade="123,45",
                    raw_valor="123456,78",
                    raw_investidores="42",
                )
            ]
        )
    )
    row = silver.row(0, named=True)

    assert silver.columns == TESOURO_DIRETO_SALES_COLUMNS
    assert row["available_date"] == date(2024, 1, 4)
    assert row["availability_policy"] == "tesouro_direto_sales_official_2bd"
    assert row["availability_basis"] == "canonical_b3_calendar"
    assert row["quantity"] == 123.45
    assert row["value"] == 123456.78
    assert row["investor_count"] == 42


def test_tesouro_redemptions_map_type_from_ckan_resource_name():
    silver = normalize_redemptions_to_silver(
        pl.DataFrame(
            [
                _bronze_row(
                    resource_name="Recompras do Tesouro Direto",
                    raw_data_resgate="02/01/2024",
                    raw_tipo_titulo="Tesouro IPCA+",
                    raw_vencimento_do_titulo="2035-05-15",
                    raw_quantidade="10",
                    raw_valor="1000",
                )
            ]
        )
    )
    row = silver.row(0, named=True)

    assert silver.columns == TESOURO_DIRETO_REDEMPTIONS_COLUMNS
    assert row["redemption_type"] == "early_repurchase"
    assert row["available_date"] == date(2024, 1, 4)
    assert row["availability_policy"] == "tesouro_direto_redemptions_conservative_2bd"
    assert row["availability_basis"] == "canonical_b3_calendar"
    assert row["value"] == 1000.0


def test_tesouro_sales_2bd_lag_skips_weekends():
    silver = normalize_sales_to_silver(
        pl.DataFrame(
            [
                _bronze_row(
                    raw_data_venda="2024-01-05",
                    raw_tipo_titulo="Tesouro Selic",
                    raw_vencimento_do_titulo="2027-03-01",
                    raw_quantidade="1",
                    raw_valor="10",
                )
            ]
        )
    )

    assert silver["available_date"].item() == date(2024, 1, 9)
    assert silver["availability_basis"].item() == "canonical_b3_calendar"


def test_tesouro_sales_2bd_lag_uses_configured_holidays():
    silver = normalize_sales_to_silver(
        pl.DataFrame(
            [
                _bronze_row(
                    raw_data_venda="2024-01-02",
                    raw_tipo_titulo="Tesouro Selic",
                    raw_vencimento_do_titulo="2027-03-01",
                    raw_quantidade="1",
                    raw_valor="10",
                )
            ]
        ),
        holidays={date(2024, 1, 3)},
    )

    assert silver["available_date"].item() == date(2024, 1, 5)
    assert silver["availability_basis"].item() == "configured_holiday_calendar"


def test_tesouro_redemptions_2bd_lag_uses_configured_holidays():
    silver = normalize_redemptions_to_silver(
        pl.DataFrame(
            [
                _bronze_row(
                    resource_name="Recompras do Tesouro Direto",
                    raw_data_resgate="2024-01-02",
                    raw_tipo_titulo="Tesouro IPCA+",
                    raw_vencimento_do_titulo="2035-05-15",
                    raw_quantidade="10",
                    raw_valor="1000",
                )
            ]
        ),
        holidays={date(2024, 1, 3)},
    )

    assert silver["available_date"].item() == date(2024, 1, 5)
    assert silver["availability_basis"].item() == "configured_holiday_calendar"


def test_tesouro_direto_stock_uses_30_b3_business_day_lag():
    silver = normalize_tesouro_direto_stock_to_silver(
        pl.DataFrame(
            [
                _bronze_row(
                    raw_data_base="01/2024",
                    raw_tipo_titulo="Tesouro Prefixado",
                    raw_data_vencimento="2027-01-01",
                    raw_quantidade="100",
                    raw_valor_estoque="98765,43",
                    raw_investidores="9",
                )
            ]
        )
    )
    row = silver.row(0, named=True)

    assert silver.columns == TESOURO_DIRETO_STOCK_COLUMNS
    expected_ref_date = date(2024, 1, 31)
    assert row["ref_date"] == expected_ref_date
    assert row["availability_policy"] == "tesouro_direto_stock_conservative_30bd"
    assert row["available_date"] == add_business_days(expected_ref_date, 30)
    assert row["stock_value"] == 98765.43
    assert row["investor_count"] == 9


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
