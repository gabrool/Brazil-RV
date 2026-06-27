from __future__ import annotations

from datetime import date

import polars as pl
import pytest

from bralpha.derived.tesouro.prices_rates import (
    build_direto_prices_rates_asof_daily,
    build_direto_prices_rates_observation,
)
from bralpha.derived.tesouro.schemas import (
    TESOURO_DIRETO_PRICES_RATES_ASOF_DAILY_COLUMNS,
    TESOURO_DIRETO_PRICES_RATES_OBSERVATION_COLUMNS,
)


def test_prices_rates_observation_preserves_official_values_and_feature_id():
    panel = build_direto_prices_rates_observation(_prices_silver())
    row = panel.filter(pl.col("ref_date") == date(2024, 1, 2)).row(0, named=True)

    assert panel.columns == TESOURO_DIRETO_PRICES_RATES_OBSERVATION_COLUMNS
    assert row["security_name"] == "Tesouro Prefixado"
    assert row["security_type"] == "Tesouro Prefixado"
    assert row["maturity_date"] == date(2027, 1, 1)
    assert row["buy_rate"] == 11.0
    assert row["sell_rate"] == 11.1
    assert row["buy_price"] == 950.0
    assert row["sell_price"] == 949.5
    assert row["has_rate"] is True
    assert row["has_price"] is True
    assert row["feature_id"] == (
        "tesouro_direto_prices_rates|tesouro_prefixado|tesouro_prefixado|2027-01-01"
    )


def test_prices_rates_asof_uses_latest_available_history_and_staleness():
    observations = build_direto_prices_rates_observation(_prices_silver(), start=None, end=None)

    panel = build_direto_prices_rates_asof_daily(
        observations,
        start=date(2024, 1, 1),
        end=date(2024, 1, 4),
        max_dense_securities=5000,
    ).sort("ref_date")

    assert panel.columns == TESOURO_DIRETO_PRICES_RATES_ASOF_DAILY_COLUMNS
    assert panel["ref_date"].to_list() == [
        date(2024, 1, 2),
        date(2024, 1, 3),
        date(2024, 1, 4),
    ]
    assert panel["observation_ref_date"].to_list() == [
        date(2023, 12, 29),
        date(2024, 1, 2),
        date(2024, 1, 2),
    ]
    assert panel["buy_rate"].to_list() == [10.0, 11.0, 11.0]
    assert panel["staleness_days"].to_list() == [0, 0, 1]
    assert panel.filter(pl.col("observation_available_date") > pl.col("ref_date")).is_empty()


def test_prices_rates_asof_emits_no_rows_before_first_availability():
    observations = build_direto_prices_rates_observation(
        pl.DataFrame([_price_row(ref_date=date(2024, 1, 2), available_date=date(2024, 1, 3))])
    )

    panel = build_direto_prices_rates_asof_daily(
        observations,
        start=date(2024, 1, 1),
        end=date(2024, 1, 3),
        max_dense_securities=5000,
    )

    assert panel["ref_date"].to_list() == [date(2024, 1, 3)]


def test_prices_rates_asof_raises_when_dense_security_limit_is_exceeded():
    observations = build_direto_prices_rates_observation(
        pl.DataFrame(
            [
                _price_row(security_name="Tesouro Prefixado", security_type="Tesouro Prefixado"),
                _price_row(security_name="Tesouro Selic", security_type="Tesouro Selic"),
            ]
        )
    )

    with pytest.raises(ValueError, match="max_dense_securities"):
        build_direto_prices_rates_asof_daily(
            observations,
            start=date(2024, 1, 2),
            end=date(2024, 1, 3),
            max_dense_securities=1,
        )


def _prices_silver() -> pl.DataFrame:
    return pl.DataFrame(
        [
            _price_row(
                ref_date=date(2023, 12, 29),
                available_date=date(2024, 1, 2),
                buy_rate=10.0,
                sell_rate=10.1,
            ),
            _price_row(
                ref_date=date(2024, 1, 2),
                available_date=date(2024, 1, 3),
                buy_rate=11.0,
                sell_rate=11.1,
            ),
        ]
    )


def _price_row(
    *,
    ref_date: date = date(2024, 1, 2),
    available_date: date = date(2024, 1, 3),
    security_name: str = "Tesouro Prefixado",
    security_type: str = "Tesouro Prefixado",
    buy_rate: float = 11.0,
    sell_rate: float = 11.1,
) -> dict[str, object]:
    return {
        "ref_date": ref_date,
        "available_date": available_date,
        "security_name": security_name,
        "security_type": security_type,
        "maturity_date": date(2027, 1, 1),
        "buy_rate": buy_rate,
        "sell_rate": sell_rate,
        "buy_price": 950.0,
        "sell_price": 949.5,
        "unit": "BRL",
        "source": "tesouro",
        "source_dataset": "tesouro_direto_prices_rates",
        "download_timestamp_utc": None,
        "raw_path": "raw.csv",
        "sha256": "abc",
        "source_version": "v0",
    }
