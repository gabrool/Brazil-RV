from __future__ import annotations

from datetime import date, datetime

import polars as pl
import pytest

import bralpha.ingestion.b3.common as b3_common
from bralpha.ingestion.b3.flows import (
    download_equities_investor_participation_for_date,
    download_foreign_investor_movement_for_date,
)
from bralpha.ingestion.b3.indexes import (
    download_indexes_current_portfolio_for_date,
    download_indexes_theoretical_portfolio_for_date,
)
from bralpha.ingestion.b3.reference_files import (
    download_derivatives_reference_prices_for_date,
    download_isin_database,
    download_trading_parameters_for_date,
)
from bralpha.normalization.b3_derivatives_reference import (
    DERIVATIVES_REFERENCE_PRICE_COLUMNS,
    normalize_derivatives_reference_prices,
    write_derivatives_reference_prices,
)
from bralpha.normalization.b3_flows import (
    FLOW_OBSERVATION_COLUMNS,
    normalize_equities_investor_participation,
    normalize_foreign_investor_movement,
    write_flow_observations,
)
from bralpha.normalization.b3_reference import (
    INDEX_COMPOSITION_COLUMNS,
    REFERENCE_SECURITY_COLUMNS,
    TRADING_PARAMETERS_COLUMNS,
    normalize_index_composition,
    normalize_traded_securities,
    normalize_trading_parameters,
    write_reference_table,
)
from bralpha.parsing.b3_reference_files import parse_tabular_reference_file
from bralpha.quality.checks import QualityCheckError, run_quality_checks


def test_url_less_p1_reference_wrappers_raise_before_http_client(repo_root, monkeypatch):
    class ExplodingClient:
        def __init__(self):
            raise AssertionError("client should not be constructed before URL validation")

    monkeypatch.setattr(b3_common, "HttpClient", ExplodingClient)

    wrappers = [
        download_indexes_current_portfolio_for_date,
        download_indexes_theoretical_portfolio_for_date,
        download_equities_investor_participation_for_date,
        download_foreign_investor_movement_for_date,
        download_isin_database,
        download_trading_parameters_for_date,
        download_derivatives_reference_prices_for_date,
    ]
    for wrapper in wrappers:
        with pytest.raises(NotImplementedError, match="no confirmed free source URL"):
            wrapper(repo_root, ref_date=date(2024, 1, 2))


def test_current_and_theoretical_index_portfolio_fixtures_write_source_specific_outputs(tmp_path):
    current = _index_composition_from_fixture("b3_indexes_current_portfolio")
    theoretical = _index_composition_from_fixture("b3_indexes_theoretical_portfolio")

    current_paths = write_reference_table(
        current,
        tmp_path / "silver" / "b3_indexes_current_portfolio",
        primary_keys=["ref_date", "index_id", "symbol"],
        ref_date_col="ref_date",
    )
    theoretical_paths = write_reference_table(
        theoretical,
        tmp_path / "silver" / "b3_indexes_theoretical_portfolio",
        primary_keys=["ref_date", "index_id", "symbol"],
        ref_date_col="ref_date",
    )

    assert current.columns == INDEX_COMPOSITION_COLUMNS
    assert current["available_date"].item() == date(2024, 1, 3)
    assert current["source_dataset"].item() == "b3_indexes_current_portfolio"
    assert current_paths[0].parent.parent.name == "b3_indexes_current_portfolio"
    assert theoretical["source_dataset"].item() == "b3_indexes_theoretical_portfolio"
    assert theoretical_paths[0].parent.parent.name == "b3_indexes_theoretical_portfolio"
    run_quality_checks(
        current,
        check_names=[
            "row_count_not_zero",
            "no_duplicate_primary_keys",
            "nonnegative_weight_where_present",
            "required_columns_present",
        ],
        primary_keys=["ref_date", "index_id", "symbol"],
        required_columns=INDEX_COMPOSITION_COLUMNS,
    )


def test_isin_security_reference_preserves_lineage_and_available_date(tmp_path):
    bronze = parse_tabular_reference_file(
        b"symbol;isin;name;market_type;asset_class;issuer\n"
        b"PETR4;BRPETRACNPR6;PETROBRAS PN;010;equity;PETROBRAS\n",
        source_dataset="b3_isin_database",
        ref_date=date(2024, 1, 2),
        download_timestamp_utc=datetime(2024, 1, 2, 18),
        raw_path="raw/isin.csv",
        sha256="isin-hash",
        required_all=["isin", "symbol"],
    )
    securities = normalize_traded_securities(bronze)
    paths = write_reference_table(
        securities,
        tmp_path / "silver" / "b3_isin_database",
        primary_keys=["isin"],
    )

    assert securities.columns == REFERENCE_SECURITY_COLUMNS
    assert securities["available_date"].item() == date(2024, 1, 3)
    assert securities["source_dataset"].item() == "b3_isin_database"
    assert securities["raw_path"].item() == "raw/isin.csv"
    assert paths == [tmp_path / "silver" / "b3_isin_database" / "data.parquet"]


def test_trading_parameters_fixture_normalizes_and_writes_by_source(tmp_path):
    bronze = parse_tabular_reference_file(
        b"symbol;isin;market_type;lot_size;tick_size;price_limit_lower;price_limit_upper\n"
        b"PETR4;BRPETRACNPR6;010;100;0,01;8,50;12,00\n",
        source_dataset="b3_trading_parameters",
        ref_date=date(2024, 1, 2),
        download_timestamp_utc=datetime(2024, 1, 2, 18),
        raw_path="raw/trading_parameters.csv",
        sha256="params-hash",
        required_all=["symbol", "lot_size"],
    )
    parameters = normalize_trading_parameters(bronze)
    paths = write_reference_table(
        parameters,
        tmp_path / "silver" / "b3_trading_parameters",
        primary_keys=["ref_date", "symbol"],
        ref_date_col="ref_date",
    )

    assert parameters.columns == TRADING_PARAMETERS_COLUMNS
    assert parameters["available_date"].item() == date(2024, 1, 3)
    assert parameters["tick_size"].item() == 0.01
    assert parameters["source_dataset"].item() == "b3_trading_parameters"
    assert paths[0].parent.parent.name == "b3_trading_parameters"


def test_flow_fixtures_preserve_lineage_available_date_and_source_outputs(tmp_path):
    equities = normalize_equities_investor_participation(
        parse_tabular_reference_file(
            b"market_segment;investor_type;buy_value;sell_value;net_value;participation_pct\n"
            b"A Vista;Pessoa Fisica;1000;900;100;12,5\n",
            source_dataset="b3_equities_investor_participation",
            ref_date=date(2024, 1, 2),
            download_timestamp_utc=datetime(2024, 1, 2, 18),
            raw_path="raw/equities_flows.html",
            sha256="flow-hash",
            required_all=["investor_type", "buy_value"],
        )
    )
    foreign = normalize_foreign_investor_movement(
        parse_tabular_reference_file(
            b"market_segment;buy_value;sell_value;net_value\nA Vista;700;600;100\n",
            source_dataset="b3_foreign_investor_movement",
            ref_date=date(2024, 1, 2),
            download_timestamp_utc=datetime(2024, 1, 2, 18),
            raw_path="raw/foreign_flows.html",
            sha256="foreign-hash",
            required_all=["market_segment", "net_value"],
        )
    )

    equities_paths = write_flow_observations(
        equities,
        tmp_path / "silver" / "b3_equities_investor_participation",
        primary_keys=["ref_date", "investor_type", "market_segment"],
    )
    foreign_paths = write_flow_observations(
        foreign,
        tmp_path / "silver" / "b3_foreign_investor_movement",
        primary_keys=["ref_date", "market_segment"],
    )

    assert equities.columns == FLOW_OBSERVATION_COLUMNS
    assert equities["available_date"].item() == date(2024, 1, 3)
    assert equities["source_dataset"].item() == "b3_equities_investor_participation"
    assert equities_paths[0].parent.parent.name == "b3_equities_investor_participation"
    assert foreign["investor_type"].item() == "FOREIGN"
    assert foreign["source_dataset"].item() == "b3_foreign_investor_movement"
    assert foreign_paths[0].parent.parent.name == "b3_foreign_investor_movement"


def test_derivatives_reference_prices_fixture_normalizes_and_checks_duplicates(tmp_path):
    bronze = parse_tabular_reference_file(
        b"commodity;maturity_code;price_type;reference_price;currency\n"
        b"DI1;F24;FINAL;10,25;BRL\n",
        source_dataset="b3_derivatives_reference_prices",
        ref_date=date(2024, 1, 2),
        download_timestamp_utc=datetime(2024, 1, 2, 18),
        raw_path="raw/reference_prices.csv",
        sha256="price-hash",
        required_all=["commodity", "reference_price"],
    )
    prices = normalize_derivatives_reference_prices(bronze)
    paths = write_derivatives_reference_prices(
        prices,
        tmp_path / "silver" / "b3_derivatives_reference_prices",
        primary_keys=["ref_date", "commodity", "maturity_code", "price_type"],
    )

    assert prices.columns == DERIVATIVES_REFERENCE_PRICE_COLUMNS
    assert prices["available_date"].item() == date(2024, 1, 3)
    assert prices["contract_id"].item() == "DI1_F24"
    assert prices["reference_price"].item() == 10.25
    assert paths[0].parent.parent.name == "b3_derivatives_reference_prices"

    with pytest.raises(QualityCheckError, match="duplicate"):
        run_quality_checks(
            pl.concat([prices, prices]),
            check_names=["no_duplicate_primary_keys"],
            primary_keys=["ref_date", "commodity", "maturity_code", "price_type"],
            required_columns=DERIVATIVES_REFERENCE_PRICE_COLUMNS,
        )


def test_index_composition_quality_fails_on_negative_p1_weight():
    bad = _index_composition_from_fixture("b3_indexes_current_portfolio").with_columns(
        pl.lit(-1.0).alias("weight")
    )

    with pytest.raises(QualityCheckError, match="nonnegative_weight"):
        run_quality_checks(
            bad,
            check_names=["nonnegative_weight_where_present"],
            primary_keys=["ref_date", "index_id", "symbol"],
            required_columns=INDEX_COMPOSITION_COLUMNS,
        )


def _index_composition_from_fixture(source_dataset: str) -> pl.DataFrame:
    html = (
        b"""
    <table>
      <tr>
        <th>ref_date</th><th>index_id</th><th>symbol</th><th>isin</th>
        <th>name</th><th>weight</th><th>theoretical_quantity</th>
      </tr>
      <tr>
        <td>2024-01-02</td><td>IBOV</td><td>PETR4</td><td>BRPETRACNPR6</td>
        <td>PETROBRAS PN</td><td>10,50</td><td>1000</td>
      </tr>
    </table>
    """
    )
    bronze = parse_tabular_reference_file(
        html,
        source_dataset=source_dataset,
        download_timestamp_utc=datetime(2024, 1, 2, 18),
        raw_path=f"raw/{source_dataset}.html",
        sha256=f"{source_dataset}-hash",
        required_all=["index_id", "symbol", "weight"],
    )
    return normalize_index_composition(bronze)
