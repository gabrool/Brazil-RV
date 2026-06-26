from __future__ import annotations

import json
from datetime import UTC, date, datetime

import polars as pl
import pytest

import bralpha.ingestion.b3.common as b3_common
from bralpha.infra.config import load_b3_dataset_registry
from bralpha.infra.http import HttpResponse
from bralpha.infra.raw_store import RawStore
from bralpha.ingestion.b3.common import download_daily_dataset_for_date
from bralpha.ingestion.b3.open_interest import download_open_interest_range
from bralpha.ingestion.b3.reference_rates import download_reference_rates_for_date
from bralpha.metadata.manifest import ManifestWriter
from bralpha.normalization.b3_curves import (
    CURVE_DAILY_COLUMNS,
    normalize_reference_rates_to_curve_daily,
    write_curve_daily,
)
from bralpha.normalization.b3_market_daily import (
    MARKET_DAILY_COLUMNS,
    normalize_open_interest_to_market_daily,
    normalize_trade_summary_to_market_daily,
    write_market_daily,
)
from bralpha.normalization.b3_reference import (
    REFERENCE_CALENDAR_COLUMNS,
    REFERENCE_CONTRACT_COLUMNS,
    load_contract_master_yaml,
    load_holiday_calendar_yaml,
    normalize_contract_master,
    normalize_holiday_calendar,
    write_reference_table,
)
from bralpha.parsing.b3_settlements import parse_settlements_bytes, parse_settlements_file
from bralpha.quality.checks import QualityCheckError, run_quality_checks

DERIVATIVES_CSV = (
    "VENCTO;AJUSTE;CONTR. ABERT.(1);NUM. NEGOC.;CONTR. NEGOC.;VOL.\n"
    "F26;10,25;1.000;50;200;123456,78\n"
)


class MockClient:
    def __init__(
        self,
        status_code: int = 200,
        content: bytes = DERIVATIVES_CSV.encode("utf-8"),
    ) -> None:
        self.status_code = status_code
        self.content = content

    def get_bytes(self, url, params=None, headers=None):
        return HttpResponse(
            url=f"{url}?mock=1",
            status_code=self.status_code,
            headers={"content-type": "text/csv"},
            content=self.content,
        )


def test_open_interest_range_closes_one_owned_client(repo_root, monkeypatch):
    events: list[str] = []

    class OwnedClient:
        def __enter__(self):
            events.append("enter")
            return self

        def __exit__(self, exc_type, exc, tb):
            events.append("exit")
            return None

        def get_bytes(self, url, params=None, headers=None):
            events.append("get")
            return HttpResponse(
                url=f"{url}?mock=1",
                status_code=200,
                headers={"content-type": "text/csv"},
                content=DERIVATIVES_CSV.encode("utf-8"),
            )

    monkeypatch.setattr(b3_common, "HttpClient", OwnedClient)

    download_open_interest_range(
        repo_root,
        start=date(2024, 1, 2),
        end=date(2024, 1, 2),
        commodities=["DI1", "DOL"],
    )

    assert events == ["enter", "get", "get", "exit"]


def test_open_interest_full_mocked_pattern_writes_source_specific_silver(repo_root, tmp_path):
    result = _download_dataset(
        repo_root,
        tmp_path,
        dataset_id="b3_derivatives_open_interest",
        commodity="DI1",
    )
    assert result.raw_path is not None

    bronze = parse_settlements_file(
        result.raw_path,
        ref_date=date(2024, 1, 2),
        commodity="DI1",
        source_dataset="b3_derivatives_open_interest",
        download_timestamp_utc=datetime(2024, 1, 2, 12, tzinfo=UTC),
        sha256=result.record.sha256,
    )
    silver = normalize_open_interest_to_market_daily(bronze)
    paths = write_market_daily(
        silver,
        tmp_path / "silver" / "b3_derivatives_open_interest",
        ["ref_date", "commodity", "maturity_code"],
    )

    record = json.loads((tmp_path / "downloads.jsonl").read_text(encoding="utf-8"))
    assert record["dataset_id"] == "b3_derivatives_open_interest"
    assert silver.columns == MARKET_DAILY_COLUMNS
    assert silver["source_dataset"].item() == "b3_derivatives_open_interest"
    assert silver["open_interest"].item() == 1000
    assert silver["raw_path"].item() == str(result.raw_path)
    assert silver["sha256"].item() == result.record.sha256
    assert paths[0].parent.parent.name == "b3_derivatives_open_interest"
    run_quality_checks(
        silver,
        check_names=[
            "row_count_not_zero",
            "no_duplicate_primary_keys",
            "nonnegative_open_interest",
            "required_columns_present",
        ],
        primary_keys=["ref_date", "commodity", "maturity_code"],
        required_columns=MARKET_DAILY_COLUMNS,
    )


def test_trade_summary_full_mocked_pattern_writes_source_specific_silver(repo_root, tmp_path):
    result = _download_dataset(
        repo_root,
        tmp_path,
        dataset_id="b3_derivatives_trade_summary",
        commodity="DOL",
    )
    assert result.raw_path is not None

    bronze = parse_settlements_file(
        result.raw_path,
        ref_date=date(2024, 1, 2),
        commodity="DOL",
        source_dataset="b3_derivatives_trade_summary",
        download_timestamp_utc=datetime(2024, 1, 2, 12, tzinfo=UTC),
        sha256=result.record.sha256,
    )
    silver = normalize_trade_summary_to_market_daily(bronze)
    paths = write_market_daily(
        silver,
        tmp_path / "silver" / "b3_derivatives_trade_summary",
        ["ref_date", "commodity", "maturity_code"],
    )

    assert silver["contract_id"].item() == "DOL_F26"
    assert silver["volume"].item() == 200
    assert silver["financial_volume"].item() == 123456.78
    assert silver["source_dataset"].item() == "b3_derivatives_trade_summary"
    assert silver["raw_path"].item() == str(result.raw_path)
    assert paths[0].parent.parent.name == "b3_derivatives_trade_summary"
    run_quality_checks(
        silver,
        check_names=[
            "row_count_not_zero",
            "no_duplicate_primary_keys",
            "nonnegative_volume",
            "nonnegative_financial_volume_where_present",
            "required_columns_present",
        ],
        primary_keys=["ref_date", "commodity", "maturity_code"],
        required_columns=MARKET_DAILY_COLUMNS,
    )


def test_market_daily_writes_do_not_cross_source_dataset_outputs(tmp_path):
    keys = ["ref_date", "commodity", "maturity_code"]
    settlement = pl.DataFrame(
        [
            {
                "ref_date": date(2024, 1, 2),
                "source_dataset": "b3_futures_settlements",
                "commodity": "DI1",
                "maturity_code": "F26",
                "settlement": 10.0,
            }
        ]
    )
    open_interest = pl.DataFrame(
        [
            {
                "ref_date": date(2024, 1, 2),
                "source_dataset": "b3_derivatives_open_interest",
                "commodity": "DI1",
                "maturity_code": "F26",
                "open_interest": 1000,
            }
        ]
    )

    settlement_paths = write_market_daily(settlement, tmp_path / "b3_futures_settlements", keys)
    open_interest_paths = write_market_daily(
        open_interest,
        tmp_path / "b3_derivatives_open_interest",
        keys,
    )

    assert pl.read_parquet(settlement_paths[0])["settlement"].item() == 10.0
    assert pl.read_parquet(open_interest_paths[0])["open_interest"].item() == 1000


def test_derivatives_html_parser_supports_open_interest_fields():
    html = b"""
    <table>
      <tr><th>VENCTO</th><th>CONTR. ABERT.(1)</th><th>CONTR. NEGOC.</th></tr>
      <tr><td>F26</td><td>1.000</td><td>200</td></tr>
    </table>
    """
    bronze = parse_settlements_bytes(
        html,
        ref_date=date(2024, 1, 2),
        commodity="DI1",
        source_dataset="b3_derivatives_open_interest",
        download_timestamp_utc=datetime(2024, 1, 2, 12, tzinfo=UTC),
        raw_path=__file__,
        sha256="abc",
    )

    assert bronze["open_interest"].item() == 1000
    assert bronze["volume"].item() == 200


def test_reference_rates_curve_daily_quality_and_source_specific_write(tmp_path):
    bronze = pl.DataFrame(
        [
            {
                "ref_date": date(2024, 1, 2),
                "curve_id": "PRE",
                "tenor_days": "252",
                "forward_date": "2025-01-02",
                "rate": "12,50",
                "download_timestamp_utc": datetime(2024, 1, 2, 12),
                "raw_path": "raw/rates.csv",
                "sha256": "abc",
            }
        ]
    )
    curve = normalize_reference_rates_to_curve_daily(bronze)
    paths = write_curve_daily(
        curve,
        tmp_path / "b3_reference_rates",
        ["ref_date", "curve_id", "tenor_days"],
    )

    assert curve.columns == CURVE_DAILY_COLUMNS
    assert curve["rate"].item() == 0.125
    assert curve["raw_path"].item() == "raw/rates.csv"
    assert paths[0].parent.parent.name == "b3_reference_rates"
    run_quality_checks(
        curve,
        check_names=[
            "row_count_not_zero",
            "no_duplicate_primary_keys",
            "rate_within_plausible_bounds",
            "available_date_on_or_after_ref_date",
            "required_columns_present",
        ],
        primary_keys=["ref_date", "curve_id", "tenor_days"],
        required_columns=CURVE_DAILY_COLUMNS,
    )


def test_reference_rates_live_download_requires_confirmed_source_url(repo_root, monkeypatch):
    class ExplodingClient:
        def __init__(self):
            raise AssertionError("client should not be created before URL validation")

    monkeypatch.setattr(b3_common, "HttpClient", ExplodingClient)

    with pytest.raises(NotImplementedError, match="no confirmed free source URL"):
        download_reference_rates_for_date(
            repo_root,
            ref_date=date(2024, 1, 2),
        )


def test_reference_rates_plausibility_check_fails():
    curve = pl.DataFrame(
        [{"ref_date": date(2024, 1, 2), "curve_id": "PRE", "tenor_days": 1, "rate": 3.0}]
    )
    with pytest.raises(QualityCheckError):
        run_quality_checks(
            curve,
            check_names=["rate_within_plausible_bounds"],
            primary_keys=["ref_date", "curve_id", "tenor_days"],
            required_columns=["ref_date", "curve_id", "tenor_days"],
        )


def test_contract_master_manual_yaml_normalizes_and_writes(repo_root, tmp_path):
    manual_path = tmp_path / "contracts.yaml"
    manual_path.write_text(
        """
contracts:
  - contract_id: DI1_F26
    symbol_root: DI1
    commodity: DI1
    asset_class: rates
    maturity_code: F26
    maturity_date: 2026-01-02
    contract_multiplier: "1"
    tick_size: "0.001"
""".lstrip(),
        encoding="utf-8",
    )

    contracts = normalize_contract_master(load_contract_master_yaml(manual_path))
    paths = write_reference_table(
        contracts,
        tmp_path / "b3_futures_contract_master",
        primary_keys=["contract_id"],
    )

    assert contracts.columns == REFERENCE_CONTRACT_COLUMNS
    assert paths == [tmp_path / "b3_futures_contract_master" / "data.parquet"]
    assert pl.read_parquet(paths[0])["contract_id"].item() == "DI1_F26"
    run_quality_checks(
        contracts,
        check_names=["no_duplicate_primary_keys", "required_columns_present"],
        primary_keys=["contract_id"],
        required_columns=REFERENCE_CONTRACT_COLUMNS,
    )


def test_holiday_calendar_manual_yaml_normalizes_and_writes(tmp_path):
    manual_path = tmp_path / "holidays.yaml"
    manual_path.write_text(
        """
holidays:
  - calendar_id: B3
    ref_date: 2024-01-01
    holiday_name: Confraternizacao
""".lstrip(),
        encoding="utf-8",
    )

    holidays = normalize_holiday_calendar(load_holiday_calendar_yaml(manual_path))
    paths = write_reference_table(
        holidays,
        tmp_path / "b3_holiday_calendar",
        primary_keys=["calendar_id", "ref_date"],
        ref_date_col="ref_date",
    )

    assert holidays.columns == REFERENCE_CALENDAR_COLUMNS
    assert holidays["available_date"].item() == date(2024, 1, 1)
    assert paths[0].parent.parent.name == "b3_holiday_calendar"
    run_quality_checks(
        holidays,
        check_names=["no_duplicate_primary_keys", "required_columns_present"],
        primary_keys=["calendar_id", "ref_date"],
        required_columns=REFERENCE_CALENDAR_COLUMNS,
    )


def _download_dataset(repo_root, tmp_path, *, dataset_id: str, commodity: str):
    dataset = load_b3_dataset_registry(repo_root).get(dataset_id)
    return download_daily_dataset_for_date(
        dataset=dataset,
        raw_store=RawStore(tmp_path / "raw"),
        manifest_writer=ManifestWriter(tmp_path / "downloads.jsonl"),
        ref_date=date(2024, 1, 2),
        client=MockClient(),
        downloaded_at=datetime(2024, 1, 2, 12, tzinfo=UTC),
        commodity=commodity,
    )
