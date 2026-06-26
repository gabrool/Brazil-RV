from __future__ import annotations

import json
from datetime import UTC, date, datetime

import polars as pl
import pytest

from bralpha.infra.hashing import sha256_bytes
from bralpha.infra.http import HttpResponse
from bralpha.infra.raw_store import RawStore
from bralpha.ingestion.b3.common import download_daily_dataset_for_date
from bralpha.metadata.manifest import ManifestWriter
from bralpha.normalization.b3_market_daily import (
    MARKET_DAILY_COLUMNS,
    normalize_settlements_to_market_daily,
    write_market_daily,
)
from bralpha.parsing.b3_settlements import parse_settlements_bytes
from bralpha.quality.checks import QualityCheckError, run_quality_checks

SETTLEMENT_CSV = (
    "VENCTO;AJUSTE;AJUSTE ANTER. (3);VAR. PTOS.;"
    "CONTR. ABERT.(1);NUM. NEGOC.;CONTR. NEGOC.;VOL.\n"
    "F26;10,25;10,10;0,15;1000;50;200;123456,78\n"
    "G26;10,40;10,35;0,05;900;40;180;111000,00\n"
)


class MockClient:
    def __init__(
        self,
        status_code: int = 200,
        content: bytes = SETTLEMENT_CSV.encode("utf-8"),
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


def test_mocked_settlement_download_writes_raw_and_manifest(repo_root, tmp_path):
    dataset = _settlement_dataset(repo_root)
    manifest_path = tmp_path / "manifest.jsonl"

    result = download_daily_dataset_for_date(
        dataset=dataset,
        raw_store=RawStore(tmp_path / "raw"),
        manifest_writer=ManifestWriter(manifest_path),
        ref_date=date(2024, 1, 2),
        client=MockClient(),
        downloaded_at=datetime(2024, 1, 2, 12, tzinfo=UTC),
        commodity="DI1",
    )

    assert result.raw_path is not None
    assert result.raw_path.read_bytes() == SETTLEMENT_CSV.encode("utf-8")
    record = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert record["success"] is True
    assert record["sha256"] == sha256_bytes(SETTLEMENT_CSV.encode("utf-8"))


def test_failed_http_call_writes_failure_manifest(repo_root, tmp_path):
    dataset = _settlement_dataset(repo_root)
    manifest_path = tmp_path / "manifest.jsonl"

    result = download_daily_dataset_for_date(
        dataset=dataset,
        raw_store=RawStore(tmp_path / "raw"),
        manifest_writer=ManifestWriter(manifest_path),
        ref_date=date(2024, 1, 2),
        client=MockClient(status_code=500, content=b"server error"),
        downloaded_at=datetime(2024, 1, 2, 12, tzinfo=UTC),
        commodity="DI1",
    )

    assert result.raw_path is None
    record = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert record["success"] is False
    assert record["error_message"] == "HTTP 500"


def test_non_trading_day_writes_skip_manifest(repo_root, tmp_path):
    dataset = _settlement_dataset(repo_root)
    manifest_path = tmp_path / "manifest.jsonl"

    result = download_daily_dataset_for_date(
        dataset=dataset,
        raw_store=RawStore(tmp_path / "raw"),
        manifest_writer=ManifestWriter(manifest_path),
        ref_date=date(2024, 1, 6),
        client=MockClient(),
        downloaded_at=datetime(2024, 1, 6, 12, tzinfo=UTC),
        commodity="DI1",
    )

    assert result.raw_path is None
    record = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert record["error_message"] == "skipped_non_business_day"


def test_bronze_parser_and_silver_normalizer(repo_root):
    timestamp = datetime(2024, 1, 2, 12, tzinfo=UTC)
    bronze = parse_settlements_bytes(
        SETTLEMENT_CSV.encode("utf-8"),
        ref_date=date(2024, 1, 2),
        commodity="DI1",
        source_dataset="b3_futures_settlements",
        download_timestamp_utc=timestamp,
        raw_path=repo_root / "raw.csv",
        sha256="abc",
    )
    silver = normalize_settlements_to_market_daily(bronze)

    assert bronze.height == 2
    assert set(MARKET_DAILY_COLUMNS) == set(silver.columns)
    assert silver["available_date"].to_list() == [date(2024, 1, 3), date(2024, 1, 3)]
    assert silver["contract_id"].to_list() == ["DI1_F26", "DI1_G26"]
    assert silver["settlement"].to_list() == [10.25, 10.4]


def test_quality_checks_pass_and_fail_on_duplicate_keys():
    frame = pl.DataFrame(
        [
            {
                "ref_date": date(2024, 1, 2),
                "available_date": date(2024, 1, 3),
                "commodity": "DI1",
                "maturity_code": "F26",
                "settlement": 10.0,
            },
            {
                "ref_date": date(2024, 1, 2),
                "available_date": date(2024, 1, 3),
                "commodity": "DI1",
                "maturity_code": "G26",
                "settlement": 11.0,
            },
        ]
    )
    run_quality_checks(
        frame,
        check_names=[
            "row_count_not_zero",
            "no_duplicate_primary_keys",
            "positive_settlement_where_present",
        ],
        primary_keys=["ref_date", "commodity", "maturity_code"],
        required_columns=["ref_date", "available_date", "commodity", "maturity_code"],
    )

    duplicated = pl.concat([frame, frame.slice(0, 1)])
    with pytest.raises(QualityCheckError):
        run_quality_checks(
            duplicated,
            check_names=["no_duplicate_primary_keys"],
            primary_keys=["ref_date", "commodity", "maturity_code"],
            required_columns=["ref_date", "available_date", "commodity", "maturity_code"],
        )


def test_market_daily_rerun_deduplicates(tmp_path):
    first = pl.DataFrame(
        [
            {
                "ref_date": date(2024, 1, 2),
                "commodity": "DI1",
                "maturity_code": "F26",
                "settlement": 10.0,
            }
        ]
    )
    second = pl.DataFrame(
        [
            {
                "ref_date": date(2024, 1, 2),
                "commodity": "DI1",
                "maturity_code": "F26",
                "settlement": 10.5,
            }
        ]
    )
    keys = ["ref_date", "commodity", "maturity_code"]

    write_market_daily(first, tmp_path / "market_daily", keys)
    paths = write_market_daily(second, tmp_path / "market_daily", keys)

    combined = pl.read_parquet(paths[0])
    assert combined.height == 1
    assert combined["settlement"].item() == 10.5


def _settlement_dataset(repo_root):
    from bralpha.infra.config import load_b3_dataset_registry

    return load_b3_dataset_registry(repo_root).get("b3_futures_settlements")
