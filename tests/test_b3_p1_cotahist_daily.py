from __future__ import annotations

from datetime import date, datetime

import polars as pl
import pytest

import bralpha.ingestion.b3.common as b3_common
import bralpha.parsing.common as parsing_common
from bralpha.ingestion.b3.cotahist import download_cotahist_daily
from bralpha.normalization.b3_market_daily import (
    MARKET_DAILY_COLUMNS,
    normalize_cotahist_to_market_daily,
    write_market_daily,
)
from bralpha.parsing.b3_cotahist import iter_cotahist_chunks, write_cotahist_bronze
from bralpha.quality.checks import QualityCheckError, run_quality_checks


def test_cotahist_daily_download_requires_confirmed_url_before_http_client(repo_root, monkeypatch):
    class ExplodingClient:
        def __init__(self):
            raise AssertionError("client should not be constructed before URL validation")

    monkeypatch.setattr(b3_common, "HttpClient", ExplodingClient)

    with pytest.raises(NotImplementedError, match="no confirmed free source URL"):
        download_cotahist_daily(repo_root, ref_date=date(2024, 1, 2))


def test_cotahist_daily_chunk_parser_and_bronze_lineage(tmp_path):
    path = tmp_path / "COTAHIST_D02012024.TXT"
    path.write_text("00HEADER\n" + _cotahist_line() + "\n99TRAILER\n", encoding="latin1")
    timestamp = datetime(2024, 1, 2, 18)

    chunks = list(
        iter_cotahist_chunks(
            path,
            chunk_size=1,
            source_dataset="b3_cotahist_daily",
            download_timestamp_utc=timestamp,
            raw_path="raw/b3_cotahist_daily/COTAHIST_D02012024.ZIP",
            sha256="dailyhash",
        )
    )
    bronze_paths = write_cotahist_bronze(iter(chunks), tmp_path / "bronze" / "b3_cotahist_daily")

    assert chunks[0]["source_dataset"].item() == "b3_cotahist_daily"
    assert chunks[0]["isin"].item() == "BRPETRACNPR6"
    assert chunks[0]["raw_path"].item() == "raw/b3_cotahist_daily/COTAHIST_D02012024.ZIP"
    assert chunks[0]["sha256"].item() == "dailyhash"
    assert bronze_paths[0].parent.parent.name == "b3_cotahist_daily"
    assert bronze_paths[0].parent.name == "year=2024"


def test_cotahist_daily_silver_write_preserves_source_and_available_date(tmp_path):
    bronze = pl.DataFrame(
        [
            {
                **next(
                    iter_cotahist_chunks(
                        _cotahist_fixture(tmp_path),
                        source_dataset="b3_cotahist_daily",
                        raw_path="raw/COTAHIST_D02012024.ZIP",
                        sha256="abc",
                    )
                ).to_dicts()[0],
                "download_timestamp_utc": datetime(2024, 1, 2, 18),
            }
        ]
    )

    silver = normalize_cotahist_to_market_daily(bronze)
    paths = write_market_daily(
        silver,
        tmp_path / "silver" / "b3_cotahist_daily",
        ["ref_date", "symbol", "market_type"],
    )

    assert silver.columns == MARKET_DAILY_COLUMNS
    assert silver["source_dataset"].item() == "b3_cotahist_daily"
    assert silver["available_date"].item() == date(2024, 1, 3)
    assert silver["isin"].item() == "BRPETRACNPR6"
    assert silver["raw_path"].item() == "raw/COTAHIST_D02012024.ZIP"
    assert paths[0].parent.parent.name == "b3_cotahist_daily"
    run_quality_checks(
        silver,
        check_names=[
            "row_count_not_zero",
            "no_duplicate_primary_keys",
            "nonnegative_volume",
            "nonnegative_prices_where_present",
            "required_columns_present",
        ],
        primary_keys=["ref_date", "symbol", "market_type"],
        required_columns=MARKET_DAILY_COLUMNS,
    )


def test_cotahist_daily_quality_fails_for_duplicate_key(tmp_path):
    silver = normalize_cotahist_to_market_daily(
        next(iter_cotahist_chunks(_cotahist_fixture(tmp_path)))
    )
    duplicated = pl.concat([silver, silver])

    with pytest.raises(QualityCheckError, match="duplicate"):
        run_quality_checks(
            duplicated,
            check_names=["no_duplicate_primary_keys"],
            primary_keys=["ref_date", "symbol", "market_type"],
            required_columns=MARKET_DAILY_COLUMNS,
        )


def test_cotahist_daily_quality_fails_for_negative_values(tmp_path):
    silver = normalize_cotahist_to_market_daily(
        next(iter_cotahist_chunks(_cotahist_fixture(tmp_path)))
    )
    negative = silver.with_columns(pl.lit(-1).alias("volume"))

    with pytest.raises(QualityCheckError, match="nonnegative_volume"):
        run_quality_checks(
            negative,
            check_names=["nonnegative_volume"],
            primary_keys=["ref_date", "symbol", "market_type"],
            required_columns=MARKET_DAILY_COLUMNS,
        )


def test_cotahist_daily_bronze_writer_appends_chunks_without_reread(tmp_path, monkeypatch):
    first = next(iter_cotahist_chunks(_cotahist_fixture(tmp_path, "PETR4")))
    second = next(iter_cotahist_chunks(_cotahist_fixture(tmp_path, "VALE3")))

    def fail_read(*args, **kwargs):
        raise AssertionError("append chunk writes must not read existing parquet")

    monkeypatch.setattr(parsing_common.pl, "read_parquet", fail_read)
    paths = write_cotahist_bronze(iter([first, second]), tmp_path / "b3_cotahist_daily")

    assert [path.name for path in paths] == ["chunk-000000.parquet", "chunk-000001.parquet"]


def _cotahist_fixture(tmp_path, symbol: str = "PETR4"):
    path = tmp_path / f"COTAHIST_D02012024_{symbol}.TXT"
    path.write_text("00HEADER\n" + _cotahist_line(symbol) + "\n99TRAILER\n", encoding="latin1")
    return path


def _cotahist_line(symbol: str = "PETR4") -> str:
    chars = [" "] * 245

    def put(start: int, end: int, value: str) -> None:
        chars[start - 1 : end] = list(value.ljust(end - start + 1)[: end - start + 1])

    def put_num(start: int, end: int, value: int) -> None:
        put(start, end, str(value).zfill(end - start + 1))

    put(1, 2, "01")
    put(3, 10, "20240102")
    put(11, 12, "02")
    put(13, 24, symbol)
    put(25, 27, "010")
    put(28, 39, "PETROBRAS")
    put(40, 49, "PN")
    put_num(57, 69, 1000)
    put_num(70, 82, 1100)
    put_num(83, 95, 900)
    put_num(96, 108, 1005)
    put_num(109, 121, 1050)
    put_num(122, 134, 1040)
    put_num(135, 147, 1060)
    put_num(148, 152, 12)
    put_num(153, 170, 1234)
    put_num(171, 188, 2000000)
    put(231, 242, "BRPETRACNPR6")
    return "".join(chars)
