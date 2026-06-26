from __future__ import annotations

import json
from datetime import UTC, date, datetime
from types import SimpleNamespace

import polars as pl
import pytest

import bralpha.ingestion.b3.cotahist as cotahist_module
import bralpha.parsing.common as parsing_common
from bralpha.infra.hashing import sha256_bytes
from bralpha.infra.http import HttpResponse
from bralpha.ingestion.b3.cotahist import download_cotahist_daily
from bralpha.normalization.b3_market_daily import (
    MARKET_DAILY_COLUMNS,
    normalize_cotahist_to_market_daily,
    write_market_daily,
)
from bralpha.parsing.b3_cotahist import iter_cotahist_chunks, write_cotahist_bronze
from bralpha.quality.checks import QualityCheckError, run_quality_checks

DAILY_BYTES = b"small-cotahist-daily-zip"


class MockDailyClient:
    def __init__(
        self,
        status_code: int = 200,
        content: bytes = DAILY_BYTES,
        error: Exception | None = None,
    ) -> None:
        self.status_code = status_code
        self.content = content
        self.error = error

    def get_bytes(self, url, params=None, headers=None):
        if self.error:
            raise self.error
        return HttpResponse(
            url=f"{url}?mock=1",
            status_code=self.status_code,
            headers={"content-type": "application/zip"},
            content=self.content,
        )


def test_cotahist_daily_download_success_writes_manifest(repo_root, tmp_path, monkeypatch):
    paths = _patch_cotahist_paths(monkeypatch, tmp_path)

    record = download_cotahist_daily(
        repo_root,
        ref_date=date(2024, 1, 2),
        client=MockDailyClient(),
        downloaded_at=datetime(2024, 1, 2, 12, tzinfo=UTC),
    )
    manifest = _read_manifest(paths)

    assert record.dataset_id == "b3_cotahist_daily"
    assert record.success is True
    assert manifest["dataset_id"] == "b3_cotahist_daily"
    assert manifest["request_params"] == {"ref_date": "2024-01-02"}
    assert manifest["sha256"] == sha256_bytes(DAILY_BYTES)
    assert manifest["raw_path"]


def test_cotahist_daily_http_failure_writes_failure_manifest(repo_root, tmp_path, monkeypatch):
    paths = _patch_cotahist_paths(monkeypatch, tmp_path)

    record = download_cotahist_daily(
        repo_root,
        ref_date=date(2024, 1, 2),
        client=MockDailyClient(status_code=404, content=b"missing"),
        downloaded_at=datetime(2024, 1, 2, 12, tzinfo=UTC),
    )
    manifest = _read_manifest(paths)

    assert record.success is False
    assert manifest["dataset_id"] == "b3_cotahist_daily"
    assert manifest["http_status"] == 404
    assert manifest["request_params"] == {"ref_date": "2024-01-02"}
    assert manifest["raw_path"] is None
    assert manifest["sha256"] is None
    assert manifest["error_message"] == "HTTP 404"


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


def _patch_cotahist_paths(monkeypatch, tmp_path):
    paths = SimpleNamespace(raw=tmp_path / "raw", manifests=tmp_path / "manifests")
    monkeypatch.setattr(
        cotahist_module,
        "resolve_project_paths",
        lambda repo_root, paths_config: paths,
    )
    return paths


def _read_manifest(paths) -> dict[str, object]:
    return json.loads((paths.manifests / "b3" / "downloads.jsonl").read_text(encoding="utf-8"))


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
    return "".join(chars)
