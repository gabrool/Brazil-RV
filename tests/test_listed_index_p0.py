from __future__ import annotations

import json
from datetime import UTC, date, datetime
from types import SimpleNamespace

import polars as pl
import pytest

import bralpha.ingestion.b3.common as b3_common
import bralpha.ingestion.b3.cotahist as cotahist_module
import bralpha.parsing.common as parsing_common
from bralpha.infra.hashing import sha256_bytes
from bralpha.infra.http import HttpResponse
from bralpha.ingestion.b3.cotahist import download_cotahist_year
from bralpha.ingestion.b3.indexes import (
    download_indexes_composition_for_date,
    download_indexes_historical_for_date,
)
from bralpha.ingestion.b3.securities import download_traded_securities_for_date
from bralpha.normalization.b3_market_daily import (
    MARKET_DAILY_COLUMNS,
    normalize_cotahist_to_market_daily,
    normalize_indexes_historical_to_market_daily,
    write_market_daily,
)
from bralpha.normalization.b3_reference import (
    INDEX_COMPOSITION_COLUMNS,
    REFERENCE_SECURITY_COLUMNS,
    normalize_index_composition,
    normalize_traded_securities,
    write_reference_table,
)
from bralpha.parsing.b3_cotahist import (
    iter_cotahist_chunks,
    parse_cotahist_file,
    parse_cotahist_line,
    write_cotahist_bronze,
)
from bralpha.quality.checks import QualityCheckError, run_quality_checks

COTAHIST_ZIP_BYTES = b"small-cotahist-zip"


class MockCotahistClient:
    def __init__(
        self,
        status_code: int = 200,
        content: bytes = COTAHIST_ZIP_BYTES,
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


def test_cotahist_fixed_width_line_parses():
    parsed = parse_cotahist_line(_cotahist_line())

    assert parsed is not None
    assert parsed["ref_date"] == date(2024, 1, 2)
    assert parsed["symbol"] == "PETR4"
    assert parsed["market_type"] == "010"
    assert parsed["open"] == 10.0
    assert parsed["close"] == 10.5
    assert parsed["volume"] == 1234


def test_cotahist_chunked_parser_adds_lineage(tmp_path):
    path = tmp_path / "COTAHIST_A2024.TXT"
    path.write_text("00HEADER\n" + _cotahist_line() + "\n99TRAILER\n", encoding="latin1")
    timestamp = datetime(2024, 1, 3, 10)

    chunks = list(
        iter_cotahist_chunks(
            path,
            chunk_size=1,
            download_timestamp_utc=timestamp,
            raw_path="raw/COTAHIST_A2024.ZIP",
            sha256="abc",
        )
    )
    parsed = parse_cotahist_file(path, chunk_size=1)

    assert "fixture" in (parse_cotahist_file.__doc__ or "")
    assert len(chunks) == 1
    assert parsed.height == 1
    assert chunks[0]["raw_path"].item() == "raw/COTAHIST_A2024.ZIP"
    assert chunks[0]["sha256"].item() == "abc"
    assert chunks[0]["financial_volume"].item() == 20000.0


def test_cotahist_bronze_writer_appends_chunk_files_without_reread(tmp_path, monkeypatch):
    first = pl.DataFrame([parse_cotahist_line(_cotahist_line("PETR4"))])
    second = pl.DataFrame([parse_cotahist_line(_cotahist_line("VALE3"))])

    def fail_read(*args, **kwargs):
        raise AssertionError("append chunk writes must not read existing parquet")

    monkeypatch.setattr(parsing_common.pl, "read_parquet", fail_read)
    paths = write_cotahist_bronze(iter([first, second]), tmp_path / "b3_cotahist_yearly")

    assert [path.name for path in paths] == ["chunk-000000.parquet", "chunk-000001.parquet"]
    assert {path.parent.name for path in paths} == {"year=2024"}


def test_cotahist_download_success_writes_manifest_and_closes_owned_client(
    repo_root,
    tmp_path,
    monkeypatch,
):
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
                headers={"content-type": "application/zip"},
                content=COTAHIST_ZIP_BYTES,
            )

    paths = _patch_cotahist_paths(monkeypatch, tmp_path)
    monkeypatch.setattr(b3_common, "HttpClient", OwnedClient)

    record = download_cotahist_year(
        repo_root,
        year=2024,
        downloaded_at=datetime(2024, 1, 2, 12, tzinfo=UTC),
    )
    manifest = _read_cotahist_manifest(paths)

    assert events == ["enter", "get", "exit"]
    assert record.success is True
    assert manifest["success"] is True
    assert manifest["sha256"] == sha256_bytes(COTAHIST_ZIP_BYTES)
    assert manifest["raw_path"]


def test_cotahist_render_failure_writes_failure_manifest(repo_root, tmp_path, monkeypatch):
    paths = _patch_cotahist_paths(monkeypatch, tmp_path)

    def fail_render(*args, **kwargs):
        raise ValueError("bad template")

    monkeypatch.setattr(cotahist_module, "render_dataset_request", fail_render)

    record = download_cotahist_year(
        repo_root,
        year=2024,
        client=MockCotahistClient(),
        downloaded_at=datetime(2024, 1, 2, 12, tzinfo=UTC),
    )
    manifest = _read_cotahist_manifest(paths)

    assert record.success is False
    assert manifest["source_url"] == ""
    assert manifest["request_params"] == {"year": 2024}
    assert manifest["raw_path"] is None
    assert manifest["sha256"] is None
    assert manifest["error_message"] == "bad template"


def test_cotahist_http_non_2xx_writes_failure_manifest(repo_root, tmp_path, monkeypatch):
    paths = _patch_cotahist_paths(monkeypatch, tmp_path)

    record = download_cotahist_year(
        repo_root,
        year=2024,
        client=MockCotahistClient(status_code=503, content=b"unavailable"),
        downloaded_at=datetime(2024, 1, 2, 12, tzinfo=UTC),
    )
    manifest = _read_cotahist_manifest(paths)

    assert record.success is False
    assert manifest["http_status"] == 503
    assert manifest["file_size_bytes"] == len(b"unavailable")
    assert manifest["raw_path"] is None
    assert manifest["sha256"] is None
    assert manifest["error_message"] == "HTTP 503"


def test_cotahist_http_exception_writes_failure_manifest(repo_root, tmp_path, monkeypatch):
    paths = _patch_cotahist_paths(monkeypatch, tmp_path)

    record = download_cotahist_year(
        repo_root,
        year=2024,
        client=MockCotahistClient(error=OSError("network down")),
        downloaded_at=datetime(2024, 1, 2, 12, tzinfo=UTC),
    )
    manifest = _read_cotahist_manifest(paths)

    assert record.success is False
    assert manifest["source_url"].startswith("https://")
    assert manifest["request_params"] == {"year": 2024}
    assert manifest["raw_path"] is None
    assert manifest["sha256"] is None
    assert manifest["error_message"] == "network down"


def test_cotahist_raw_store_failure_writes_failure_manifest(repo_root, tmp_path, monkeypatch):
    paths = _patch_cotahist_paths(monkeypatch, tmp_path)

    class FailingRawStore:
        def __init__(self, root):
            self.root = root

        def write_bytes(self, *args, **kwargs):
            raise OSError("raw store unavailable")

    monkeypatch.setattr(cotahist_module, "RawStore", FailingRawStore)

    record = download_cotahist_year(
        repo_root,
        year=2024,
        client=MockCotahistClient(),
        downloaded_at=datetime(2024, 1, 2, 12, tzinfo=UTC),
    )
    manifest = _read_cotahist_manifest(paths)

    assert record.success is False
    assert manifest["http_status"] == 200
    assert manifest["file_size_bytes"] == len(COTAHIST_ZIP_BYTES)
    assert manifest["raw_path"] is None
    assert manifest["sha256"] == sha256_bytes(COTAHIST_ZIP_BYTES)
    assert manifest["error_message"] == "raw store unavailable"


def test_cotahist_silver_market_daily_quality_and_source_specific_write(tmp_path):
    bronze = pl.DataFrame(
        [
            {
                **parse_cotahist_line(_cotahist_line()),
                "download_timestamp_utc": datetime(2024, 1, 3, 10),
                "raw_path": "raw/COTAHIST_A2024.ZIP",
                "sha256": "abc",
            }
        ]
    )
    silver = normalize_cotahist_to_market_daily(bronze)
    paths = write_market_daily(
        silver,
        tmp_path / "b3_cotahist_yearly",
        ["ref_date", "symbol", "market_type"],
    )

    assert silver.columns == MARKET_DAILY_COLUMNS
    assert silver["asset_class"].item() == "equity"
    assert silver["market_type"].item() == "010"
    assert silver["close"].item() == 10.5
    assert silver["raw_path"].item() == "raw/COTAHIST_A2024.ZIP"
    assert paths[0].parent.parent.name == "b3_cotahist_yearly"
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


def test_index_history_normalizes_to_market_daily_and_checks_positive_value(tmp_path):
    bronze = pl.DataFrame(
        [
            {
                "ref_date": date(2024, 1, 2),
                "index_id": "IBOV",
                "index_value": 130000.0,
                "source": "b3",
                "source_dataset": "b3_indexes_historical_data",
                "download_timestamp_utc": datetime(2024, 1, 2, 18),
                "raw_path": "raw/indexes.csv",
                "sha256": "def",
            }
        ]
    )
    silver = normalize_indexes_historical_to_market_daily(bronze)
    paths = write_market_daily(
        silver,
        tmp_path / "b3_indexes_historical_data",
        ["ref_date", "index_id"],
    )

    assert silver["asset_class"].item() == "index"
    assert silver["index_id"].item() == "IBOV"
    assert silver["close"].item() == 130000.0
    assert silver["sha256"].item() == "def"
    assert paths[0].parent.parent.name == "b3_indexes_historical_data"
    run_quality_checks(
        silver,
        check_names=["row_count_not_zero", "no_duplicate_primary_keys", "positive_index_value"],
        primary_keys=["ref_date", "index_id"],
        required_columns=["ref_date", "index_id", "close"],
    )


def test_index_composition_and_security_reference_outputs(tmp_path):
    composition = normalize_index_composition(
        pl.DataFrame(
            [
                {
                    "ref_date": date(2024, 1, 2),
                    "index_id": "IBOV",
                    "symbol": "PETR4",
                    "isin": "BRPETRACNPR6",
                    "name": "PETROBRAS PN",
                    "weight": "10,5",
                    "theoretical_quantity": "1000",
                    "download_timestamp_utc": datetime(2024, 1, 2, 18),
                    "raw_path": "raw/composition.csv",
                    "sha256": "ghi",
                }
            ]
        )
    )
    securities = normalize_traded_securities(
        pl.DataFrame(
            [
                {
                    "symbol": "PETR4",
                    "isin": "BRPETRACNPR6",
                    "name": "PETROBRAS PN",
                    "market_type": "010",
                    "asset_class": "equity",
                    "download_timestamp_utc": datetime(2024, 1, 2, 18),
                    "raw_path": "raw/securities.csv",
                    "sha256": "jkl",
                }
            ]
        )
    )
    composition_paths = write_reference_table(
        composition,
        tmp_path / "b3_indexes_composition",
        primary_keys=["ref_date", "index_id", "symbol"],
        ref_date_col="ref_date",
    )
    security_paths = write_reference_table(
        securities,
        tmp_path / "b3_traded_securities",
        primary_keys=["symbol", "market_type"],
    )

    assert composition.columns == INDEX_COMPOSITION_COLUMNS
    assert composition["weight"].item() == 10.5
    assert composition["raw_path"].item() == "raw/composition.csv"
    assert composition_paths[0].parent.parent.name == "b3_indexes_composition"
    assert securities.columns == REFERENCE_SECURITY_COLUMNS
    assert securities["security_id"].item() == "PETR4_010"
    assert securities["sha256"].item() == "jkl"
    assert security_paths == [tmp_path / "b3_traded_securities" / "data.parquet"]


def test_index_and_security_downloads_require_confirmed_source_urls(repo_root, monkeypatch):
    class ExplodingClient:
        def __init__(self):
            raise AssertionError("client should not be created before URL validation")

    monkeypatch.setattr(b3_common, "HttpClient", ExplodingClient)

    expected = "no confirmed free source URL"
    with pytest.raises(NotImplementedError, match=expected):
        download_indexes_historical_for_date(repo_root, ref_date=date(2024, 1, 2))
    with pytest.raises(NotImplementedError, match=expected):
        download_indexes_composition_for_date(repo_root, ref_date=date(2024, 1, 2))
    with pytest.raises(NotImplementedError, match=expected):
        download_traded_securities_for_date(repo_root, ref_date=date(2024, 1, 2))


def test_index_composition_weight_check_fails_on_negative():
    composition = pl.DataFrame(
        [{"ref_date": date(2024, 1, 2), "index_id": "IBOV", "symbol": "PETR4", "weight": -1.0}]
    )
    with pytest.raises(QualityCheckError):
        run_quality_checks(
            composition,
            check_names=["nonnegative_weight_where_present"],
            primary_keys=["ref_date", "index_id", "symbol"],
            required_columns=["ref_date", "index_id", "symbol", "weight"],
        )


def _patch_cotahist_paths(monkeypatch, tmp_path):
    paths = SimpleNamespace(raw=tmp_path / "raw", manifests=tmp_path / "manifests")
    monkeypatch.setattr(
        cotahist_module,
        "resolve_project_paths",
        lambda repo_root, paths_config: paths,
    )
    return paths


def _read_cotahist_manifest(paths) -> dict[str, object]:
    return json.loads((paths.manifests / "b3" / "downloads.jsonl").read_text(encoding="utf-8"))


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
