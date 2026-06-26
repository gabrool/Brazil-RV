from __future__ import annotations

import json
import shutil
from datetime import UTC, date, datetime

from bralpha.infra.config import load_bcb_dataset_registry
from bralpha.infra.http import HttpResponse
from bralpha.ingestion.bcb.sgs import (
    SgsSeriesConfig,
    build_sgs_request,
    download_sgs_series,
    sgs_date_windows,
)
from bralpha.normalization.bcb_sgs import normalize_sgs_to_silver
from bralpha.parsing.bcb_sgs import parse_sgs_bytes


class MockClient:
    def __init__(self, content: bytes = b"[]") -> None:
        self.content = content
        self.requests = []

    def get_bytes(self, url, params=None, headers=None):
        self.requests.append({"url": url, "params": params or {}})
        return HttpResponse(
            url=f"{url}?mock=1",
            status_code=200,
            headers={"content-type": "application/json"},
            content=self.content,
        )


def test_sgs_url_construction_uses_documented_parameters(repo_root):
    dataset = load_bcb_dataset_registry(repo_root).get("bcb_sgs_series")

    url, params, filename = build_sgs_request(
        dataset,
        series_id=11,
        start=date(2024, 1, 2),
        end=date(2024, 1, 31),
    )

    assert url.endswith("/bcdata.sgs.11/dados")
    assert params == {
        "formato": "json",
        "dataInicial": "02/01/2024",
        "dataFinal": "31/01/2024",
    }
    assert filename == "bcb_sgs_11_20240102_20240131.json"


def test_sgs_daily_range_is_chunked_to_ten_year_windows():
    windows = sgs_date_windows(
        date(2000, 1, 1),
        date(2022, 1, 1),
        frequency="daily",
    )

    assert windows == [
        (date(2000, 1, 1), date(2009, 12, 31)),
        (date(2010, 1, 1), date(2019, 12, 31)),
        (date(2020, 1, 1), date(2022, 1, 1)),
    ]


def test_sgs_downloader_writes_series_id_to_manifest(repo_root, tmp_path):
    shutil.copytree(repo_root / "configs", tmp_path / "configs")
    client = MockClient(content=json.dumps([{"data": "02/01/2024", "valor": "11.65"}]).encode())

    results = download_sgs_series(
        tmp_path,
        start=date(2024, 1, 2),
        end=date(2024, 1, 2),
        series_ids=[11],
        client=client,
        downloaded_at=datetime(2024, 1, 2, 12, tzinfo=UTC),
    )

    assert len(results) == 1
    assert client.requests[0]["params"]["formato"] == "json"
    assert results[0].record.request_params["series_id"] == 11
    assert results[0].raw_path is not None
    assert "data/raw/bcb/bcb_sgs_series" in str(results[0].raw_path).replace("\\", "/")


def test_sgs_parser_reads_official_data_valor_keys(repo_root):
    bronze = parse_sgs_bytes(
        b'[{"data":"02/01/2024","valor":"11.65"}]',
        series_id=11,
        source_dataset="bcb_sgs_series",
        download_timestamp_utc=datetime(2024, 1, 2, 12, tzinfo=UTC),
        raw_path=repo_root / "raw.json",
        sha256="abc",
    )

    assert bronze["data"].item() == "02/01/2024"
    assert bronze["valor"].item() == "11.65"


def test_sgs_normalizer_applies_availability_policy(repo_root):
    bronze = parse_sgs_bytes(
        b'[{"data":"02/01/2024","valor":"11.65"}]',
        series_id=11,
        source_dataset="bcb_sgs_series",
        download_timestamp_utc=datetime(2024, 1, 2, 12, tzinfo=UTC),
        raw_path=repo_root / "raw.json",
        sha256="abc",
    )
    config = [
        SgsSeriesConfig(
            series_id=11,
            slug="selic_over",
            name="Selic overnight / Selic daily",
            category="rates",
            frequency="daily",
            unit="percent_annualized",
            availability_policy="next_business_day",
            model_usable=True,
        )
    ]

    silver = normalize_sgs_to_silver(bronze, series_config=config)

    assert silver["ref_date"].item() == date(2024, 1, 2)
    assert silver["available_date"].item() == date(2024, 1, 3)
    assert silver["value"].item() == 11.65
    assert silver["model_usable"].item() is True


def test_sgs_unknown_availability_policy_is_not_model_usable(repo_root):
    bronze = parse_sgs_bytes(
        b'[{"data":"02/01/2024","valor":"1.23"}]',
        series_id=999,
        source_dataset="bcb_sgs_series",
        download_timestamp_utc=datetime(2024, 1, 2, 12, tzinfo=UTC),
        raw_path=repo_root / "raw.json",
        sha256="abc",
    )
    config = [
        SgsSeriesConfig(
            series_id=999,
            slug="unknown",
            name="Unknown",
            category="test",
            frequency="monthly",
            unit="index",
            availability_policy="unknown",
            model_usable=True,
        )
    ]

    silver = normalize_sgs_to_silver(bronze, series_config=config)

    assert silver["available_date"].item() is None
    assert silver["model_usable"].item() is False
