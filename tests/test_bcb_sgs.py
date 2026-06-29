from __future__ import annotations

import json
import os
import shutil
from datetime import UTC, date, datetime
from urllib.request import urlopen

import polars as pl
import pytest

from bralpha.infra.config import load_bcb_dataset_registry
from bralpha.infra.http import HttpResponse
from bralpha.ingestion.bcb.sgs import (
    SgsSeriesConfig,
    build_sgs_request,
    download_sgs_series,
    load_sgs_series_config,
    sgs_date_windows,
)
from bralpha.normalization.bcb_sgs import normalize_sgs_to_silver
from bralpha.parsing.bcb_sgs import parse_sgs_bytes, write_sgs_bronze


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


def test_sgs_config_covers_required_categories_with_lineage_metadata(repo_root):
    series = load_sgs_series_config(repo_root)
    ids = [item.series_id for item in series]
    categories = {item.category for item in series}

    assert len(ids) == len(set(ids))
    assert {
        "rates",
        "inflation",
        "monetary_liquidity",
        "activity",
        "credit",
        "fiscal",
        "external_reserves",
    } <= categories
    for item in series:
        assert item.notes.strip()
        assert item.source_reference_url.startswith("https://")
        assert str(item.series_id) in item.source_reference_url
        assert item.availability_policy
        assert item.availability_basis
        assert item.revision_policy
        if item.availability_policy == "unknown":
            assert item.model_usable is False
        if item.model_usable:
            assert item.availability_policy != "unknown"
            assert item.availability_basis != "unknown"
            assert item.revision_policy != "current_snapshot_reference_only"
            assert item.source_reference_url
            assert item.notes.strip()
        else:
            assert item.non_model_usable_reason
            assert item.alternate_source_family


def test_sgs_model_usable_categories_are_documented_subset(repo_root):
    series = load_sgs_series_config(repo_root)
    model_usable_categories = {item.category for item in series if item.model_usable}
    model_usable_ids = {item.series_id for item in series if item.model_usable}

    assert model_usable_categories == {"rates", "inflation", "external_reserves"}
    assert model_usable_ids == {11, 432, 433, 13982}


def test_sgs_live_configured_ids_resolve_through_official_api(repo_root):
    if os.environ.get("BCB_LIVE_TESTS") != "1":
        pytest.skip("Set BCB_LIVE_TESTS=1 to verify configured SGS IDs against BCB API")

    for item in load_sgs_series_config(repo_root):
        with urlopen(  # noqa: S310
            f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{item.series_id}/dados/ultimos/1?formato=json",
            timeout=15,
        ) as response:
            payload = json.loads(response.read().decode("utf-8"))
        assert isinstance(payload, list), item.series_id


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
    assert bronze["ref_date"].item() == date(2024, 1, 2)


@pytest.mark.parametrize(
    ("series_id", "payload", "expected"),
    [
        (13982, b'[{"data":"02/01/2024","valor":"355000.50"}]', 355000.50),
        (22701, b'[{"data":"31/01/2024","valor":"-4500.25"}]', -4500.25),
        (27810, b'[{"data":"31/01/2024","valor":"6000000.00"}]', 6000000.00),
    ],
)
def test_sgs_parser_handles_representative_official_series_shapes(
    repo_root,
    series_id,
    payload,
    expected,
):
    bronze = parse_sgs_bytes(
        payload,
        series_id=series_id,
        source_dataset="bcb_sgs_series",
        download_timestamp_utc=datetime(2024, 2, 1, 12, tzinfo=UTC),
        raw_path=repo_root / "raw.json",
        sha256="abc",
    )

    silver = normalize_sgs_to_silver(
        bronze,
        series_config=load_sgs_series_config(repo_root),
    )

    assert silver["series_id"].item() == series_id
    assert silver["value"].item() == expected


def test_sgs_bronze_writer_partitions_by_series_and_year(repo_root, tmp_path):
    bronze = parse_sgs_bytes(
        b'[{"data":"02/01/2024","valor":"11.65"}]',
        series_id=11,
        source_dataset="bcb_sgs_series",
        download_timestamp_utc=datetime(2024, 1, 2, 12, tzinfo=UTC),
        raw_path=repo_root / "raw.json",
        sha256="abc",
    )

    paths = write_sgs_bronze(bronze, tmp_path)
    write_sgs_bronze(bronze, tmp_path)
    written = pl.read_parquet(paths[0])

    assert paths[0].parent == tmp_path / "series_id=11" / "year=2024"
    assert written.height == 1


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
    assert silver["availability_basis"].item() == "source_date_only"
    assert silver["revision_policy"].item() == "unrevised"
    assert silver["model_usable"].item() is True


def test_sgs_reserves_use_next_business_day_availability(repo_root):
    bronze = parse_sgs_bytes(
        b'[{"data":"02/01/2024","valor":"355000.50"}]',
        series_id=13982,
        source_dataset="bcb_sgs_series",
        download_timestamp_utc=datetime(2024, 1, 2, 12, tzinfo=UTC),
        raw_path=repo_root / "raw.json",
        sha256="abc",
    )

    silver = normalize_sgs_to_silver(
        bronze,
        series_config=load_sgs_series_config(repo_root),
    )

    assert silver["available_date"].item() == date(2024, 1, 3)
    assert silver["availability_policy"].item() == "date_only_next_business_day"
    assert silver["availability_basis"].item() == "source_date_only"
    assert silver["revision_policy"].item() == "unrevised"
    assert silver["model_usable"].item() is True


def test_sgs_bop_four_week_policy_remains_reference_only(repo_root):
    bronze = parse_sgs_bytes(
        b'[{"data":"31/01/2024","valor":"-4500.25"}]',
        series_id=22701,
        source_dataset="bcb_sgs_series",
        download_timestamp_utc=datetime(2024, 2, 1, 12, tzinfo=UTC),
        raw_path=repo_root / "raw.json",
        sha256="abc",
    )

    silver = normalize_sgs_to_silver(
        bronze,
        series_config=load_sgs_series_config(repo_root),
    )

    assert silver["available_date"].item() == date(2024, 2, 29)
    assert silver["availability_policy"].item() == "bcb_tempestividade_up_to_4_weeks"
    assert silver["availability_basis"].item() == "official_tempestividade_date_only"
    assert silver["revision_policy"].item() == "current_snapshot_reference_only"
    assert silver["model_usable"].item() is False
    assert silver["non_model_usable_reason"].item()
    assert silver["alternate_source_family"].item() == "bcb_focus_external_expectations"


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
            availability_basis="unknown",
            revision_policy="current_snapshot_reference_only",
            model_usable=True,
            source_reference_url="https://api.bcb.gov.br/dados/serie/bcdata.sgs.999/dados?formato=json",
            notes="test",
        )
    ]

    silver = normalize_sgs_to_silver(bronze, series_config=config)

    assert silver["available_date"].item() is None
    assert silver["availability_basis"].item() == "unknown"
    assert silver["revision_policy"].item() == "current_snapshot_reference_only"
    assert silver["model_usable"].item() is False
