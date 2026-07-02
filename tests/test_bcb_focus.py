from __future__ import annotations

import shutil
from datetime import UTC, date, datetime

import polars as pl
import yaml

from bralpha.infra.config import load_bcb_dataset_registry
from bralpha.infra.http import HttpResponse
from bralpha.ingestion.bcb.focus import build_focus_request, download_focus_dataset
from bralpha.metadata.datasets import dataset_endpoint_names
from bralpha.normalization.bcb_focus import (
    BCB_FOCUS_EXPECTATION_COLUMNS,
    FOCUS_EXPECTATION_PRIMARY_KEYS,
    FOCUS_REFERENCE_DATE_PRIMARY_KEYS,
    normalize_focus_expectations_to_silver,
    normalize_focus_reference_dates_to_silver,
)
from bralpha.parsing.bcb_focus import parse_focus_bytes, write_focus_bronze
from bralpha.quality.checks import run_quality_checks


class MockClient:
    def __init__(self) -> None:
        self.requests = []

    def get_bytes(self, url, params=None, headers=None):
        self.requests.append({"url": url, "params": params or {}})
        content = (
            b'{"value":[{"Indicador":"IPCA","Data":"2024-01-02",'
            b'"DataReferencia":"01/2025","Media":4.0}]}'
            if len(self.requests) == 1
            else b'{"value":[]}'
        )
        return HttpResponse(
            url=f"{url}?mock={len(self.requests)}",
            status_code=200,
            headers={"content-type": "application/json"},
            content=content,
        )


def test_focus_request_uses_filter_top_skip_and_orderby():
    url, params, filename = build_focus_request(
        endpoint="ExpectativaMercadoMensais",
        start=date(2024, 1, 1),
        end=date(2024, 1, 31),
        skip=20,
        top=10,
    )

    assert url.endswith("/ExpectativaMercadoMensais")
    assert params["$format"] == "json"
    assert params["$filter"] == "Data ge '2024-01-01' and Data le '2024-01-31'"
    assert params["$orderby"] == "Data asc"
    assert params["$top"] == "10"
    assert params["$skip"] == "20"
    assert filename == "bcb_focus_ExpectativaMercadoMensais_20240101_20240131_skip20.json"


def test_focus_downloader_paginates_with_top_and_skip(repo_root, tmp_path):
    shutil.copytree(repo_root / "configs", tmp_path / "configs")
    _set_page_size(tmp_path, "bcb_focus_expectations", 1)
    client = MockClient()

    results = download_focus_dataset(
        tmp_path,
        dataset_id="bcb_focus_expectations",
        start=date(2024, 1, 1),
        end=date(2024, 1, 31),
        endpoints=["ExpectativaMercadoMensais"],
        client=client,
        downloaded_at=datetime(2024, 1, 2, 12, tzinfo=UTC),
    )

    assert len(results) == 2
    assert [request["params"]["$skip"] for request in client.requests] == ["0", "1"]
    assert all(request["params"]["$top"] == "1" for request in client.requests)


def test_focus_parser_preserves_official_odata_fields(repo_root):
    bronze = parse_focus_bytes(
        b'{"value":[{"Indicador":"IPCA","Data":"2024-01-02",'
        b'"DataReferencia":"01/2025","Media":4.0,"Mediana":4.1,'
        b'"numeroRespondentes":50,"baseCalculo":1}]}',
        endpoint="ExpectativaMercadoMensais",
        source_dataset="bcb_focus_expectations",
        download_timestamp_utc=datetime(2024, 1, 2, 12, tzinfo=UTC),
        raw_path=repo_root / "raw.json",
        sha256="abc",
    )

    assert bronze["Indicador"].item() == "IPCA"
    assert bronze["Data"].item() == "2024-01-02"
    assert bronze["DataReferencia"].item() == "01/2025"
    assert bronze["Media"].item() == 4.0
    assert bronze["baseCalculo"].item() == 1


def test_focus_bronze_writer_preserves_indicator_detail_rows(repo_root, tmp_path):
    bronze = parse_focus_bytes(
        b'{"value":[{"Indicador":"IPCA","IndicadorDetalhe":"Livres",'
        b'"Data":"2024-01-02","DataReferencia":"2025","Media":4.0,'
        b'"baseCalculo":1},{"Indicador":"IPCA","IndicadorDetalhe":"Administrados",'
        b'"Data":"2024-01-02","DataReferencia":"2025","Media":5.0,'
        b'"baseCalculo":1}]}',
        endpoint="ExpectativasMercadoAnuais",
        source_dataset="bcb_focus_expectations",
        download_timestamp_utc=datetime(2024, 1, 2, 12, tzinfo=UTC),
        raw_path=repo_root / "raw_focus.json",
        sha256="abc",
    )

    paths = write_focus_bronze(bronze, tmp_path)
    write_focus_bronze(bronze, tmp_path)
    written = pl.read_parquet(paths[0])

    assert paths[0].parent == tmp_path / "endpoint=ExpectativasMercadoAnuais" / "year=2024"
    assert written.height == 2
    assert set(written["IndicadorDetalhe"].to_list()) == {"Administrados", "Livres"}


def test_focus_bronze_writer_preserves_lowercase_top5_rows(repo_root, tmp_path):
    bronze = parse_focus_bytes(
        b'{"value":[{"indicador":"Selic","Data":"2024-01-02","reuniao":"R1",'
        b'"tipoCalculo":"C","media":9.8},{"indicador":"Selic",'
        b'"Data":"2024-01-02","reuniao":"R2","tipoCalculo":"C",'
        b'"media":9.7}]}',
        endpoint="ExpectativasMercadoTop5Selic",
        source_dataset="bcb_focus_top5_expectations",
        download_timestamp_utc=datetime(2024, 1, 2, 12, tzinfo=UTC),
        raw_path=repo_root / "raw_top5.json",
        sha256="def",
    )

    paths = write_focus_bronze(bronze, tmp_path)
    written = pl.read_parquet(paths[0])

    assert written.height == 2
    assert written["Indicador"].null_count() == 2
    assert set(written["reuniao"].to_list()) == {"R1", "R2"}


def test_focus_bronze_writer_handles_datasreferencia_without_data(repo_root, tmp_path):
    bronze = parse_focus_bytes(
        b'{"value":[{"Indicador":"IGP-M","periodo":"11/2001",'
        b'"DataReferencia1":"2001-11-07","DataReferencia2":"2001-11-08"}]}',
        endpoint="DatasReferencia",
        source_dataset="bcb_focus_top5_reference_dates",
        download_timestamp_utc=datetime(2024, 1, 2, 12, tzinfo=UTC),
        raw_path=repo_root / "raw_ref.json",
        sha256="ghi",
    )

    paths = write_focus_bronze(bronze, tmp_path)
    written = pl.read_parquet(paths[0])

    assert paths[0].parent == tmp_path / "endpoint=DatasReferencia"
    assert written.height == 1


def test_focus_normalizer_handles_generic_selic_top5_and_reference_dates():
    generic = parse_focus_bytes(
        b'{"value":[{"Indicador":"IPCA","IndicadorDetalhe":"Livres",'
        b'"Data":"2024-01-02","DataReferencia":"2025","Media":4.0,'
        b'"Mediana":4.1,"DesvioPadrao":0.2,"Minimo":3.5,"Maximo":4.5,'
        b'"numeroRespondentes":50,"baseCalculo":1}]}',
        endpoint="ExpectativasMercadoAnuais",
        source_dataset="bcb_focus_expectations",
        download_timestamp_utc=datetime(2024, 1, 2, 12, tzinfo=UTC),
        raw_path=__file__,
        sha256="abc",
    )
    selic = parse_focus_bytes(
        b'{"value":[{"Indicador":"Selic","Data":"2024-01-02","Reuniao":"R1",'
        b'"Media":10.0,"Mediana":10.0,"baseCalculo":0}]}',
        endpoint="ExpectativasMercadoSelic",
        source_dataset="bcb_focus_expectations",
        download_timestamp_utc=datetime(2024, 1, 2, 12, tzinfo=UTC),
        raw_path=__file__,
        sha256="abc",
    )
    top5 = parse_focus_bytes(
        b'{"value":[{"indicador":"Selic","Data":"2024-01-02","reuniao":"R2",'
        b'"tipoCalculo":"C","media":9.8,"mediana":9.7}]}',
        endpoint="ExpectativasMercadoTop5Selic",
        source_dataset="bcb_focus_top5_expectations",
        download_timestamp_utc=datetime(2024, 1, 2, 12, tzinfo=UTC),
        raw_path=__file__,
        sha256="abc",
    )
    reference_dates = parse_focus_bytes(
        b'{"value":[{"Indicador":"IGP-M","periodo":"11/2001",'
        b'"DataReferencia1":"2001-11-07","DataReferencia2":"2001-11-08"}]}',
        endpoint="DatasReferencia",
        source_dataset="bcb_focus_top5_reference_dates",
        download_timestamp_utc=datetime(2024, 1, 2, 12, tzinfo=UTC),
        raw_path=__file__,
        sha256="abc",
    )

    expectations = normalize_focus_expectations_to_silver(
        pl.concat([generic, selic, top5], how="diagonal_relaxed")
    )
    refs = normalize_focus_reference_dates_to_silver(reference_dates)

    annual = expectations.filter(pl.col("endpoint") == "ExpectativasMercadoAnuais").row(
        0, named=True
    )
    assert annual["indicator_detail"] == "Livres"
    assert annual["available_date"] == date(2024, 1, 2)
    assert (
        annual["availability_basis"]
        == "official_weekly_publication_date_assumed_pre_eod_cutoff"
    )
    assert annual["reference_year"] == 2025
    assert annual["base_calculation"] == 1
    selic_row = expectations.filter(pl.col("meeting") == "R1").row(0, named=True)
    assert selic_row["indicator"] == "Selic"
    top5_row = expectations.filter(pl.col("is_top5")).row(0, named=True)
    assert top5_row["calculation_type"] == "C"
    assert top5_row["mean"] == 9.8
    reference_rows = refs.select(
        ["indicator", "period", "reference_date_type", "reference_date"]
    ).to_dicts()
    assert reference_rows == [
        {
            "indicator": "IGP-M",
            "period": "11/2001",
            "reference_date_type": "DataReferencia1",
            "reference_date": date(2001, 11, 7),
        },
        {
            "indicator": "IGP-M",
            "period": "11/2001",
            "reference_date_type": "DataReferencia2",
            "reference_date": date(2001, 11, 8),
        },
    ]


def test_focus_reference_dates_keep_context_for_shared_dates():
    bronze = parse_focus_bytes(
        b'{"value":[{"Indicador":"IGP-M","periodo":"11/2001",'
        b'"DataReferencia1":"2001-11-07"},{"Indicador":"IPCA",'
        b'"periodo":"11/2001","DataReferencia1":"2001-11-07"}]}',
        endpoint="DatasReferencia",
        source_dataset="bcb_focus_top5_reference_dates",
        download_timestamp_utc=datetime(2024, 1, 2, 12, tzinfo=UTC),
        raw_path=__file__,
        sha256="abc",
    )

    refs = normalize_focus_reference_dates_to_silver(bronze)

    assert refs.height == 2
    assert refs.group_by(FOCUS_REFERENCE_DATE_PRIMARY_KEYS).len().height == 2


def test_focus_primary_key_keeps_indicator_detail_from_colliding():
    bronze = parse_focus_bytes(
        b'{"value":[{"Indicador":"IPCA","IndicadorDetalhe":"Livres",'
        b'"Data":"2024-01-02","DataReferencia":"2025","Media":4.0,'
        b'"baseCalculo":1},{"Indicador":"IPCA","IndicadorDetalhe":"Administrados",'
        b'"Data":"2024-01-02","DataReferencia":"2025","Media":5.0,'
        b'"baseCalculo":1}]}',
        endpoint="ExpectativasMercadoAnuais",
        source_dataset="bcb_focus_expectations",
        download_timestamp_utc=datetime(2024, 1, 2, 12, tzinfo=UTC),
        raw_path=__file__,
        sha256="abc",
    )

    silver = normalize_focus_expectations_to_silver(bronze)

    assert silver.height == 2
    assert silver.group_by(FOCUS_EXPECTATION_PRIMARY_KEYS).len().height == 2


def test_focus_config_quality_uses_full_expectation_key_for_indicator_detail(repo_root):
    bronze = parse_focus_bytes(
        b'{"value":[{"Indicador":"IPCA","IndicadorDetalhe":"Livres",'
        b'"Data":"2024-01-02","DataReferencia":"2025","Media":4.0,'
        b'"baseCalculo":1},{"Indicador":"IPCA","IndicadorDetalhe":"Administrados",'
        b'"Data":"2024-01-02","DataReferencia":"2025","Media":5.0,'
        b'"baseCalculo":1}]}',
        endpoint="ExpectativasMercadoAnuais",
        source_dataset="bcb_focus_expectations",
        download_timestamp_utc=datetime(2024, 1, 2, 12, tzinfo=UTC),
        raw_path=__file__,
        sha256="abc",
    )
    silver = normalize_focus_expectations_to_silver(bronze)

    _run_focus_config_quality(repo_root, "bcb_focus_expectations", silver)


def test_focus_top5_config_quality_uses_full_expectation_key_for_meeting(repo_root):
    bronze = parse_focus_bytes(
        b'{"value":[{"indicador":"Selic","Data":"2024-01-02","reuniao":"R1",'
        b'"tipoCalculo":"C","media":9.8},{"indicador":"Selic",'
        b'"Data":"2024-01-02","reuniao":"R2","tipoCalculo":"C",'
        b'"media":9.7}]}',
        endpoint="ExpectativasMercadoTop5Selic",
        source_dataset="bcb_focus_top5_expectations",
        download_timestamp_utc=datetime(2024, 1, 2, 12, tzinfo=UTC),
        raw_path=__file__,
        sha256="def",
    )
    silver = normalize_focus_expectations_to_silver(bronze)

    _run_focus_config_quality(repo_root, "bcb_focus_top5_expectations", silver)


def test_deactivated_institution_level_expectations_resource_is_absent(repo_root):
    registry = load_bcb_dataset_registry(repo_root)
    endpoints = {
        endpoint
        for dataset in registry.datasets
        for endpoint in dataset_endpoint_names(dataset)
    }

    assert "ExpectativasMercadoInstituicoes" not in endpoints


def _run_focus_config_quality(repo_root, dataset_id: str, silver: pl.DataFrame) -> None:
    dataset = load_bcb_dataset_registry(repo_root).get(dataset_id)

    assert dataset.primary_keys == FOCUS_EXPECTATION_PRIMARY_KEYS
    run_quality_checks(
        silver,
        check_names=dataset.quality_checks,
        primary_keys=dataset.primary_keys,
        required_columns=BCB_FOCUS_EXPECTATION_COLUMNS,
    )


def _set_page_size(repo_root, dataset_id: str, page_size: int) -> None:
    path = repo_root / "configs" / "datasets" / "bcb.yaml"
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    for dataset in data["datasets"]:
        if dataset["dataset_id"] == dataset_id:
            dataset.setdefault("request_defaults", {})["page_size"] = page_size
    path.write_text(yaml.safe_dump(data, sort_keys=False), encoding="utf-8")
