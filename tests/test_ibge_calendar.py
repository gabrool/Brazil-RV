from __future__ import annotations

import shutil
from datetime import UTC, date, datetime, time

from bralpha.infra.config import load_ibge_dataset_registry
from bralpha.infra.http import HttpResponse
from bralpha.ingestion.ibge.calendar import build_calendar_request, download_release_calendar
from bralpha.normalization.ibge_calendar import normalize_calendar_to_silver
from bralpha.parsing.ibge_calendar import parse_calendar_bytes


class PagedMockClient:
    def __init__(self, *contents: bytes) -> None:
        self.contents = list(contents)
        self.requests = []

    def get_bytes(self, url, params=None, headers=None):
        self.requests.append({"url": url, "params": params or {}})
        content = self.contents.pop(0) if self.contents else b'{"items":[]}'
        return HttpResponse(
            url=f"{url}?mock={len(self.requests)}",
            status_code=200,
            headers={"content-type": "application/json"},
            content=content,
        )


def test_ibge_calendar_request_uses_product_endpoint_and_configured_params(repo_root):
    dataset = load_ibge_dataset_registry(repo_root).get("ibge_release_calendar")

    url, params, filename = build_calendar_request(
        dataset,
        start=date(2024, 1, 1),
        end=date(2024, 1, 31),
        product_id=9256,
        page=2,
        page_size=50,
    )

    assert url.endswith("/api/v3/calendario/9256")
    assert params["de"] == "2024-01-01"
    assert params["ate"] == "2024-01-31"
    assert params["qtd"] == "50"
    assert params["page"] == "2"
    assert filename == "ibge_calendar_9256_20240101_20240131_page2.json"


def test_ibge_calendar_downloader_paginates_until_empty_page(repo_root, tmp_path):
    shutil.copytree(repo_root / "configs", tmp_path / "configs")
    client = PagedMockClient(_calendar_payload("09/02/2024 09:00:00"), b'{"items":[]}')

    results = download_release_calendar(
        tmp_path,
        start=date(2024, 1, 1),
        end=date(2024, 1, 31),
        product_id=9256,
        page_size=1,
        client=client,
    )

    assert [result.record.request_params["page"] for result in results] == ["1", "2"]
    assert [request["params"]["page"] for request in client.requests] == ["1", "2"]
    assert all(result.record.success for result in results)


def test_ibge_calendar_parser_preserves_official_fields(repo_root):
    bronze = parse_calendar_bytes(
        _calendar_payload("09/02/2024 09:00:00"),
        source_dataset="ibge_release_calendar",
        download_timestamp_utc=datetime(2024, 1, 1, 12, tzinfo=UTC),
        raw_path=repo_root / "calendar.json",
        sha256="abc",
    )

    row = bronze.row(0, named=True)
    assert row["id"] == 123
    assert row["titulo"] == "IPCA Jan"
    assert row["data_divulgacao"] == "09/02/2024 09:00:00"
    assert row["produto_id"] == 9256
    assert row["nome_produto"] == "IPCA"


def test_ibge_calendar_before_cutoff_is_available_same_day(repo_root):
    silver = _normalize(repo_root, "09/02/2024 09:00:00")

    row = silver.row(0, named=True)
    assert row["release_date"] == date(2024, 2, 9)
    assert row["release_time_local"] == time(9, 0)
    assert row["available_date"] == date(2024, 2, 9)
    assert row["availability_policy"] == "exact_timestamp_cutoff"
    assert row["reference_period_start"] == date(2024, 1, 1)
    assert row["reference_period_end"] == date(2024, 1, 31)


def test_ibge_calendar_after_cutoff_is_available_next_business_day(repo_root):
    silver = _normalize(repo_root, "09/02/2024 18:31:00")

    assert silver["available_date"].item() == date(2024, 2, 12)


def test_ibge_calendar_date_only_is_available_next_business_day(repo_root):
    silver = _normalize(repo_root, "09/02/2024")

    row = silver.row(0, named=True)
    assert row["release_time_local"] is None
    assert row["available_datetime_local"] is None
    assert row["available_date"] == date(2024, 2, 12)
    assert row["availability_policy"] == "date_only_next_business_day"


def _normalize(repo_root, release_datetime: str):
    bronze = parse_calendar_bytes(
        _calendar_payload(release_datetime),
        source_dataset="ibge_release_calendar",
        download_timestamp_utc=datetime(2024, 1, 1, 12, tzinfo=UTC),
        raw_path=repo_root / "calendar.json",
        sha256="abc",
    )
    return normalize_calendar_to_silver(bronze)


def _calendar_payload(release_datetime: str) -> bytes:
    return (
        b'{"items":[{'
        b'"id":123,'
        b'"titulo":"IPCA Jan",'
        b'"descricao":"",'
        b'"data_divulgacao":"'
        + release_datetime.encode()
        + b'",'
        b'"tipo_id":1,'
        b'"tipo":"Divulgacao de Indicadores",'
        b'"produto_id":9256,'
        b'"nome_produto":"IPCA",'
        b'"alias_produto":"indice-nacional-de-precos-ao-consumidor-amplo",'
        b'"descricao_produto":"",'
        b'"ano_referencia_inicio":2024,'
        b'"mes_referencia_inicio":1,'
        b'"ano_referencia_fim":2024,'
        b'"mes_referencia_fim":1,'
        b'"link":""'
        b"}]}"
    )
