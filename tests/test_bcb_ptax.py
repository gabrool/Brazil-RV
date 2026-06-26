from __future__ import annotations

import shutil
from datetime import UTC, date, datetime

import polars as pl

from bralpha.infra.http import HttpResponse
from bralpha.ingestion.bcb.ptax import build_ptax_request, download_ptax_exchange_rates
from bralpha.normalization.bcb_ptax import normalize_ptax_to_silver
from bralpha.parsing.bcb_ptax import parse_ptax_bytes


class MockClient:
    def __init__(self) -> None:
        self.requests = []

    def get_bytes(self, url, params=None, headers=None):
        self.requests.append({"url": url, "params": params or {}})
        return HttpResponse(
            url=f"{url}?mock={len(self.requests)}",
            status_code=200,
            headers={"content-type": "application/json"},
            content=b'{"value":[]}',
        )


def test_ptax_request_uses_official_endpoint_codes():
    url, params, filename = build_ptax_request(
        endpoint="ExchangeRatePeriod",
        start=date(2024, 1, 2),
        end=date(2024, 1, 31),
        currency="EUR",
        skip=20,
        top=10,
    )

    assert "/ExchangeRatePeriod(" in url
    assert params["@moeda"] == "'EUR'"
    assert params["@dataInicial"] == "'01-02-2024'"
    assert params["@dataFinalCotacao"] == "'01-31-2024'"
    assert params["$format"] == "json"
    assert params["$top"] == "10"
    assert params["$skip"] == "20"
    assert filename == "bcb_ptax_ExchangeRatePeriod_EUR_20240102_20240131_skip20.json"


def test_ptax_downloader_uses_configured_endpoint_codes(repo_root, tmp_path):
    shutil.copytree(repo_root / "configs", tmp_path / "configs")
    client = MockClient()

    download_ptax_exchange_rates(
        tmp_path,
        start=date(2024, 1, 2),
        end=date(2024, 1, 2),
        currencies=["EUR"],
        include_currencies=True,
        client=client,
        downloaded_at=datetime(2024, 1, 2, 12, tzinfo=UTC),
    )

    urls = [request["url"] for request in client.requests]
    assert any(url.endswith("/Currencies") for url in urls)
    assert any("/DollarRatePeriod(" in url for url in urls)
    assert any("/ExchangeRatePeriod(" in url for url in urls)


def test_ptax_parser_accepts_portuguese_and_english_aliases(repo_root):
    bronze = parse_ptax_bytes(
        b'{"value":[{"bid_rate":5.0,"ask_rate":5.1,'
        b'"quote_datetime":"2024-01-02 13:10:00","bulletin_type":"Fechamento",'
        b'"currency_code":"EUR"}]}',
        endpoint="ExchangeRatePeriod",
        source_dataset="bcb_ptax_exchange_rates",
        download_timestamp_utc=datetime(2024, 1, 2, 12, tzinfo=UTC),
        raw_path=repo_root / "raw.json",
        sha256="abc",
        currency_code="EUR",
    )

    assert bronze["cotacaoCompra"].item() == 5.0
    assert bronze["cotacaoVenda"].item() == 5.1
    assert bronze["dataHoraCotacao"].item() == "2024-01-02 13:10:00"
    assert bronze["tipoBoletim"].item() == "Fechamento"


def test_ptax_normalizer_marks_closing_bulletin_and_keeps_all_rows():
    currencies = pl.DataFrame(
        [{"endpoint": "Currencies", "simbolo": "EUR", "nomeFormatado": "Euro"}]
    )
    bronze = pl.DataFrame(
        [
            _ptax_row("Abertura", "2024-01-02 10:00:00", 5.0),
            _ptax_row("Intermediario", "2024-01-02 11:00:00", 5.1),
            _ptax_row("Intermediario", "2024-01-02 12:00:00", 5.2),
            _ptax_row("Fechamento", "2024-01-02 13:00:00", 5.3),
        ]
    )

    silver = normalize_ptax_to_silver(bronze, currencies=currencies)

    assert silver.height == 4
    assert silver["ref_date"].unique().to_list() == [date(2024, 1, 2)]
    assert silver["currency_name"].unique().to_list() == ["Euro"]
    selected = silver.filter(pl.col("is_selected_bulletin")).row(0, named=True)
    assert selected["bulletin_type"] == "Fechamento"
    assert selected["bid_rate"] == 5.3
    assert sorted(silver["bulletin_type"].to_list()) == [
        "Abertura",
        "Fechamento",
        "Intermediario_1",
        "Intermediario_2",
    ]


def test_ptax_normalizer_selects_latest_when_no_closing_bulletin():
    bronze = pl.DataFrame(
        [
            _ptax_row("Abertura", "2024-01-02 10:00:00", 5.0),
            _ptax_row("Intermediario", "2024-01-02 12:00:00", 5.2),
        ]
    )

    silver = normalize_ptax_to_silver(bronze)

    selected = silver.filter(pl.col("is_selected_bulletin")).row(0, named=True)
    assert selected["bulletin_type"] == "Intermediario"
    assert selected["quote_datetime"] == datetime(2024, 1, 2, 12)


def _ptax_row(bulletin: str, quote_datetime: str, bid_rate: float):
    return {
        "endpoint": "ExchangeRatePeriod",
        "currency_code": "EUR",
        "moeda": "EUR",
        "cotacaoCompra": bid_rate,
        "cotacaoVenda": bid_rate + 0.01,
        "paridadeCompra": 1.1,
        "paridadeVenda": 1.2,
        "dataHoraCotacao": quote_datetime,
        "tipoBoletim": bulletin,
        "source": "bcb",
        "source_dataset": "bcb_ptax_exchange_rates",
        "download_timestamp_utc": datetime(2024, 1, 2, 12),
        "raw_path": "raw.json",
        "sha256": "abc",
    }
