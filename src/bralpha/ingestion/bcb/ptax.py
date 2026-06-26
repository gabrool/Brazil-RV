from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from typing import Any

from bralpha.infra.http import HttpClient
from bralpha.metadata.datasets import DatasetConfig, dataset_endpoint_names

from .common import (
    PTAX_ODATA_BASE,
    BcbDownloadResult,
    bcb_dataset,
    bcb_manifest_writer,
    bcb_paths,
    bcb_raw_store,
    client_context,
    download_bcb_request,
    odata_value_count,
)


def download_ptax_exchange_rates(
    repo_root: Path,
    *,
    start: date,
    end: date,
    currencies: list[str] | None = None,
    include_currencies: bool | None = None,
    client: HttpClient | None = None,
    downloaded_at: datetime | None = None,
) -> list[BcbDownloadResult]:
    dataset = bcb_dataset(repo_root, "bcb_ptax_exchange_rates")
    paths = bcb_paths(repo_root)
    endpoints = set(dataset_endpoint_names(dataset))
    page_size = int(dataset.request_defaults.get("page_size", 10000))
    configured_currencies = [
        str(currency).upper() for currency in dataset.request_defaults.get("currencies", [])
    ]
    selected_currencies = [currency.upper() for currency in (currencies or configured_currencies)]
    include_currency_list = (
        bool(dataset.request_defaults.get("include_currencies", True))
        if include_currencies is None
        else include_currencies
    )

    results: list[BcbDownloadResult] = []
    with client_context(client) as owned_client:
        if include_currency_list and "Currencies" in endpoints:
            results.extend(
                _download_odata_pages(
                    dataset=dataset,
                    endpoint="Currencies",
                    client=owned_client,
                    downloaded_at=downloaded_at,
                    page_size=page_size,
                    raw_store=bcb_raw_store(paths),
                    manifest_writer=bcb_manifest_writer(paths),
                )
            )
        if "DollarRatePeriod" in endpoints:
            results.extend(
                _download_odata_pages(
                    dataset=dataset,
                    endpoint="DollarRatePeriod",
                    client=owned_client,
                    downloaded_at=downloaded_at,
                    page_size=page_size,
                    raw_store=bcb_raw_store(paths),
                    manifest_writer=bcb_manifest_writer(paths),
                    start=start,
                    end=end,
                )
            )
        if "ExchangeRatePeriod" in endpoints:
            for currency in selected_currencies:
                results.extend(
                    _download_odata_pages(
                        dataset=dataset,
                        endpoint="ExchangeRatePeriod",
                        client=owned_client,
                        downloaded_at=downloaded_at,
                        page_size=page_size,
                        raw_store=bcb_raw_store(paths),
                        manifest_writer=bcb_manifest_writer(paths),
                        start=start,
                        end=end,
                        currency=currency,
                    )
                )
    return results


def build_ptax_request(
    *,
    endpoint: str,
    start: date | None = None,
    end: date | None = None,
    currency: str | None = None,
    skip: int = 0,
    top: int = 10000,
) -> tuple[str, dict[str, Any], str]:
    params: dict[str, Any] = {"$format": "json", "$top": str(top), "$skip": str(skip)}
    suffix = endpoint
    window = "all"
    if endpoint == "DollarRatePeriod":
        if start is None or end is None:
            raise ValueError("DollarRatePeriod requires start and end")
        suffix = "DollarRatePeriod(dataInicial=@dataInicial,dataFinalCotacao=@dataFinalCotacao)"
        params["@dataInicial"] = f"'{_ptax_date(start)}'"
        params["@dataFinalCotacao"] = f"'{_ptax_date(end)}'"
        window = f"{start:%Y%m%d}_{end:%Y%m%d}"
    elif endpoint == "ExchangeRatePeriod":
        if start is None or end is None or currency is None:
            raise ValueError("ExchangeRatePeriod requires start, end, and currency")
        suffix = (
            "ExchangeRatePeriod("
            "moeda=@moeda,dataInicial=@dataInicial,dataFinalCotacao=@dataFinalCotacao)"
        )
        params["@moeda"] = f"'{currency.upper()}'"
        params["@dataInicial"] = f"'{_ptax_date(start)}'"
        params["@dataFinalCotacao"] = f"'{_ptax_date(end)}'"
        window = f"{currency.upper()}_{start:%Y%m%d}_{end:%Y%m%d}"
    elif endpoint != "Currencies":
        raise ValueError(f"Unsupported PTAX endpoint: {endpoint}")
    url = f"{PTAX_ODATA_BASE}/{suffix}"
    filename = f"bcb_ptax_{endpoint}_{window}_skip{skip}.json"
    return url, params, filename


def _download_odata_pages(
    *,
    dataset: DatasetConfig,
    endpoint: str,
    client: HttpClient,
    downloaded_at: datetime | None,
    page_size: int,
    raw_store,
    manifest_writer,
    start: date | None = None,
    end: date | None = None,
    currency: str | None = None,
) -> list[BcbDownloadResult]:
    results = []
    skip = 0
    while True:
        url, params, filename = build_ptax_request(
            endpoint=endpoint,
            start=start,
            end=end,
            currency=currency,
            skip=skip,
            top=page_size,
        )
        result = download_bcb_request(
            dataset=dataset,
            raw_store=raw_store,
            manifest_writer=manifest_writer,
            url=url,
            params=params,
            filename=filename,
            client=client,
            downloaded_at=downloaded_at,
            manifest_params={
                "endpoint": endpoint,
                "currency": currency,
                "start": start.isoformat() if start is not None else None,
                "end": end.isoformat() if end is not None else None,
                **params,
            },
        )
        results.append(result)
        if result.raw_path is None:
            break
        if odata_value_count(result.raw_path.read_bytes()) < page_size:
            break
        skip += page_size
    return results


def _ptax_date(value: date) -> str:
    return value.strftime("%m-%d-%Y")
