from __future__ import annotations

from datetime import date, datetime, time
from pathlib import Path
from typing import Any

import polars as pl

from bralpha.parsing.common import parse_decimal, write_source_partitioned
from bralpha.timing.availability import (
    DEFAULT_DECISION_CUTOFF_TIME,
    DEFAULT_TIMING_TIMEZONE,
    localize_source_timestamp,
    usable_date_from_available_datetime,
    usable_date_from_date_only,
)

BCB_PTAX_SILVER_COLUMNS = [
    "ref_date",
    "available_date",
    "currency_code",
    "currency_name",
    "endpoint",
    "bulletin_type",
    "quote_datetime",
    "is_selected_bulletin",
    "bid_rate",
    "ask_rate",
    "bid_parity",
    "ask_parity",
    "source",
    "source_dataset",
    "download_timestamp_utc",
    "raw_path",
    "sha256",
    "source_version",
]


def normalize_ptax_to_silver(
    bronze: pl.DataFrame,
    *,
    currencies: pl.DataFrame | None = None,
    source_version: str = "v0",
) -> pl.DataFrame:
    currency_names = _currency_names(currencies if currencies is not None else bronze)
    quote_rows = []
    for row in bronze.to_dicts():
        release_date = _parse_release_date(row.get("dataHoraCotacao"))
        if release_date is None:
            continue
        quote_datetime = _parse_datetime(row.get("dataHoraCotacao"))
        quote_datetime_local = (
            localize_source_timestamp(
                quote_datetime,
                source_tz_name=DEFAULT_TIMING_TIMEZONE,
            )
            if quote_datetime is not None
            else None
        )
        available_date = (
            usable_date_from_available_datetime(
                quote_datetime_local,
                cutoff_time=DEFAULT_DECISION_CUTOFF_TIME,
            )
            if quote_datetime_local is not None
            else usable_date_from_date_only(release_date)
        )
        currency_code = _currency_code(row)
        bulletin_type = _clean_text(row.get("tipoBoletim")) or "Fechamento"
        quote_rows.append(
            {
                "ref_date": release_date,
                "available_date": available_date,
                "currency_code": currency_code,
                "currency_name": currency_names.get(currency_code),
                "endpoint": row.get("endpoint"),
                "bulletin_type": bulletin_type,
                "quote_datetime": (
                    quote_datetime_local.replace(tzinfo=None)
                    if quote_datetime_local is not None
                    else None
                ),
                "is_selected_bulletin": False,
                "bid_rate": parse_decimal(row.get("cotacaoCompra")),
                "ask_rate": parse_decimal(row.get("cotacaoVenda")),
                "bid_parity": parse_decimal(row.get("paridadeCompra")),
                "ask_parity": parse_decimal(row.get("paridadeVenda")),
                "source": row.get("source", "bcb"),
                "source_dataset": row.get("source_dataset", "bcb_ptax_exchange_rates"),
                "download_timestamp_utc": row.get("download_timestamp_utc"),
                "raw_path": row.get("raw_path"),
                "sha256": row.get("sha256"),
                "source_version": source_version,
            }
        )
    _mark_selected_bulletins(quote_rows)
    _disambiguate_bulletin_types(quote_rows)
    return _frame(quote_rows)


def write_ptax_silver(frame: pl.DataFrame, output_root: Path) -> list[Path]:
    return write_source_partitioned(
        frame,
        output_root,
        primary_keys=["ref_date", "currency_code", "bulletin_type"],
    )


def _mark_selected_bulletins(rows: list[dict[str, Any]]) -> None:
    by_key: dict[tuple[date, str | None], list[dict[str, Any]]] = {}
    for row in rows:
        by_key.setdefault((row["ref_date"], row["currency_code"]), []).append(row)
    for group in by_key.values():
        closing = [
            row for row in group if str(row.get("bulletin_type", "")).casefold() == "fechamento"
        ]
        candidates = closing or group
        selected = max(candidates, key=_quote_sort_key)
        selected["is_selected_bulletin"] = True


def _disambiguate_bulletin_types(rows: list[dict[str, Any]]) -> None:
    by_key: dict[tuple[date, str | None, str], list[dict[str, Any]]] = {}
    for row in rows:
        key = (row["ref_date"], row["currency_code"], row["bulletin_type"])
        by_key.setdefault(key, []).append(row)
    for group in by_key.values():
        if len(group) <= 1:
            continue
        sorted_group = sorted(group, key=_quote_sort_key)
        for index, row in enumerate(sorted_group, start=1):
            row["bulletin_type"] = f"{row['bulletin_type']}_{index}"


def _currency_names(frame: pl.DataFrame) -> dict[str, str]:
    if frame is None or frame.is_empty():
        return {}
    names = {}
    for row in frame.to_dicts():
        code = _upper_text(row.get("simbolo") or row.get("currency_code"))
        name = row.get("nomeFormatado")
        if code and name:
            names[code] = str(name)
    return names


def _currency_code(row: dict[str, Any]) -> str | None:
    code = row.get("currency_code") or row.get("moeda") or row.get("simbolo")
    if code is None and row.get("endpoint") == "DollarRatePeriod":
        code = "USD"
    return _upper_text(code)


def _parse_datetime(value: object) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    text = str(value).strip()
    if not text:
        return None
    if "T" not in text and " " not in text.strip():
        return None
    return datetime.fromisoformat(text.replace("Z", "+00:00"))


def _parse_release_date(value: object) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value).strip()
    if not text:
        return None
    return date.fromisoformat(text[:10])


def _quote_sort_key(row: dict[str, Any]) -> datetime:
    quote_datetime = row.get("quote_datetime")
    if isinstance(quote_datetime, datetime):
        return quote_datetime
    return datetime.combine(row["ref_date"], time.min)


def _upper_text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip().upper()
    return text or None


def _clean_text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _frame(rows: list[dict[str, object]]) -> pl.DataFrame:
    if not rows:
        return pl.DataFrame(schema={column: pl.Null for column in BCB_PTAX_SILVER_COLUMNS})
    return pl.DataFrame(rows).select(BCB_PTAX_SILVER_COLUMNS)
