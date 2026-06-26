from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from bralpha.ingestion.bcb.common import write_bronze_frame

BCB_PTAX_BRONZE_COLUMNS = [
    "endpoint",
    "currency_code",
    "simbolo",
    "nomeFormatado",
    "tipoMoeda",
    "moeda",
    "cotacaoCompra",
    "cotacaoVenda",
    "paridadeCompra",
    "paridadeVenda",
    "dataHoraCotacao",
    "tipoBoletim",
    "source",
    "source_dataset",
    "download_timestamp_utc",
    "raw_path",
    "sha256",
]

PTAX_ALIASES = {
    "moeda": ["moeda", "currency_code"],
    "simbolo": ["simbolo", "currency_code"],
    "nomeFormatado": ["nomeFormatado", "currency_name"],
    "tipoMoeda": ["tipoMoeda", "currency_type"],
    "cotacaoCompra": ["cotacaoCompra", "bid_rate"],
    "cotacaoVenda": ["cotacaoVenda", "ask_rate"],
    "paridadeCompra": ["paridadeCompra", "bid_parity"],
    "paridadeVenda": ["paridadeVenda", "ask_parity"],
    "dataHoraCotacao": ["dataHoraCotacao", "quote_datetime"],
    "tipoBoletim": ["tipoBoletim", "bulletin_type"],
}


def parse_ptax_bytes(
    content: bytes,
    *,
    endpoint: str,
    source_dataset: str,
    download_timestamp_utc: datetime,
    raw_path: Path,
    sha256: str,
    currency_code: str | None = None,
) -> pl.DataFrame:
    rows = _odata_rows(content)
    timestamp = _naive_utc(download_timestamp_utc)
    parsed = []
    for row in rows:
        parsed_row = {
            canonical: _first(row, aliases) for canonical, aliases in PTAX_ALIASES.items()
        }
        code = currency_code or parsed_row.get("moeda") or parsed_row.get("simbolo")
        if endpoint == "DollarRatePeriod" and code is None:
            code = "USD"
        parsed.append(
            {
                "endpoint": endpoint,
                "currency_code": str(code).upper() if code is not None else None,
                **parsed_row,
                "source": "bcb",
                "source_dataset": source_dataset,
                "download_timestamp_utc": timestamp,
                "raw_path": str(raw_path),
                "sha256": sha256,
            }
        )
    return _frame(parsed)


def parse_ptax_file(
    raw_path: Path,
    *,
    endpoint: str,
    source_dataset: str,
    download_timestamp_utc: datetime,
    sha256: str,
    currency_code: str | None = None,
) -> pl.DataFrame:
    return parse_ptax_bytes(
        raw_path.read_bytes(),
        endpoint=endpoint,
        source_dataset=source_dataset,
        download_timestamp_utc=download_timestamp_utc,
        raw_path=raw_path,
        sha256=sha256,
        currency_code=currency_code,
    )


def write_ptax_bronze(frame: pl.DataFrame, output_root: Path) -> list[Path]:
    return write_bronze_frame(
        frame,
        output_root,
        primary_keys=["endpoint", "currency_code", "dataHoraCotacao", "tipoBoletim"],
    )


def _odata_rows(content: bytes) -> list[dict[str, object]]:
    payload = json.loads(content.decode("utf-8-sig"))
    if not isinstance(payload, dict) or not isinstance(payload.get("value"), list):
        raise ValueError("PTAX OData JSON payload must contain a value list")
    return [row for row in payload["value"] if isinstance(row, dict)]


def _first(row: dict[str, object], aliases: list[str]) -> object | None:
    for alias in aliases:
        value = row.get(alias)
        if value is not None:
            return value
    return None


def _frame(rows: list[dict[str, object]]) -> pl.DataFrame:
    if not rows:
        return pl.DataFrame(schema={column: pl.Null for column in BCB_PTAX_BRONZE_COLUMNS})
    return pl.DataFrame(rows).select(BCB_PTAX_BRONZE_COLUMNS)


def _naive_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value
    return value.astimezone(UTC).replace(tzinfo=None)
