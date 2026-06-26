from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

import polars as pl

from bralpha.parsing.common import (
    parse_decimal,
    parse_int,
    read_delimited_or_html,
    select_first_existing,
    write_partitioned_by_year,
)

ALIASES = {
    "commodity": ["commodity", "mercadoria", "symbol_root"],
    "maturity_code": ["maturity_code", "vencto", "vencimento"],
    "open_interest": ["open_interest", "contr_abert_1", "contr_aberto", "contr_abert"],
    "number_of_trades": ["number_of_trades", "num_negoc", "nro_negocios"],
    "volume": ["volume", "contr_negoc", "quantidade", "quatot"],
    "financial_volume": ["financial_volume", "vol", "volume_financeiro", "voltot"],
    "settlement": ["settlement", "ajuste", "preco_ajuste"],
    "previous_settlement": ["previous_settlement", "ajuste_anter_3", "ajuste_anterior"],
    "price_change": ["price_change", "var_ptos", "variacao"],
    "low": ["low", "preco_min", "minimo"],
    "high": ["high", "preco_max", "maximo"],
    "open": ["open", "preco_abertura", "preabe"],
    "close": ["close", "ult_preco", "ultimo"],
}

FLOAT_COLUMNS = {
    "financial_volume",
    "settlement",
    "previous_settlement",
    "price_change",
    "low",
    "high",
    "open",
    "close",
}
INT_COLUMNS = {"open_interest", "number_of_trades", "volume"}


def parse_settlements_bytes(
    content: bytes,
    *,
    ref_date: date,
    commodity: str | None,
    source_dataset: str,
    download_timestamp_utc: datetime,
    raw_path: Path,
    sha256: str,
) -> pl.DataFrame:
    source = read_delimited_or_html(content)
    columns: dict[str, list[object]] = {}
    row_count = source.height
    for canonical, aliases in ALIASES.items():
        column = select_first_existing(source, aliases)
        if column is not None:
            values = source[column].to_list()
        elif canonical == "commodity" and commodity is not None:
            values = [commodity] * row_count
        else:
            values = [None] * row_count
        if canonical in FLOAT_COLUMNS:
            values = [parse_decimal(value) for value in values]
        elif canonical in INT_COLUMNS:
            values = [parse_int(value) for value in values]
        elif canonical in {"commodity", "maturity_code"}:
            values = [str(value).strip().upper() if value is not None else None for value in values]
        columns[canonical] = values

    frame = pl.DataFrame(columns)
    frame = frame.filter(pl.col("maturity_code").is_not_null())
    timestamp = (
        download_timestamp_utc
        if isinstance(download_timestamp_utc, datetime)
        else datetime.combine(download_timestamp_utc, datetime.min.time())
    )
    if timestamp.tzinfo is not None:
        timestamp = timestamp.astimezone(UTC).replace(tzinfo=None)
    return frame.with_columns(
        pl.lit(ref_date).alias("ref_date"),
        pl.lit("b3").alias("source"),
        pl.lit(source_dataset).alias("source_dataset"),
        pl.lit(timestamp).alias("download_timestamp_utc"),
        pl.lit(str(raw_path)).alias("raw_path"),
        pl.lit(sha256).alias("sha256"),
    )


def parse_settlements_file(
    raw_path: Path,
    *,
    ref_date: date,
    commodity: str | None,
    source_dataset: str,
    download_timestamp_utc: datetime,
    sha256: str,
) -> pl.DataFrame:
    return parse_settlements_bytes(
        raw_path.read_bytes(),
        ref_date=ref_date,
        commodity=commodity,
        source_dataset=source_dataset,
        download_timestamp_utc=download_timestamp_utc,
        raw_path=raw_path,
        sha256=sha256,
    )


def write_settlements_bronze(frame: pl.DataFrame, output_root: Path) -> list[Path]:
    return write_partitioned_by_year(
        frame,
        output_root,
        primary_keys=["ref_date", "commodity", "maturity_code"],
    )
