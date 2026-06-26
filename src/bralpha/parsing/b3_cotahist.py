from __future__ import annotations

import zipfile
from collections.abc import Iterator
from datetime import date, datetime
from pathlib import Path

import polars as pl

from bralpha.parsing.common import write_source_partitioned


def iter_cotahist_chunks(
    path: Path,
    *,
    chunk_size: int = 50_000,
    source_dataset: str = "b3_cotahist_yearly",
    download_timestamp_utc: datetime | None = None,
    raw_path: Path | str | None = None,
    sha256: str | None = None,
) -> Iterator[pl.DataFrame]:
    rows: list[dict[str, object]] = []
    for line in _iter_lines(path):
        parsed = parse_cotahist_line(line)
        if parsed is None:
            continue
        parsed["source_dataset"] = source_dataset
        parsed["download_timestamp_utc"] = download_timestamp_utc
        parsed["raw_path"] = str(raw_path or path)
        parsed["sha256"] = sha256
        rows.append(parsed)
        if len(rows) >= chunk_size:
            yield pl.DataFrame(rows)
            rows = []
    if rows:
        yield pl.DataFrame(rows)


def parse_cotahist_file(path: Path, *, chunk_size: int = 50_000) -> pl.DataFrame:
    """Materialize a tiny COTAHIST fixture; production paths should stream chunks."""
    chunks = list(iter_cotahist_chunks(path, chunk_size=chunk_size))
    if not chunks:
        return pl.DataFrame()
    return pl.concat(chunks, how="diagonal_relaxed")


def parse_cotahist_line(line: str) -> dict[str, object] | None:
    if not line.startswith("01"):
        return None
    ref_date = _date_yyyymmdd(line[2:10])
    return {
        "ref_date": ref_date,
        "bdi_code": line[10:12].strip(),
        "symbol": line[12:24].strip(),
        "market_type": line[24:27].strip(),
        "name": line[27:39].strip(),
        "specification": line[39:49].strip(),
        "open": _price(line[56:69]),
        "high": _price(line[69:82]),
        "low": _price(line[82:95]),
        "average": _price(line[95:108]),
        "close": _price(line[108:121]),
        "best_bid": _price(line[121:134]),
        "best_ask": _price(line[134:147]),
        "number_of_trades": _integer(line[147:152]),
        "volume": _integer(line[152:170]),
        "financial_volume": _price(line[170:188]),
        "isin": line[230:242].strip(),
        "source": "b3",
        "source_dataset": "b3_cotahist_yearly",
    }


def write_cotahist_bronze(
    chunks: Iterator[pl.DataFrame],
    output_root: Path,
) -> list[Path]:
    paths: list[Path] = []
    for chunk in chunks:
        paths.extend(write_source_partitioned(chunk, output_root, mode="append_chunks"))
    return paths


def _iter_lines(path: Path) -> Iterator[str]:
    if path.suffix.lower() == ".zip":
        with zipfile.ZipFile(path) as archive:
            names = [name for name in archive.namelist() if not name.endswith("/")]
            if not names:
                return
            with archive.open(names[0]) as handle:
                for raw in handle:
                    yield raw.decode("latin1").rstrip("\r\n")
    else:
        with path.open("r", encoding="latin1") as handle:
            for line in handle:
                yield line.rstrip("\r\n")


def _date_yyyymmdd(text: str) -> date:
    return date(int(text[:4]), int(text[4:6]), int(text[6:8]))


def _integer(text: str) -> int | None:
    stripped = text.strip()
    return int(stripped) if stripped else None


def _price(text: str) -> float | None:
    value = _integer(text)
    return None if value is None else value / 100
