from __future__ import annotations

from pathlib import Path

import polars as pl

IBGE_PRODUCTS_SILVER_COLUMNS = [
    "product_id",
    "product_name",
    "product_type",
    "parent_product_id",
    "alias",
    "acronym",
    "category_id",
    "category_name",
    "parent_category_id",
    "parent_category_name",
    "path",
    "source",
    "source_dataset",
    "download_timestamp_utc",
    "raw_path",
    "sha256",
    "source_version",
]


def normalize_products_to_silver(
    bronze: pl.DataFrame,
    *,
    source_version: str = "v0",
) -> pl.DataFrame:
    rows = []
    for row in bronze.to_dicts():
        rows.append(
            {
                "product_id": _int_or_none(row.get("id")),
                "product_name": _clean_text(row.get("titulo")),
                "product_type": _clean_text(row.get("tipo")),
                "parent_product_id": None,
                "alias": _clean_text(row.get("alias")),
                "acronym": _clean_text(row.get("sigla")),
                "category_id": _int_or_none(row.get("catId")),
                "category_name": _clean_text(row.get("catTitle")),
                "parent_category_id": _int_or_none(row.get("parentCatId")),
                "parent_category_name": _clean_text(row.get("parentCatTitle")),
                "path": _clean_text(row.get("path")),
                "source": row.get("source", "ibge"),
                "source_dataset": row.get("source_dataset", "ibge_products_metadata"),
                "download_timestamp_utc": row.get("download_timestamp_utc"),
                "raw_path": row.get("raw_path"),
                "sha256": row.get("sha256"),
                "source_version": source_version,
            }
        )
    return _frame(rows)


def write_products_silver(frame: pl.DataFrame, output_root: Path) -> list[Path]:
    if frame.is_empty():
        return []
    output_root.mkdir(parents=True, exist_ok=True)
    path = output_root / "data.parquet"
    if path.exists():
        frame = pl.concat([pl.read_parquet(path), frame], how="diagonal_relaxed")
        frame = frame.unique(subset=["product_id"], keep="last", maintain_order=True)
    frame.write_parquet(path)
    return [path]


def _int_or_none(value: object) -> int | None:
    if value is None or value == "":
        return None
    return int(value)


def _clean_text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _frame(rows: list[dict[str, object]]) -> pl.DataFrame:
    if not rows:
        return pl.DataFrame(schema={column: pl.Null for column in IBGE_PRODUCTS_SILVER_COLUMNS})
    return pl.DataFrame(rows).select(IBGE_PRODUCTS_SILVER_COLUMNS)
