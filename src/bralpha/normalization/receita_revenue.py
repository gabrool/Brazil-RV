from __future__ import annotations

from calendar import monthrange
from datetime import date, timedelta
from pathlib import Path

import polars as pl

from bralpha.domain.b3_calendar import is_business_day, previous_business_day
from bralpha.ingestion.receita.common import write_partitioned_frame
from bralpha.parsing.common import normalize_column_name, parse_decimal
from bralpha.timing.vintages import (
    AVAILABILITY_CONSERVATIVE_HEURISTIC,
    REVISION_CURRENT_SNAPSHOT_REFERENCE_ONLY,
    REVISION_REVISED_USE_FIRST_SEEN,
    available_date_from_first_seen,
    available_date_from_source_datetime,
    choose_model_usable,
    make_vintage_id,
)

RECEITA_COLLECTION_AVAILABILITY_POLICY = "receita_conservative_heuristic_plus_first_seen"
RECEITA_COLLECTION_REFERENCE_ONLY_POLICY = (
    "receita_monthly_collection_conservative_next_month_end_plus_5bd"
    "_reference_only"
)
RECEITA_HEURISTIC_SNAPSHOT_BASIS = "conservative_heuristic+first_seen_download_timestamp"

RECEITA_PIT_COLUMNS = [
    "availability_basis",
    "revision_policy",
    "release_date",
    "source_publication_datetime_utc",
    "source_last_modified_utc",
    "first_seen_timestamp_utc",
    "vintage_id",
    "revision_sequence",
    "model_usable",
    "model_usable_reason",
]

RECEITA_TAX_COLLECTION_COLUMNS = [
    "ref_date",
    "available_date",
    "availability_policy",
    *RECEITA_PIT_COLUMNS,
    "year",
    "month",
    "collection_scope",
    "revenue_category",
    "revenue_subcategory",
    "revenue_code",
    "revenue_key",
    "revenue_name",
    "table_kind",
    "collection_amount_brl",
    "unit",
    "source_table",
    "source",
    "source_dataset",
    "download_timestamp_utc",
    "raw_path",
    "sha256",
    "source_version",
]

RECEITA_SILVER_COLUMNS_BY_DATASET = {
    "receita_tax_collection_monthly": RECEITA_TAX_COLLECTION_COLUMNS,
}

RECEITA_PRIMARY_KEYS_BY_DATASET = {
    "receita_tax_collection_monthly": [
        "ref_date",
        "collection_scope",
        "revenue_category",
        "revenue_code",
        "revenue_key",
        "table_kind",
        "vintage_id",
    ],
}


class ReceitaNormalizationError(ValueError):
    pass


def normalize_receita_to_silver(
    dataset_id: str,
    bronze: pl.DataFrame,
    *,
    source_version: str = "v0",
) -> pl.DataFrame:
    if dataset_id == "receita_tax_collection_monthly":
        return normalize_receita_tax_collection_monthly(bronze, source_version=source_version)
    raise ValueError(f"Unsupported Receita silver dataset: {dataset_id}")


def normalize_receita_tax_collection_monthly(
    bronze: pl.DataFrame,
    *,
    source_version: str = "v0",
) -> pl.DataFrame:
    if bronze.is_empty():
        return pl.DataFrame(schema={column: pl.Null for column in RECEITA_TAX_COLLECTION_COLUMNS})

    long_frame = _long_layout(bronze)
    if long_frame is None:
        long_frame = _wide_layout(bronze)
    if long_frame is None or long_frame.is_empty():
        raise ReceitaNormalizationError(
            "Could not identify Receita collection period/value columns in structured data"
        )

    frame = long_frame.with_columns(
        [
            _text_expr(long_frame, "collection_scope", "escopo").alias("collection_scope_raw"),
            _text_expr(
                long_frame,
                "categoria",
                "grupo",
                "classificacao",
                "classificacao_receita",
                "revenue_category",
            ).alias("revenue_category_raw"),
            _text_expr(long_frame, "subcategoria", "subgrupo", "revenue_subcategory").alias(
                "revenue_subcategory"
            ),
            _text_expr(long_frame, "codigo", "codigo_receita", "cod_receita").alias(
                "revenue_code_raw"
            ),
            _text_expr(
                long_frame,
                "tributo",
                "receita",
                "rubrica",
                "item",
                "descricao",
                "nome_receita",
                "revenue_name",
            ).alias("revenue_name_raw"),
            _source_table_expr(long_frame).alias("source_table"),
            _existing_or_literal(long_frame, "source", "receita").alias("source"),
            _existing_or_literal(long_frame, "source_dataset", None).alias("source_dataset"),
            _existing_or_literal(long_frame, "download_timestamp_utc", None).alias(
                "download_timestamp_utc"
            ),
            _existing_or_literal(long_frame, "source_publication_datetime_utc", None).alias(
                "source_publication_datetime_utc"
            ),
            _source_last_modified_expr(long_frame).alias("source_last_modified_utc"),
            _first_seen_expr(long_frame).alias("first_seen_timestamp_utc"),
            _existing_or_literal(long_frame, "raw_path", None).alias("raw_path"),
            _existing_or_literal(long_frame, "sha256", None).alias("sha256"),
            pl.lit(source_version).alias("source_version"),
        ]
    )
    frame = frame.with_columns(
        [
            pl.col("ref_date").dt.year().alias("year"),
            pl.col("ref_date").dt.month().alias("month"),
            pl.col("ref_date")
            .map_elements(_heuristic_available_date, return_dtype=pl.Date)
            .alias("heuristic_available_date"),
            pl.lit(None).alias("release_date"),
            pl.col("collection_scope_raw")
            .map_elements(
                lambda value: _text_or_default(value, "federal_total"),
                return_dtype=pl.Utf8,
                skip_nulls=False,
            )
            .alias("collection_scope"),
            pl.col("revenue_category_raw")
            .map_elements(
                lambda value: _text_or_default(value, "unknown"),
                return_dtype=pl.Utf8,
                skip_nulls=False,
            )
            .alias("revenue_category"),
            pl.col("revenue_code_raw")
            .map_elements(
                lambda value: _text_or_default(value, "unknown"),
                return_dtype=pl.Utf8,
                skip_nulls=False,
            )
            .alias("revenue_code"),
            pl.col("revenue_name_raw")
            .map_elements(
                lambda value: _text_or_default(value, "unknown"),
                return_dtype=pl.Utf8,
                skip_nulls=False,
            )
            .alias("revenue_name"),
            pl.col("source_table")
            .map_elements(_table_kind, return_dtype=pl.Utf8)
            .alias("table_kind"),
            pl.lit("BRL").alias("unit"),
        ]
    )
    frame = frame.with_columns(
        pl.struct(
            [
                "revenue_code",
                "revenue_category",
                "revenue_subcategory",
                "revenue_name",
                "source_table",
            ]
        )
        .map_elements(_revenue_key, return_dtype=pl.Utf8)
        .alias("revenue_key")
    )
    frame = frame.with_columns(
        [
            pl.struct(
                [
                    "source_publication_datetime_utc",
                    "source_last_modified_utc",
                    "first_seen_timestamp_utc",
                ]
            )
            .map_elements(_snapshot_available_date, return_dtype=pl.Date)
            .alias("snapshot_available_date"),
            pl.struct(
                [
                    "source_dataset",
                    "source_table",
                    "raw_path",
                    "sha256",
                    "source_publication_datetime_utc",
                    "source_last_modified_utc",
                    "first_seen_timestamp_utc",
                ]
            )
            .map_elements(_snapshot_vintage_id, return_dtype=pl.Utf8)
            .alias("vintage_id"),
        ]
    )
    frame = frame.with_columns(
        [
            pl.when(pl.col("snapshot_available_date").is_not_null())
            .then(pl.max_horizontal("heuristic_available_date", "snapshot_available_date"))
            .otherwise(pl.col("heuristic_available_date"))
            .alias("available_date"),
            pl.when(pl.col("snapshot_available_date").is_not_null())
            .then(pl.lit(RECEITA_COLLECTION_AVAILABILITY_POLICY))
            .otherwise(pl.lit(RECEITA_COLLECTION_REFERENCE_ONLY_POLICY))
            .alias("availability_policy"),
            pl.when(pl.col("snapshot_available_date").is_not_null())
            .then(pl.lit(RECEITA_HEURISTIC_SNAPSHOT_BASIS))
            .otherwise(pl.lit(AVAILABILITY_CONSERVATIVE_HEURISTIC))
            .alias("availability_basis"),
            pl.when(pl.col("snapshot_available_date").is_not_null())
            .then(pl.lit(REVISION_REVISED_USE_FIRST_SEEN))
            .otherwise(pl.lit(REVISION_CURRENT_SNAPSHOT_REFERENCE_ONLY))
            .alias("revision_policy"),
            pl.lit(0).alias("revision_sequence"),
            pl.when(pl.col("snapshot_available_date").is_not_null())
            .then(pl.lit(RECEITA_COLLECTION_AVAILABILITY_POLICY))
            .otherwise(pl.lit(RECEITA_COLLECTION_REFERENCE_ONLY_POLICY))
            .alias("model_usable_reason"),
        ]
    )
    frame = frame.with_columns(
        pl.struct(
            [
                "availability_basis",
                "revision_policy",
                "available_date",
                "vintage_id",
                "first_seen_timestamp_utc",
                "source_last_modified_utc",
            ]
        )
        .map_elements(_model_usable, return_dtype=pl.Boolean)
        .alias("model_usable")
    )
    frame = frame.select(RECEITA_TAX_COLLECTION_COLUMNS)
    missing_required = frame.filter(
        pl.col("ref_date").is_null() | pl.col("collection_amount_brl").is_null()
    )
    if missing_required.height:
        raise ReceitaNormalizationError("Receita collection rows require ref_date and amount")
    return frame.unique(
        subset=RECEITA_PRIMARY_KEYS_BY_DATASET["receita_tax_collection_monthly"],
        keep="last",
        maintain_order=True,
    )


def write_receita_silver(
    frame: pl.DataFrame,
    output_root: Path,
    *,
    primary_keys: list[str],
    partition_cols: list[str],
    ref_date_col: str = "ref_date",
) -> list[Path]:
    return write_partitioned_frame(
        frame,
        output_root,
        primary_keys=primary_keys,
        ref_date_col=ref_date_col,
        partition_cols=partition_cols,
    )


def _long_layout(bronze: pl.DataFrame) -> pl.DataFrame | None:
    amount_column = _first_existing(
        bronze,
        "valor",
        "arrecadacao",
        "valor_arrecadado",
        "arrecadacao_r",
        "collection_amount_brl",
    )
    if amount_column is None:
        return None
    frame = bronze.with_columns(
        [
            _ref_date_expr(bronze).alias("ref_date"),
            pl.col(amount_column)
            .map_elements(parse_decimal, return_dtype=pl.Float64)
            .alias("collection_amount_brl"),
        ]
    )
    if frame["ref_date"].null_count() == frame.height:
        return None
    return frame


def _wide_layout(bronze: pl.DataFrame) -> pl.DataFrame | None:
    value_columns = [column for column in bronze.columns if _month_from_column(column) is not None]
    value_columns = [column for column in value_columns if column.startswith("raw_")]
    if not value_columns:
        return None

    frames: list[pl.DataFrame] = []
    id_columns = [column for column in bronze.columns if column not in value_columns]
    for value_column in value_columns:
        month_info = _month_from_column(value_column)
        if month_info is None:
            continue
        column_year, column_month = month_info
        ref_date_expr = (
            pl.lit(_month_end(column_year, column_month))
            if column_year is not None
            else _year_expr(bronze).map_elements(
                lambda year, month=column_month: _month_end(int(year), month)
                if year is not None
                else None,
                return_dtype=pl.Date,
            )
        )
        frames.append(
            bronze.select([*id_columns, pl.col(value_column).alias("raw_wide_value")]).with_columns(
                [
                    ref_date_expr.alias("ref_date"),
                    pl.col("raw_wide_value")
                    .map_elements(parse_decimal, return_dtype=pl.Float64)
                    .alias("collection_amount_brl"),
                ]
            )
        )
    if not frames:
        return None
    return pl.concat(frames, how="diagonal_relaxed")


def _ref_date_expr(frame: pl.DataFrame) -> pl.Expr:
    period_expr = _coalesced_expr(
        frame,
        "periodo",
        "competencia",
        "mes_de_arrecadacao",
        "mes_arrecadacao",
        "reference_period",
    )
    return pl.struct(
        [
            period_expr.alias("period_text"),
            _year_expr(frame).alias("year_value"),
            _month_expr(frame).alias("month_value"),
        ]
    ).map_elements(_ref_date_from_values, return_dtype=pl.Date)


def _year_expr(frame: pl.DataFrame) -> pl.Expr:
    return _coalesced_expr(frame, "ano", "year").map_elements(
        _year_from_text,
        return_dtype=pl.Int64,
    )


def _month_expr(frame: pl.DataFrame) -> pl.Expr:
    return _coalesced_expr(frame, "mes", "mês", "month").map_elements(
        _month_from_text,
        return_dtype=pl.Int64,
    )


def _ref_date_from_values(values: dict[str, object]) -> date | None:
    period = _period_to_month_end(values.get("period_text"))
    if period is not None:
        return period
    year = values.get("year_value")
    month = values.get("month_value")
    if year is None or month is None:
        return None
    return _month_end(int(year), int(month))


def _text_expr(frame: pl.DataFrame, *aliases: str) -> pl.Expr:
    return _coalesced_expr(frame, *aliases).map_elements(_text, return_dtype=pl.Utf8)


def _coalesced_expr(frame: pl.DataFrame, *aliases: str) -> pl.Expr:
    exprs: list[pl.Expr] = []
    for alias in aliases:
        normalized = normalize_column_name(alias)
        for candidate in (normalized, f"raw_{normalized}"):
            if candidate in frame.columns:
                exprs.append(pl.col(candidate).cast(pl.Utf8, strict=False))
    if not exprs:
        return pl.lit(None, dtype=pl.Utf8)
    if len(exprs) == 1:
        return exprs[0]
    return pl.coalesce(exprs)


def _first_existing(frame: pl.DataFrame, *aliases: str) -> str | None:
    columns = set(frame.columns)
    for alias in aliases:
        normalized = normalize_column_name(alias)
        for candidate in (normalized, f"raw_{normalized}"):
            if candidate in columns:
                return candidate
    return None


def _existing_or_literal(frame: pl.DataFrame, column: str, value: object) -> pl.Expr:
    if column in frame.columns:
        return pl.col(column)
    return pl.lit(value)


def _source_last_modified_expr(frame: pl.DataFrame) -> pl.Expr:
    exprs = [
        pl.col(column)
        for column in (
            "resource_last_modified",
            "ckan_resource_last_modified",
            "http_last_modified",
            "resource_updated_at",
        )
        if column in frame.columns
    ]
    if not exprs:
        return pl.lit(None)
    return pl.coalesce(exprs)


def _first_seen_expr(frame: pl.DataFrame) -> pl.Expr:
    exprs = [
        pl.col(column)
        for column in ("first_seen_timestamp_utc", "download_timestamp_utc")
        if column in frame.columns
    ]
    if not exprs:
        return pl.lit(None)
    return pl.coalesce(exprs)


def _source_table_expr(frame: pl.DataFrame) -> pl.Expr:
    exprs = []
    for column in ("sheet_name", "resource_name", "resource_family", "inner_filename"):
        if column in frame.columns:
            exprs.append(pl.col(column).cast(pl.Utf8, strict=False))
    return pl.coalesce(exprs) if exprs else pl.lit("unknown")


def _text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _text_or_default(value: object, default: str) -> str:
    text = _text(value)
    return text if text is not None else default


def _year_from_text(value: object) -> int | None:
    if value is None:
        return None
    digits = "".join(ch for ch in str(value).strip() if ch.isdigit())
    if len(digits) >= 4:
        return int(digits[:4])
    return None


def _month_from_text(value: object) -> int | None:
    if value is None:
        return None
    text = normalize_column_name(str(value))
    if not text:
        return None
    digits = "".join(ch for ch in text if ch.isdigit())
    if digits:
        month = int(digits[-2:] if len(digits) > 1 else digits)
        return month if 1 <= month <= 12 else None
    months = {
        "jan": 1,
        "janeiro": 1,
        "fev": 2,
        "fevereiro": 2,
        "mar": 3,
        "marco": 3,
        "abr": 4,
        "abril": 4,
        "mai": 5,
        "maio": 5,
        "jun": 6,
        "junho": 6,
        "jul": 7,
        "julho": 7,
        "ago": 8,
        "agosto": 8,
        "set": 9,
        "setembro": 9,
        "out": 10,
        "outubro": 10,
        "nov": 11,
        "novembro": 11,
        "dez": 12,
        "dezembro": 12,
    }
    return months.get(text)


def _period_to_month_end(value: object) -> date | None:
    if value is None:
        return None
    text = normalize_column_name(str(value))
    digits = "".join(ch for ch in text if ch.isdigit())
    if len(digits) >= 6:
        year = int(digits[:4])
        month = int(digits[4:6])
        if 1 <= month <= 12:
            return _month_end(year, month)
    parts = text.split("_")
    if len(parts) >= 2:
        month = _month_from_text(parts[0])
        year = _year_from_text(parts[-1])
        if month is not None and year is not None:
            return _month_end(year, month)
    return None


def _month_from_column(column: str) -> tuple[int | None, int] | None:
    token = normalize_column_name(column.removeprefix("raw_"))
    match = None
    import re

    match = re.fullmatch(r"(?P<year>\d{4})_(?P<month>\d{1,2})", token)
    if match:
        month = int(match.group("month"))
        return (int(match.group("year")), month) if 1 <= month <= 12 else None
    match = re.fullmatch(r"(?P<month>\d{1,2})_(?P<year>\d{4})", token)
    if match:
        month = int(match.group("month"))
        return (int(match.group("year")), month) if 1 <= month <= 12 else None
    month = _month_from_text(token)
    return (None, month) if month is not None else None


def _heuristic_available_date(ref_date: date | None) -> date | None:
    if ref_date is None:
        return None
    next_month_year = ref_date.year + (1 if ref_date.month == 12 else 0)
    next_month = 1 if ref_date.month == 12 else ref_date.month + 1
    next_month_end = _month_end(next_month_year, next_month)
    release_anchor = _last_business_day_on_or_before(next_month_end)
    return _add_business_days(release_anchor, 5)


def _snapshot_available_date(values: dict[str, object]) -> date | None:
    source_publication = values.get("source_publication_datetime_utc")
    if source_publication is not None:
        return available_date_from_source_datetime(source_publication)
    source_last_modified = values.get("source_last_modified_utc")
    if source_last_modified is not None:
        return available_date_from_source_datetime(source_last_modified)
    return available_date_from_first_seen(values.get("first_seen_timestamp_utc"))


def _snapshot_vintage_id(values: dict[str, object]) -> str:
    dataset_id = _text(values.get("source_dataset")) or "receita_tax_collection_monthly"
    resource_id = _text(values.get("source_table")) or _text(values.get("raw_path")) or dataset_id
    content_hash = _text(values.get("sha256"))
    publication_timestamp = None
    first_seen_timestamp_utc = None
    if content_hash is None:
        publication_timestamp = values.get("source_publication_datetime_utc") or values.get(
            "source_last_modified_utc"
        )
        if publication_timestamp is None:
            first_seen_timestamp_utc = values.get("first_seen_timestamp_utc")
    return make_vintage_id(
        source="receita",
        dataset_id=dataset_id,
        resource_id=resource_id,
        publication_timestamp=publication_timestamp,
        first_seen_timestamp_utc=first_seen_timestamp_utc,
        content_hash=content_hash,
    )


def _model_usable(values: dict[str, object]) -> bool:
    first_seen_or_source = values.get("first_seen_timestamp_utc") or values.get(
        "source_last_modified_utc"
    )
    return choose_model_usable(
        configured_model_usable=True,
        availability_basis=_text(values.get("availability_basis")),
        revision_policy=_text(values.get("revision_policy")) or "",
        available_date=values.get("available_date"),
        vintage_id=_text(values.get("vintage_id")),
        first_seen_timestamp_utc=first_seen_or_source,
    )


def _last_business_day_on_or_before(value: date) -> date:
    if is_business_day(value):
        return value
    return previous_business_day(value)


def _add_business_days(value: date, days: int) -> date:
    candidate = value
    remaining = days
    while remaining:
        candidate += timedelta(days=1)
        if is_business_day(candidate):
            remaining -= 1
    return candidate


def _month_end(year: int, month: int) -> date:
    return date(year, month, monthrange(year, month)[1])


def _table_kind(source_table: object) -> str:
    text = normalize_column_name(source_table or "")
    if "total" in text:
        return "total"
    if any(token in text for token in ("categoria", "grupo", "classificacao")):
        return "by_revenue_category"
    if any(token in text for token in ("tributo", "receita", "arrecadacao", "tax")):
        return "by_tax"
    return "unknown"


def _revenue_key(values: dict[str, object]) -> str:
    code = _text(values.get("revenue_code"))
    if code and code != "unknown":
        return normalize_column_name(code) or "unknown"
    payload = "|".join(
        normalize_column_name(_text(values.get(column)) or "unknown")
        for column in ("revenue_category", "revenue_subcategory", "revenue_name", "source_table")
    )
    return payload or "unknown"
