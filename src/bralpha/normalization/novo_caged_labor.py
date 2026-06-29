from __future__ import annotations

import hashlib
import re
from calendar import monthrange
from datetime import date, timedelta
from pathlib import Path

import polars as pl

from bralpha.domain.b3_calendar import is_business_day, previous_business_day
from bralpha.ingestion.novo_caged.common import write_partitioned_frame
from bralpha.parsing.common import normalize_column_name, parse_decimal, parse_int
from bralpha.timing.availability import usable_date_from_date_only
from bralpha.timing.vintages import (
    AVAILABILITY_CONSERVATIVE_HEURISTIC,
    AVAILABILITY_OFFICIAL_RELEASE_CALENDAR,
    REVISION_CURRENT_SNAPSHOT_REFERENCE_ONLY,
    REVISION_UNREVISED,
    choose_model_usable,
    make_vintage_id,
)

NOVO_CAGED_MOVEMENT_AVAILABILITY_POLICY = (
    "novo_caged_conservative_next_month_end_plus_2bd_reference_only"
)
NOVO_CAGED_CALENDAR_AVAILABILITY_POLICY = "novo_caged_official_release_calendar"

NOVO_CAGED_PIT_COLUMNS = [
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
NOVO_CAGED_PIT_COLUMNS_WITHOUT_RELEASE_DATE = [
    column for column in NOVO_CAGED_PIT_COLUMNS if column != "release_date"
]

NOVO_CAGED_MOVEMENT_COLUMNS = [
    "movement_record_id",
    "ref_date",
    "available_date",
    "availability_policy",
    *NOVO_CAGED_PIT_COLUMNS,
    "competence",
    "year",
    "month",
    "record_kind",
    "region",
    "state",
    "municipality_code",
    "cnae_section",
    "cnae_subclass",
    "occupation_code",
    "movement_type_code",
    "movement_sign",
    "employment_category",
    "education_degree",
    "age",
    "sex",
    "race_color",
    "disability_type",
    "employer_type",
    "establishment_type",
    "establishment_size_jan",
    "contract_hours",
    "wage",
    "wage_unit",
    "is_apprentice",
    "is_intermittent",
    "is_part_time",
    "source_system",
    "raw_competenciamov",
    "raw_saldomovimentacao",
    "raw_tipomovimentacao",
    "raw_salario",
    "raw_valorsalariofixo",
    "source",
    "source_dataset",
    "download_timestamp_utc",
    "raw_path",
    "sha256",
    "source_version",
]

NOVO_CAGED_RELEASE_CALENDAR_COLUMNS = [
    "ref_date",
    "release_date",
    "available_date",
    "availability_policy",
    *NOVO_CAGED_PIT_COLUMNS_WITHOUT_RELEASE_DATE,
    "release_year",
    "competence_label",
    "source",
    "source_dataset",
    "download_timestamp_utc",
    "raw_path",
    "sha256",
    "source_version",
]

NOVO_CAGED_SILVER_COLUMNS_BY_DATASET = {
    "novo_caged_movements_monthly": NOVO_CAGED_MOVEMENT_COLUMNS,
    "novo_caged_release_calendar": NOVO_CAGED_RELEASE_CALENDAR_COLUMNS,
}


def normalize_novo_caged_to_silver(
    dataset_id: str,
    bronze: pl.DataFrame,
    *,
    source_version: str = "v0",
) -> pl.DataFrame:
    if dataset_id == "novo_caged_movements_monthly":
        return normalize_novo_caged_movements_monthly(bronze, source_version=source_version)
    if dataset_id == "novo_caged_release_calendar":
        return normalize_novo_caged_release_calendar(bronze, source_version=source_version)
    raise ValueError(f"Unsupported Novo CAGED silver dataset: {dataset_id}")


def normalize_novo_caged_movements_monthly(
    bronze: pl.DataFrame,
    *,
    source_version: str = "v0",
) -> pl.DataFrame:
    if bronze.is_empty():
        return pl.DataFrame(schema={column: pl.Null for column in NOVO_CAGED_MOVEMENT_COLUMNS})

    wage_expr = pl.coalesce(
        [
            _decimal_expr(bronze, "salario", "salário"),
            _decimal_expr(bronze, "valorsalariofixo", "valor salario fixo"),
        ]
    )
    frame = bronze.with_columns(
        [
            _text_expr(bronze, "competenciamov", "competência", "competencia").alias(
                "competence"
            ),
            _text_expr(bronze, "regiao", "região").alias("region"),
            _text_expr(bronze, "uf").alias("state"),
            _text_expr(bronze, "municipio", "município").alias("municipality_code"),
            _text_expr(bronze, "secao", "seção").alias("cnae_section"),
            _text_expr(bronze, "subclasse").alias("cnae_subclass"),
            _text_expr(bronze, "cbo2002ocupacao", "cbo2002ocupação", "cbo").alias(
                "occupation_code"
            ),
            _text_expr(bronze, "tipomovimentacao", "tipomovimentação").alias(
                "movement_type_code"
            ),
            _text_expr(bronze, "saldomovimentacao", "saldomovimentação").alias(
                "movement_sign"
            ),
            _text_expr(bronze, "categoria").alias("employment_category"),
            _text_expr(bronze, "graudeinstrucao", "grau de instrução").alias(
                "education_degree"
            ),
            _int_expr(bronze, "idade").alias("age"),
            _text_expr(bronze, "sexo").alias("sex"),
            _text_expr(bronze, "racacor", "raça cor", "raça_cor").alias("race_color"),
            _text_expr(bronze, "tipodedeficiencia", "tipo de deficiência").alias(
                "disability_type"
            ),
            _text_expr(bronze, "tipoempregador").alias("employer_type"),
            _text_expr(bronze, "tipoestabelecimento").alias("establishment_type"),
            _text_expr(bronze, "tamestabjan").alias("establishment_size_jan"),
            _decimal_expr(bronze, "horascontratuais").alias("contract_hours"),
            wage_expr.alias("wage"),
            _text_expr(bronze, "unidadesalariocodigo", "unidade salario codigo").alias(
                "wage_unit"
            ),
            _bool_expr(bronze, "indicadoraprendiz").alias("is_apprentice"),
            _bool_expr(bronze, "indtrabintermitente").alias("is_intermittent"),
            _bool_expr(bronze, "indtrabparcial").alias("is_part_time"),
            _text_expr(bronze, "origemdainformacao", "origem da informação").alias(
                "source_system"
            ),
            _existing_or_literal(bronze, "record_kind", "movement").alias("record_kind"),
            _existing_or_literal(bronze, "resource_name", None).alias("resource_name"),
            _existing_or_literal(bronze, "inner_filename", None).alias("inner_filename"),
            _existing_or_literal(bronze, "period", None).alias("period"),
            _existing_or_literal(bronze, "row_index", None).alias("row_index"),
            _raw_expr(bronze, "competenciamov").alias("raw_competenciamov"),
            _raw_expr(bronze, "saldomovimentacao").alias("raw_saldomovimentacao"),
            _raw_expr(bronze, "tipomovimentacao").alias("raw_tipomovimentacao"),
            _raw_expr(bronze, "salario").alias("raw_salario"),
            _raw_expr(bronze, "valorsalariofixo").alias("raw_valorsalariofixo"),
            _existing_or_literal(bronze, "source", "novo_caged").alias("source"),
            _existing_or_literal(bronze, "source_dataset", None).alias("source_dataset"),
            _existing_or_literal(bronze, "download_timestamp_utc", None).alias(
                "download_timestamp_utc"
            ),
            _existing_or_literal(bronze, "source_publication_datetime_utc", None).alias(
                "source_publication_datetime_utc"
            ),
            _source_last_modified_expr(bronze).alias("source_last_modified_utc"),
            _first_seen_expr(bronze).alias("first_seen_timestamp_utc"),
            _existing_or_literal(bronze, "raw_path", None).alias("raw_path"),
            _existing_or_literal(bronze, "sha256", None).alias("sha256"),
            pl.lit(source_version).alias("source_version"),
        ]
    )
    frame = frame.with_columns(
        [
            pl.col("competence")
            .map_elements(_competence_to_month_end, return_dtype=pl.Date)
            .alias("ref_date")
        ]
    )
    frame = frame.with_columns(
        [
            pl.col("ref_date").dt.year().alias("year"),
            pl.col("ref_date").dt.month().alias("month"),
            pl.col("ref_date")
            .map_elements(_movement_available_date, return_dtype=pl.Date)
            .alias("available_date"),
            pl.lit(NOVO_CAGED_MOVEMENT_AVAILABILITY_POLICY).alias("availability_policy"),
            pl.lit(AVAILABILITY_CONSERVATIVE_HEURISTIC).alias("availability_basis"),
            pl.lit(REVISION_CURRENT_SNAPSHOT_REFERENCE_ONLY).alias("revision_policy"),
            pl.lit(None).alias("release_date"),
            pl.lit(0).alias("revision_sequence"),
            pl.lit(False).alias("model_usable"),
            pl.lit(NOVO_CAGED_MOVEMENT_AVAILABILITY_POLICY).alias("model_usable_reason"),
        ]
    )
    id_columns = [
        "source_dataset",
        "resource_name",
        "inner_filename",
        "period",
        "record_kind",
        "row_index",
        "competence",
        "municipality_code",
        "cnae_subclass",
        "occupation_code",
        "movement_type_code",
        "movement_sign",
        "wage",
    ]
    frame = frame.with_columns(
        pl.struct(id_columns)
        .map_elements(_movement_record_id, return_dtype=pl.Utf8)
        .alias("movement_record_id")
    )
    frame = frame.with_columns(
        pl.struct(
            [
                "source_dataset",
                "resource_name",
                "raw_path",
                "first_seen_timestamp_utc",
                "sha256",
            ]
        )
        .map_elements(_snapshot_vintage_id, return_dtype=pl.Utf8)
        .alias("vintage_id")
    )
    return frame.select(NOVO_CAGED_MOVEMENT_COLUMNS)


def normalize_novo_caged_release_calendar(
    bronze: pl.DataFrame,
    *,
    source_version: str = "v0",
) -> pl.DataFrame:
    text = "\n".join(str(value) for value in bronze.get_column("raw_text").to_list())
    rows: list[dict[str, object]] = []
    lineage = _first_lineage(bronze, source_version=source_version)
    for release_date, competence_label in _calendar_rows(text):
        ref_date = _competence_label_to_month_end(competence_label)
        rows.append(
            {
                "ref_date": ref_date,
                "release_date": release_date,
                "available_date": usable_date_from_date_only(release_date),
                "availability_policy": NOVO_CAGED_CALENDAR_AVAILABILITY_POLICY,
                "availability_basis": AVAILABILITY_OFFICIAL_RELEASE_CALENDAR,
                "revision_policy": REVISION_UNREVISED,
                "source_publication_datetime_utc": None,
                "source_last_modified_utc": lineage.get("source_last_modified_utc"),
                "first_seen_timestamp_utc": lineage.get("first_seen_timestamp_utc"),
                "vintage_id": make_vintage_id(
                    source="novo_caged",
                    dataset_id="novo_caged_release_calendar",
                    resource_id=str(lineage.get("raw_path") or "release_calendar"),
                    observation_key=ref_date.isoformat() if ref_date else None,
                    publication_timestamp=release_date,
                    first_seen_timestamp_utc=lineage.get("first_seen_timestamp_utc"),
                    content_hash=str(lineage.get("sha256") or ""),
                ),
                "revision_sequence": 0,
                "model_usable": choose_model_usable(
                    configured_model_usable=True,
                    availability_basis=AVAILABILITY_OFFICIAL_RELEASE_CALENDAR,
                    revision_policy=REVISION_UNREVISED,
                    available_date=usable_date_from_date_only(release_date),
                ),
                "model_usable_reason": NOVO_CAGED_CALENDAR_AVAILABILITY_POLICY,
                "release_year": release_date.year,
                "competence_label": competence_label,
                **lineage,
            }
        )
    if not rows:
        raise ValueError("No Novo CAGED release-calendar date rows found")
    return (
        pl.DataFrame(rows)
        .unique(subset=["ref_date"], keep="last", maintain_order=True)
        .select(NOVO_CAGED_RELEASE_CALENDAR_COLUMNS)
    )


def write_novo_caged_silver(
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


def _text_expr(frame: pl.DataFrame, *aliases: str) -> pl.Expr:
    return _coalesced_expr(frame, *aliases).map_elements(_text, return_dtype=pl.Utf8)


def _decimal_expr(frame: pl.DataFrame, *aliases: str) -> pl.Expr:
    return _coalesced_expr(frame, *aliases).map_elements(parse_decimal, return_dtype=pl.Float64)


def _int_expr(frame: pl.DataFrame, *aliases: str) -> pl.Expr:
    return _coalesced_expr(frame, *aliases).map_elements(parse_int, return_dtype=pl.Int64)


def _bool_expr(frame: pl.DataFrame, *aliases: str) -> pl.Expr:
    return _coalesced_expr(frame, *aliases).map_elements(_bool_or_none, return_dtype=pl.Boolean)


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


def _raw_expr(frame: pl.DataFrame, alias: str) -> pl.Expr:
    column = f"raw_{normalize_column_name(alias)}"
    if column in frame.columns:
        return pl.col(column).cast(pl.Utf8, strict=False)
    return pl.lit(None, dtype=pl.Utf8)


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


def _text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _bool_or_none(value: object) -> bool | None:
    if value is None:
        return None
    text = normalize_column_name(str(value))
    if not text:
        return None
    if text in {"1", "s", "sim", "true", "t", "yes"}:
        return True
    if text in {"0", "n", "nao", "false", "f", "no"}:
        return False
    return None


def _competence_to_month_end(value: object) -> date | None:
    period = _period_text(value)
    if period is None:
        return None
    year = int(period[:4])
    month = int(period[4:6])
    return _month_end(year, month)


def _period_text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    digits = "".join(ch for ch in text if ch.isdigit())
    if len(digits) >= 6:
        return digits[:6]
    return None


def _movement_available_date(ref_date: date | None) -> date | None:
    if ref_date is None:
        return None
    next_month_year = ref_date.year + (1 if ref_date.month == 12 else 0)
    next_month = 1 if ref_date.month == 12 else ref_date.month + 1
    next_month_end = _month_end(next_month_year, next_month)
    release_anchor = _last_business_day_on_or_before(next_month_end)
    return _add_business_days(release_anchor, 2)


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


def _movement_record_id(values: dict[str, object]) -> str:
    payload = "|".join("" if value is None else str(value) for value in values.values())
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _snapshot_vintage_id(values: dict[str, object]) -> str:
    dataset_id = _text(values.get("source_dataset")) or "novo_caged_movements_monthly"
    resource_id = _text(values.get("resource_name")) or _text(values.get("raw_path")) or dataset_id
    return make_vintage_id(
        source="novo_caged",
        dataset_id=dataset_id,
        resource_id=resource_id,
        publication_timestamp=None,
        first_seen_timestamp_utc=None,
        content_hash=None,
    )


def _calendar_rows(text: str) -> list[tuple[date, str]]:
    pattern = re.compile(
        r"(?P<release>\d{2}/\d{2}/\d{4})\s*-\s*Compet[eê]ncia:\s*"
        r"(?P<label>[A-Za-zÀ-ÿçÇãõáéíóúâêôàü\s]+?\s+de\s+\d{4})",
        re.IGNORECASE,
    )
    return [
        (_parse_brazil_date(match.group("release")), " ".join(match.group("label").split()))
        for match in pattern.finditer(text)
    ]


def _parse_brazil_date(value: str) -> date:
    day, month, year = value.split("/")
    return date(int(year), int(month), int(day))


def _competence_label_to_month_end(value: str) -> date:
    normalized = normalize_column_name(value)
    parts = normalized.split("_")
    year = int(parts[-1])
    month_name = parts[0]
    months = {
        "janeiro": 1,
        "fevereiro": 2,
        "marco": 3,
        "abril": 4,
        "maio": 5,
        "junho": 6,
        "julho": 7,
        "agosto": 8,
        "setembro": 9,
        "outubro": 10,
        "novembro": 11,
        "dezembro": 12,
    }
    month = months[month_name]
    return _month_end(year, month)


def _month_end(year: int, month: int) -> date:
    return date(year, month, monthrange(year, month)[1])


def _first_lineage(bronze: pl.DataFrame, *, source_version: str) -> dict[str, object]:
    row = bronze.to_dicts()[0] if not bronze.is_empty() else {}
    return {
        "source": row.get("source", "novo_caged"),
        "source_dataset": row.get("source_dataset"),
        "download_timestamp_utc": row.get("download_timestamp_utc"),
        "raw_path": row.get("raw_path"),
        "sha256": row.get("sha256"),
        "source_last_modified_utc": _source_last_modified(row),
        "first_seen_timestamp_utc": row.get("first_seen_timestamp_utc")
        or row.get("download_timestamp_utc"),
        "source_version": source_version,
    }


def _source_last_modified(row: dict[str, object]) -> object:
    for column in (
        "resource_last_modified",
        "ckan_resource_last_modified",
        "http_last_modified",
        "resource_updated_at",
    ):
        value = row.get(column)
        if value is not None:
            return value
    return None
