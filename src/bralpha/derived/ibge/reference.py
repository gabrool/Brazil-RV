from __future__ import annotations

import polars as pl

from bralpha.derived.ibge.quality import validate_panel
from bralpha.derived.ibge.schemas import (
    IBGE_NEWS_RELEASE_METADATA_COLUMNS,
    IBGE_PRODUCTS_REFERENCE_COLUMNS,
    IBGE_RELEASE_CALENDAR_REFERENCE_COLUMNS,
    PANEL_PRIMARY_KEYS,
)


def build_release_calendar_reference(silver: pl.DataFrame) -> pl.DataFrame:
    frame = silver.select(IBGE_RELEASE_CALENDAR_REFERENCE_COLUMNS)
    validate_panel(
        frame,
        required_columns=IBGE_RELEASE_CALENDAR_REFERENCE_COLUMNS,
        primary_keys=PANEL_PRIMARY_KEYS["release_calendar_reference"],
    )
    return frame


def build_products_reference(silver: pl.DataFrame) -> pl.DataFrame:
    frame = silver.select(IBGE_PRODUCTS_REFERENCE_COLUMNS)
    validate_panel(
        frame,
        required_columns=IBGE_PRODUCTS_REFERENCE_COLUMNS,
        primary_keys=PANEL_PRIMARY_KEYS["products_reference"],
    )
    return frame


def build_news_release_metadata(silver: pl.DataFrame) -> pl.DataFrame:
    frame = silver.select(IBGE_NEWS_RELEASE_METADATA_COLUMNS)
    validate_panel(
        frame,
        required_columns=IBGE_NEWS_RELEASE_METADATA_COLUMNS,
        primary_keys=PANEL_PRIMARY_KEYS["news_release_metadata"],
    )
    return frame
