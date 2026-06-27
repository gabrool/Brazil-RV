from __future__ import annotations

from datetime import date, datetime

import polars as pl

from bralpha.derived.ibge.reference import (
    build_news_release_metadata,
    build_products_reference,
    build_release_calendar_reference,
)
from bralpha.derived.ibge.schemas import (
    IBGE_NEWS_RELEASE_METADATA_COLUMNS,
    IBGE_PRODUCTS_REFERENCE_COLUMNS,
    IBGE_RELEASE_CALENDAR_REFERENCE_COLUMNS,
)


def test_release_calendar_reference_preserves_required_metadata_and_timing_fields():
    panel = build_release_calendar_reference(
        pl.DataFrame(
            [
                {
                    "event_id": "9256-202401",
                    "product_id": 9256,
                    "product_name": "IPCA",
                    "survey_code": "snipc",
                    "survey_name": "Sistema Nacional de Indices de Precos ao Consumidor",
                    "release_title": "IPCA janeiro 2024",
                    "release_date": date(2024, 2, 9),
                    "release_time_local": "09:00:00",
                    "available_datetime_local": datetime(2024, 2, 9, 9),
                    "available_datetime_utc": datetime(2024, 2, 9, 12),
                    "available_date": date(2024, 2, 9),
                    "availability_policy": "exact_timestamp_cutoff",
                    "reference_period": "202401",
                    "reference_period_start": date(2024, 1, 1),
                    "reference_period_end": date(2024, 1, 31),
                    "source_version": "v0",
                    "extra_source_field": "kept out of gold",
                }
            ]
        )
    )

    row = panel.row(0, named=True)
    assert panel.columns == IBGE_RELEASE_CALENDAR_REFERENCE_COLUMNS
    assert row["event_id"] == "9256-202401"
    assert row["available_datetime_local"] == datetime(2024, 2, 9, 9)
    assert row["available_datetime_utc"] == datetime(2024, 2, 9, 12)
    assert row["available_date"] == date(2024, 2, 9)
    assert row["availability_policy"] == "exact_timestamp_cutoff"


def test_products_reference_preserves_category_fields_without_fake_parent_product():
    panel = build_products_reference(
        pl.DataFrame(
            [
                {
                    "product_id": 9256,
                    "product_name": "IPCA",
                    "product_type": "Pesquisa",
                    "parent_product_id": None,
                    "alias": "ipca",
                    "acronym": "IPCA",
                    "category_id": 1,
                    "category_name": "Precos",
                    "parent_category_id": 10,
                    "parent_category_name": "Indicadores",
                    "path": "Indicadores/Precos/IPCA",
                    "source_version": "v0",
                }
            ]
        )
    )

    row = panel.row(0, named=True)
    assert panel.columns == IBGE_PRODUCTS_REFERENCE_COLUMNS
    assert row["product_id"] == 9256
    assert row["parent_product_id"] is None
    assert row["category_id"] == 1
    assert row["parent_category_id"] == 10
    assert row["path"] == "Indicadores/Precos/IPCA"


def test_news_release_metadata_is_metadata_only_and_preserves_timestamp_availability():
    panel = build_news_release_metadata(
        pl.DataFrame(
            [
                {
                    "news_id": 123,
                    "product_id": 9256,
                    "product_name": "IPCA",
                    "title": "IPCA varia 0,42% em janeiro",
                    "type": "Noticia",
                    "published_datetime_local": datetime(2024, 2, 9, 9, 5),
                    "published_datetime_utc": datetime(2024, 2, 9, 12, 5),
                    "published_date": date(2024, 2, 9),
                    "available_date": date(2024, 2, 9),
                    "url": "https://agenciadenoticias.ibge.gov.br/noticia/123",
                    "source_version": "v0",
                    "intro": "Do not parse or store article intro text",
                    "body": "Do not parse or store article body text",
                }
            ]
        )
    )

    row = panel.row(0, named=True)
    assert panel.columns == IBGE_NEWS_RELEASE_METADATA_COLUMNS
    assert row["news_id"] == 123
    assert row["title"] == "IPCA varia 0,42% em janeiro"
    assert row["published_datetime_local"] == datetime(2024, 2, 9, 9, 5)
    assert row["published_datetime_utc"] == datetime(2024, 2, 9, 12, 5)
    assert row["available_date"] == date(2024, 2, 9)
    assert "intro" not in panel.columns
    assert "body" not in panel.columns
