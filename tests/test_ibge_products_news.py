from __future__ import annotations

from datetime import UTC, date, datetime

from bralpha.infra.config import load_ibge_dataset_registry
from bralpha.ingestion.ibge.news import build_news_request
from bralpha.ingestion.ibge.products import build_products_request
from bralpha.normalization.ibge_news import normalize_news_to_silver
from bralpha.normalization.ibge_products import normalize_products_to_silver
from bralpha.parsing.ibge_news import parse_news_bytes
from bralpha.parsing.ibge_products import parse_products_bytes


def test_ibge_products_request_uses_statistics_endpoint(repo_root):
    dataset = load_ibge_dataset_registry(repo_root).get("ibge_products_metadata")

    url, params, filename = build_products_request(dataset)

    assert url.endswith("/api/v1/produtos/estatisticas")
    assert params == {}
    assert filename == "ibge_products_statistics.json"


def test_ibge_products_parser_and_normalizer_preserve_category_hierarchy(repo_root):
    bronze = parse_products_bytes(
        b'[{"id":9256,"tipo":"Estatisticas","titulo":"IPCA","alias":"ipca",'
        b'"sigla":"IPCA","catId":1,"catTitle":"NP-IPCA","parentCatId":2,'
        b'"parentCatTitle":"NP-Precos","path":"novo-portal/ipca"}]',
        source_dataset="ibge_products_metadata",
        download_timestamp_utc=datetime(2024, 1, 1, 12, tzinfo=UTC),
        raw_path=repo_root / "products.json",
        sha256="abc",
    )
    silver = normalize_products_to_silver(bronze)

    row = silver.row(0, named=True)
    assert row["product_id"] == 9256
    assert row["product_name"] == "IPCA"
    assert row["parent_product_id"] is None
    assert row["category_id"] == 1
    assert row["parent_category_id"] == 2


def test_ibge_news_request_uses_metadata_filters(repo_root):
    dataset = load_ibge_dataset_registry(repo_root).get("ibge_news_releases_metadata")

    url, params, filename = build_news_request(
        dataset,
        start=date(2024, 1, 1),
        end=date(2024, 1, 31),
        tipo="release",
        product_id=9256,
        page=3,
        page_size=25,
    )

    assert url.endswith("/api/v3/noticias/")
    assert params["tipo"] == "release"
    assert params["idproduto"] == "9256"
    assert params["page"] == "3"
    assert filename == "ibge_news_release_20240101_20240131_page3.json"


def test_ibge_news_parser_is_metadata_only_and_timing_is_strict(repo_root):
    bronze = parse_news_bytes(
        b'{"items":[{"id":47316,"tipo":"Release","titulo":"PNAD release",'
        b'"introducao":"Do not store this text",'
        b'"data_publicacao":"26/06/2026 19:00:00","produto_id":9171,'
        b'"produtos":"9171|Divulgacao mensal#pnadc1|slug|2511",'
        b'"editorias":"sociais","produtos_relacionados":"9171",'
        b'"destaque":true,"link":"https://example.test/release"}]}',
        source_dataset="ibge_news_releases_metadata",
        download_timestamp_utc=datetime(2026, 6, 26, 12, tzinfo=UTC),
        raw_path=repo_root / "news.json",
        sha256="abc",
    )
    silver = normalize_news_to_silver(bronze)

    assert "introducao" not in bronze.columns
    row = silver.row(0, named=True)
    assert row["news_id"] == 47316
    assert row["product_id"] == 9171
    assert row["product_name"] == "Divulgacao mensal"
    assert row["published_date"] == date(2026, 6, 26)
    assert row["available_date"] == date(2026, 6, 29)
    assert row["url"] == "https://example.test/release"


def test_ibge_news_missing_metadata_preserves_nulls(repo_root):
    bronze = parse_news_bytes(
        b'{"items":[{"id":1,"tipo":"Release","titulo":"Untimed"}]}',
        source_dataset="ibge_news_releases_metadata",
        download_timestamp_utc=datetime(2026, 6, 26, 12, tzinfo=UTC),
        raw_path=repo_root / "news.json",
        sha256="abc",
    )
    silver = normalize_news_to_silver(bronze)

    row = silver.row(0, named=True)
    assert row["published_date"] is None
    assert row["available_date"] is None
    assert row["product_id"] is None
