from __future__ import annotations

import json
import shutil
from datetime import UTC, date, datetime

import polars as pl

from bralpha.infra.config import load_ibge_dataset_registry
from bralpha.infra.http import HttpResponse
from bralpha.ingestion.ibge.sidra import (
    SidraSeriesConfig,
    build_sidra_request,
    load_sidra_series_config,
    resolve_sidra_periods,
)
from bralpha.normalization.ibge_calendar import write_calendar_silver
from bralpha.normalization.ibge_sidra import normalize_sidra_to_silver
from bralpha.parsing.ibge_sidra import parse_sidra_bytes
from bralpha.pipelines.ibge_ingest import run_ibge_ingest


class MockClient:
    def __init__(self, content: bytes) -> None:
        self.content = content
        self.requests = []

    def get_bytes(self, url, params=None, headers=None):
        self.requests.append({"url": url, "params": params or {}})
        return HttpResponse(
            url=f"{url}?mock={len(self.requests)}",
            status_code=200,
            headers={"content-type": "application/json"},
            content=self.content,
        )


def test_sidra_request_uses_official_endpoint_and_configured_params(repo_root):
    dataset = load_ibge_dataset_registry(repo_root).get("ibge_sidra_series")
    series = load_sidra_series_config(repo_root)[0]

    url, params, filename = build_sidra_request(dataset, series=series, periods="202401")

    assert url.endswith("/agregados/7060/periodos/202401/variaveis/all")
    assert params["localidades"] == "N1[all]"
    assert params["classificacao"] == "315[all]"
    assert params["view"] == ""
    assert filename.startswith("ibge_sidra_ipca_7060_")


def test_sidra_period_resolver_builds_monthly_and_quarterly_ranges():
    monthly = _series(frequency="monthly")
    quarterly = _series(frequency="quarterly")

    assert resolve_sidra_periods(
        monthly,
        start=date(2024, 1, 15),
        end=date(2024, 3, 2),
    ) == "202401|202402|202403"
    assert resolve_sidra_periods(
        quarterly,
        start=date(2024, 2, 1),
        end=date(2024, 8, 1),
    ) == "202401|202402|202403"


def test_sidra_parser_handles_nested_shape_and_classifications(repo_root):
    bronze = parse_sidra_bytes(
        _sidra_payload({"202401": "0.42"}),
        dataset_slug="ipca",
        aggregate_id=7060,
        source_dataset="ibge_sidra_series",
        download_timestamp_utc=datetime(2024, 2, 9, 12, tzinfo=UTC),
        raw_path=repo_root / "raw.json",
        sha256="abc",
    )

    row = bronze.row(0, named=True)
    assert row["variable_id"] == "63"
    assert row["unit"] == "%"
    assert row["geography_level"] == "N1"
    assert row["classification_key"] == "315=7169"
    assert json.loads(row["classifications_json"])[0]["category_name"] == "Indice geral"
    assert row["raw_value"] == "0.42"


def test_sidra_normalizer_preserves_missing_symbols_and_calendar_availability(repo_root):
    bronze = parse_sidra_bytes(
        _sidra_payload({"202401": "0.42", "202402": "...", "202403": "X"}),
        dataset_slug="ipca",
        aggregate_id=7060,
        source_dataset="ibge_sidra_series",
        download_timestamp_utc=datetime(2024, 2, 9, 12, tzinfo=UTC),
        raw_path=repo_root / "raw.json",
        sha256="abc",
    )
    calendar = pl.DataFrame(
        [
            {
                "event_id": 1,
                "product_id": 9256,
                "reference_period_start": date(2024, 1, 1),
                "reference_period_end": date(2024, 1, 31),
                "release_date": date(2024, 2, 9),
                "available_datetime_local": datetime(2024, 2, 9, 9),
                "available_datetime_utc": datetime(2024, 2, 9, 12),
                "available_date": date(2024, 2, 9),
                "availability_policy": "exact_timestamp_cutoff",
            }
        ]
    )

    silver = normalize_sidra_to_silver(
        bronze,
        series_config=load_sidra_series_config(repo_root),
        release_calendar=calendar,
    ).sort("period_code")

    matched = silver.row(0, named=True)
    assert matched["ref_period_start"] == date(2024, 1, 1)
    assert matched["ref_period_end"] == date(2024, 1, 31)
    assert matched["available_date"] == date(2024, 2, 9)
    assert matched["model_usable"] is True
    assert matched["value"] == 0.42
    assert matched["value_status"] == "ok"
    assert silver["value_status"].to_list()[1:] == ["missing", "withheld"]
    assert silver["model_usable"].to_list()[1:] == [False, False]
    assert silver["availability_policy"].to_list()[1:] == [
        "unmatched_release_calendar",
        "unmatched_release_calendar",
    ]


def test_sidra_normalizer_parses_quarterly_and_moving_quarter_periods(repo_root):
    bronze = parse_sidra_bytes(
        _sidra_payload({"202401": "1.0"}),
        dataset_slug="gdp_volume_change",
        aggregate_id=5932,
        source_dataset="ibge_sidra_series",
        download_timestamp_utc=datetime(2024, 2, 9, 12, tzinfo=UTC),
        raw_path=repo_root / "raw.json",
        sha256="abc",
    )
    pnad = parse_sidra_bytes(
        _sidra_payload({"202405": "5.6"}),
        dataset_slug="pnad_unemployment_rate",
        aggregate_id=6381,
        source_dataset="ibge_sidra_series",
        download_timestamp_utc=datetime(2024, 6, 26, 12, tzinfo=UTC),
        raw_path=repo_root / "pnad.json",
        sha256="def",
    )

    silver = normalize_sidra_to_silver(
        pl.concat([bronze, pnad], how="diagonal_relaxed"),
        series_config=load_sidra_series_config(repo_root),
    ).sort("dataset_slug")

    gdp = silver.filter(pl.col("dataset_slug") == "gdp_volume_change").row(0, named=True)
    pnad_row = silver.filter(pl.col("dataset_slug") == "pnad_unemployment_rate").row(
        0,
        named=True,
    )
    assert gdp["ref_period_start"] == date(2024, 1, 1)
    assert gdp["ref_period_end"] == date(2024, 3, 31)
    assert pnad_row["ref_period_start"] == date(2024, 3, 1)
    assert pnad_row["ref_period_end"] == date(2024, 5, 31)


def test_ibge_sidra_pipeline_uses_calendar_and_upserts_silver(repo_root, tmp_path):
    shutil.copytree(repo_root / "configs", tmp_path / "configs")
    write_calendar_silver(
        pl.DataFrame(
            [
                {
                    "event_id": 1,
                    "product_id": 9256,
                    "product_name": "IPCA",
                    "survey_code": "ipca",
                    "survey_name": "IPCA",
                    "release_title": "IPCA Jan",
                    "release_date": date(2024, 2, 9),
                    "release_time_local": None,
                    "available_datetime_local": None,
                    "available_datetime_utc": None,
                    "available_date": date(2024, 2, 12),
                    "availability_policy": "date_only_next_business_day",
                    "reference_period": "2024-01/2024-01",
                    "reference_period_start": date(2024, 1, 1),
                    "reference_period_end": date(2024, 1, 31),
                    "source": "ibge",
                    "source_dataset": "ibge_release_calendar",
                    "download_timestamp_utc": datetime(2024, 2, 1, 12),
                    "raw_path": "calendar.json",
                    "sha256": "calendar",
                    "source_version": "v0",
                }
            ]
        ),
        tmp_path / "data" / "silver" / "ibge_release_calendar",
    )
    client = MockClient(_sidra_payload({"202401": "0.42"}))

    status = run_ibge_ingest(
        repo_root=tmp_path,
        dataset_id="ibge_sidra_series",
        start=date(2024, 1, 1),
        end=date(2024, 1, 31),
        client=client,
        dataset_slugs=["ipca"],
    )
    run_ibge_ingest(
        repo_root=tmp_path,
        dataset_id="ibge_sidra_series",
        start=date(2024, 1, 1),
        end=date(2024, 1, 31),
        client=client,
        dataset_slugs=["ipca"],
    )
    silver = pl.read_parquet(
        tmp_path / "data" / "silver" / "ibge_sidra_series" / "year=2024" / "data.parquet"
    )

    assert status["downloads"] == 1
    assert silver.height == 1
    assert silver["available_date"].item() == date(2024, 2, 12)
    assert silver["model_usable"].item() is True
    assert (tmp_path / "data" / "manifests" / "ibge" / "downloads.jsonl").exists()


def _series(frequency: str) -> SidraSeriesConfig:
    return SidraSeriesConfig(
        dataset_slug="test",
        priority="P0",
        aggregate_id=1,
        table_name="Test",
        survey_code="test",
        frequency=frequency,
        period_selector="date_range",
        variables="all",
        locations="N1[all]",
        classifications="",
        view="",
        model_usable=True,
        release_calendar_product_id=None,
        availability_policy="calendar_or_date_only_next_business_day",
    )


def _sidra_payload(values: dict[str, str]) -> bytes:
    return json.dumps(
        [
            {
                "id": "63",
                "variavel": "IPCA - Variacao mensal",
                "unidade": "%",
                "resultados": [
                    {
                        "classificacoes": [
                            {
                                "id": "315",
                                "nome": "Geral, grupo, subgrupo, item e subitem",
                                "categoria": {"7169": "Indice geral"},
                            }
                        ],
                        "series": [
                            {
                                "localidade": {
                                    "id": "1",
                                    "nivel": {"id": "N1", "nome": "Brasil"},
                                    "nome": "Brasil",
                                },
                                "serie": values,
                            }
                        ],
                    }
                ],
            }
        ]
    ).encode()
