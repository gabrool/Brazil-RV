from __future__ import annotations

from datetime import date

import polars as pl
import pytest

from bralpha.derived.receita.schemas import PANEL_PRIMARY_KEYS
from bralpha.derived.receita.tax_collection import (
    build_tax_collection_feature_observation,
    build_tax_collection_observation,
    receita_tax_collection_feature_id,
)


def test_tax_collection_observation_preserves_official_rows_and_missingness():
    silver = pl.DataFrame([_silver_row("001", 10.0), _silver_row("002", None)])

    panel = build_tax_collection_observation(silver)
    by_code = panel.sort("revenue_code")

    assert panel.height == 2
    assert by_code["collection_amount_brl"].to_list() == [10.0, None]
    assert by_code["has_collection_amount_brl"].to_list() == [True, False]
    assert by_code["revenue_key"].to_list() == ["001_irpj", "002_cofins"]
    assert panel.group_by(PANEL_PRIMARY_KEYS["tax_collection_observation"]).len().height == 2
    assert "download_timestamp_utc" not in panel.columns
    assert "raw_path" not in panel.columns
    assert "sha256" not in panel.columns


def test_tax_collection_observation_filters_by_ref_date_without_aggregation():
    silver = pl.DataFrame(
        [
            _silver_row("001", 10.0, ref_date=date(2024, 1, 31)),
            _silver_row("002", 20.0, ref_date=date(2024, 2, 29)),
        ]
    )

    panel = build_tax_collection_observation(
        silver,
        start=date(2024, 2, 1),
        end=date(2024, 2, 29),
    )

    assert panel.height == 1
    assert panel["collection_amount_brl"].to_list() == [20.0]


def test_tax_collection_feature_observation_uses_deterministic_feature_ids():
    observations = build_tax_collection_observation(
        pl.DataFrame([_silver_row("001", 10.0, raw_path="different/path.csv")])
    )

    panel = build_tax_collection_feature_observation(observations, max_features=10)

    expected = "receita_tax_collection|federal_total|principal|001_irpj"
    assert panel["feature_id"].to_list() == [expected]
    assert receita_tax_collection_feature_id("Federal Total", "Principal", "001 IRPJ") == expected
    assert panel["collection_amount_brl"].to_list() == [10.0]
    assert "2024" not in panel["feature_id"][0]
    assert "path" not in panel["feature_id"][0]
    assert "sha" not in panel["feature_id"][0]
    duplicate_check = panel.group_by(PANEL_PRIMARY_KEYS["tax_collection_feature_observation"]).len()
    assert duplicate_check.height == 1


def test_tax_collection_feature_observation_uses_unknown_tokens_for_blank_values():
    observations = build_tax_collection_observation(
        pl.DataFrame(
            [
                _silver_row(
                    "001",
                    10.0,
                    collection_scope="",
                    table_kind=None,
                    revenue_key="",
                )
            ]
        )
    )

    panel = build_tax_collection_feature_observation(observations, max_features=10)

    assert panel["feature_id"].to_list() == ["receita_tax_collection|unknown|unknown|unknown"]


def test_tax_collection_feature_observation_raises_on_same_date_feature_collision():
    observations = pl.DataFrame(
        [
            _observation_row("001", "same_key", "IR", "IRPJ"),
            _observation_row("002", "same_key", "COFINS", "COFINS"),
        ]
    )

    with pytest.raises(ValueError, match="feature_id collision"):
        build_tax_collection_feature_observation(observations, max_features=10)


def test_tax_collection_feature_observation_max_features_guard():
    observations = build_tax_collection_observation(
        pl.DataFrame([_silver_row("001", 10.0), _silver_row("002", 20.0)])
    )

    with pytest.raises(ValueError, match="max_features=1"):
        build_tax_collection_feature_observation(observations, max_features=1)


def _silver_row(
    code: str,
    amount: float | None,
    *,
    ref_date: date = date(2024, 1, 31),
    collection_scope: str | None = "federal_total",
    table_kind: str | None = "principal",
    revenue_key: str | None = None,
    raw_path: str = "data/raw/receita/file.csv",
) -> dict[str, object]:
    revenue_name = "IRPJ" if code == "001" else "COFINS"
    key = revenue_key if revenue_key is not None else f"{code}_{revenue_name.lower()}"
    return {
        "ref_date": ref_date,
        "available_date": date(2024, 3, 7),
        "availability_policy": "receita_monthly_collection_conservative_next_month_end_plus_5bd",
        "year": ref_date.year,
        "month": ref_date.month,
        "collection_scope": collection_scope,
        "revenue_category": "IR" if code == "001" else "COFINS",
        "revenue_subcategory": revenue_name,
        "revenue_code": code,
        "revenue_key": key,
        "revenue_name": revenue_name,
        "table_kind": table_kind,
        "collection_amount_brl": amount,
        "unit": "BRL",
        "source_table": "arrecadacao",
        "source": "receita",
        "source_dataset": "receita_tax_collection_monthly",
        "download_timestamp_utc": "2024-03-08T12:00:00Z",
        "raw_path": raw_path,
        "sha256": "abc",
        "source_version": "v0",
    }


def _observation_row(
    code: str,
    revenue_key: str,
    category: str,
    name: str,
) -> dict[str, object]:
    return {
        "ref_date": date(2024, 1, 31),
        "available_date": date(2024, 3, 7),
        "availability_policy": "receita_monthly_collection_conservative_next_month_end_plus_5bd",
        "collection_scope": "federal_total",
        "revenue_category": category,
        "revenue_subcategory": name,
        "revenue_code": code,
        "revenue_key": revenue_key,
        "revenue_name": name,
        "table_kind": "principal",
        "feature_id": "not_used",
        "collection_amount_brl": 10.0,
        "unit": "BRL",
        "source_table": "arrecadacao",
        "has_collection_amount_brl": True,
        "source_version": "v0",
    }
