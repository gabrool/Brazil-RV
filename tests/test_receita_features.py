from __future__ import annotations

from datetime import date

import polars as pl
import pytest

from bralpha.derived.receita.features import build_receita_feature_daily


def test_receita_category_share_and_real_yoy_use_available_inflation():
    rows = []
    for index in range(13):
        ref_date = date(2024 + index // 12, index % 12 + 1, 28)
        value = 100.0 if index == 0 else 100.0 + 5.0 * index
        if index == 12:
            value = 200.0
        rows.append(_collection_row(ref_date, "receita_tax_collection|all|irpj", value))
    current = date(2025, 1, 28)
    rows.append(_collection_row(current, "receita_tax_collection|all|cofins", 300.0))
    inflation = pl.DataFrame(
        [
            {
                "ref_date": current,
                "source_family": "bcb_sgs_feature",
                "feature_id": "bcb_sgs_feature:inflation:ipca_12m_sum_pct",
                "value_name": "ipca_12m_sum_pct",
                "value": 4.0,
            }
        ]
    )

    features = build_receita_feature_daily(
        pl.DataFrame(rows),
        inflation_feature_daily=inflation,
        start=current,
        end=current,
    )

    feature_id = "receita:receita_tax_collection|all|irpj"
    assert _value(features, feature_id, "collection_yoy_pct") == pytest.approx(100.0)
    assert _value(features, feature_id, "real_collection_yoy_pct") == pytest.approx(96.0)
    assert _value(features, feature_id, "category_share_pct") == pytest.approx(40.0)
    assert set(features["ref_date"].to_list()) == {current}


def test_receita_category_share_prefers_explicit_total_collection_denominator():
    ref_date = date(2024, 1, 28)
    metadata = {
        "availability_policy": "receita_first_seen",
        "availability_basis": "first_seen_timestamp",
        "revision_policy": "revised_with_snapshots",
        "vintage_id": "receita-v1",
        "model_usable": True,
        "model_usable_reason": "fixture",
    }
    rows = [
        _collection_row(
            ref_date,
            "receita_tax_collection|all|total",
            1000.0,
            metadata=metadata,
        ),
        _collection_row(
            ref_date,
            "receita_tax_collection|all|irpj",
            200.0,
            metadata=metadata,
        ),
        _collection_row(
            ref_date,
            "receita_tax_collection|all|cofins",
            300.0,
            metadata=metadata,
        ),
    ]

    features = build_receita_feature_daily(pl.DataFrame(rows), start=ref_date, end=ref_date)

    irpj_row = _feature_row(
        features,
        "receita:receita_tax_collection|all|irpj",
        "category_share_pct",
    )
    assert irpj_row["value"] == pytest.approx(20.0)
    assert _value(
        features,
        "receita:receita_tax_collection|all|cofins",
        "category_share_pct",
    ) == pytest.approx(30.0)
    assert _value(
        features,
        "receita:receita_tax_collection|all|total",
        "category_share_pct",
    ) == pytest.approx(100.0)
    assert irpj_row["availability_policy"] == "receita_first_seen"
    assert irpj_row["availability_basis"] == "first_seen_timestamp"
    assert irpj_row["revision_policy"] == "revised_with_snapshots"
    assert irpj_row["vintage_id"] == "receita-v1"
    assert irpj_row["model_usable"] is True
    assert irpj_row["model_usable_reason"] == "fixture"


def _collection_row(
    ref_date: date,
    feature_id: str,
    value: float,
    *,
    metadata: dict[str, object] | None = None,
) -> dict[str, object]:
    return {
        "ref_date": ref_date,
        "available_date": ref_date,
        "source_family": "receita_tax_collection",
        "feature_id": feature_id,
        "observation_ref_date": ref_date,
        "observation_available_date": ref_date,
        "value_name": "collection_amount_brl",
        "value": value,
        "unit": "BRL",
        "is_available": True,
        "is_observed_on_ref_date": True,
        "staleness_days": 0,
        "source_version": "fixture",
        **(metadata or {}),
    }


def _value(frame: pl.DataFrame, feature_id: str, value_name: str) -> float:
    return _feature_row(frame, feature_id, value_name)["value"]


def _feature_row(frame: pl.DataFrame, feature_id: str, value_name: str) -> dict[str, object]:
    return frame.filter(
        (pl.col("feature_id") == feature_id) & (pl.col("value_name") == value_name)
    ).row(0, named=True)
