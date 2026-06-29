from __future__ import annotations

from datetime import date

import polars as pl
import pytest

from bralpha.derived.receita.daily_long import (
    build_receita_daily_long,
    build_receita_state_asof_daily,
)
from bralpha.derived.receita.schemas import PANEL_PRIMARY_KEYS
from bralpha.timing.vintages import AVAILABILITY_CONSERVATIVE_HEURISTIC


def test_state_asof_uses_pre_window_history_and_staleness():
    features = pl.DataFrame(
        [
            _feature_row(
                ref_date=date(2024, 1, 31),
                available_date=date(2024, 3, 7),
                value=100.0,
            )
        ]
    )

    panel = build_receita_state_asof_daily(
        feature_observations=features,
        start=date(2024, 3, 8),
        end=date(2024, 3, 11),
        max_features=10,
    )

    assert panel["ref_date"].to_list() == [date(2024, 3, 8), date(2024, 3, 11)]
    assert panel["value"].to_list() == [100.0, 100.0]
    assert panel["staleness_days"].to_list() == [1, 4]
    assert panel["observation_ref_date"].to_list() == [date(2024, 1, 31), date(2024, 1, 31)]


def test_state_asof_emits_no_rows_before_first_availability():
    features = pl.DataFrame(
        [
            _feature_row(
                ref_date=date(2024, 1, 31),
                available_date=date(2024, 3, 7),
                value=100.0,
            )
        ]
    )

    panel = build_receita_state_asof_daily(
        feature_observations=features,
        start=date(2024, 3, 1),
        end=date(2024, 3, 8),
        max_features=10,
    )

    assert panel["ref_date"].to_list() == [date(2024, 3, 7), date(2024, 3, 8)]
    assert panel.filter(pl.col("observation_available_date") > pl.col("ref_date")).is_empty()


def test_state_asof_preserves_latest_missing_observation():
    features = pl.DataFrame(
        [
            _feature_row(
                ref_date=date(2024, 1, 31),
                available_date=date(2024, 3, 7),
                value=100.0,
            ),
            _feature_row(
                ref_date=date(2024, 2, 29),
                available_date=date(2024, 4, 5),
                value=None,
            ),
        ]
    )

    panel = build_receita_state_asof_daily(
        feature_observations=features,
        start=date(2024, 4, 8),
        end=date(2024, 4, 8),
        max_features=10,
    )

    assert panel.height == 1
    assert panel["value"].to_list() == [None]
    assert panel["observation_ref_date"].to_list() == [date(2024, 2, 29)]
    assert panel["staleness_days"].to_list() == [3]


def test_state_asof_max_features_guard():
    features = pl.DataFrame(
        [
            _feature_row(feature_id="receita_tax_collection|all|principal|a"),
            _feature_row(feature_id="receita_tax_collection|all|principal|b"),
        ]
    )

    with pytest.raises(ValueError, match="max_features=1"):
        build_receita_state_asof_daily(
            feature_observations=features,
            start=date(2024, 3, 8),
            end=date(2024, 3, 8),
            max_features=1,
        )


def test_daily_long_drops_null_values_and_keeps_long_primary_key():
    state = build_receita_state_asof_daily(
        feature_observations=pl.DataFrame(
            [
                _feature_row(
                    ref_date=date(2024, 1, 31),
                    available_date=date(2024, 3, 7),
                    value=100.0,
                ),
                _feature_row(
                    feature_id="receita_tax_collection|all|principal|b",
                    ref_date=date(2024, 1, 31),
                    available_date=date(2024, 3, 7),
                    value=None,
                ),
            ]
        ),
        start=date(2024, 3, 8),
        end=date(2024, 3, 8),
        max_features=10,
    )

    panel = build_receita_daily_long(state_asof_daily=state, include_tax_collection=True)

    assert panel.height == 1
    assert panel.filter(pl.col("value").is_null()).is_empty()
    assert panel.group_by(PANEL_PRIMARY_KEYS["daily_long"]).len().height == panel.height
    assert set(panel["source_family"].to_list()) == {"receita_tax_collection"}
    assert "collection_scope" not in panel.columns
    assert "revenue_key" not in panel.columns


def test_daily_long_can_disable_tax_collection():
    state = build_receita_state_asof_daily(
        feature_observations=pl.DataFrame([_feature_row()]),
        start=date(2024, 3, 8),
        end=date(2024, 3, 8),
        max_features=10,
    )

    panel = build_receita_daily_long(state_asof_daily=state, include_tax_collection=False)

    assert panel.is_empty()


def test_daily_long_excludes_reference_only_tax_collection_rows():
    state = build_receita_state_asof_daily(
        feature_observations=pl.DataFrame(
            [
                {
                    **_feature_row(),
                    "availability_basis": AVAILABILITY_CONSERVATIVE_HEURISTIC,
                    "model_usable": False,
                }
            ]
        ),
        start=date(2024, 3, 8),
        end=date(2024, 3, 8),
        max_features=10,
    )

    panel = build_receita_daily_long(state_asof_daily=state, include_tax_collection=True)

    assert panel.is_empty()


def _feature_row(
    *,
    feature_id: str = "receita_tax_collection|all|principal|001_irpj",
    ref_date: date = date(2024, 1, 31),
    available_date: date = date(2024, 3, 7),
    value: float | None = 100.0,
) -> dict[str, object]:
    return {
        "ref_date": ref_date,
        "available_date": available_date,
        "availability_policy": "receita_monthly_collection_conservative_next_month_end_plus_5bd",
        "collection_scope": "federal_total",
        "revenue_category": "IR",
        "revenue_subcategory": "IRPJ",
        "revenue_code": "001",
        "revenue_key": "001_irpj",
        "revenue_name": "IRPJ",
        "table_kind": "principal",
        "feature_id": feature_id,
        "collection_amount_brl": value,
        "unit": "BRL",
        "source_table": "arrecadacao",
        "has_collection_amount_brl": value is not None,
        "source_version": "v0",
    }
