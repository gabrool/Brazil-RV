from __future__ import annotations

from datetime import date

import polars as pl

from bralpha.derived.tesouro.flows import build_direto_flows_daily
from bralpha.derived.tesouro.schemas import TESOURO_DIRETO_FLOWS_DAILY_COLUMNS


def test_flows_align_to_available_date_without_forward_fill():
    panel = build_direto_flows_daily(
        sales=pl.DataFrame([_sales_row()]),
        redemptions=pl.DataFrame([_redemption_row()]),
        include_sales=True,
        include_redemptions=True,
        start=date(2024, 1, 2),
        end=date(2024, 1, 3),
    )

    assert panel.columns == TESOURO_DIRETO_FLOWS_DAILY_COLUMNS
    assert panel["ref_date"].to_list() == [date(2024, 1, 3), date(2024, 1, 3)]
    assert panel["available_date"].to_list() == [date(2024, 1, 3), date(2024, 1, 3)]
    assert panel["observation_ref_date"].to_list() == [date(2024, 1, 1), date(2024, 1, 1)]
    assert set(panel["availability_policy"].to_list()) == {
        "tesouro_direto_sales_official_2bd",
        "tesouro_direto_redemptions_conservative_2bd",
    }
    assert set(panel["flow_type"].to_list()) == {"sale", "redemption"}
    assert set(panel["availability_basis"].to_list()) == {"canonical_b3_calendar"}
    assert panel.filter(pl.col("ref_date") == date(2024, 1, 2)).is_empty()


def test_flows_preserve_redemption_type_and_null_safe_sales_key():
    panel = build_direto_flows_daily(
        sales=pl.DataFrame([_sales_row()]),
        redemptions=pl.DataFrame([_redemption_row()]),
        include_sales=True,
        include_redemptions=True,
        start=date(2024, 1, 3),
        end=date(2024, 1, 3),
    )
    sale = panel.filter(pl.col("flow_type") == "sale").row(0, named=True)
    redemption = panel.filter(pl.col("flow_type") == "redemption").row(0, named=True)

    assert sale["redemption_type"] is None
    assert "|sale|null|" in sale["feature_id"]
    assert redemption["redemption_type"] == "early_repurchase"
    assert "|redemption|early_repurchase|" in redemption["feature_id"]
    assert panel.group_by(
        ["ref_date", "flow_type", "redemption_type", "security_name", "maturity_date"]
    ).len().height == panel.height


def _sales_row() -> dict[str, object]:
    return {
        "ref_date": date(2024, 1, 1),
        "available_date": date(2024, 1, 3),
        "availability_policy": "tesouro_direto_sales_official_2bd",
        "availability_basis": "canonical_b3_calendar",
        "security_name": "Tesouro Selic",
        "security_type": "Tesouro Selic",
        "maturity_date": date(2027, 3, 1),
        "quantity": 10.0,
        "value": 1000.0,
        "investor_count": 4,
        "unit": "BRL",
        "source_dataset": "tesouro_direto_sales",
        "source_version": "v0",
    }


def _redemption_row() -> dict[str, object]:
    return {
        "ref_date": date(2024, 1, 1),
        "available_date": date(2024, 1, 3),
        "availability_policy": "tesouro_direto_redemptions_conservative_2bd",
        "availability_basis": "canonical_b3_calendar",
        "redemption_type": "early_repurchase",
        "security_name": "Tesouro Selic",
        "security_type": "Tesouro Selic",
        "maturity_date": date(2027, 3, 1),
        "quantity": 2.0,
        "value": 200.0,
        "unit": "BRL",
        "source_dataset": "tesouro_direto_redemptions",
        "source_version": "v0",
    }
