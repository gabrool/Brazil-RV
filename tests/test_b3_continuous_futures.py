from __future__ import annotations

from datetime import date

import polars as pl

from bralpha.derived.b3.continuous_futures import build_continuous_futures_daily


def test_continuous_futures_roll_flags_and_same_contract_changes():
    panel = build_continuous_futures_daily(
        pl.DataFrame(
            [
                _contract(date(2024, 1, 2), "DI1_F26", "F26", 1, 3, 10.0),
                _contract(date(2024, 1, 2), "DI1_G26", "G26", 2, 30, 11.0),
                _contract(date(2024, 1, 2), "DI1_H26", "H26", 3, 60, 12.0),
                _contract(date(2024, 1, 3), "DI1_F26", "F26", 1, 1, 10.5),
                _contract(date(2024, 1, 3), "DI1_G26", "G26", 2, 29, 11.5),
                _contract(date(2024, 1, 3), "DI1_H26", "H26", 3, 59, 12.5),
            ]
        ),
        roots=["DI1"],
        max_front_rank=3,
        min_days_to_maturity=2,
        prefer_liquidity_when_available=True,
        roll_policy="maturity_rank",
    )

    assert set(panel.filter(pl.col("ref_date") == date(2024, 1, 2))["continuous_id"]) == {
        "DI1_R1",
        "DI1_R2",
        "DI1_R3",
    }
    rolled = panel.filter(
        (pl.col("ref_date") == date(2024, 1, 3)) & (pl.col("continuous_id") == "DI1_R1")
    ).row(0, named=True)
    assert rolled["selected_contract_id"] == "DI1_G26"
    assert rolled["previous_contract_id"] == "DI1_F26"
    assert rolled["is_roll_date"] is True
    assert rolled["same_contract_quote_diff_1d"] is None


def test_continuous_futures_uses_only_same_date_liquidity():
    panel = build_continuous_futures_daily(
        pl.DataFrame(
            [
                _contract(date(2024, 1, 2), "DI1_F26", "F26", 1, 30, 10.0, volume=0, oi=0),
                _contract(date(2024, 1, 2), "DI1_G26", "G26", 2, 60, 11.0, volume=100, oi=0),
            ]
        ),
        roots=["DI1"],
        max_front_rank=1,
        min_days_to_maturity=2,
        prefer_liquidity_when_available=True,
        roll_policy="maturity_rank",
    )

    assert panel["selected_contract_id"].item() == "DI1_G26"


def _contract(
    ref_date: date,
    contract_id: str,
    maturity_code: str,
    rank: int,
    days: int,
    settlement: float,
    *,
    volume: int = 100,
    oi: int = 1000,
) -> dict[str, object]:
    return {
        "ref_date": ref_date,
        "available_date": ref_date,
        "root": "DI1",
        "commodity": "DI1",
        "maturity_code": maturity_code,
        "contract_id": contract_id,
        "maturity_date": date(2026, 1, rank),
        "days_to_maturity_calendar": days,
        "business_days_to_maturity": days,
        "contract_rank_by_maturity": rank,
        "settlement": settlement,
        "volume": volume,
        "open_interest": oi,
        "is_tradeable": True,
        "source_version": "v0",
    }
