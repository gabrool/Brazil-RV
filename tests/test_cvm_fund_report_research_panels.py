from __future__ import annotations

from datetime import date

import polars as pl
import pytest

from bralpha.derived.cvm.fund_reports import (
    build_fund_daily_observation,
    build_fund_flows_daily,
    build_fund_group_observation,
    build_fund_state_asof_daily,
)


def test_fund_daily_observation_preserves_values_and_adds_missingness_flags():
    panel = build_fund_daily_observation(pl.DataFrame(_daily_rows()))

    row = panel.filter(pl.col("fund_id") == "fund-b").row(0, named=True)
    assert row["portfolio_value"] is None
    assert row["nav"] == 40.0
    assert row["quota_value"] == 1.0
    assert row["raw_vl_total"] is None
    assert row["raw_vl_patrim_liq"] == "40.0"
    assert row["has_portfolio_value"] is False
    assert row["has_nav"] is True
    assert row["has_quota_value"] is True
    assert row["has_subscriptions"] is False
    assert row["has_redemptions"] is True
    assert row["has_shareholder_count"] is False


def test_fund_group_observation_aggregates_all_and_fund_type_groups():
    observations = build_fund_daily_observation(pl.DataFrame(_daily_rows()))

    panel = build_fund_group_observation(
        observations,
        group_by=["all", "fund_type"],
        max_groups=100,
    ).sort(["group_type", "group_value"])

    all_row = panel.filter(pl.col("group_type") == "all").row(0, named=True)
    assert all_row["group_value"] == "all"
    assert all_row["feature_id"] == "cvm_fund_group|all|all"
    assert all_row["available_date"] == date(2024, 1, 5)
    assert all_row["portfolio_value"] == 100.0
    assert all_row["nav"] == 130.0
    assert all_row["subscriptions"] == 10.0
    assert all_row["redemptions"] == 6.0
    assert all_row["shareholder_count"] == 100
    assert all_row["fund_count"] == 2
    assert all_row["portfolio_value_count"] == 1
    assert all_row["subscriptions_count"] == 1
    assert all_row["shareholder_count_count"] == 1

    fi_row = panel.filter(
        (pl.col("group_type") == "fund_type") & (pl.col("group_value") == "fi")
    ).row(0, named=True)
    assert fi_row["feature_id"] == "cvm_fund_group|fund_type|fi"
    assert fi_row["fund_count"] == 1
    assert fi_row["portfolio_value"] == 100.0


def test_fund_group_observation_null_aware_sums_remain_null_when_all_values_missing():
    observations = build_fund_daily_observation(
        pl.DataFrame(
            [
                _daily_row(
                    fund_id="fund-a",
                    fund_type="FI",
                    portfolio_value=None,
                    nav=None,
                    subscriptions=None,
                    redemptions=None,
                    shareholder_count=None,
                )
            ]
        )
    )

    panel = build_fund_group_observation(observations, group_by=["all"], max_groups=100)
    row = panel.row(0, named=True)

    assert row["portfolio_value"] is None
    assert row["nav"] is None
    assert row["subscriptions"] is None
    assert row["redemptions"] is None
    assert row["shareholder_count"] is None
    assert row["portfolio_value_count"] == 0


def test_fund_group_observation_max_groups_guard():
    observations = build_fund_daily_observation(
        pl.DataFrame(
            [
                _daily_row(
                    fund_id=f"fund-{index}",
                    fund_type=f"TYPE {index}",
                    portfolio_value=1.0,
                    nav=1.0,
                    subscriptions=0.0,
                    redemptions=0.0,
                    shareholder_count=1,
                )
                for index in range(3)
            ]
        )
    )

    with pytest.raises(ValueError, match="max_groups=2"):
        build_fund_group_observation(observations, group_by=["fund_type"], max_groups=2)


def test_fund_flows_daily_aligns_to_availability_without_forward_fill():
    observations = build_fund_daily_observation(pl.DataFrame(_daily_rows()))
    groups = build_fund_group_observation(observations, group_by=["all"], max_groups=100)

    panel = build_fund_flows_daily(
        groups,
        start=date(2024, 1, 4),
        end=date(2024, 1, 8),
    )

    assert panel["ref_date"].to_list() == [date(2024, 1, 5)]
    assert panel["available_date"].to_list() == [date(2024, 1, 5)]
    assert panel["observation_ref_date"].to_list() == [date(2024, 1, 2)]
    assert panel["observation_available_date"].to_list() == [date(2024, 1, 5)]
    assert panel["subscriptions"].to_list() == [10.0]
    assert panel["redemptions"].to_list() == [6.0]


def test_fund_state_asof_daily_uses_pre_window_history_and_staleness():
    observations = build_fund_daily_observation(
        pl.DataFrame(
            [
                _daily_row(
                    fund_id="fund-a",
                    fund_type="FI",
                    ref_date=date(2023, 12, 29),
                    available_date=date(2024, 1, 2),
                    portfolio_value=100.0,
                    nav=90.0,
                    subscriptions=5.0,
                    redemptions=1.0,
                    shareholder_count=10,
                ),
                _daily_row(
                    fund_id="fund-a",
                    fund_type="FI",
                    ref_date=date(2024, 1, 3),
                    available_date=date(2024, 1, 5),
                    portfolio_value=110.0,
                    nav=95.0,
                    subscriptions=7.0,
                    redemptions=2.0,
                    shareholder_count=11,
                ),
            ]
        )
    )
    groups = build_fund_group_observation(observations, group_by=["all"], max_groups=100)

    panel = build_fund_state_asof_daily(
        groups,
        start=date(2024, 1, 1),
        end=date(2024, 1, 5),
        max_groups=100,
    ).sort("ref_date")

    assert panel["ref_date"].to_list() == [
        date(2024, 1, 2),
        date(2024, 1, 3),
        date(2024, 1, 4),
        date(2024, 1, 5),
    ]
    assert panel["portfolio_value"].to_list() == [100.0, 100.0, 100.0, 110.0]
    assert panel["observation_ref_date"].to_list() == [
        date(2023, 12, 29),
        date(2023, 12, 29),
        date(2023, 12, 29),
        date(2024, 1, 3),
    ]
    assert panel["staleness_days"].to_list() == [0, 1, 2, 0]
    assert panel.filter(pl.col("ref_date") == date(2024, 1, 1)).is_empty()
    assert panel.filter(pl.col("observation_available_date") > pl.col("ref_date")).is_empty()
    assert "subscriptions" not in panel.columns
    assert "redemptions" not in panel.columns


def test_fund_state_asof_daily_uses_latest_snapshot_only_after_it_is_available():
    observations = build_fund_daily_observation(
        pl.DataFrame(
            [
                _daily_row(
                    fund_id="fund-a",
                    fund_type="FI",
                    ref_date=date(2024, 1, 2),
                    available_date=date(2024, 1, 4),
                    portfolio_value=100.0,
                    nav=90.0,
                    subscriptions=5.0,
                    redemptions=1.0,
                    shareholder_count=10,
                    vintage_id="cvm:v1",
                    first_seen_timestamp_utc=date(2024, 1, 4),
                ),
                _daily_row(
                    fund_id="fund-a",
                    fund_type="FI",
                    ref_date=date(2024, 1, 2),
                    available_date=date(2024, 1, 8),
                    portfolio_value=110.0,
                    nav=95.0,
                    subscriptions=7.0,
                    redemptions=2.0,
                    shareholder_count=11,
                    vintage_id="cvm:v2",
                    first_seen_timestamp_utc=date(2024, 1, 8),
                    revision_sequence=1,
                ),
            ]
        )
    )
    groups = build_fund_group_observation(observations, group_by=["all"], max_groups=100)

    panel = build_fund_state_asof_daily(
        groups,
        start=date(2024, 1, 4),
        end=date(2024, 1, 8),
        max_groups=100,
    )

    assert panel.filter(pl.col("ref_date") == date(2024, 1, 5))["portfolio_value"].item() == 100.0
    jan8 = panel.filter(pl.col("ref_date") == date(2024, 1, 8)).row(0, named=True)
    assert jan8["portfolio_value"] == 110.0
    assert jan8["vintage_id"] == "cvm:v2"


def _daily_rows() -> list[dict[str, object]]:
    return [
        _daily_row(
            fund_id="fund-a",
            fund_type="FI",
            portfolio_value=100.0,
            nav=90.0,
            subscriptions=10.0,
            redemptions=1.0,
            shareholder_count=100,
        ),
        _daily_row(
            fund_id="fund-b",
            fund_type="FIDC",
            available_date=date(2024, 1, 5),
            portfolio_value=None,
            nav=40.0,
            subscriptions=None,
            redemptions=5.0,
            shareholder_count=None,
        ),
    ]


def _daily_row(
    *,
    fund_id: str,
    fund_type: str | None,
    portfolio_value: float | None,
    nav: float | None,
    subscriptions: float | None,
    redemptions: float | None,
    shareholder_count: int | None,
    ref_date: date = date(2024, 1, 2),
    available_date: date = date(2024, 1, 4),
    vintage_id: str = "legacy",
    first_seen_timestamp_utc: date | None = None,
    revision_sequence: int = 0,
) -> dict[str, object]:
    return {
        "ref_date": ref_date,
        "available_date": available_date,
        "availability_policy": "cvm_first_seen_snapshot",
        "availability_basis": "first_seen_download_timestamp",
        "revision_policy": "revised_use_first_seen_snapshots",
        "release_date": None,
        "source_publication_datetime_utc": None,
        "source_last_modified_utc": None,
        "first_seen_timestamp_utc": first_seen_timestamp_utc or available_date,
        "vintage_id": vintage_id,
        "revision_sequence": revision_sequence,
        "model_usable": True,
        "model_usable_reason": "cvm_first_seen_snapshot",
        "fund_id": fund_id,
        "fund_type": fund_type,
        "portfolio_value": portfolio_value,
        "nav": nav,
        "quota_value": 1.0,
        "subscriptions": subscriptions,
        "redemptions": redemptions,
        "shareholder_count": shareholder_count,
        "raw_vl_total": None if portfolio_value is None else str(portfolio_value),
        "raw_vl_patrim_liq": None if nav is None else str(nav),
        "raw_vl_quota": "1.0",
        "raw_captc_dia": None if subscriptions is None else str(subscriptions),
        "raw_resg_dia": None if redemptions is None else str(redemptions),
        "raw_nr_cotst": None if shareholder_count is None else str(shareholder_count),
        "source": "cvm",
        "source_dataset": "cvm_fund_daily_reports",
        "download_timestamp_utc": None,
        "raw_path": "raw.zip",
        "sha256": "abc",
        "source_version": "v0",
    }
