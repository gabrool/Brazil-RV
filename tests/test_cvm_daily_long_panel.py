from __future__ import annotations

from datetime import date

import polars as pl

from bralpha.derived.cvm.daily_long import build_cvm_daily_long
from bralpha.timing.vintages import AVAILABILITY_CURRENT_SNAPSHOT_NO_VINTAGE


def test_cvm_daily_long_uses_aggregate_flow_and_state_rows_only():
    panel = build_cvm_daily_long(
        fund_flows_daily=pl.DataFrame(
            [
                _flow_row(
                    observation_ref_date=date(2024, 1, 2),
                    subscriptions=10.0,
                    redemptions=None,
                ),
                _flow_row(
                    observation_ref_date=date(2024, 1, 3),
                    subscriptions=20.0,
                    redemptions=5.0,
                ),
            ]
        ),
        fund_state_asof_daily=pl.DataFrame([_state_row()]),
        include_fund_flows=True,
        include_fund_state=True,
    ).sort(["source_family", "value_name", "observation_ref_date"])

    assert set(panel["source_family"].to_list()) == {"cvm_fund_flows", "cvm_fund_state"}
    assert set(panel["value_name"].to_list()) >= {
        "subscriptions",
        "redemptions",
        "portfolio_value",
        "nav",
        "shareholder_count",
    }
    assert not panel.filter(
        (pl.col("value_name") == "redemptions")
        & (pl.col("observation_ref_date") == date(2024, 1, 2))
    ).height
    assert "fund_id" not in panel.columns
    assert "fund_name" not in panel.columns
    assert "group_type" not in panel.columns
    assert "group_value" not in panel.columns


def test_cvm_daily_long_primary_key_allows_multiple_flow_observations_on_same_model_date():
    flows = pl.DataFrame(
        [
            _flow_row(observation_ref_date=date(2024, 1, 2), subscriptions=10.0),
            _flow_row(observation_ref_date=date(2024, 1, 3), subscriptions=20.0),
        ]
    )

    panel = build_cvm_daily_long(
        fund_flows_daily=flows,
        fund_state_asof_daily=None,
        include_fund_flows=True,
        include_fund_state=True,
    )

    flow_subscriptions = panel.filter(pl.col("value_name") == "subscriptions")
    assert flow_subscriptions.height == 2
    assert flow_subscriptions["observation_ref_date"].to_list() == [
        date(2024, 1, 2),
        date(2024, 1, 3),
    ]
    assert flow_subscriptions["value"].to_list() == [10.0, 20.0]


def test_cvm_daily_long_drops_null_values_and_stays_long():
    panel = build_cvm_daily_long(
        fund_flows_daily=None,
        fund_state_asof_daily=pl.DataFrame(
            [
                _state_row(
                    portfolio_value=None,
                    nav=90.0,
                    shareholder_count=None,
                )
            ]
        ),
        include_fund_flows=True,
        include_fund_state=True,
    )

    assert set(panel["value_name"].to_list()) == {
        "nav",
        "portfolio_value_count",
        "nav_count",
        "shareholder_count_count",
        "fund_count",
    }
    assert "value" in panel.columns
    assert not {"portfolio_value", "nav", "shareholder_count"} & set(panel.columns)


def test_cvm_daily_long_excludes_non_model_usable_current_snapshots():
    row = _flow_row(observation_ref_date=date(2024, 1, 2), subscriptions=10.0)
    row.update(
        {
            "availability_basis": AVAILABILITY_CURRENT_SNAPSHOT_NO_VINTAGE,
            "revision_policy": "current_snapshot_reference_only",
            "model_usable": False,
            "model_usable_reason": "cvm_fund_daily_conservative_2bd_reference_only",
        }
    )

    panel = build_cvm_daily_long(
        fund_flows_daily=pl.DataFrame([row]),
        fund_state_asof_daily=None,
        include_fund_flows=True,
        include_fund_state=True,
    )

    assert panel.is_empty()


def _flow_row(
    *,
    observation_ref_date: date,
    subscriptions: float | None = 10.0,
    redemptions: float | None = 1.0,
) -> dict[str, object]:
    return {
        "ref_date": date(2024, 1, 5),
        "available_date": date(2024, 1, 5),
        "observation_ref_date": observation_ref_date,
        "observation_available_date": date(2024, 1, 5),
        "availability_policy": "cvm_first_seen_snapshot",
        "availability_basis": "first_seen_download_timestamp",
        "revision_policy": "revised_use_first_seen_snapshots",
        "release_date": None,
        "source_publication_datetime_utc": None,
        "source_last_modified_utc": None,
        "first_seen_timestamp_utc": date(2024, 1, 5),
        "vintage_id": f"cvm:flow:{observation_ref_date.isoformat()}",
        "revision_sequence": 0,
        "model_usable": True,
        "model_usable_reason": "cvm_first_seen_snapshot",
        "group_type": "all",
        "group_value": "all",
        "feature_id": "cvm_fund_group|all|all",
        "subscriptions": subscriptions,
        "redemptions": redemptions,
        "subscriptions_count": 1 if subscriptions is not None else 0,
        "redemptions_count": 1 if redemptions is not None else 0,
        "fund_count": 1,
        "source_version": "v0",
    }


def _state_row(
    *,
    portfolio_value: float | None = 100.0,
    nav: float | None = 90.0,
    shareholder_count: int | None = 10,
) -> dict[str, object]:
    return {
        "ref_date": date(2024, 1, 5),
        "available_date": date(2024, 1, 5),
        "group_type": "all",
        "group_value": "all",
        "feature_id": "cvm_fund_group|all|all",
        "observation_ref_date": date(2024, 1, 2),
        "observation_available_date": date(2024, 1, 4),
        "availability_policy": "cvm_first_seen_snapshot",
        "availability_basis": "first_seen_download_timestamp",
        "revision_policy": "revised_use_first_seen_snapshots",
        "release_date": None,
        "source_publication_datetime_utc": None,
        "source_last_modified_utc": None,
        "first_seen_timestamp_utc": date(2024, 1, 4),
        "vintage_id": "cvm:state:2024-01-02",
        "revision_sequence": 0,
        "model_usable": True,
        "model_usable_reason": "cvm_first_seen_snapshot",
        "portfolio_value": portfolio_value,
        "nav": nav,
        "shareholder_count": shareholder_count,
        "fund_count": 1,
        "portfolio_value_count": 1 if portfolio_value is not None else 0,
        "nav_count": 1 if nav is not None else 0,
        "shareholder_count_count": 1 if shareholder_count is not None else 0,
        "is_available": True,
        "is_observed_on_ref_date": False,
        "staleness_days": 1,
        "source_version": "v0",
    }
