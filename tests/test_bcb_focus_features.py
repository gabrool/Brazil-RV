from __future__ import annotations

from datetime import date, timedelta
from math import log1p

import polars as pl
import pytest

from bralpha.derived.bcb.focus_features import build_focus_feature_daily


def test_focus_features_compute_revisions_dispersion_and_top5_spreads():
    start = date(2024, 1, 2)
    rows = []
    for offset in range(6):
        ref_date = start + timedelta(days=offset)
        rows.append(_focus_row(ref_date, False, median=4.0 + offset * 0.1, mean=4.2))
        rows.append(_focus_row(ref_date, True, median=4.5 + offset * 0.1, mean=4.7))

    features = build_focus_feature_daily(
        pl.DataFrame(rows),
        start=start + timedelta(days=5),
        end=start + timedelta(days=5),
    )

    assert _value(features, "bcb_focus:focus_key_general", "median_level") == pytest.approx(4.5)
    assert _value(
        features,
        "bcb_focus:focus_key_general",
        "median_revision_5bd",
    ) == pytest.approx(0.5)
    assert _value(features, "bcb_focus:focus_key_general", "respondents_log1p") == pytest.approx(
        log1p(20.0)
    )
    assert _value(features, "bcb_focus:focus_key_general", "std_dev_log1p") == pytest.approx(
        log1p(0.2)
    )
    assert _value(
        features,
        "bcb_focus:focus_key_general",
        "dispersion_to_abs_median",
    ) == pytest.approx(0.2 / 4.5)
    spread = features.filter(pl.col("value_name") == "top5_minus_general_median")["value"].item()
    assert spread == pytest.approx(0.5)


def test_focus_dispersion_uses_one_floor_for_zero_median():
    ref_date = date(2024, 1, 2)
    features = build_focus_feature_daily(
        pl.DataFrame([_focus_row(ref_date, False, median=0.0, mean=0.1)]),
    )

    assert _value(features, "bcb_focus:focus_key_general", "std_dev_log1p") == pytest.approx(
        log1p(0.2)
    )
    assert _value(
        features,
        "bcb_focus:focus_key_general",
        "dispersion_to_abs_median",
    ) == pytest.approx(0.2)


def _focus_row(ref_date: date, is_top5: bool, *, median: float, mean: float) -> dict[str, object]:
    key = "focus_key_top5" if is_top5 else "focus_key_general"
    return {
        "ref_date": ref_date,
        "available_date": ref_date,
        "expectation_key": key,
        "observation_ref_date": ref_date,
        "observation_available_date": ref_date,
        "endpoint": "expectativas",
        "indicator": "IPCA",
        "indicator_detail": None,
        "reference_period": "2024",
        "reference_year": 2024,
        "reference_month": None,
        "meeting": None,
        "horizon_label": "year",
        "is_top5": is_top5,
        "calculation_type": "median",
        "statistic_scope": "top5" if is_top5 else "general",
        "mean": mean,
        "median": median,
        "std_dev": 0.2,
        "min_value": median - 1.0,
        "max_value": median + 1.0,
        "respondents": 20,
        "base_calculation": "0",
        "is_available": True,
        "is_observed_on_ref_date": True,
        "staleness_days": 0,
        "availability_note": "fixture",
        "source_dataset": "fixture",
        "source_version": "v0",
    }


def _value(frame: pl.DataFrame, feature_id: str, value_name: str) -> float | None:
    return frame.filter(
        (pl.col("feature_id") == feature_id) & (pl.col("value_name") == value_name)
    )["value"].item()
