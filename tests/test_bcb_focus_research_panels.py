from __future__ import annotations

from datetime import date

import polars as pl
import pytest

from bralpha.derived.bcb.focus import (
    build_focus_expectation_asof_daily,
    build_focus_expectation_observation_daily,
    build_focus_reference_dates,
)

AVAILABILITY_NOTE = "date_only_next_business_day_until_publication_calendar"


def test_focus_observation_panel_builds_stable_key_and_keeps_top5_coexistence():
    general = pl.DataFrame(
        [
            _focus_row(
                endpoint="ExpectativasMercadoAnuais",
                indicator="IPCA",
                indicator_detail="Livres",
                is_top5=False,
                mean=4.0,
                base_calculation=1,
            )
        ]
    )
    top5 = pl.DataFrame(
        [
            _focus_row(
                endpoint="ExpectativasMercadoTop5Anuais",
                indicator="IPCA",
                indicator_detail="Livres",
                is_top5=True,
                calculation_type="C",
                mean=4.1,
            )
        ]
    )

    panel = build_focus_expectation_observation_daily(
        general=general,
        top5=top5,
        availability_note=AVAILABILITY_NOTE,
        include_general=True,
        include_top5=True,
        start=date(2024, 1, 2),
        end=date(2024, 1, 2),
    )
    rebuilt = build_focus_expectation_observation_daily(
        general=general,
        top5=top5,
        availability_note=AVAILABILITY_NOTE,
        include_general=True,
        include_top5=True,
        start=date(2024, 1, 2),
        end=date(2024, 1, 2),
    )

    assert panel.height == 2
    assert panel["expectation_key"].to_list() == rebuilt["expectation_key"].to_list()
    assert panel.select("expectation_key").unique().height == 2
    assert set(panel["is_top5"].to_list()) == {False, True}
    assert set(panel["availability_note"].to_list()) == {AVAILABILITY_NOTE}


def test_focus_asof_respects_availability_and_selected_indicators():
    observations = build_focus_expectation_observation_daily(
        general=pl.DataFrame(
            [
                _focus_row(
                    endpoint="ExpectativasMercadoAnuais",
                    indicator="IPCA",
                    indicator_detail=None,
                    is_top5=False,
                    mean=4.0,
                    base_calculation=1,
                ),
                _focus_row(
                    endpoint="ExpectativasMercadoAnuais",
                    indicator="IGP-M",
                    indicator_detail=None,
                    is_top5=False,
                    mean=5.0,
                    base_calculation=1,
                ),
            ]
        ),
        top5=None,
        availability_note=AVAILABILITY_NOTE,
        include_general=True,
        include_top5=True,
        start=date(2024, 1, 2),
        end=date(2024, 1, 2),
    )

    panel = build_focus_expectation_asof_daily(
        observations,
        selected_indicators=["IPCA"],
        max_dense_keys=5000,
        start=date(2024, 1, 1),
        end=date(2024, 1, 3),
    )

    assert panel["ref_date"].to_list() == [date(2024, 1, 2), date(2024, 1, 3)]
    assert set(panel["indicator"].to_list()) == {"IPCA"}
    assert panel.filter(pl.col("observation_available_date") > pl.col("ref_date")).is_empty()
    assert panel["staleness_days"].to_list() == [0, 1]
    assert set(panel["availability_note"].to_list()) == {AVAILABILITY_NOTE}


def test_focus_asof_uses_pre_window_observation_available_at_output_start():
    observations = build_focus_expectation_observation_daily(
        general=pl.DataFrame(
            [
                _focus_row(
                    endpoint="ExpectativasMercadoAnuais",
                    indicator="IPCA",
                    indicator_detail=None,
                    is_top5=False,
                    mean=4.0,
                    base_calculation=1,
                    ref_date=date(2023, 12, 29),
                    available_date=date(2024, 1, 2),
                )
            ]
        ),
        top5=None,
        availability_note=AVAILABILITY_NOTE,
        include_general=True,
        include_top5=True,
        start=None,
        end=date(2024, 1, 5),
    )

    panel = build_focus_expectation_asof_daily(
        observations,
        selected_indicators=["IPCA"],
        max_dense_keys=5000,
        start=date(2024, 1, 2),
        end=date(2024, 1, 5),
    ).sort("ref_date")

    assert panel["ref_date"].to_list() == [
        date(2024, 1, 2),
        date(2024, 1, 3),
        date(2024, 1, 4),
        date(2024, 1, 5),
    ]
    assert panel["mean"].to_list() == [4.0, 4.0, 4.0, 4.0]
    assert panel["observation_ref_date"].to_list() == [date(2023, 12, 29)] * 4


def test_focus_asof_waits_for_date_only_next_business_day_availability():
    observations = build_focus_expectation_observation_daily(
        general=pl.DataFrame(
            [
                _focus_row(
                    endpoint="ExpectativasMercadoAnuais",
                    indicator="IPCA",
                    indicator_detail=None,
                    is_top5=False,
                    mean=4.0,
                    base_calculation=1,
                    ref_date=date(2024, 1, 2),
                    available_date=date(2024, 1, 3),
                )
            ]
        ),
        top5=None,
        availability_note=AVAILABILITY_NOTE,
        include_general=True,
        include_top5=True,
        start=date(2024, 1, 2),
        end=date(2024, 1, 2),
    )

    panel = build_focus_expectation_asof_daily(
        observations,
        selected_indicators=["IPCA"],
        max_dense_keys=5000,
        start=date(2024, 1, 2),
        end=date(2024, 1, 3),
    )

    assert panel["ref_date"].to_list() == [date(2024, 1, 3)]
    assert panel["observation_ref_date"].to_list() == [date(2024, 1, 2)]


def test_focus_asof_raises_when_selected_keys_exceed_limit():
    observations = build_focus_expectation_observation_daily(
        general=pl.DataFrame(
            [
                _focus_row(
                    endpoint="ExpectativasMercadoAnuais",
                    indicator="IPCA",
                    indicator_detail="Livres",
                    is_top5=False,
                    mean=4.0,
                    base_calculation=1,
                ),
                _focus_row(
                    endpoint="ExpectativasMercadoAnuais",
                    indicator="IPCA",
                    indicator_detail="Administrados",
                    is_top5=False,
                    mean=5.0,
                    base_calculation=1,
                ),
            ]
        ),
        top5=None,
        availability_note=AVAILABILITY_NOTE,
        include_general=True,
        include_top5=True,
        start=date(2024, 1, 2),
        end=date(2024, 1, 2),
    )

    with pytest.raises(ValueError, match="max_dense_keys"):
        build_focus_expectation_asof_daily(
            observations,
            selected_indicators=["IPCA"],
            max_dense_keys=1,
            start=date(2024, 1, 2),
            end=date(2024, 1, 3),
        )


def test_focus_reference_dates_preserve_context_columns():
    refs = pl.DataFrame(
        [
            {
                "indicator": "IPCA",
                "period": "01/2025",
                "reference_date_type": "DataReferencia1",
                "reference_date": date(2024, 1, 10),
                "available_date": date(2024, 1, 10),
                "raw_reference_date": "2024-01-10",
                "source_version": "v0",
            }
        ]
    )

    panel = build_focus_reference_dates(refs)

    context = panel.select(
        ["indicator", "period", "reference_date_type", "reference_date"]
    ).to_dicts()

    assert context == [
        {
            "indicator": "IPCA",
            "period": "01/2025",
            "reference_date_type": "DataReferencia1",
            "reference_date": date(2024, 1, 10),
        }
    ]


def _focus_row(
    *,
    endpoint: str,
    indicator: str,
    indicator_detail: str | None,
    is_top5: bool,
    mean: float,
    calculation_type: str | None = None,
    base_calculation: int | None = None,
    ref_date: date | None = None,
    available_date: date | None = None,
) -> dict[str, object]:
    return {
        "ref_date": ref_date or date(2024, 1, 2),
        "available_date": available_date or date(2024, 1, 2),
        "endpoint": endpoint,
        "indicator": indicator,
        "indicator_detail": indicator_detail,
        "reference_period": "2025",
        "reference_year": 2025,
        "reference_month": None,
        "meeting": None,
        "horizon_label": "2025",
        "is_top5": is_top5,
        "calculation_type": calculation_type,
        "statistic_scope": str(base_calculation) if base_calculation is not None else None,
        "mean": mean,
        "median": mean,
        "std_dev": None,
        "min_value": None,
        "max_value": None,
        "respondents": None,
        "base_calculation": base_calculation,
        "source_dataset": "bcb_focus_top5_expectations" if is_top5 else "bcb_focus_expectations",
        "source_version": "v0",
    }
