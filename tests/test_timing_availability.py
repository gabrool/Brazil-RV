from __future__ import annotations

from datetime import date, datetime, time, timedelta

from bralpha.timing.availability import (
    decision_cutoff_datetime,
    usable_date_from_available_datetime,
    usable_date_from_date_only,
    usable_date_from_same_day_eod_release,
)


def test_decision_cutoff_datetime_uses_configured_timezone():
    cutoff = decision_cutoff_datetime(
        date(2024, 1, 2),
        cutoff_time=time(18, 30),
        tz_name="America/Sao_Paulo",
    )

    assert cutoff.date() == date(2024, 1, 2)
    assert cutoff.time() == time(18, 30)
    assert cutoff.tzinfo is not None
    assert cutoff.utcoffset() == timedelta(hours=-3)


def test_timestamp_before_cutoff_is_usable_same_day():
    assert (
        usable_date_from_available_datetime(
            datetime(2024, 1, 2, 18, 0),
            cutoff_time=time(18, 30),
        )
        == date(2024, 1, 2)
    )


def test_timestamp_before_cutoff_on_non_business_day_moves_to_next_b3_day():
    assert (
        usable_date_from_available_datetime(
            datetime(2024, 1, 1, 10, 0),
            cutoff_time=time(18, 30),
        )
        == date(2024, 1, 2)
    )


def test_timestamp_after_cutoff_is_usable_next_business_day():
    assert (
        usable_date_from_available_datetime(
            datetime(2024, 1, 2, 18, 31),
            cutoff_time=time(18, 30),
        )
        == date(2024, 1, 3)
    )


def test_date_only_release_is_usable_next_business_day():
    assert usable_date_from_date_only(date(2024, 1, 2)) == date(2024, 1, 3)


def test_official_same_day_eod_release_uses_release_date_when_b3_business_day():
    assert usable_date_from_same_day_eod_release(date(2024, 1, 2)) == date(2024, 1, 2)
    assert usable_date_from_same_day_eod_release(date(2024, 1, 1)) == date(2024, 1, 2)


def test_friday_after_cutoff_and_date_only_move_to_monday_without_holidays():
    assert (
        usable_date_from_available_datetime(
            datetime(2024, 1, 5, 18, 31),
            cutoff_time=time(18, 30),
        )
        == date(2024, 1, 8)
    )
    assert usable_date_from_date_only(date(2024, 1, 5)) == date(2024, 1, 8)


def test_explicit_holidays_are_respected():
    holidays = {date(2024, 1, 3)}

    assert (
        usable_date_from_available_datetime(
            datetime(2024, 1, 2, 18, 31),
            cutoff_time=time(18, 30),
            holidays=holidays,
        )
        == date(2024, 1, 4)
    )
    assert usable_date_from_date_only(date(2024, 1, 2), holidays=holidays) == date(
        2024, 1, 4
    )
