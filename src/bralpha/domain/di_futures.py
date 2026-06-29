from __future__ import annotations

from math import log
from typing import SupportsFloat

DI_NOTIONAL = 100_000.0
DI_BDAY_BASIS = 252


def discount_factor_from_pu(pu: SupportsFloat | None) -> float | None:
    value = _positive_float(pu)
    if value is None:
        return None
    return value / DI_NOTIONAL


def annual_rate_from_pu(
    pu: SupportsFloat | None,
    business_days: int | None,
) -> float | None:
    value = _positive_float(pu)
    if value is None or business_days is None or business_days <= 0:
        return None
    return (DI_NOTIONAL / value) ** (DI_BDAY_BASIS / business_days) - 1.0


def pu_from_annual_rate(
    rate: SupportsFloat | None,
    business_days: int | None,
) -> float | None:
    if rate is None or business_days is None or business_days <= 0:
        return None
    rate_value = float(rate)
    if rate_value <= -1.0:
        return None
    return DI_NOTIONAL / ((1.0 + rate_value) ** (business_days / DI_BDAY_BASIS))


def log_discount_factor_from_pu(pu: SupportsFloat | None) -> float | None:
    discount_factor = discount_factor_from_pu(pu)
    if discount_factor is None or discount_factor <= 0:
        return None
    return log(discount_factor)


def annual_rate_from_discount_factor(
    discount_factor: SupportsFloat | None,
    business_days: int | None,
) -> float | None:
    value = _positive_float(discount_factor)
    if value is None or business_days is None or business_days <= 0:
        return None
    return (1.0 / value) ** (DI_BDAY_BASIS / business_days) - 1.0


def _positive_float(value: SupportsFloat | None) -> float | None:
    if value is None:
        return None
    number = float(value)
    if number <= 0:
        return None
    return number
