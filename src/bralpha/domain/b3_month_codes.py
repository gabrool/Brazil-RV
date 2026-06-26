from __future__ import annotations

from dataclasses import dataclass

MONTH_CODES = {
    "F": 1,
    "G": 2,
    "H": 3,
    "J": 4,
    "K": 5,
    "M": 6,
    "N": 7,
    "Q": 8,
    "U": 9,
    "V": 10,
    "X": 11,
    "Z": 12,
}


@dataclass(frozen=True)
class B3Maturity:
    code: str
    month_code: str
    month: int
    year: int


def parse_b3_maturity_code(code: str, pivot_year: int = 80) -> B3Maturity:
    normalized = code.strip().upper()
    if len(normalized) not in {3, 5}:
        raise ValueError(f"Malformed B3 maturity code: {code!r}")
    month_code = normalized[0]
    if month_code not in MONTH_CODES:
        raise ValueError(f"Invalid B3 futures month code: {month_code!r}")
    year_text = normalized[1:]
    if not year_text.isdigit():
        raise ValueError(f"Malformed B3 maturity year: {code!r}")
    if len(year_text) == 2:
        yy = int(year_text)
        year = 1900 + yy if yy >= pivot_year else 2000 + yy
    else:
        year = int(year_text)
    return B3Maturity(
        code=normalized,
        month_code=month_code,
        month=MONTH_CODES[month_code],
        year=year,
    )
