from __future__ import annotations

from bralpha.domain.b3_month_codes import parse_b3_maturity_code


def build_b3_contract_id(symbol_root: str, maturity_code: str) -> str:
    root = symbol_root.strip().upper()
    if not root:
        raise ValueError("symbol_root must be non-empty")
    maturity = parse_b3_maturity_code(maturity_code).code
    return f"{root}_{maturity}"
