from __future__ import annotations

from datetime import date
from typing import Any

from bralpha.derived.feature_utils import max_date

PIT_METADATA_COLUMNS = [
    "availability_policy",
    "availability_basis",
    "revision_policy",
    "vintage_id",
    "model_usable",
    "model_usable_reason",
]

_TEXT_FIELDS = [
    "availability_policy",
    "availability_basis",
    "revision_policy",
    "vintage_id",
    "model_usable_reason",
]


def copy_pit_metadata(row: dict[str, Any]) -> dict[str, Any]:
    return {field: row.get(field) for field in PIT_METADATA_COLUMNS}


def merge_pit_metadata(*rows: dict[str, Any] | None) -> dict[str, Any]:
    present = [row for row in rows if row is not None]
    metadata = {field: _join_text(*(row.get(field) for row in present)) for field in _TEXT_FIELDS}
    metadata["model_usable"] = _merge_model_usable(*(row.get("model_usable") for row in present))
    return metadata


def max_available_date(ref_date: date, *rows: dict[str, Any] | None) -> date:
    return max_date(
        ref_date,
        *(row.get("available_date") for row in rows if row is not None),
        *(row.get("observation_available_date") for row in rows if row is not None),
    ) or ref_date


def _join_text(*values: Any) -> str | None:
    unique = sorted({str(value) for value in values if value is not None and str(value).strip()})
    return "|".join(unique) if unique else None


def _merge_model_usable(*values: Any) -> bool | None:
    bools = [_as_bool(value) for value in values if value is not None and str(value).strip()]
    if not bools:
        return None
    return all(bools)


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    return text not in {"false", "0", "no", "n"}
