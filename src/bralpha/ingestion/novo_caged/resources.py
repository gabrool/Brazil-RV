from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from bralpha.metadata.datasets import DatasetConfig


@dataclass(frozen=True)
class NovoCagedResourceRequest:
    dataset_id: str
    resource_name: str
    url: str
    filename: str
    period: str | None = None
    year: int | None = None
    month: int | None = None
    record_kind: str | None = None


def novo_caged_monthly_resources(
    dataset_config: DatasetConfig,
    start: date,
    end: date,
) -> list[NovoCagedResourceRequest]:
    if start > end:
        raise ValueError("Novo CAGED monthly resources require start <= end")

    requests: list[NovoCagedResourceRequest] = []
    for family in dataset_config.model_extra.get("resource_families", []):
        mode = family["mode"]
        if mode != "monthly_7z":
            raise ValueError(f"Unsupported Novo CAGED resource mode: {mode}")
        requests.extend(_monthly_7z_requests(dataset_config, family, start, end))
    return sorted(requests, key=lambda item: (item.year or 0, item.month or 0))


def novo_caged_release_calendar_resource(
    dataset_config: DatasetConfig,
) -> list[NovoCagedResourceRequest]:
    source_url = dataset_config.first_source_url()
    url, _params, _headers, filename = source_url.render()
    return [
        NovoCagedResourceRequest(
            dataset_id=dataset_config.dataset_id,
            resource_name=source_url.name or "official_release_calendar",
            url=url,
            filename=filename or "novo_caged_release_calendar.html",
        )
    ]


def _monthly_7z_requests(
    dataset_config: DatasetConfig,
    family: dict[str, object],
    start: date,
    end: date,
) -> list[NovoCagedResourceRequest]:
    applies_from = _parse_date(family.get("applies_from")) or date(2020, 1, 1)
    range_start = max(start, applies_from)
    if end < range_start:
        return []

    requests: list[NovoCagedResourceRequest] = []
    year = range_start.year
    month = range_start.month
    while (year, month) <= (end.year, end.month):
        period = f"{year}{month:02d}"
        values = {"year": year, "month": month, "period": period}
        filename = str(family["filename_template"]).format(**values)
        family_name = str(family["name"])
        requests.append(
            NovoCagedResourceRequest(
                dataset_id=dataset_config.dataset_id,
                resource_name=f"{family_name}-{period}",
                url=str(family["url_template"]).format(**values),
                filename=filename,
                period=period,
                year=year,
                month=month,
                record_kind=str(family.get("record_kind") or "movement"),
            )
        )
        month += 1
        if month == 13:
            month = 1
            year += 1
    return requests


def _parse_date(value: object) -> date | None:
    if value is None:
        return None
    if isinstance(value, date):
        return value
    text = str(value).strip()
    return date.fromisoformat(text) if text else None
