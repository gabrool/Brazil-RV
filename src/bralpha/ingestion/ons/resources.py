from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from bralpha.metadata.datasets import DatasetConfig


@dataclass(frozen=True)
class ONSResourceRequest:
    dataset_id: str
    resource_name: str
    url: str
    filename: str
    year: int


def ons_annual_resources(
    dataset_config: DatasetConfig,
    *,
    start: date,
    end: date,
) -> list[ONSResourceRequest]:
    if start > end:
        raise ValueError("ONS annual resources require start <= end")

    extra = dataset_config.model_extra or {}
    url_template = extra.get("direct_url_template")
    filename_template = extra.get("filename_template")
    if not url_template or not filename_template:
        raise ValueError(
            f"Dataset has no annual ONS URL template: {dataset_config.dataset_id}"
        )

    prefix = extra.get("resource_name_prefix") or dataset_config.dataset_id
    requests: list[ONSResourceRequest] = []
    for year in range(start.year, end.year + 1):
        requests.append(
            ONSResourceRequest(
                dataset_id=dataset_config.dataset_id,
                resource_name=f"{prefix}-{year}",
                url=str(url_template).format(year=year, dataset_id=dataset_config.dataset_id),
                filename=str(filename_template).format(
                    year=year, dataset_id=dataset_config.dataset_id
                ),
                year=year,
            )
        )
    return requests
