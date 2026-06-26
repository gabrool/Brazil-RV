from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path

import yaml

from bralpha.infra.http import HttpClient
from bralpha.metadata.datasets import DatasetConfig

from .common import (
    BcbDownloadResult,
    bcb_dataset,
    bcb_manifest_writer,
    bcb_paths,
    bcb_raw_store,
    client_context,
    download_bcb_request,
)


@dataclass(frozen=True)
class SgsSeriesConfig:
    series_id: int
    slug: str
    name: str
    category: str
    frequency: str
    unit: str
    availability_policy: str
    model_usable: bool
    availability_lag_days: int | None = None


def load_sgs_series_config(repo_root: Path) -> list[SgsSeriesConfig]:
    path = repo_root / "configs" / "series" / "bcb_sgs.yaml"
    with path.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle)
    rows = data.get("series", []) if isinstance(data, dict) else []
    return [SgsSeriesConfig(**row) for row in rows]


def download_sgs_series(
    repo_root: Path,
    *,
    start: date,
    end: date,
    series_ids: list[int] | None = None,
    client: HttpClient | None = None,
    downloaded_at: datetime | None = None,
) -> list[BcbDownloadResult]:
    dataset = bcb_dataset(repo_root, "bcb_sgs_series")
    paths = bcb_paths(repo_root)
    selected = _select_series(load_sgs_series_config(repo_root), series_ids)
    results: list[BcbDownloadResult] = []
    with client_context(client) as owned_client:
        for series in selected:
            for window_start, window_end in sgs_date_windows(
                start,
                end,
                frequency=series.frequency,
            ):
                url, params, filename = build_sgs_request(
                    dataset,
                    series_id=series.series_id,
                    start=window_start,
                    end=window_end,
                )
                results.append(
                    download_bcb_request(
                        dataset=dataset,
                        raw_store=bcb_raw_store(paths),
                        manifest_writer=bcb_manifest_writer(paths),
                        url=url,
                        params=params,
                        filename=filename,
                        client=owned_client,
                        downloaded_at=downloaded_at,
                        manifest_params={
                            "series_id": series.series_id,
                            "start": window_start.isoformat(),
                            "end": window_end.isoformat(),
                            **params,
                        },
                    )
                )
    return results


def build_sgs_request(
    dataset: DatasetConfig,
    *,
    series_id: int,
    start: date,
    end: date,
) -> tuple[str, dict[str, str], str]:
    url, params, _, filename = dataset.first_source_url().render(
        series_id=series_id,
        start=start,
        end=end,
    )
    if filename is None:
        filename = f"bcb_sgs_{series_id}_{start:%Y%m%d}_{end:%Y%m%d}.json"
    return url, params, filename


def sgs_date_windows(
    start: date,
    end: date,
    *,
    frequency: str,
    max_years: int = 10,
) -> list[tuple[date, date]]:
    if start > end:
        raise ValueError("start must be on or before end")
    windows = []
    current = start
    while current <= end:
        next_start = _add_years(current, max_years)
        window_end = min(end, next_start - timedelta(days=1))
        windows.append((current, window_end))
        current = window_end + timedelta(days=1)
    return windows


def _select_series(
    series: list[SgsSeriesConfig],
    series_ids: list[int] | None,
) -> list[SgsSeriesConfig]:
    if series_ids is None:
        return series
    requested = set(series_ids)
    selected = [item for item in series if item.series_id in requested]
    missing = sorted(requested - {item.series_id for item in selected})
    if missing:
        raise ValueError(f"Unknown configured SGS series_id(s): {missing}")
    return selected


def _add_years(value: date, years: int) -> date:
    try:
        return value.replace(year=value.year + years)
    except ValueError:
        return value.replace(month=2, day=28, year=value.year + years)
