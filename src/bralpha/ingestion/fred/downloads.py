from __future__ import annotations

from collections.abc import Sequence
from datetime import date, datetime
from pathlib import Path

from bralpha.infra.http import HttpClient
from bralpha.metadata.datasets import DatasetConfig

from .common import (
    FRED_LATEST_SNAPSHOT_REQUEST,
    FRED_VINTAGE_REQUEST,
    FredDownloadResult,
    FredSeriesConfig,
    client_context,
    download_fred_request,
    fred_dataset,
    fred_manifest_writer,
    fred_paths,
    fred_raw_store,
    load_fred_series_config,
    resolve_fred_api_key,
)


def download_fred_series_observations(
    repo_root: Path,
    *,
    start: date,
    end: date,
    realtime_start: date | None = None,
    realtime_end: date | None = None,
    series_ids: list[str] | None = None,
    api_key: str | None = None,
    client: HttpClient | None = None,
    downloaded_at: datetime | None = None,
) -> list[FredDownloadResult]:
    if start > end:
        raise ValueError("start must be on or before end")
    dataset = fred_dataset(repo_root, "fred_series_observations")
    key = resolve_fred_api_key(api_key)
    configured = load_fred_series_config(repo_root)
    selected = _select_series(configured, series_ids)
    paths = fred_paths(repo_root)
    results: list[FredDownloadResult] = []

    with client_context(client) as owned_client:
        for config in selected:
            url, params, filename = build_fred_observations_request(
                dataset,
                series_id=config.series_id,
                observation_start=start,
                observation_end=end,
                api_key=key,
                vintage_request_mode=config.vintage_request_mode,
                realtime_start=_configured_date(realtime_start, config.realtime_start),
                realtime_end=_configured_date(realtime_end, config.realtime_end),
                vintage_dates=config.vintage_dates,
            )
            results.append(
                download_fred_request(
                    dataset=dataset,
                    raw_store=fred_raw_store(paths),
                    manifest_writer=fred_manifest_writer(paths),
                    url=url,
                    params=params,
                    filename=filename,
                    client=owned_client,
                    downloaded_at=downloaded_at,
                )
            )
    return results


def build_fred_observations_request(
    dataset: DatasetConfig,
    *,
    series_id: str,
    observation_start: date,
    observation_end: date,
    api_key: str,
    vintage_request_mode: str = FRED_LATEST_SNAPSHOT_REQUEST,
    realtime_start: date | None = None,
    realtime_end: date | None = None,
    vintage_dates: Sequence[date | str] | None = None,
) -> tuple[str, dict[str, str], str]:
    url, params, _, filename = dataset.first_source_url().render(
        series_id=series_id,
        start=observation_start,
        end=observation_end,
        api_key=api_key,
    )
    request_mode = vintage_request_mode.strip()
    if request_mode not in {FRED_LATEST_SNAPSHOT_REQUEST, FRED_VINTAGE_REQUEST}:
        raise ValueError(f"Unsupported FRED vintage_request_mode: {vintage_request_mode}")
    if request_mode == FRED_VINTAGE_REQUEST:
        params["output_type"] = "2"
        if vintage_dates:
            params["vintage_dates"] = ",".join(_date_param_text(item) for item in vintage_dates)
        else:
            params["realtime_start"] = (realtime_start or observation_start).isoformat()
            params["realtime_end"] = (realtime_end or date.today()).isoformat()
    if filename is None:
        filename = f"fred_{series_id}_{observation_start:%Y%m%d}_{observation_end:%Y%m%d}.json"
    if request_mode == FRED_VINTAGE_REQUEST:
        filename = filename.replace(".json", "_vintages.json")
    return url, params, filename


def _configured_date(override: date | None, configured: date | str | None) -> date | None:
    if override is not None:
        return override
    if configured is None:
        return None
    if isinstance(configured, date):
        return configured
    text = str(configured).strip()
    return date.fromisoformat(text) if text else None


def _date_param_text(value: date | str) -> str:
    if isinstance(value, date):
        return value.isoformat()
    text = str(value).strip()
    if not text:
        raise ValueError("FRED vintage_dates must not contain empty values")
    date.fromisoformat(text)
    return text


def _select_series(
    configured: list[FredSeriesConfig],
    series_ids: list[str] | None,
) -> list[FredSeriesConfig]:
    configured_by_id = {item.series_id.strip().upper(): item for item in configured}
    configured_upper = [item.series_id.strip().upper() for item in configured]
    if series_ids is None:
        return [configured_by_id[series_id] for series_id in configured_upper]
    requested = {series_id.strip().upper() for series_id in series_ids}
    selected = [series_id for series_id in configured_upper if series_id in requested]
    missing = sorted(requested - set(selected))
    if missing:
        raise ValueError(f"Unknown configured FRED series_id(s): {missing}")
    return [configured_by_id[series_id] for series_id in selected]
