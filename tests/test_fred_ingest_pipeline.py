from __future__ import annotations

import json
import shutil
from datetime import date

import polars as pl

from bralpha.infra.http import HttpResponse
from bralpha.pipelines.fred_ingest import run_fred_ingest


class MockFredClient:
    def __init__(self) -> None:
        self.requests = []

    def get_bytes(self, url, params=None, headers=None):
        request_params = params or {}
        self.requests.append({"url": url, "params": request_params, "headers": headers or {}})
        series_id = str(request_params["series_id"])
        return HttpResponse(
            url=f"{url}?series_id={series_id}",
            status_code=200,
            headers={"content-type": "application/json"},
            content=_payload(series_id).encode(),
        )


def test_fred_pipeline_mocked_raw_to_bronze_to_silver(repo_root, tmp_path):
    shutil.copytree(repo_root / "configs", tmp_path / "configs")
    client = MockFredClient()

    status = run_fred_ingest(
        repo_root=tmp_path,
        start=date(2024, 1, 2),
        end=date(2024, 1, 2),
        series_ids=["DGS10", "DGS2"],
        api_key="test-key",
        client=client,
    )

    assert status == {"downloads": 2, "bronze_rows": 4, "silver_rows": 2}
    assert len(client.requests) == 2
    assert {request["params"]["series_id"] for request in client.requests} == {"DGS10", "DGS2"}
    assert (
        tmp_path
        / "data"
        / "bronze"
        / "fred"
        / "fred_series_observations"
        / "series_id=DGS10"
        / "year=2024"
        / "data.parquet"
    ).exists()
    silver_path = (
        tmp_path
        / "data"
        / "silver"
        / "fred_series_observations"
        / "series_id=DGS10"
        / "year=2024"
        / "data.parquet"
    )
    silver = pl.read_parquet(silver_path)
    assert silver["ref_date"].to_list() == [date(2024, 1, 2)]
    assert silver["value"].to_list() == [4.25]
    assert silver["available_date"].to_list() == [date(2024, 1, 3)]


def test_fred_pipeline_rerun_is_idempotent_for_silver_primary_key(repo_root, tmp_path):
    shutil.copytree(repo_root / "configs", tmp_path / "configs")

    for _ in range(2):
        run_fred_ingest(
            repo_root=tmp_path,
            start=date(2024, 1, 2),
            end=date(2024, 1, 2),
            series_ids=["DGS10", "DGS2"],
            api_key="test-key",
            client=MockFredClient(),
        )

    silver_frames = [
        pl.read_parquet(path)
        for path in (tmp_path / "data" / "silver" / "fred_series_observations").glob(
            "series_id=*/year=2024/data.parquet"
        )
    ]
    silver = pl.concat(silver_frames, how="diagonal_relaxed")

    assert silver.height == 2
    assert silver.group_by(["series_id", "ref_date"]).len().height == 2


def _payload(series_id: str) -> str:
    values = {
        "DGS10": ("4.10", "4.25"),
        "DGS2": ("3.90", "4.00"),
    }
    old_value, in_window_value = values[series_id]
    return json.dumps(
        {
            "realtime_start": "2024-01-04",
            "realtime_end": "2024-01-04",
            "observation_start": "2024-01-01",
            "observation_end": "2024-01-02",
            "units": "lin",
            "output_type": 1,
            "file_type": "json",
            "order_by": "observation_date",
            "sort_order": "asc",
            "count": 2,
            "offset": 0,
            "limit": 100000,
            "observations": [
                {
                    "realtime_start": "2024-01-04",
                    "realtime_end": "2024-01-04",
                    "date": "2024-01-01",
                    "value": old_value,
                },
                {
                    "realtime_start": "2024-01-04",
                    "realtime_end": "2024-01-04",
                    "date": "2024-01-02",
                    "value": in_window_value,
                },
            ],
        }
    )
