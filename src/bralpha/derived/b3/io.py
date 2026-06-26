from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl

from bralpha.infra.paths import ResolvedPaths
from bralpha.parsing.common import write_source_partitioned


class ResearchInputMissingError(FileNotFoundError):
    pass


def silver_dataset_root(paths: ResolvedPaths, dataset_id: str) -> Path:
    return paths.silver / dataset_id


def gold_panel_root(paths: ResolvedPaths, panel: str) -> Path:
    root = paths.gold / "b3" / panel
    _assert_inside(root, paths.gold / "b3")
    return root


def read_silver_dataset(
    paths: ResolvedPaths,
    dataset_id: str,
    *,
    required: bool = False,
) -> pl.DataFrame | None:
    return read_parquet_root(silver_dataset_root(paths, dataset_id), required=required)


def read_gold_panel(
    paths: ResolvedPaths,
    panel: str,
    *,
    required: bool = False,
) -> pl.DataFrame | None:
    return read_parquet_root(gold_panel_root(paths, panel), required=required)


def read_parquet_root(root: Path, *, required: bool = False) -> pl.DataFrame | None:
    files = sorted(root.glob("**/*.parquet")) if root.exists() else []
    if not files:
        if required:
            raise ResearchInputMissingError(f"Missing required parquet input: {root}")
        return None
    frames = [pl.read_parquet(path) for path in files]
    if not frames:
        return None
    return pl.concat(frames, how="diagonal_relaxed")


def filter_date_range(
    frame: pl.DataFrame | None,
    *,
    start: date | None,
    end: date | None,
    date_col: str = "ref_date",
) -> pl.DataFrame | None:
    if frame is None or date_col not in frame.columns:
        return frame
    filtered = frame
    if start is not None:
        filtered = filtered.filter(pl.col(date_col) >= start)
    if end is not None:
        filtered = filtered.filter(pl.col(date_col) <= end)
    return filtered


def write_gold_panel(
    frame: pl.DataFrame,
    paths: ResolvedPaths,
    *,
    panel: str,
    primary_keys: list[str],
    ref_date_col: str = "ref_date",
) -> list[Path]:
    output_root = gold_panel_root(paths, panel)
    return write_source_partitioned(
        frame,
        output_root,
        ref_date_col=ref_date_col,
        primary_keys=primary_keys,
    )


def _assert_inside(path: Path, root: Path) -> None:
    path.resolve().relative_to(root.resolve())
