from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl

from bralpha.infra.paths import ResolvedPaths
from bralpha.parsing.common import write_source_partitioned


class BCBResearchInputMissingError(FileNotFoundError):
    pass


def silver_dataset_root(paths: ResolvedPaths, dataset_id: str) -> Path:
    return paths.silver / dataset_id


def gold_panel_root(paths: ResolvedPaths, panel: str) -> Path:
    root = paths.gold / "bcb" / panel
    _assert_inside(root, paths.gold / "bcb")
    return root


def read_silver_dataset(
    paths: ResolvedPaths,
    dataset_id: str,
    *,
    required: bool = False,
    start: date | None = None,
    end: date | None = None,
    date_col: str = "ref_date",
) -> pl.DataFrame | None:
    return read_parquet_root(
        silver_dataset_root(paths, dataset_id),
        required=required,
        start=start,
        end=end,
        date_col=date_col,
    )


def read_gold_panel(
    paths: ResolvedPaths,
    panel: str,
    *,
    required: bool = False,
    start: date | None = None,
    end: date | None = None,
    date_col: str = "ref_date",
) -> pl.DataFrame | None:
    return read_parquet_root(
        gold_panel_root(paths, panel),
        required=required,
        start=start,
        end=end,
        date_col=date_col,
    )


def read_parquet_root(
    root: Path,
    *,
    required: bool = False,
    start: date | None = None,
    end: date | None = None,
    date_col: str = "ref_date",
) -> pl.DataFrame | None:
    files = _select_parquet_files(root, start=start, end=end)
    if not files:
        if required:
            raise BCBResearchInputMissingError(f"Missing required parquet input: {root}")
        return None
    scan = pl.scan_parquet([str(path) for path in files])
    if date_col in set(scan.collect_schema().names()):
        if start is not None:
            scan = scan.filter(pl.col(date_col) >= start)
        if end is not None:
            scan = scan.filter(pl.col(date_col) <= end)
    return scan.collect()


def write_gold_panel(
    frame: pl.DataFrame,
    paths: ResolvedPaths,
    *,
    panel: str,
    primary_keys: list[str],
    ref_date_col: str = "ref_date",
) -> list[Path]:
    return write_source_partitioned(
        frame,
        gold_panel_root(paths, panel),
        ref_date_col=ref_date_col,
        primary_keys=primary_keys,
        augment_source_dataset_key=False,
    )


def _assert_inside(path: Path, root: Path) -> None:
    path.resolve().relative_to(root.resolve())


def _select_parquet_files(
    root: Path,
    *,
    start: date | None,
    end: date | None,
) -> list[Path]:
    if not root.exists():
        return []
    if start is None and end is None:
        return sorted(root.glob("**/*.parquet"))

    partition_dirs = _year_partition_dirs(root)
    if not partition_dirs:
        return sorted(root.glob("**/*.parquet"))

    years = [year for year, _ in partition_dirs]
    first_year = start.year if start is not None else min(years)
    last_year = end.year if end is not None else max(years)
    if first_year > last_year:
        return []

    files: list[Path] = []
    seen: set[Path] = set()
    for year, part_dir in partition_dirs:
        if year < first_year or year > last_year:
            continue
        for path in sorted(part_dir.glob("**/*.parquet")):
            resolved = path.resolve()
            if resolved in seen:
                continue
            seen.add(resolved)
            files.append(path)
    return files


def _year_partition_dirs(root: Path) -> list[tuple[int, Path]]:
    partitions: list[tuple[int, Path]] = []
    for child in root.glob("**/year=*"):
        if not child.is_dir():
            continue
        try:
            year = int(child.name.removeprefix("year="))
        except ValueError:
            continue
        partitions.append((year, child))
    return sorted(partitions, key=lambda item: (item[0], str(item[1])))
