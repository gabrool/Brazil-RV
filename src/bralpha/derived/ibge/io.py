from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl

from bralpha.infra.paths import ResolvedPaths
from bralpha.parsing.common import write_source_partitioned


class IBGEResearchInputMissingError(FileNotFoundError):
    pass


def silver_dataset_root(paths: ResolvedPaths, dataset_id: str) -> Path:
    return paths.silver / dataset_id


def gold_panel_root(paths: ResolvedPaths, panel: str) -> Path:
    root = paths.gold / "ibge" / panel
    _assert_inside(root, paths.gold / "ibge")
    return root


def read_silver_dataset(
    paths: ResolvedPaths,
    dataset_id: str,
    *,
    required: bool = False,
    start: date | None = None,
    end: date | None = None,
    date_col: str | None = "ref_date",
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
    date_col: str | None = "ref_date",
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
    date_col: str | None = "ref_date",
) -> pl.DataFrame | None:
    files = _select_parquet_files(root, start=start, end=end)
    if not files:
        if required:
            raise IBGEResearchInputMissingError(f"Missing required parquet input: {root}")
        return None

    scan = pl.scan_parquet([str(path) for path in files])
    if date_col is not None and date_col in set(scan.collect_schema().names()):
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
    ref_date_col: str | None = "ref_date",
) -> list[Path]:
    root = gold_panel_root(paths, panel)
    if ref_date_col is None:
        return _write_unpartitioned(frame, root, primary_keys=primary_keys)
    return write_source_partitioned(
        frame,
        root,
        ref_date_col=ref_date_col,
        primary_keys=primary_keys,
        augment_source_dataset_key=False,
    )


def _write_unpartitioned(
    frame: pl.DataFrame,
    output_root: Path,
    *,
    primary_keys: list[str],
    filename: str = "data.parquet",
) -> list[Path]:
    if frame.is_empty():
        return []
    output_root.mkdir(parents=True, exist_ok=True)
    path = output_root / filename
    if path.exists():
        frame = pl.concat([pl.read_parquet(path), frame], how="diagonal_relaxed")
        frame = frame.unique(subset=primary_keys, keep="last", maintain_order=True)
    frame.write_parquet(path)
    return [path]


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
