from __future__ import annotations

from pathlib import Path

from pydantic import BaseModel, ConfigDict


class ResolvedPaths(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True, frozen=True)

    repo_root: Path
    data_root: Path
    raw: Path
    bronze: Path
    silver: Path
    gold: Path
    manifests: Path
    external: Path
    reports: Path


def _resolve(repo_root: Path, path: Path) -> Path:
    return path if path.is_absolute() else (repo_root / path).resolve()


def resolve_project_paths(repo_root: Path, paths: object) -> ResolvedPaths:
    """Resolve configured paths without creating any directories."""

    section = getattr(paths, "paths", paths)
    return ResolvedPaths(
        repo_root=repo_root.resolve(),
        data_root=_resolve(repo_root, section.data_root),
        raw=_resolve(repo_root, section.raw),
        bronze=_resolve(repo_root, section.bronze),
        silver=_resolve(repo_root, section.silver),
        gold=_resolve(repo_root, section.gold),
        manifests=_resolve(repo_root, section.manifests),
        external=_resolve(repo_root, section.external),
        reports=_resolve(repo_root, section.reports),
    )
