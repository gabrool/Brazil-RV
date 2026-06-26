from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from bralpha.infra.hashing import sha256_bytes, sha256_file


@dataclass(frozen=True)
class RawStore:
    root: Path
    source: str = "b3"

    def write_bytes(
        self,
        dataset_id: str,
        content: bytes,
        filename: str,
        downloaded_at: datetime,
    ) -> Path:
        content_hash = sha256_bytes(content)
        download_date = downloaded_at.date().isoformat()
        target_dir = self.root / self.source / dataset_id / download_date
        target_dir.mkdir(parents=True, exist_ok=True)

        safe_name = _safe_filename(filename)
        target = target_dir / safe_name
        if target.exists():
            if sha256_file(target) == content_hash:
                return target
            target = target.with_name(f"{target.stem}.{content_hash[:12]}{target.suffix}")
            if target.exists():
                if sha256_file(target) == content_hash:
                    return target
                raise FileExistsError(f"Conflicting raw file already exists: {target}")

        tmp = target.with_name(f".{target.name}.{os.getpid()}.tmp")
        try:
            with tmp.open("wb") as handle:
                handle.write(content)
                handle.flush()
                os.fsync(handle.fileno())
            tmp.replace(target)
        finally:
            if tmp.exists():
                tmp.unlink()
        return target


def _safe_filename(filename: str) -> str:
    sanitized = filename.replace("\\", "_").replace("/", "_").strip()
    if not sanitized or sanitized in {".", ".."}:
        raise ValueError("filename must name a file")
    return sanitized
