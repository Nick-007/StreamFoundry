from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from .config import get


@dataclass(frozen=True)
class JobPaths:
    """Convenience container for per-job workspace layout."""

    stem: str
    root: Path
    input_dir: Path
    work_dir: Path
    dist_dir: Path

    def ensure(self, *, parents: bool = True) -> "JobPaths":
        """Create directories if they do not yet exist."""
        dirs: Iterable[Path] = (self.root, self.input_dir, self.work_dir, self.dist_dir)
        for d in dirs:
            d.mkdir(parents=parents, exist_ok=True)
        return self


def job_paths(stem: str, *, create: bool = True) -> JobPaths:
    """
    Return the canonical directory layout for a job under TMP_DIR.
    When create=True (default) each directory is created if missing.
    """
    tmp_dir = Path(get("TMP_DIR", "/tmp/ingestor"))
    root = tmp_dir / stem
    paths = JobPaths(
        stem=stem,
        root=root,
        input_dir=root / "input",
        work_dir=root / "work",
        dist_dir=root / "dist",
    )
    return paths.ensure() if create else paths

