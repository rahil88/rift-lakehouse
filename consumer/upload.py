"""Local landing files -> Unity Catalog Volume (Files API), then archive them locally.

State is just the filesystem: a file in ``landing/`` is not uploaded yet; once the upload
succeeds it moves to ``landed/`` with the same relative path. Uploads use ``overwrite=True`` and
the same relative path in the Volume, so a retried upload replaces the file with identical
content. Combined with COPY INTO's per-file load tracking, retries never duplicate data.
"""

from __future__ import annotations

import logging
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)


@dataclass
class UploadReport:
    files: int = 0
    bytes: int = 0
    skipped: int = 0

    def as_dict(self) -> dict[str, int]:
        return {"files": self.files, "bytes": self.bytes, "skipped": self.skipped}


def upload_landing(
    files_api: Any,  # noqa: ANN401 - databricks.sdk FilesAPI
    landing_dir: Path,
    landed_dir: Path,
    volume_root: str,
) -> UploadReport:
    report = UploadReport()
    for path in sorted(landing_dir.rglob("*.jsonl")):
        rel = path.relative_to(landing_dir).as_posix()
        target = f"{volume_root}/{rel}"
        try:
            size = path.stat().st_size
            with path.open("rb") as fh:
                files_api.upload(target, fh, overwrite=True)
            archived = landed_dir / rel
            archived.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(path, archived)
        except FileNotFoundError:
            # Another uploader archived it first (it's already in the Volume). The Airflow pool
            # prevents this; this is the safety net, so a race can't fail the run.
            log.warning("skipped %s: already taken by another uploader", rel)
            report.skipped += 1
            continue
        report.files += 1
        report.bytes += size
        log.info("uploaded %s (%d bytes)", rel, size)
    return report
