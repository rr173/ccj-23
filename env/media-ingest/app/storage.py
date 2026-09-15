from __future__ import annotations

import json
import os
import shutil
import time
import uuid
from pathlib import Path


class Storage:
    """Filesystem layout (all on one volume so renames are atomic):

    <root>/staging/<upload_id>/<index:08d>.part   validated chunks, atomically renamed into place
    <root>/objects/<version_id>.bin               sealed objects, chmod 444, never overwritten
    <root>/objects/<version_id>.json              per-chunk digest manifest
    <root>/tmp/*.tmp                              in-flight writes; invisible to clients, swept if orphaned
    """

    def __init__(self, root: str | Path):
        self.root = Path(root)
        self.staging = self.root / "staging"
        self.objects = self.root / "objects"
        self.tmp = self.root / "tmp"
        for d in (self.staging, self.objects, self.tmp):
            d.mkdir(parents=True, exist_ok=True)

    # ---- chunks ----
    def staging_dir(self, upload_id: str) -> Path:
        return self.staging / upload_id

    def chunk_path(self, upload_id: str, index: int) -> Path:
        return self.staging_dir(upload_id) / f"{index:08d}.part"

    def new_tmp(self, prefix: str) -> Path:
        return self.tmp / f"{prefix}-{uuid.uuid4().hex}.tmp"

    @staticmethod
    def _atomic_replace(src: Path, dst: Path) -> None:
        os.replace(src, dst)  # same filesystem -> atomic
        dir_fd = os.open(dst.parent, os.O_DIRECTORY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)

    def commit_chunk(self, tmp_path: Path, upload_id: str, index: int) -> Path:
        dst = self.chunk_path(upload_id, index)
        dst.parent.mkdir(parents=True, exist_ok=True)
        self._atomic_replace(tmp_path, dst)
        return dst

    def remove_staging(self, upload_id: str) -> None:
        shutil.rmtree(self.staging_dir(upload_id), ignore_errors=True)

    # ---- sealed objects ----
    def object_path(self, version_id: str) -> Path:
        return self.objects / f"{version_id}.bin"

    def manifest_path(self, version_id: str) -> Path:
        return self.objects / f"{version_id}.json"

    def seal_object(self, tmp_path: Path, version_id: str, manifest: dict) -> Path:
        """Atomically publish the merged object and its manifest, then make both read-only."""
        dst = self.object_path(version_id)
        if dst.exists():
            # Version id already sealed — never overwrite an immutable object.
            tmp_path.unlink(missing_ok=True)
            return dst
        self._atomic_replace(tmp_path, dst)
        mtmp = self.new_tmp("manifest")
        mtmp.write_text(json.dumps(manifest, indent=2))
        self._atomic_replace(mtmp, self.manifest_path(version_id))
        os.chmod(dst, 0o444)
        os.chmod(self.manifest_path(version_id), 0o444)
        return dst

    def remove_object(self, version_id: str) -> None:
        """Only used to discard a losing concurrent merge before its version row commits."""
        for p in (self.object_path(version_id), self.manifest_path(version_id)):
            if p.exists():
                os.chmod(p, 0o644)
                p.unlink()

    def sweep_tmp(self, older_than_seconds: float) -> int:
        now = time.time()
        removed = 0
        for p in self.tmp.glob("*.tmp"):
            try:
                if now - p.stat().st_mtime > older_than_seconds:
                    p.unlink()
                    removed += 1
            except FileNotFoundError:
                pass
        return removed
