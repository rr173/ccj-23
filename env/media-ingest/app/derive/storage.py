"""Physical staging for derivation jobs.

Layout under the derivation data root::

    <root>/staging/<job_id>/seg-<i>.part   verified segment bytes
    <root>/staging/<job_id>/assembled.bin  full concatenation, staged
    <root>/tmp/copy-<uuid>.tmp             in-flight segment/assembly files

Every write goes tmp-file + fsync + os.replace (same volume => atomic), so a
crash leaves either a complete file or nothing: recovery never has to reason
about a half-written part. Final bytes are published through the archive
ContentStore (content-addressed), which performs the same atomic move into the
immutable content tree — staging bytes are never downloadable directly.
"""

from __future__ import annotations

import hashlib
import os
import shutil
import uuid
from pathlib import Path

COPY_CHUNK = 1024 * 1024


class DerivationStore:
    def __init__(self, root: str | Path):
        self.root = Path(root)
        self.staging = self.root / "staging"
        self.tmp = self.root / "tmp"
        for d in (self.staging, self.tmp):
            d.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------- paths
    def job_dir(self, job_id: str) -> Path:
        return self.staging / job_id

    def seg_path(self, job_id: str, index: int) -> Path:
        return self.job_dir(job_id) / f"seg-{index}.part"

    def assembled_path(self, job_id: str) -> Path:
        return self.job_dir(job_id) / "assembled.bin"

    def _new_tmp(self) -> Path:
        return self.tmp / f"copy-{uuid.uuid4().hex}.tmp"

    @staticmethod
    def _fsync_dir(path: Path) -> None:
        fd = os.open(path, os.O_DIRECTORY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)

    def _atomic_put(self, dst: Path, write) -> None:
        dst.parent.mkdir(parents=True, exist_ok=True)
        tmp = self._new_tmp()
        with open(tmp, "wb") as f:
            write(f)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, dst)
        self._fsync_dir(dst.parent)

    # ------------------------------------------------------------- segment IO
    def has_segment(self, job_id: str, index: int) -> bool:
        return self.seg_path(job_id, index).exists()

    def extract_segment(self, job_id: str, index: int, src: Path, start: int, end: int) -> tuple[str, int]:
        """Copy bytes [start, end) of ``src`` into this segment's part file.

        Returns (sha256, size) of the copied bytes. Any leftover file from a
        previous attempt is overwritten atomically."""
        h = hashlib.sha256()
        remaining = end - start

        def write(dst_f):
            with open(src, "rb") as sf:
                sf.seek(start)
                left = remaining
                while left > 0:
                    buf = sf.read(min(COPY_CHUNK, left))
                    if not buf:
                        # Source shorter than advertised; the digest/size check
                        # by the caller turns this into a deterministic failure.
                        break
                    dst_f.write(buf)
                    h.update(buf)
                    left -= len(buf)

        self._atomic_put(self.seg_path(job_id, index), write)
        size = os.path.getsize(self.seg_path(job_id, index))
        return h.hexdigest(), size

    def assemble(self, job_id: str, count: int) -> tuple[str, int]:
        """Concatenate verified segment files in order into assembled.bin.

        Returns (sha256, size). Idempotent across crashes: a complete
        assembled.bin already on disk is re-hashed and reused verbatim."""
        final = self.assembled_path(job_id)
        if final.exists():
            h = hashlib.sha256()
            with open(final, "rb") as f:
                for buf in iter(lambda: f.read(COPY_CHUNK), b""):
                    h.update(buf)
            return h.hexdigest(), os.path.getsize(final)

        def write(dst_f):
            for i in range(count):
                with open(self.seg_path(job_id, i), "rb") as sf:
                    shutil.copyfileobj(sf, dst_f, COPY_CHUNK)

        self._atomic_put(final, write)
        h = hashlib.sha256()
        with open(final, "rb") as f:
            for buf in iter(lambda: f.read(COPY_CHUNK), b""):
                h.update(buf)
        return h.hexdigest(), os.path.getsize(final)

    def read_assembled(self, job_id: str) -> bytes:
        with open(self.assembled_path(job_id), "rb") as f:
            return f.read()

    def cleanup_job(self, job_id: str) -> None:
        """Remove all invisible staging output of one job. Idempotent."""
        d = self.job_dir(job_id)
        shutil.rmtree(d, ignore_errors=True)

    def sweep_tmp(self) -> int:
        """Remove abandoned tmp files (a process died mid write)."""
        removed = 0
        for f in self.tmp.glob("copy-*.tmp"):
            try:
                f.unlink()
                removed += 1
            except FileNotFoundError:
                pass
        return removed
