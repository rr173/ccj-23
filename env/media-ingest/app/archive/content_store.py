"""Content-addressed physical storage for the archive.

Blobs are stored once under their SHA-256 in a two-level sharded layout::

    <root>/content/ab/<sha256>

Publishing is crash-safe: bytes are first written to ``tmp/`` and fsynced, then
``os.replace``d into the final path (same volume => atomic). A blob file exists
if and only if its ``content_blobs`` row is ``active`` or ``pending_purge``.
"""

from __future__ import annotations

import hashlib
import os
import uuid
from pathlib import Path


class ContentStore:
    def __init__(self, root: str | Path):
        self.root = Path(root)
        self.content = self.root / "content"
        self.tmp = self.root / "tmp"
        for d in (self.content, self.tmp):
            d.mkdir(parents=True, exist_ok=True)

    def blob_path(self, sha256: str) -> Path:
        return self.content / sha256[:2] / sha256

    def new_tmp(self) -> Path:
        return self.tmp / f"blob-{uuid.uuid4().hex}.tmp"

    @staticmethod
    def _fsync_dir(path: Path) -> None:
        fd = os.open(path, os.O_DIRECTORY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)

    def put(self, data: bytes) -> tuple[str, int, Path]:
        """Hash, persist (if absent) and return (sha256, size, final path).

        If the final path already exists the temporary copy is discarded: content
        is addressed by hash, so identical bytes are the same blob.
        """
        sha256 = hashlib.sha256(data).hexdigest()
        dst = self.blob_path(sha256)
        if dst.exists():
            return sha256, len(data), dst
        tmp = self.new_tmp()
        with open(tmp, "wb") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
        dst.parent.mkdir(parents=True, exist_ok=True)
        os.replace(tmp, dst)
        self._fsync_dir(dst.parent)
        return sha256, len(data), dst

    def read(self, sha256: str) -> bytes:
        with open(self.blob_path(sha256), "rb") as f:
            return f.read()

    def path_for(self, sha256: str) -> Path:
        """Absolute path of a stored blob. Used by the derivation worker to
        stream byte ranges directly out of the immutable content tree."""
        return self.blob_path(sha256)

    def open(self, sha256: str):
        return open(self.blob_path(sha256), "rb")

    def hash_range(self, sha256: str, start: int, end: int) -> tuple[str, int]:
        """Hash bytes [start, end) of a blob without loading it all. Raises
        FileNotFoundError like read() when the physical copy is gone."""
        import hashlib

        h = hashlib.sha256()
        size = 0
        with open(self.blob_path(sha256), "rb") as f:
            f.seek(start)
            remaining = end - start
            while remaining > 0:
                buf = f.read(min(1024 * 1024, remaining))
                if not buf:
                    break
                h.update(buf)
                size += len(buf)
                remaining -= len(buf)
        return h.hexdigest(), size

    def exists(self, sha256: str) -> bool:
        return self.blob_path(sha256).exists()

    def purge(self, sha256: str) -> bool:
        """Unlink the physical blob. Idempotent: returns False if already gone."""
        path = self.blob_path(sha256)
        try:
            path.unlink()
        except FileNotFoundError:
            return False
        self._fsync_dir(path.parent)
        return True
