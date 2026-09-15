from __future__ import annotations

import hashlib
import logging
import os

from sqlalchemy import select, update
from sqlalchemy.exc import IntegrityError

from .db import Chunk, Upload, Version, new_id
from .storage import Storage

log = logging.getLogger("ingest.merge")

MERGEABLE = ("uploading", "merging", "failed")


class MergeError(Exception):
    """Transient failure (e.g. object store temporarily unwritable) — safe to retry."""


def _claim(session, upload_id: str) -> bool:
    """Flip the upload into 'merging' iff it has no version yet. Returns False when
    a previous merge already sealed it — retries are then a no-op."""
    res = session.execute(
        update(Upload)
        .where(Upload.id == upload_id, Upload.status.in_(MERGEABLE), Upload.version_id.is_(None))
        .values(status="merging", error=None)
    )
    session.commit()
    return res.rowcount > 0


def _reset_to_uploading(session_factory, upload_id: str, error: str) -> None:
    with session_factory() as s:
        s.execute(update(Upload).where(Upload.id == upload_id).values(status="uploading", error=error))
        s.commit()


def run_merge(session_factory, storage: Storage, upload_id: str) -> dict:
    """Merge all stored chunks of `upload_id` into one sealed, immutable version.

    Exactly-once: the conditional claim plus the versions.upload_id UNIQUE
    constraint guarantee that any number of retries or concurrent runs produce
    at most one Version row. A crash mid-merge leaves only files under tmp/
    (never visible to clients) and status='merging', which the worker re-enqueues.
    """
    with session_factory() as s:
        upload = s.get(Upload, upload_id)
        if upload is None:
            return {"status": "gone"}
        if upload.status == "sealed" and upload.version_id:
            return {"status": "sealed", "version_id": upload.version_id}
        if not _claim(s, upload_id):
            s.expire(upload)
            upload = s.get(Upload, upload_id)
            return {"status": upload.status, "version_id": upload.version_id}
        chunks = (
            s.execute(select(Chunk).where(Chunk.upload_id == upload_id).order_by(Chunk.index))
            .scalars()
            .all()
        )
        total_chunks = upload.total_chunks
        expected_sha256 = upload.expected_sha256

    stored = [c for c in chunks if c.state == "stored"]
    if len(stored) != total_chunks:
        _reset_to_uploading(session_factory, upload_id, "chunks missing at merge time")
        return {"status": "uploading", "error": "chunks missing"}

    tmp = storage.new_tmp("merge")
    digest = hashlib.sha256()
    size = 0
    try:
        with open(tmp, "wb") as out:
            for c in stored:
                h = hashlib.sha256()
                got = 0
                with open(storage.chunk_path(upload_id, c.index), "rb") as f:
                    while True:
                        buf = f.read(1024 * 1024)
                        if not buf:
                            break
                        h.update(buf)
                        digest.update(buf)
                        out.write(buf)
                        got += len(buf)
                if h.hexdigest() != c.sha256 or got != c.size:
                    # Chunk file corrupted after it was validated — make the
                    # client re-send this index instead of sealing bad bytes.
                    tmp.unlink(missing_ok=True)
                    storage.chunk_path(upload_id, c.index).unlink(missing_ok=True)
                    with session_factory() as s:
                        s.execute(
                            update(Chunk)
                            .where(Chunk.upload_id == upload_id, Chunk.index == c.index)
                            .values(state="failed", sha256=None, size=0, failures=Chunk.failures + 1)
                        )
                        s.commit()
                    _reset_to_uploading(
                        session_factory, upload_id, f"chunk {c.index} failed re-verification"
                    )
                    return {"status": "uploading", "error": f"chunk {c.index} re-verification failed"}
                size += got
            out.flush()
            os.fsync(out.fileno())
    except Exception:
        tmp.unlink(missing_ok=True)
        raise  # status stays 'merging'; nothing client-visible exists yet

    final_sha = digest.hexdigest()
    if expected_sha256 and final_sha != expected_sha256:
        tmp.unlink(missing_ok=True)
        with session_factory() as s:
            s.execute(
                update(Upload)
                .where(Upload.id == upload_id)
                .values(status="failed", error=f"final digest {final_sha} != expected {expected_sha256}")
            )
            s.commit()
        return {"status": "failed", "sha256": final_sha}

    version_id = new_id()
    manifest = {
        "upload_id": upload_id,
        "version_id": version_id,
        "sha256": final_sha,
        "size": size,
        "chunks": [{"index": c.index, "sha256": c.sha256, "size": c.size} for c in stored],
    }
    try:
        path = storage.seal_object(tmp, version_id, manifest)
    except Exception as exc:
        tmp.unlink(missing_ok=True)
        raise MergeError("object store not writable") from exc

    # Commit the version row; UNIQUE(upload_id) collapses concurrent winners.
    with session_factory() as s:
        existing = s.scalar(select(Version).where(Version.upload_id == upload_id))
        if existing is None:
            s.add(Version(id=version_id, upload_id=upload_id, sha256=final_sha, size=size, path=str(path)))
            try:
                s.commit()
            except IntegrityError:
                s.rollback()
                existing = s.scalar(select(Version).where(Version.upload_id == upload_id))
        if existing is not None:
            # Lost a concurrent race: discard our object file, adopt the winner.
            storage.remove_object(version_id)
            version_id = existing.id
            final_sha = existing.sha256
            size = existing.size
        s.execute(
            update(Upload)
            .where(Upload.id == upload_id)
            .values(status="sealed", version_id=version_id, error=None)
        )
        s.commit()

    storage.remove_staging(upload_id)
    log.info("sealed upload %s as version %s (sha256=%s, %d bytes)", upload_id, version_id, final_sha, size)
    return {"status": "sealed", "version_id": version_id, "sha256": final_sha, "size": size}
