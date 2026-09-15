from __future__ import annotations

import hashlib
import logging
import os
import uuid

from sqlalchemy import select, update

from . import accounting
from .db import Chunk, MergeJob, Upload, Version, utcnow
from .storage import Storage

log = logging.getLogger("ingest.merge")

MERGEABLE = ("uploading", "merging", "failed", "queued")


class MergeError(Exception):
    """Transient failure (e.g. object store temporarily unwritable) — safe to retry."""


def deterministic_version_id(upload_id: str) -> str:
    """Same upload always seals to the same object path: a crash between
    seal_object() and the DB commit leaves an object the retry adopts instead
    of duplicating."""
    return uuid.uuid5(uuid.NAMESPACE_URL, f"media-ingest:upload:{upload_id}").hex


def _claim(session, upload_id: str) -> bool:
    """Flip the upload into 'merging' iff it has no version yet. Returns False when
    a previous merge already sealed it — retries are then a no-op.

    A status of 'merging' matches too: that is the exact state left by a crash
    between seal_object() and the final commit, so the retried run must be able
    to adopt the orphan object and finish."""
    res = session.execute(
        update(Upload)
        .where(Upload.id == upload_id, Upload.status.in_(MERGEABLE), Upload.version_id.is_(None))
        .values(status="merging", error=None)
    )
    return res.rowcount > 0


def _reset_to_uploading(session_factory, upload_id: str, error: str) -> None:
    with session_factory() as s:
        s.execute(update(Upload).where(Upload.id == upload_id).values(status="uploading", error=error))
        s.commit()


def run_merge(session_factory, storage: Storage, upload_id: str,
              heartbeat_cb=None) -> dict:
    """Merge all stored chunks of `upload_id` into one sealed, immutable version.

    Exactly-once across retries and crashes:
      * claim is conditional (status mergeable AND version_id NULL);
      * the sealed object path is deterministic per upload, so a retry adopts an
        orphan object left by a crash instead of creating a second one;
      * the Version row, upload status flip and capacity ledger conversion
        (reserve -> used) all commit in ONE transaction — a crash before it
        leaves zero effects and the requeued job simply retries; a crash after it
        is a no-op replay.
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
        s.commit()
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
                if heartbeat_cb is not None:
                    heartbeat_cb()  # prove liveness to the lease reaper
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
            from . import jobs
            jobs.mark_done(
                s, upload_id, result="failed",
                error=f"final digest {final_sha} != expected {expected_sha256}",
            )
            s.commit()
        return {"status": "failed", "sha256": final_sha}

    version_id = deterministic_version_id(upload_id)
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

    # One atomic commit: version row + upload flip + reservation -> used
    # conversion + job completion. UNIQUE(versions.upload_id) and
    # UNIQUE(ledger upload_id+event_type) collapse any concurrent winner.
    with session_factory() as s:
        version = s.scalar(select(Version).where(Version.upload_id == upload_id))
        upload = s.get(Upload, upload_id)
        converted = False
        if version is None:
            s.add(
                Version(
                    id=version_id,
                    upload_id=upload_id,
                    tenant_id=upload.tenant_id,
                    sha256=final_sha,
                    size=size,
                    path=str(path),
                )
            )
            converted = accounting.commit_used(s, upload)
        else:
            version_id = version.id
            final_sha = version.sha256
            size = version.size
        flip = s.execute(
            update(Upload)
            .where(
                Upload.id == upload_id,
                Upload.version_id.is_(None),
                Upload.status.in_(MERGEABLE),
            )
            .values(status="sealed", version_id=version_id, error=None)
        )
        if flip.rowcount == 0:
            # Defensive: abort/expiry never targets an active merge (see the
            # sweeper and DELETE handler), so this should be unreachable. Drop
            # this transaction's writes and report the terminal state.
            s.rollback()
            with session_factory() as s2:
                u2 = s2.get(Upload, upload_id)
                return {"status": u2.status if u2 else "gone"}
        s.execute(
            update(MergeJob)
            .where(MergeJob.upload_id == upload_id)
            .values(
                status="done",
                result="sealed",
                error=None,
                finished_at=utcnow(),
                lease_owner=None,
                lease_expires_at=None,
            )
        )
        s.commit()

    storage.remove_staging(upload_id)
    log.info(
        "sealed upload %s as version %s (sha256=%s, %d bytes, ledger_converted=%s)",
        upload_id, version_id, final_sha, size, converted,
    )
    return {"status": "sealed", "version_id": version_id, "sha256": final_sha, "size": size}
