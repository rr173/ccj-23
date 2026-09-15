from __future__ import annotations

import logging
from datetime import datetime, timedelta

from sqlalchemy import delete, select

from .db import Chunk, Upload, utcnow
from .storage import Storage

log = logging.getLogger("ingest.sweeper")


def sweep_expired(session_factory, storage: Storage, now: datetime | None = None) -> list[str]:
    """Expire uploads that never completed before their TTL. Removes chunk rows
    and staging files; keeps the upload row so clients can see status=expired."""
    now = now or utcnow()
    with session_factory() as s:
        rows = (
            s.execute(
                select(Upload).where(
                    Upload.status.in_(("uploading", "failed")),
                    Upload.expires_at < now,
                )
            )
            .scalars()
            .all()
        )
        expired = [u.id for u in rows]
        for u in rows:
            u.status = "expired"
            u.error = "upload TTL exceeded before completion"
            s.execute(delete(Chunk).where(Chunk.upload_id == u.id))
        s.commit()
    for uid in expired:
        storage.remove_staging(uid)
    return expired


def requeue_stale_merges(session_factory, queue, stale_seconds: int) -> int:
    """Re-enqueue uploads stuck in 'merging' (worker crashed after claiming).
    Duplicates are harmless — run_merge is idempotent and lock-guarded."""
    cutoff = utcnow() - timedelta(seconds=stale_seconds)
    with session_factory() as s:
        ids = (
            s.execute(select(Upload.id).where(Upload.status == "merging", Upload.updated_at < cutoff))
            .scalars()
            .all()
        )
    for uid in ids:
        queue.enqueue(uid)
    return len(ids)
