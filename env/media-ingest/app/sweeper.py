from __future__ import annotations

import logging
from datetime import datetime

from sqlalchemy import delete, select

from . import accounting, jobs
from .db import Chunk, MergeJob, Upload, utcnow
from .storage import Storage

log = logging.getLogger("ingest.sweeper")


def sweep_expired(session_factory, storage: Storage, now: datetime | None = None) -> list[str]:
    """Expire uploads that never completed before their TTL. Removes chunk rows,
    staging files and any still-queued job, and releases the byte reservation
    exactly once. Sealed uploads are never expired and never release (their
    bytes are now permanent 'used' capacity)."""
    now = now or utcnow()
    expired: list[str] = []
    with session_factory() as s:
        running = select(MergeJob.upload_id).where(MergeJob.status == jobs.RUNNING)
        rows = (
            s.execute(
                select(Upload).where(
                    Upload.status.in_(("uploading", "failed", "queued")),
                    Upload.expires_at < now,
                    Upload.id.not_in(running),
                )
            )
            .scalars()
            .all()
        )
        for u in rows:
            released = accounting.release_once(s, u)
            if released:
                u.status = "expired"
                u.error = "upload TTL exceeded before completion"
                jobs.cancel_queued(s, u.id)
                s.execute(delete(Chunk).where(Chunk.upload_id == u.id))
                expired.append(u.id)
        s.commit()
    for uid in expired:
        storage.remove_staging(uid)
    return expired
