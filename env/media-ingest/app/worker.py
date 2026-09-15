from __future__ import annotations

import logging
import time

from .config import Settings
from .db import init_db, make_engine, make_session_factory
from .merge import run_merge
from .queue import MergeQueue
from .storage import Storage
from .sweeper import requeue_stale_merges, sweep_expired

log = logging.getLogger("ingest.worker")


def serve(settings: Settings) -> None:
    engine = make_engine(settings.database_url)
    init_db(engine)
    sf = make_session_factory(engine)
    storage = Storage(settings.data_dir)
    queue = MergeQueue(settings.redis_url)

    recovered = queue.recover()
    requeued = requeue_stale_merges(sf, queue, stale_seconds=0)
    log.info("startup: recovered %d in-flight jobs, requeued %d stuck merges", recovered, requeued)

    last_sweep = 0.0
    while True:
        upload_id = queue.dequeue(timeout=5)
        if upload_id is not None:
            if queue.acquire_merge_lock(upload_id, settings.merge_lock_seconds):
                try:
                    result = run_merge(sf, storage, upload_id)
                    log.info("merge %s -> %s", upload_id, result)
                    queue.ack(upload_id)
                except Exception:
                    log.exception("merge %s failed (transient?); requeueing", upload_id)
                    queue.requeue(upload_id)
                    time.sleep(1.0)  # backoff for e.g. temporarily unwritable storage
                finally:
                    queue.release_merge_lock(upload_id)
            else:
                # Another worker holds the lock; its retry/stale-requeue covers this job.
                queue.ack(upload_id)

        now = time.monotonic()
        if now - last_sweep >= settings.sweep_interval_seconds:
            last_sweep = now
            expired = sweep_expired(sf, storage)
            if expired:
                log.info("expired %d incomplete uploads: %s", len(expired), expired)
            requeue_stale_merges(sf, queue, settings.stale_merge_seconds)
            storage.sweep_tmp(older_than_seconds=3600)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    serve(Settings.from_env())


if __name__ == "__main__":
    main()
