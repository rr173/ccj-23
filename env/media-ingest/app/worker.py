from __future__ import annotations

import logging
import socket
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta

from . import jobs
from .config import Settings
from .db import init_db, make_engine, make_session_factory, utcnow
from .merge import MergeError, run_merge
from .storage import Storage
from .sweeper import sweep_expired

log = logging.getLogger("ingest.worker")


class MergeWorker:
    """Merge process.

    The durable queue (merge_jobs table) is the single source of truth:
      * claim_next does the weighted-fair selection and leases one job per call;
      * a ThreadPoolExecutor runs up to worker_threads merges in parallel, so the
        per-tenant max_parallel_merges caps can actually be exercised;
      * a long merge heartbeats its lease; a crash stops the heart and the lease
        expiry returns the job to the queue for exactly-once reprocessing.
    """

    def __init__(self, settings: Settings, worker_id: str | None = None):
        self.settings = settings
        self.worker_id = worker_id or f"{socket.gethostname()}-{id(self):x}"
        self.engine = make_engine(settings.database_url)
        init_db(self.engine)
        self.sf = make_session_factory(self.engine)
        self.storage = Storage(settings.data_dir)
        self.pool = ThreadPoolExecutor(
            max_workers=settings.worker_threads, thread_name_prefix="merge"
        )
        self.inflight = 0
        self.lock = threading.Lock()
        self.stop = threading.Event()

    # ---------------------------------------------------------------- single job

    def _heartbeat(self, upload_id: str) -> None:
        from . import jobs as _jobs

        until = utcnow() + timedelta(seconds=self.settings.merge_lease_seconds)
        try:
            with self.sf() as s:
                _jobs.heartbeat(s, upload_id, self.worker_id, until)
        except Exception:
            log.exception("heartbeat failed for %s", upload_id)

    def _run_one(self, upload_id: str) -> None:
        try:
            result = run_merge(self.sf, self.storage, upload_id, heartbeat_cb=lambda: self._heartbeat(upload_id))
            log.info("merge %s -> %s", upload_id, result)
            if result.get("status") == "uploading":
                # Chunks vanished/failed re-verification mid-run; back the job off
                # until the client re-uploads, instead of spinning hot.
                with self.sf() as s:
                    jobs.requeue_failed(
                        s, upload_id, result.get("error", "chunks missing"),
                        not_before=utcnow() + timedelta(seconds=self.settings.retry_backoff_seconds),
                    )
                    s.commit()
        except MergeError:
            log.exception("merge %s transient failure; requeue with backoff", upload_id)
            with self.sf() as s:
                jobs.requeue_failed(
                    s, upload_id, "transient merge failure",
                    not_before=utcnow() + timedelta(seconds=self.settings.retry_backoff_seconds),
                )
                s.commit()
        except Exception:
            # Unknown error: be safe, requeue with backoff. The claim/version
            # unique constraints guarantee the merge stays idempotent.
            log.exception("merge %s unexpected error; requeue with backoff", upload_id)
            with self.sf() as s:
                jobs.requeue_failed(
                    s, upload_id, "unexpected worker error",
                    not_before=utcnow() + timedelta(seconds=self.settings.retry_backoff_seconds),
                )
                s.commit()
        finally:
            with self.lock:
                self.inflight -= 1

    # ---------------------------------------------------------------- loop

    def tick(self) -> int:
        """Try to fill free local slots. Returns number of jobs dispatched."""
        dispatched = 0
        while True:
            with self.lock:
                if self.inflight >= self.settings.worker_threads:
                    break
            upload_id = jobs.claim_next(
                self.sf, self.worker_id, lease_seconds=self.settings.merge_lease_seconds
            )
            if upload_id is None:
                break
            with self.lock:
                self.inflight += 1
            self.pool.submit(self._run_one, upload_id)
            dispatched += 1
        return dispatched

    def serve(self) -> None:
        # Startup: a cleanly-stopped process leaves nothing running; a crashed
        # process leaves rows in 'running' whose leases expire and are reclaimed
        # by claim_next itself. Sweep once to release expired reservations.
        expired = sweep_expired(self.sf, self.storage)
        if expired:
            log.info("startup: expired %d uploads: %s", len(expired), expired)
        log.info(
            "worker %s starting: threads=%d lease=%ss tenant caps/weights from DB",
            self.worker_id, self.settings.worker_threads, self.settings.merge_lease_seconds,
        )

        last_sweep = 0.0
        idle_sleep = min(1.0, self.settings.sweep_interval_seconds)
        while not self.stop.is_set():
            try:
                self.tick()
            except Exception:
                log.exception("scheduling tick failed")
                time.sleep(1.0)

            now = time.monotonic()
            if now - last_sweep >= self.settings.sweep_interval_seconds:
                last_sweep = now
                try:
                    expired = sweep_expired(self.sf, self.storage)
                    if expired:
                        log.info("expired %d incomplete uploads: %s", len(expired), expired)
                    self.storage.sweep_tmp(older_than_seconds=3600)
                except Exception:
                    log.exception("sweep failed")

            self.stop.wait(idle_sleep)

        self.pool.shutdown(wait=True)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    MergeWorker(Settings.from_env()).serve()


if __name__ == "__main__":
    main()
