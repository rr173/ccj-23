"""Background derivation worker process.

Shares the metadata database and data volume with the API (same model as the
merge worker). Run independently and scale horizontally::

    python -m app.derive.worker
    python -m app.derive.worker --once        # claim+process one job, then exit
    python -m app.derive.worker --recover     # force-resume crashed work, exit

Only one worker ever advances a given job: claim takes a lease and bumps a
fence token; all later state transitions are conditional on that token. A
crashed worker stops heartbeating, its lease expires and another worker (or
``--recover``) resumes from durable per-segment states without rewriting
verified bytes or double billing.
"""

from __future__ import annotations

import argparse
import logging
import socket
import threading

from ..config import Settings
from ..db import init_db, make_engine
from .service import DerivationService

log = logging.getLogger("derive.worker")


class DerivationWorker:
    def __init__(self, settings: Settings, worker_id: str | None = None):
        self.settings = settings
        self.worker_id = worker_id or f"{socket.gethostname()}-{id(self):x}"
        self.engine = make_engine(settings.database_url)
        init_db(self.engine)
        self.svc = DerivationService(
            settings.database_url,
            f"{settings.data_dir}/derive",
            content_dir=f"{settings.data_dir}/archive",
            default_capacity_bytes=settings.default_capacity_bytes,
            lease_seconds=settings.derive_lease_seconds,
            init_schema=False,
        )
        self.stop = threading.Event()

    def tick(self) -> bool:
        # Reclaim crashed owners' expired leases first.
        self.svc.reclaim_stale()
        job_id = self.svc.claim_next(self.worker_id)
        if job_id is None:
            return False
        with self.svc.sf() as s:
            from . import models as dm

            job = s.get(dm.DerivationJob, job_id)
            fence = job.fence
        log.info("worker %s claimed job %s (fence=%d)", self.worker_id, job_id, fence)
        result = self.svc.process_job(job_id, self.worker_id, fence)
        log.info("job %s -> %s", job_id, result)
        return True

    def serve(self) -> None:
        idle = 0.5
        log.info(
            "derivation worker %s starting (lease=%ss, data=%s)",
            self.worker_id, self.settings.derive_lease_seconds, self.settings.data_dir,
        )
        while not self.stop.is_set():
            try:
                worked = self.tick()
            except Exception:
                log.exception("derivation tick failed")
                worked = False
            if not worked:
                self.stop.wait(idle)

    def run_once(self) -> bool:
        self.svc.reclaim_stale()
        job_id = self.svc.claim_next(self.worker_id)
        if job_id is None:
            return False
        from . import models as dm

        with self.svc.sf() as s:
            fence = s.get(dm.DerivationJob, job_id).fence
        self.svc.process_job(job_id, self.worker_id, fence)
        return True

    def recover(self) -> dict:
        """Single-process deployment recovery: expire leases and finish all
        unfinished jobs, including point-1/2/3 crash windows."""
        return self.svc.resume(force=True, worker_id=self.worker_id)


def main() -> None:
    parser = argparse.ArgumentParser(description="derivation worker")
    parser.add_argument("--once", action="store_true", help="process one job and exit")
    parser.add_argument("--recover", action="store_true", help="force-resume and exit")
    parser.add_argument("--id", default=None, help="worker id override")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    w = DerivationWorker(Settings.from_env(), worker_id=args.id)
    if args.recover:
        print(w.recover())
        return
    if args.once:
        w.run_once()
        return
    try:
        w.serve()
    except KeyboardInterrupt:
        w.stop.set()


if __name__ == "__main__":
    main()
