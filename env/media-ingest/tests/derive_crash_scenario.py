"""Subprocess helper for real crash-restart derivation tests.

Driven by tests/test_derive.py with arguments::

    phase1 <point>   seed sources, submit a derivation, install a hard crash
                     hook, then run the job — the process exits with code 37 at
                     one of the three documented crash points:
                       point1  a segment part file is durable, segment not yet
                               marked verified in the DB
                       point2  all segments written and the full concatenation
                               staged, but nothing published yet
                       point3  the result ArchivedVersion row + job='published'
                               committed, billing/protection release not done
    inspect-before   open a fresh service (no resume) and dump state as JSON
    phase2           open a fresh service, resume(force=True), dump state JSON

os._exit mimics a hard crash: no finally/atexit, no rollback — exactly the
committed transactions and fsynced files survive.
"""

from __future__ import annotations

import hashlib
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.archive import ArchiveService
from app.archive.models import ArchivedVersion, ContentBlob
from app.derive import DerivationService
from app.derive.models import (
    DerivationJob,
    DerivationProtection,
    DerivationSegment,
)

DATA_DIR = Path(os.environ["DERIVE_TEST_DIR"])
DB_URL = f"sqlite:///{DATA_DIR}/derive.db"
ARCHIVE_DIR = DATA_DIR / "archive"
DERIVE_DIR = DATA_DIR / "derive"
CRASH_EXIT_CODE = 37

SRC_A = b"AAAABBBBCCCCDDDD"
SRC_B = b"0123456789abcdef"
EXPECTED = SRC_A[2:10] + SRC_B[4:12]

HOOKS = {
    "point1": "derive.after_segment_written",
    "point2": "derive.after_assembled",
    "point3": "derive.before_billing",
}


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def services():
    arch = ArchiveService(DB_URL, str(ARCHIVE_DIR), proof_key=b"k")
    svc = DerivationService(DB_URL, str(DERIVE_DIR), content_dir=str(ARCHIVE_DIR))
    return arch, svc


def seed_and_crash(point: str) -> None:
    arch, svc = services()
    arch.publish_policy("t", retention_seconds=100000)
    arch.seal_version("t", "a", SRC_A)
    arch.seal_version("t", "b", SRC_B)
    recipe = {
        "segments": [
            {"object_id": "a", "version": 1, "range": [2, 10], "sha256": sha(SRC_A[2:10])},
            {"object_id": "b", "version": 1, "range": [4, 12], "sha256": sha(SRC_B[4:12])},
        ]
    }
    job = svc.submit("t", recipe, f"key-{point}", output_object_id="out")
    job_id = job["job_id"]

    hook = HOOKS[point]

    def crash(when: str) -> None:
        if when == hook:
            os._exit(CRASH_EXIT_CODE)

    svc.crash = crash
    worker = "crashing-worker"
    claimed = svc.claim_next(worker, now=1_000_000.0)
    assert claimed == job_id, claimed
    with svc.sf() as s:
        fence = s.get(DerivationJob, job_id).fence
    svc.process_job(job_id, worker, fence)
    raise RuntimeError("phase1 should have crashed")


def _state(arch, svc, job_id: str) -> dict:
    with svc.sf() as s:
        job = s.get(DerivationJob, job_id)
        segments = [
            (seg.index, seg.state)
            for seg in s.query(DerivationSegment).order_by(DerivationSegment.index)
        ]
        open_prots = (
            s.query(DerivationProtection)
            .filter(DerivationProtection.released_at.is_(None))
            .count()
        )
        total_prots = s.query(DerivationProtection).count()
        events = {
            ev: int(n)
            for ev, n in s.execute(
                __import__("sqlalchemy").text(
                    "select event_type, count(*) from derivation_ledger group by event_type"
                )
            ).all()
        }
        reserved, used = svc._occupancy(s, "t")
        result = s.get(ArchivedVersion, ("t", "out", 1))
        blob = None
        if result is not None:
            blob = s.get(ContentBlob, result.content_sha256)

    downloadable = False
    result_sha = None
    if result is not None:
        try:
            data, _ = arch.download("t", "out", 1)
            downloadable = data == EXPECTED
            result_sha = result.content_sha256
        except Exception:
            downloadable = False

    staging_files = sorted(
        str(p.relative_to(DERIVE_DIR))
        for p in (DERIVE_DIR / "staging" / job_id).glob("**/*")
        if p.is_file()
    ) if (DERIVE_DIR / "staging" / job_id).exists() else []

    return {
        "status": job.status,
        "segments": segments,
        "open_protections": int(open_prots),
        "total_protections": int(total_prots),
        "ledger_events": events,
        "reserved": reserved,
        "used": used,
        "result_exists": result is not None,
        "result_download_correct": downloadable,
        "result_sha256": result_sha,
        "blob_refcount": blob.refcount if blob else None,
        "staging_files": staging_files,
        "cancel_requested": job.cancel_requested,
    }


def find_job(svc):
    with svc.sf() as s:
        job = s.query(DerivationJob).one()
        return job.id


def inspect_before() -> dict:
    arch, svc = services()  # NO resume
    return _state(arch, svc, find_job(svc))


def phase2() -> dict:
    arch, svc = services()
    job_id = find_job(svc)
    recovered = svc.resume(force=True, worker_id="recovery")
    out = _state(arch, svc, job_id)
    out["recovery"] = recovered
    return out


def main() -> int:
    action = sys.argv[1]
    if action == "phase1":
        seed_and_crash(sys.argv[2])
        return 0
    if action == "inspect-before":
        print(json.dumps(inspect_before()))
        return 0
    if action == "phase2":
        print(json.dumps(phase2()))
        return 0
    return 2


if __name__ == "__main__":
    sys.exit(main())
