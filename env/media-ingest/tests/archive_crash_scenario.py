"""Subprocess helper for real crash-restart tests.

Driven by tests/test_archive.py with arguments::

    <kind> phase1            seed data, then os._exit(17) at the crash point
    <kind> inspect-before    print pre-recovery state as JSON (no resume)
    <kind> phase2            open a fresh service, resume(), print post-state JSON

kind = "pending": two versions share one blob; crash right after the logical
delete of version 1 is durable (the blob must be retained for version 2).

kind = "purging": version 1 is a sole reference; crash while the blob is in
'purging' (claimed for unlink, the physical unlink may have happened or not).

os._exit mimics a hard crash: no finally blocks, no atexit, no transaction rollback
— everything committed up to the crash point is exactly what survives.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

# Make `app` importable when invoked by path as a subprocess.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.archive import ArchiveService, NotFound
from app.archive.models import ContentBlob, DeletionCertificate

DATA_DIR = Path(os.environ["ARCHIVE_TEST_DIR"])
DB_URL = f"sqlite:///{DATA_DIR}/archive.db"
STORE_DIR = DATA_DIR / "store"

CRASH_EXIT_CODE = 17
SHARED_CONTENT = b"crash-shared-content"


class Clock:
    """Deterministic clock: retention window elapsed before any delete."""

    def __init__(self):
        self.t = 1_000_000.0

    def __call__(self):
        return self.t

    def advance(self, seconds: float) -> None:
        self.t += seconds


def service():
    return ArchiveService(DB_URL, str(STORE_DIR), proof_key=b"test-key", clock=Clock())


def crash(_when):
    # Hard process exit, bypassing cleanup.
    os._exit(CRASH_EXIT_CODE)


def seed(kind: str) -> None:
    svc = service()
    svc.publish_policy("t", retention_seconds=10)
    if kind == "pending":
        v1 = svc.seal_version("t", "obj", SHARED_CONTENT)
        v2 = svc.seal_version("t", "obj", SHARED_CONTENT)
        assert v1["content_sha256"] == v2["content_sha256"]
    else:  # purging: sole physical reference
        v1 = svc.seal_version("t", "solo", SHARED_CONTENT)
    svc.clock.advance(50)

    if kind == "pending":
        # Crash once, immediately after the logical-delete commit.
        svc.crash = lambda when: crash(when) if when == "delete.after_refs_released" else None
        svc.delete_version("t", "obj", 1)
    else:
        # Crash after the blob was claimed ('purging') but before confirmation.
        svc.crash = lambda when: crash(when) if when == "delete.after_physical_unlink" else None
        svc.delete_version("t", "solo", 1)
    raise RuntimeError("phase1 should have crashed")


def state(svc, object_id: str) -> dict:
    info = svc.get_version("t", object_id, 1)
    other_ok = False
    if object_id == "obj":
        try:
            svc.download("t", "obj", 2)
            other_ok = True
        except NotFound:
            other_ok = False
    target_ok = True
    try:
        svc.download("t", object_id, 1)
    except NotFound:
        target_ok = False
    with svc.sf() as s:
        b = s.get(ContentBlob, info["content_sha256"])
        blob_state = b.state
        refcount = b.refcount
        cert_count = s.query(DeletionCertificate).count()
    return {
        "target_state": info["state"],
        "target_download_ok": target_ok,
        "other_download_ok": other_ok,
        "blob_state": blob_state,
        "blob_refcount": refcount,
        "file_exists": svc.store.exists(info["content_sha256"]),
        "cert_count": cert_count,
        "content_sha256": info["content_sha256"],
    }


def inspect_before(kind: str) -> dict:
    svc = service()  # NO resume()
    object_id = "obj" if kind == "pending" else "solo"
    return state(svc, object_id)


def phase2(kind: str) -> dict:
    svc = service()
    recovered = svc.resume()
    object_id = "obj" if kind == "pending" else "solo"
    out = state(svc, object_id)
    out["recovery"] = recovered
    cert = svc.get_certificate_for_object("t", object_id, 1)
    out["cert_physical_result"] = cert["payload"]["physical_cleanup"]["result"]
    out["chain_valid"] = svc.verify_chain()["valid"]
    return out


def main() -> int:
    kind = sys.argv[1]
    action = sys.argv[2]
    if action == "phase1":
        seed(kind)
        return 0
    if action == "inspect-before":
        print(json.dumps(inspect_before(kind)))
        return 0
    if action == "phase2":
        print(json.dumps(phase2(kind)))
        return 0
    return 2


if __name__ == "__main__":
    sys.exit(main())
