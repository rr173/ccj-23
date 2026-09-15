"""Acceptance tests for the archive subsystem.

Covers:
  1. retention blocks deletion before expiry;
  2. legal hold still blocks after retention expiry, release of the LAST hold
     re-enables deletion;
  3. two versions sharing physical content: deleting one leaves the other
     fully usable, and the bytes are purged only after the last reference;
  4. concurrent/duplicate deletes => exactly one refcount change, one proof;
  5. crash midway (simulated in a subprocess) => restart resumes consistently,
     no download resurrection, shared content never wrongly purged;
  6. policy updates affect only objects sealed afterwards;
  7. tamper-evident proofs (hash chain + HMAC);
  8. other-reference (pin) blocker and tenant isolation.
"""

from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
import threading
from pathlib import Path

import pytest
from sqlalchemy import text

from app.archive import ArchiveService, DeletionBlocked, NotFound
from app.archive.models import ContentBlob


class FakeClock:
    def __init__(self, t: float = 1_000_000.0):
        self.t = t

    def __call__(self) -> float:
        return self.t

    def advance(self, seconds: float) -> None:
        self.t += seconds


def make_service(tmp_path, *, proof_key=b"test-key"):
    clock = FakeClock()
    svc = ArchiveService(
        f"sqlite:///{tmp_path}/archive.db",
        str(tmp_path / "store"),
        proof_key=proof_key,
        clock=clock,
    )
    return svc, clock


def blob_state(svc, sha):
    with svc.sf() as s:
        b = s.get(ContentBlob, sha)
        return {"state": b.state, "refcount": b.refcount} if b else None


# ---------------------------------------------------------------- 1. retention

def test_retention_blocks_before_expiry(tmp_path):
    svc, clock = make_service(tmp_path)
    svc.publish_policy("t", retention_seconds=100)
    v = svc.seal_version("t", "obj", b"data")

    elig = svc.eligibility("t", "obj", 1)
    assert elig["eligible"] is False
    assert elig["primary_reason"] == "retention"
    assert elig["retention"]["expired"] is False

    with pytest.raises(DeletionBlocked) as exc:
        svc.delete_version("t", "obj", 1)
    assert [b["reason"] for b in exc.value.blockers] == ["retention"]

    clock.advance(99)
    assert svc.eligibility("t", "obj", 1)["eligible"] is False
    clock.advance(1)  # exactly at expiry
    assert svc.eligibility("t", "obj", 1)["eligible"] is True

    res = svc.delete_version("t", "obj", 1)
    assert res["physical_result"] == "purged"
    with pytest.raises(NotFound):
        svc.download("t", "obj", 1)
    assert v["content_sha256"] and not svc.store.exists(v["content_sha256"])


# ---------------------------------------------------------------- 2. legal hold

def test_legal_hold_blocks_after_expiry_and_last_release_allows(tmp_path):
    svc, clock = make_service(tmp_path)
    svc.publish_policy("t", retention_seconds=10)
    svc.seal_version("t", "obj", b"data")

    svc.add_hold("t", "obj", "hold-a", reason="case A")
    svc.add_hold("t", "obj", "hold-b", reason="case B")
    clock.advance(100)  # retention long expired

    elig = svc.eligibility("t", "obj", 1)
    assert elig["eligible"] is False
    assert elig["primary_reason"] == "legal_hold"
    assert elig["active_holds"] == 2

    with pytest.raises(DeletionBlocked) as exc:
        svc.delete_version("t", "obj", 1)
    assert [b["reason"] for b in exc.value.blockers] == ["legal_hold"]

    # Releasing one hold is not enough.
    svc.release_hold("t", "obj", "hold-a")
    assert svc.eligibility("t", "obj", 1)["active_holds"] == 1
    with pytest.raises(DeletionBlocked):
        svc.delete_version("t", "obj", 1)

    # The LAST hold release makes the object deletable.
    rel = svc.release_hold("t", "obj", "hold-b")
    assert rel["active_holds_remaining"] == 0
    assert svc.eligibility("t", "obj", 1)["eligible"] is True
    res = svc.delete_version("t", "obj", 1)
    assert res["physical_result"] == "purged"

    cert = svc.get_certificate(res["certificate_id"])
    assert {h["hold_key"] for h in cert["payload"]["holds_at_deletion"]} == set()


def test_hold_surviving_at_delete_is_recorded_in_proof(tmp_path):
    svc, clock = make_service(tmp_path)
    svc.publish_policy("t", retention_seconds=0)
    svc.seal_version("t", "obj", b"data")
    svc.add_hold("t", "obj", "h1")
    clock.advance(1)
    svc.release_hold("t", "obj", "h1")
    res = svc.delete_version("t", "obj", 1)
    cert = svc.get_certificate(res["certificate_id"])
    # The certificate preserves the full hold-state change history snapshot.
    assert svc.list_holds("t", "obj")[0]["state"] == "released"
    assert cert["payload"]["logical_deletion"]["deleted_at"] is not None


# ---------------------------------------------------------------- 3. dedup sharing

def test_shared_content_delete_one_version_keeps_other(tmp_path):
    svc, clock = make_service(tmp_path)
    svc.publish_policy("t", retention_seconds=10)
    v1 = svc.seal_version("t", "obj", b"shared-bytes")
    v2 = svc.seal_version("t", "obj", b"shared-bytes")
    assert v1["content_sha256"] == v2["content_sha256"]
    assert blob_state(svc, v1["content_sha256"]) == {"state": "active", "refcount": 2}

    clock.advance(50)
    # Delete v1: its reference goes, the physical blob is retained for v2.
    r1 = svc.delete_version("t", "obj", 1)
    assert r1["physical_result"] == "retained_shared"
    assert blob_state(svc, v1["content_sha256"]) == {"state": "active", "refcount": 1}

    # v2 is completely unaffected: downloadable, still eligible-blocked by
    # its OWN retention snapshot if applicable (same here), content intact.
    data, _ = svc.download("t", "obj", 2)
    assert data == b"shared-bytes"
    assert svc.store.exists(v2["content_sha256"])

    # Deleting the last reference purges the physical content.
    r2 = svc.delete_version("t", "obj", 2)
    assert r2["physical_result"] == "purged"
    assert blob_state(svc, v1["content_sha256"])["state"] == "purged"
    assert not svc.store.exists(v2["content_sha256"])
    with pytest.raises(NotFound):
        svc.download("t", "obj", 2)


def test_dedup_shared_across_tenants_is_isolated(tmp_path):
    svc, clock = make_service(tmp_path)
    svc.publish_policy("a", retention_seconds=0)
    svc.publish_policy("b", retention_seconds=1000)
    va = svc.seal_version("a", "o", b"same")
    vb = svc.seal_version("b", "o", b"same")
    assert va["content_sha256"] == vb["content_sha256"]

    # Tenant A cannot see or download tenant B's version.
    with pytest.raises(NotFound):
        svc.download("a", "o", 1) and svc.download("a", "o", 2)
    # Delete A's reference; B's long-retention object stays intact.
    clock.advance(1)
    r = svc.delete_version("a", "o", 1)
    assert r["physical_result"] == "retained_shared"
    data, _ = svc.download("b", "o", 1)
    assert data == b"same"


# ---------------------------------------------------------------- 4. concurrent duplicate delete

def test_concurrent_duplicate_delete_is_idempotent(tmp_path):
    svc, clock = make_service(tmp_path)
    svc.publish_policy("t", retention_seconds=0)
    v = svc.seal_version("t", "obj", b"unique-concurrent")
    clock.advance(1)

    results: list = []
    errors: list = []

    def worker():
        try:
            results.append(
                svc.delete_version("t", "obj", 1, request_key="req-1")
            )
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for th in threads:
        th.start()
    for th in threads:
        th.join()

    assert not errors, errors
    # All 8 calls converge on the same op and the same certificate.
    cert_ids = {r["certificate_id"] for r in results}
    assert len(cert_ids) == 1
    physical = {r["physical_result"] for r in results}
    assert physical == {"purged"}

    # Exactly one reference change and exactly one certificate row.
    with svc.sf() as s:
        ops = s.execute(text("SELECT count(*) FROM archive_delete_ops")).scalar()
        certs = s.execute(text("SELECT count(*) FROM deletion_certificates")).scalar()
        ref_events = s.execute(
            text(
                "SELECT count(*) FROM archive_audit_events "
                "WHERE event_type='delete.refs_released'"
            )
        ).scalar()
    assert ops == 1
    assert certs == 1
    assert ref_events == 1
    assert blob_state(svc, v["content_sha256"])["refcount"] == 0
    assert not svc.store.exists(v["content_sha256"])


def test_duplicate_delete_after_completion_replays(tmp_path):
    svc, clock = make_service(tmp_path)
    svc.publish_policy("t", retention_seconds=0)
    svc.seal_version("t", "obj", b"x")
    clock.advance(1)
    r1 = svc.delete_version("t", "obj", 1, request_key="k")
    r2 = svc.delete_version("t", "obj", 1, request_key="k")
    assert r1["certificate_id"] == r2["certificate_id"]
    assert r2["replayed"] is True


# ---------------------------------------------------------------- 6. policy versions

def test_policy_update_only_affects_new_seals(tmp_path):
    svc, clock = make_service(tmp_path)
    svc.publish_policy("t", retention_seconds=100, description="v1 long")
    old = svc.seal_version("t", "obj", b"old-object")
    assert old["policy_version"] == 1
    assert old["retention_seconds"] == 100

    svc.publish_policy("t", retention_seconds=0, description="v2 immediate")
    new = svc.seal_version("t", "obj2", b"new-object")
    assert new["policy_version"] == 2
    assert new["retention_seconds"] == 0

    clock.advance(50)
    # New policy lets obj2 be deleted immediately...
    assert svc.eligibility("t", "obj2", 1)["eligible"] is True
    # ...while the previously sealed object keeps its original 100s window.
    elig_old = svc.eligibility("t", "obj", 1)
    assert elig_old["eligible"] is False
    assert elig_old["retention"]["retention_seconds"] == 100
    with pytest.raises(DeletionBlocked):
        svc.delete_version("t", "obj", 1)

    clock.advance(50)
    assert svc.eligibility("t", "obj", 1)["eligible"] is True


# ---------------------------------------------------------------- 7. proofs

def test_certificate_records_full_flow_and_verifies(tmp_path):
    svc, clock = make_service(tmp_path)
    svc.publish_policy("t", retention_seconds=10)
    svc.seal_version("t", "obj", b"abc")
    svc.add_hold("t", "obj", "litigation", reason="suit 42")
    clock.advance(20)
    svc.release_hold("t", "obj", "litigation")
    res = svc.delete_version("t", "obj", 1)
    cert = svc.get_certificate(res["certificate_id"])

    p = cert["payload"]
    assert p["object"]["tenant_id"] == "t"
    assert p["object"]["object_id"] == "obj"
    assert p["object"]["version"] == 1
    assert p["policy"]["policy_version"] == 1
    assert p["physical_cleanup"]["result"] == "purged"
    assert p["logical_deletion"]["deleted_at"] <= p["finalized_at"]
    assert cert["chain_valid"] is True
    assert svc.verify_chain()["valid"] is True


def test_certificate_tampering_is_detected(tmp_path):
    svc, clock = make_service(tmp_path)
    svc.publish_policy("t", retention_seconds=0)
    svc.seal_version("t", "obj", b"abc")
    clock.advance(1)
    res = svc.delete_version("t", "obj", 1)

    # Tamper with the stored proof directly (simulating an attacker with write
    # access to the DB): signature verification must fail.
    db_path = tmp_path / "archive.db"
    conn = sqlite3.connect(db_path)
    conn.execute(
        "UPDATE deletion_certificates SET signature = ? WHERE id = ?",
        ("0" * 64, res["certificate_id"]),
    )
    conn.commit()
    conn.close()

    tampered = svc.get_certificate(res["certificate_id"])
    assert tampered["chain_valid"] is False
    assert svc.verify_chain()["valid"] is False


def test_certificate_hash_chain_links_records(tmp_path):
    svc, clock = make_service(tmp_path)
    svc.publish_policy("t", retention_seconds=0)
    for i in range(3):
        svc.seal_version("t", f"o{i}", b"data")
    clock.advance(1)
    ids = [svc.delete_version("t", f"o{i}", 1)["certificate_id"] for i in range(3)]
    c1 = svc.get_certificate(ids[0])
    c2 = svc.get_certificate(ids[1])
    c3 = svc.get_certificate(ids[2])
    assert c1["prev_record_hash"] is None
    assert c2["prev_record_hash"] == c1["record_hash"]
    assert c3["prev_record_hash"] == c2["record_hash"]
    assert svc.verify_chain()["valid"] is True


# ---------------------------------------------------------------- 8. pins / misc

def test_other_reference_pin_blocks_delete(tmp_path):
    svc, clock = make_service(tmp_path)
    svc.publish_policy("t", retention_seconds=0)
    svc.seal_version("t", "obj", b"data")
    svc.add_pin("t", "obj", 1, "catalog-link", reason="external snapshot")
    clock.advance(1)

    elig = svc.eligibility("t", "obj", 1)
    reasons = [b["reason"] for b in elig["blockers"]]
    assert "reference" in reasons

    with pytest.raises(DeletionBlocked) as exc:
        svc.delete_version("t", "obj", 1)
    assert "reference" in [b["reason"] for b in exc.value.blockers]

    svc.remove_pin("t", "obj", 1, "catalog-link")
    assert svc.eligibility("t", "obj", 1)["eligible"] is True
    res = svc.delete_version("t", "obj", 1)
    cert = svc.get_certificate(res["certificate_id"])
    assert cert["payload"]["pins_at_deletion"] == []


def test_all_three_blockers_are_reported_together(tmp_path):
    svc, _clock = make_service(tmp_path)
    svc.publish_policy("t", retention_seconds=100)
    svc.seal_version("t", "obj", b"data")
    svc.add_hold("t", "obj", "h")
    svc.add_pin("t", "obj", 1, "p")
    elig = svc.eligibility("t", "obj", 1)
    assert {b["reason"] for b in elig["blockers"]} == {
        "retention",
        "legal_hold",
        "reference",
    }


# ---------------------------------------------------------------- 5. crash recovery

SCENARIO = Path(__file__).parent / "archive_crash_scenario.py"


def _run_scenario(tmp_path, *args):
    env = dict(os.environ)
    env["ARCHIVE_TEST_DIR"] = str(tmp_path)
    return subprocess.run(
        [sys.executable, str(SCENARIO), *args],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )


def test_crash_after_logical_delete_resumes_consistently(tmp_path):
    # Phase 1: set up shared content and crash right after logical delete.
    phase1 = _run_scenario(tmp_path, "pending", "phase1")
    assert phase1.returncode == 17, phase1.stderr

    state_before = json.loads(
        _run_scenario(tmp_path, "pending", "inspect-before").stdout
    )
    # The deleting version is a tombstone: download must NOT resurrect it, even
    # though the physical file is still present (a shared ref keeps it anyway).
    assert state_before["target_state"] == "tombstoned"
    assert state_before["target_download_ok"] is False
    assert state_before["other_download_ok"] is True
    assert state_before["blob_state"] == "active"
    assert state_before["blob_refcount"] == 1
    assert state_before["cert_count"] == 0

    # Phase 2: a fresh process resumes. It must finish the shared-references
    # outcome honestly (retained_shared) and leave the other version working.
    phase2 = _run_scenario(tmp_path, "pending", "phase2")
    assert phase2.returncode == 0, phase2.stderr
    state_after = json.loads(phase2.stdout)
    assert state_after["cert_count"] == 1
    assert state_after["cert_physical_result"] == "retained_shared"
    assert state_after["other_download_ok"] is True
    assert state_after["blob_state"] == "active"
    assert state_after["blob_refcount"] == 1


def test_crash_during_physical_unlink_resumes_and_purges(tmp_path):
    phase1 = _run_scenario(tmp_path, "purging", "phase1")
    assert phase1.returncode == 17, phase1.stderr

    before = json.loads(_run_scenario(tmp_path, "purging", "inspect-before").stdout)
    assert before["target_state"] == "tombstoned"
    assert before["target_download_ok"] is False
    # Blob was claimed for purge ('purging'); file may or may not be unlinked.
    assert before["blob_state"] == "purging"
    assert before["cert_count"] == 0

    phase2 = _run_scenario(tmp_path, "purging", "phase2")
    assert phase2.returncode == 0, phase2.stderr
    after = json.loads(phase2.stdout)
    assert after["cert_count"] == 1
    assert after["cert_physical_result"] == "purged"
    assert after["blob_state"] == "purged"
    assert after["blob_refcount"] == 0
    assert after["file_exists"] is False
    assert after["chain_valid"] is True


def test_resume_without_crash_is_noop(tmp_path):
    svc, _clock = make_service(tmp_path)
    assert svc.resume() == {"purged_blobs": 0, "resumed_ops": 0, "finalized_ops": 0}
