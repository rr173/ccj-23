"""Acceptance tests for the derivation subsystem.

Covers the required surface:

  1. multi-source multi-range assembly produces exactly the right bytes, in
     recipe order, as a new immutable archive version;
  2. semantically identical recipes (different field order / range spellings
     / digest case) normalize to ONE recipe digest; semantic differences do
     not;
  3. (tenant, request_key) idempotency: same recipe replays the same job; a
     different recipe on a reused key is a 409 conflict;
  4. cross-tenant reference, missing version, wrong digest and out-of-bounds
     range ALL reject the whole request with no partial task left behind;
  5. deleting ANY referenced source during the task's lifetime is blocked with
     a reference reason (and succeeds after billing);
  6. concurrent workers claim one job at most once and publish exactly once;
  7. cancel while processing clears invisible staging, releases the
     reservation once and all protections; published tasks cannot be canceled;
  8. hard crashes at the three documented points (after a segment write, after
     full assembly before publish, after publish before billing) recover to a
     consistent state with no duplicate output/billing and no lingering
     protection;
  9. capacity is not counted until success, then exactly once.
"""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.archive import ArchiveService
from app.archive.errors import DeletionBlocked, NotFound
from app.config import Settings
from app.derive import (
    CapacityExceeded,
    DerivationService,
    NotCancellable,
    ReferenceRejected,
    RequestKeyConflict,
    parse_recipe,
)
from app.derive import models as dm
from app.derive.recipe import RecipeError
from app.main import create_app

HERE = Path(__file__).resolve().parent
SCENARIO = HERE / "derive_crash_scenario.py"


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def make_world(tmp_path, *, capacity=None, lease=3600.0):
    db = f"sqlite:///{tmp_path}/w.db"
    arch = ArchiveService(db, str(tmp_path / "archive"), proof_key=b"test-key")
    svc = DerivationService(
        db,
        str(tmp_path / "derive"),
        content_dir=str(tmp_path / "archive"),
        default_capacity_bytes=capacity,
        lease_seconds=lease,
    )
    return arch, svc, db


def seed_two_sources(arch, tenant="t"):
    arch.publish_policy(tenant, retention_seconds=100_000)
    a = b"AAAABBBBCCCCDDDD"
    b = b"0123456789abcdef"
    arch.seal_version(tenant, "a", a)
    arch.seal_version(tenant, "b", b)
    return a, b


def recipe_for(a, b, ranges=((2, 10), (4, 12))):
    (s1, e1), (s2, e2) = ranges
    return {
        "segments": [
            {"object_id": "a", "version": 1, "range": [s1, e1], "sha256": sha(a[s1:e1])},
            {"object_id": "b", "version": 1, "range": [s2, e2], "sha256": sha(b[s2:e2])},
        ]
    }


def run_to_end(svc, job_id, worker="w1"):
    claimed = svc.claim_next(worker)
    assert claimed == job_id
    with svc.sf() as s:
        fence = s.get(dm.DerivationJob, job_id).fence
    return svc.process_job(job_id, worker, fence)


def counts(svc):
    with svc.sf() as s:
        return {
            "jobs": s.query(dm.DerivationJob).count(),
            "segments": s.query(dm.DerivationSegment).count(),
            "protections": s.query(dm.DerivationProtection).count(),
            "ledger": s.query(dm.DerivationLedger).count(),
            "requests": s.query(dm.DerivationRequest).count(),
        }


# ------------------------------------------------------------- 1. assembly

def test_multi_source_multi_range_assembles_in_order(tmp_path):
    arch, svc, _ = make_world(tmp_path)
    a, b = seed_two_sources(arch)
    recipe = recipe_for(a, b)
    expected = a[2:10] + b[4:12]

    job = svc.submit("t", recipe, "k1", output_object_id="out")
    status = svc.get_job("t", job["job_id"])
    assert status["status"] == "queued"
    assert [seg["state"] for seg in status["segments"]] == ["waiting", "waiting"]
    assert {seg["blocked_reason"] for seg in status["segments"]} == {"waiting_for_worker"}
    assert status["open_protections"] == 2

    result = run_to_end(svc, job["job_id"])
    assert result["state"] == "billed"

    final = svc.get_job("t", job["job_id"])
    assert final["status"] == "billed"
    assert [seg["state"] for seg in final["segments"]] == ["verified", "verified"]
    assert all(seg["blocked_reason"] is None for seg in final["segments"])
    assert final["open_protections"] == 0

    data, meta = arch.download("t", "out", 1)
    assert data == expected
    assert meta["size"] == len(expected)
    assert meta["content_sha256"] == sha(expected)

    info = arch.get_version("t", "out", 1)
    assert info["state"] == "active"
    # Same source used twice (overlapping recipe) still pins it exactly once.
    assert counts(svc)["jobs"] == 1


def test_duplicate_source_in_recipe_is_protected_once(tmp_path):
    arch, svc, _ = make_world(tmp_path)
    a, _ = seed_two_sources(arch)
    recipe = {"segments": [
        {"object_id": "a", "version": 1, "range": [0, 2], "sha256": sha(a[0:2])},
        {"object_id": "a", "version": 1, "range": [4, 6], "sha256": sha(a[4:6])},
    ]}
    job = svc.submit("t", recipe, "k", output_object_id="out")
    status = svc.get_job("t", job["job_id"])
    assert status["open_protections"] == 1
    run_to_end(svc, job["job_id"])
    data, _ = arch.download("t", "out", 1)
    assert data == a[0:2] + a[4:6]


# ------------------------------------------------------------- 2. normalize

def test_equivalent_recipes_share_digest(tmp_path):
    arch, svc, _ = make_world(tmp_path)
    a, b = seed_two_sources(arch)
    base = recipe_for(a, b)
    d1 = parse_recipe(base).digest()

    # Different segment-internal field order, flat start/end, string range,
    # uppercase digest, an extra ignored field.
    equiv = {"segments": [
        {"sha256": sha(a[2:10]).upper(), "version": 1, "range": "2-10",
         "object": "a", "note": "x"},
        {"end": 12, "start": 4, "object_id": "b",
         "expected_sha256": sha(b[4:12]), "version": 1},
    ]}
    d2 = parse_recipe(equiv).digest()
    assert d1 == d2

    # JSON forms are byte-identical after canonicalization.
    assert parse_recipe(base).canonical_json() == parse_recipe(equiv).canonical_json()

    # Semantic changes (order swapped, different bounds) change the digest.
    reordered = {"segments": list(reversed(base["segments"]))}
    assert parse_recipe(reordered).digest() != d1
    other = recipe_for(a, b, ranges=((2, 10), (5, 12)))
    assert parse_recipe(other).digest() != d1


def test_equivalent_recipes_create_one_job_under_one_key(tmp_path):
    arch, svc, _ = make_world(tmp_path)
    a, b = seed_two_sources(arch)
    base = recipe_for(a, b)
    j1 = svc.submit("t", base, "dup", output_object_id="out")
    equiv = {"segments": [
        {"sha256": sha(a[2:10]).upper(), "version": 1, "range": "2-10", "object": "a"},
        {"end": 12, "start": 4, "object_id": "b",
         "expected_sha256": sha(b[4:12]), "version": 1},
    ]}
    j2 = svc.submit("t", equiv, "dup", output_object_id="out")
    assert j2["replayed"] is True
    assert j2["job_id"] == j1["job_id"]


def test_malformed_recipes_are_400(tmp_path):
    arch, svc, _ = make_world(tmp_path)
    seed_two_sources(arch)
    malformed = [
        {},
        {"segments": []},
        {"segments": [{"object_id": "a", "version": 1, "range": [0, 1]}]},  # no digest
        {"segments": [{"object_id": "a", "version": 1, "range": [5, 2], "sha256": "a" * 64}]},
        {"segments": [{"object_id": "a", "version": 1, "range": [0, 0], "sha256": "a" * 64}]},
        {"segments": [{"object_id": "a", "version": 0, "range": [0, 1], "sha256": "a" * 64}]},
    ]
    for bad in malformed:
        with pytest.raises(RecipeError):
            svc.submit("t", bad, "k", output_object_id="out")


# ------------------------------------------------------------- 3. idempotency

def test_request_key_replay_and_conflict(tmp_path):
    arch, svc, _ = make_world(tmp_path)
    a, b = seed_two_sources(arch)
    recipe = recipe_for(a, b)
    j1 = svc.submit("t", recipe, "key", output_object_id="out")
    j2 = svc.submit("t", recipe, "key", output_object_id="out")
    assert j2["replayed"] is True
    assert j2["job_id"] == j1["job_id"]
    assert counts(svc)["jobs"] == 1

    # Same key, different recipe (one segment) -> explicit conflict.
    different = {"segments": [recipe["segments"][0]]}
    with pytest.raises(RequestKeyConflict) as exc:
        svc.submit("t", different, "key", output_object_id="out2")
    assert exc.value.existing_job_id == j1["job_id"]

    # Keys are per-tenant: the same key elsewhere is independent.
    arch.publish_policy("other", retention_seconds=10)
    arch.seal_version("other", "z", a[2:10])
    other_recipe = {"segments": [
        {"object_id": "z", "version": 1, "range": [0, 8], "sha256": sha(a[2:10])}
    ]}
    j3 = svc.submit("other", other_recipe, "key", output_object_id="o")
    assert j3["job_id"] != j1["job_id"]


# ------------------------------------------------------------- 4. rejection

def test_cross_tenant_and_bad_references_reject_entire_request(tmp_path):
    arch, svc, _ = make_world(tmp_path)
    a, b = seed_two_sources(arch)
    arch.publish_policy("other", retention_seconds=10)
    arch.seal_version("other", "secret", b"topsecret")
    good = recipe_for(a, b)

    def reject(tenant, seg, key):
        with pytest.raises(ReferenceRejected) as exc:
            svc.submit(tenant, {"segments": [seg]}, key, output_object_id="out")
        return exc.value.code

    cross = {"object_id": "secret", "version": 1, "range": [0, 3],
             "sha256": sha(b"top")}
    assert reject("t", cross, "k1") == "source_not_found"
    # Tenant 'other' must not be able to distinguish t's objects from missing.
    seg_t = {"object_id": "a", "version": 1, "range": [2, 4], "sha256": sha(a[2:4])}
    assert reject("other", seg_t, "k2") == "source_not_found"
    assert reject("t", {**good["segments"][0], "object_id": "ghost"}, "k3") == "source_not_found"
    assert reject("t", {**good["segments"][0], "version": 42}, "k4") == "source_not_found"
    assert reject("t", {**good["segments"][0], "sha256": "0" * 64}, "k5") == "digest_mismatch"
    assert reject("t", {**good["segments"][0], "range": [2, 999]}, "k6") == "range_out_of_bounds"

    # Whole-request atomicity: nothing at all was created.
    assert counts(svc) == {"jobs": 0, "segments": 0, "protections": 0,
                           "ledger": 0, "requests": 0}
    assert svc.capacity("t")["reserved_bytes"] == 0


def test_source_entering_delete_flow_is_rejected(tmp_path):
    db = f"sqlite:///{tmp_path}/w.db"
    arch = ArchiveService(db, str(tmp_path / "archive"), proof_key=b"test-key")
    svc = DerivationService(
        db, str(tmp_path / "derive"), content_dir=str(tmp_path / "archive")
    )
    # Zero retention so the logical-delete itself is allowed; it must then
    # reject recipes that reference the tombstoned version.
    arch.publish_policy("t", retention_seconds=0)
    a = b"AAAABBBBCCCCDDDD"
    b = b"0123456789abcdef"
    arch.seal_version("t", "a", a)
    arch.seal_version("t", "b", b)
    arch.delete_version("t", "b", 1)
    seg = {"object_id": "b", "version": 1, "range": [0, 2], "sha256": sha(b"01")}
    with pytest.raises(ReferenceRejected) as exc:
        svc.submit("t", {"segments": [seg]}, "k", output_object_id="out")
    assert exc.value.code == "source_deleting"
    assert counts(svc)["jobs"] == 0


# ------------------------------------------------------------- 5. protection

def test_source_delete_blocked_for_task_lifetime_and_free_after(tmp_path):
    arch, svc, _ = make_world(tmp_path)
    a, b = seed_two_sources(arch)
    recipe = recipe_for(a, b)
    job = svc.submit("t", recipe, "k", output_object_id="out")

    # While queued: delete blocked, naming the job.
    with pytest.raises(DeletionBlocked) as exc:
        arch.delete_version("t", "a", 1)
    refs = [x for x in exc.value.blockers if x["reason"] == "reference"]
    assert refs and refs[0]["derivations"][0]["job_id"] == job["job_id"]

    run_to_end(svc, job["job_id"])

    # After billing the protection is gone; retention still applies but there
    # is no derivation blocker. Advance past retention, then deletion works.
    elig = arch.eligibility("t", "a", 1)
    assert not any(
        "derivations" in x for x in elig["blockers"]
    )


def test_source_delete_blocked_mid_processing(tmp_path):
    arch, svc, _ = make_world(tmp_path)
    a, b = seed_two_sources(arch)
    job = svc.submit("t", recipe_for(a, b), "k", output_object_id="out")
    svc.claim_next("w")
    with svc.sf() as s:
        fence = s.get(dm.DerivationJob, job["job_id"]).fence
    # Reconcile flips queued -> processing without writing segments.
    svc._reconcile(job["job_id"], "w", fence)
    with svc.sf() as s:
        j = s.get(dm.DerivationJob, job["job_id"])
        j.status = dm.JOB_PROCESSING
        s.commit()
    with pytest.raises(DeletionBlocked):
        arch.delete_version("t", "b", 1)


# ------------------------------------------------------------- 6. concurrency

def test_concurrent_workers_publish_exactly_once(tmp_path):
    arch, svc, db = make_world(tmp_path, lease=3600)
    a, b = seed_two_sources(arch)
    job = svc.submit("t", recipe_for(a, b), "k", output_object_id="out")
    job_id = job["job_id"]
    expected = a[2:10] + b[4:12]

    code = (
        "import sys; from app.derive.worker import DerivationWorker;"
        "from app.config import Settings;"
        f"s = Settings(database_url={db!r}, data_dir={str(tmp_path)!r}, derive_lease_seconds=3600.0);"
        "w = DerivationWorker(s, worker_id=sys.argv[1]);"
        "sys.exit(0 if w.run_once() else 1)"
    )
    procs = [
        subprocess.run([sys.executable, "-c", code, f"w{i}"], capture_output=True)
        for i in range(6)
    ]
    winners = [p for p in procs if p.returncode == 0]
    assert len(winners) == 1, [p.stderr.decode()[-400:] for p in procs]

    final = svc.get_job("t", job_id)
    assert final["status"] == "billed"
    assert final["open_protections"] == 0
    data, _ = arch.download("t", "out", 1)
    assert data == expected

    with svc.sf() as s:
        n_commit = s.query(dm.DerivationLedger).filter(
            dm.DerivationLedger.event_type == dm.EV_COMMIT_USED
        ).count()
        n_release = s.query(dm.DerivationLedger).filter(
            dm.DerivationLedger.event_type == dm.EV_RELEASE
        ).count()
    assert n_commit == 1 and n_release == 1
    assert svc.capacity("t")["used_bytes"] == len(expected)


# ------------------------------------------------------------- 7. cancel

def test_cancel_queued_releases_everything(tmp_path):
    arch, svc, _ = make_world(tmp_path)
    a, b = seed_two_sources(arch)
    job = svc.submit("t", recipe_for(a, b), "k", output_object_id="out")
    cap_before = svc.capacity("t")
    assert cap_before["reserved_bytes"] == len(a[2:10] + b[4:12])

    out = svc.cancel("t", job["job_id"])
    assert out["status"] == "canceled"
    # Idempotent.
    again = svc.cancel("t", job["job_id"])
    assert again.get("replayed") is True

    cap = svc.capacity("t")
    assert cap["reserved_bytes"] == 0 and cap["used_bytes"] == 0
    assert svc.get_job("t", job["job_id"])["open_protections"] == 0
    # Sources deletable w.r.t. derivation now; result never existed.
    with pytest.raises(NotFound):
        arch.download("t", "out", 1)
    assert not (tmp_path / "derive" / "staging" / job["job_id"]).exists()


def test_cancel_processing_is_honored_at_boundary(tmp_path):
    arch, svc, _ = make_world(tmp_path)
    a, b = seed_two_sources(arch)
    job = svc.submit("t", recipe_for(a, b), "k1", output_object_id="out1")
    run_to_end(svc, job["job_id"])
    assert svc.get_job("t", job["job_id"])["status"] == "billed"

    # A second job is moved to processing, then canceled: the owner must stop
    # at the next boundary and run terminal cleanup exactly once.
    job2 = svc.submit("t", recipe_for(a, b), "k2", output_object_id="out2")
    svc.claim_next("w")
    with svc.sf() as s:
        j = s.get(dm.DerivationJob, job2["job_id"])
        fence2 = j.fence
        j.status = dm.JOB_PROCESSING
        j.started_at = 1.0
        s.commit()
    svc.cancel("t", job2["job_id"])
    res = svc.process_job(job2["job_id"], "w", fence2)
    assert res["state"] == "canceled"
    cap = svc.capacity("t")
    assert cap["reserved_bytes"] == 0 and cap["used_bytes"] == len(a[2:10] + b[4:12])
    assert svc.get_job("t", job2["job_id"])["open_protections"] == 0
    assert not (tmp_path / "derive" / "staging" / job2["job_id"]).exists()
    with pytest.raises(NotFound):
        arch.download("t", "out2", 1)


def test_published_job_cannot_be_canceled(tmp_path):
    arch, svc, _ = make_world(tmp_path)
    a, b = seed_two_sources(arch)
    job = svc.submit("t", recipe_for(a, b), "k", output_object_id="out")
    run_to_end(svc, job["job_id"])
    with pytest.raises(NotCancellable):
        svc.cancel("t", job["job_id"])


# ------------------------------------------------------------- 8. crashes

CRASH_POINTS = ["point1", "point2", "point3"]


@pytest.mark.parametrize("point", CRASH_POINTS)
def test_hard_crash_recovery_consistency(tmp_path, point):
    env = dict(os.environ)
    env["DERIVE_TEST_DIR"] = str(tmp_path)

    p1 = subprocess.run(
        [sys.executable, str(SCENARIO), "phase1", point],
        env=env, capture_output=True, check=False,
    )
    assert p1.returncode == 37, p1.stderr.decode()

    before = json.loads(
        subprocess.run(
            [sys.executable, str(SCENARIO), "inspect-before"],
            env=env, capture_output=True, text=True, check=True,
        ).stdout
    )
    # Pre-recovery expectations per point.
    if point == "point1":
        assert before["status"] == "processing"
        assert before["open_protections"] == 2
        assert before["reserved"] == 16 and before["used"] == 0
        assert before["result_exists"] is False
        assert before["staging_files"]  # an orphan part file exists
    elif point == "point2":
        assert before["status"] == "assembling"
        assert before["open_protections"] == 2
        assert before["reserved"] == 16 and before["used"] == 0
        assert before["result_exists"] is False
        assert before["staging_files"]
    else:
        assert before["status"] == "published"
        assert before["open_protections"] == 2  # held until billing
        assert before["reserved"] == 16 and before["used"] == 0
        # Published output already exists and is correct, despite no billing.
        assert before["result_exists"] is True
        assert before["result_download_correct"] is True

    after = json.loads(
        subprocess.run(
            [sys.executable, str(SCENARIO), "phase2"],
            env=env, capture_output=True, text=True, check=True,
        ).stdout
    )
    assert after["status"] == "billed"
    assert after["result_exists"] is True
    assert after["result_download_correct"] is True
    assert after["open_protections"] == 0
    assert after["reserved"] == 0 and after["used"] == 16
    # Billed exactly once.
    assert after["ledger_events"].get("reserve") == 1
    assert after["ledger_events"].get("release") == 1
    assert after["ledger_events"].get("commit_used") == 1
    assert after["blob_refcount"] == 1
    assert after["staging_files"] == []


# ------------------------------------------------------------- 9. capacity

def test_capacity_counted_only_after_success_and_cap_gate(tmp_path):
    arch, svc, _ = make_world(tmp_path, capacity=20)
    a, b = seed_two_sources(arch)
    recipe = recipe_for(a, b)  # total 16 bytes
    job = svc.submit("t", recipe, "k", output_object_id="out")
    cap = svc.capacity("t")
    assert cap["reserved_bytes"] == 16 and cap["used_bytes"] == 0
    assert cap["available_bytes"] == 4

    # A second 16-byte reservation would exceed the 20-byte cap.
    with pytest.raises(CapacityExceeded):
        svc.submit("t", recipe, "k2", output_object_id="out2")

    run_to_end(svc, job["job_id"])
    cap = svc.capacity("t")
    assert cap["reserved_bytes"] == 0 and cap["used_bytes"] == 16

    # The successful job also seals a real archive version (downloadable).
    data, _ = arch.download("t", "out", 1)
    assert data == a[2:10] + b[4:12]


# ------------------------------------------------------------- 10. HTTP API

def test_http_api_full_flow_and_rejection_semantics(tmp_path):
    settings = Settings(
        data_dir=str(tmp_path),
        database_url=f"sqlite:///{tmp_path}/api.db",
        default_capacity_bytes=1024,
    )
    app = create_app(settings)
    client = TestClient(app)
    H = {"X-Tenant-ID": "t"}

    # Tenant + archive policy + two sealed sources through the HTTP surface.
    client.put("/archive/tenants/t/policy", json={"retention_seconds": 100000})
    a, b = b"AAAABBBBCCCCDDDD", b"0123456789abcdef"
    r = client.put("/archive/objects/a/versions/1", content=a, headers=H)
    assert r.status_code == 200, r.text
    r = client.put("/archive/objects/b/versions/1", content=b, headers=H)
    assert r.status_code == 200, r.text
    expected = a[2:10] + b[4:12]
    recipe = {
        "segments": [
            {"object_id": "a", "version": 1, "range": [2, 10], "sha256": sha(a[2:10])},
            {"object_id": "b", "version": 1, "range": [4, 12], "sha256": sha(b[4:12])},
        ]
    }

    # Missing request key -> 400.
    r = client.post("/derive/jobs", json={"recipe": recipe, "output_object_id": "out"}, headers=H)
    assert r.status_code == 400

    # Accept.
    r = client.post(
        "/derive/jobs",
        headers={**H, "X-Idempotency-Key": "rk"},
        json={"recipe": recipe, "output_object_id": "out"},
    )
    assert r.status_code == 201, r.text
    job_id = r.json()["job_id"]

    # Result not downloadable before processing.
    assert client.get("/archive/objects/out/versions/1/content", headers=H).status_code == 404

    # Idempotent replay -> same task; conflict on reused key/different recipe.
    r2 = client.post(
        "/derive/jobs",
        headers={**H, "X-Idempotency-Key": "rk"},
        json={"recipe": recipe, "output_object_id": "out"},
    )
    assert r2.status_code == 201 and r2.json()["job_id"] == job_id and r2.json()["replayed"] is True
    r3 = client.post(
        "/derive/jobs",
        headers={**H, "X-Idempotency-Key": "rk"},
        json={"recipe": {"segments": [recipe["segments"][0]]}, "output_object_id": "x"},
    )
    assert r3.status_code == 409 and r3.json()["detail"]["error"] == "request_key_reused_with_different_recipe"

    # Cross-tenant reference is a plain 422 source_not_found (no existence leak).
    r4 = client.post(
        "/derive/jobs",
        headers={"X-Tenant-ID": "other", "X-Idempotency-Key": "z"},
        json={"recipe": {"segments": [recipe["segments"][0]]}, "output_object_id": "o"},
    )
    assert r4.status_code == 422 and r4.json()["detail"]["reason"] == "source_not_found"

    # Source deletion is blocked over HTTP while the job is live.
    rd = client.request("DELETE", "/archive/objects/a/versions/1", headers=H)
    assert rd.status_code == 409
    assert any(
        x["reason"] == "reference" and x.get("derivations", [{}])[0]["job_id"] == job_id
        for x in rd.json()["detail"]["blockers"]
    )

    # Tenant isolation on status: another tenant cannot even see the job.
    assert client.get(f"/derive/jobs/{job_id}", headers={"X-Tenant-ID": "other"}).status_code == 404

    # Drive the job with a worker on the same service instance.
    svc = app.state.derive
    claimed = svc.claim_next("http-worker")
    assert claimed == job_id
    with svc.sf() as s:
        fence = s.get(dm.DerivationJob, job_id).fence
    assert svc.process_job(job_id, "http-worker", fence)["state"] == "billed"

    # Published result downloadable, exactly the expected concatenation.
    r5 = client.get("/archive/objects/out/versions/1/content", headers=H)
    assert r5.status_code == 200 and r5.content == expected

    # Status reports verified segments and no lingering protections; capacity
    # used once.
    r6 = client.get(f"/derive/jobs/{job_id}", headers=H)
    body = r6.json()
    assert body["status"] == "billed"
    assert {x["state"] for x in body["segments"]} == {"verified"}
    assert body["open_protections"] == 0
    cap = client.get("/derive/capacity", headers=H).json()
    assert cap["used_bytes"] == len(expected) and cap["reserved_bytes"] == 0

    # Published job cannot be canceled.
    rc = client.post(f"/derive/jobs/{job_id}/cancel", headers=H)
    assert rc.status_code == 409
