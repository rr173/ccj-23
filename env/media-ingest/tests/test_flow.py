from __future__ import annotations

import hashlib
import os
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import func, select

from app import accounting, jobs
from app.config import Settings
from app.db import CapacityLedger, MergeJob, Upload, Version, utcnow
from app.main import create_app
from app.merge import run_merge
from app.sweeper import sweep_expired

# --------------------------------------------------------------------------- helpers


@pytest.fixture()
def env(tmp_path):
    settings = Settings(
        data_dir=str(tmp_path / "data"),
        database_url=f"sqlite:///{tmp_path}/test.db",
        upload_ttl_seconds=3600,
        worker_threads=4,
        merge_lease_seconds=120,
    )
    app = create_app(settings)
    client = TestClient(app)
    return client, app, settings


def sha256(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def make_tenant(client, tid, capacity=10_000, parallel=1, weight=1, expect=201):
    r = client.post(
        f"/tenants/{tid}",
        json={"capacity_bytes": capacity, "max_parallel_merges": parallel, "weight": weight},
    )
    assert r.status_code == expect, r.text
    return r.json()


def put_policy(client, tid, **kw):
    payload = {"capacity_bytes": 10_000, "max_parallel_merges": 1, "weight": 1}
    payload.update(kw)
    r = client.put(f"/tenants/{tid}/policy", json=payload)
    assert r.status_code == 200, r.text
    return r.json()


def create_payload(size: int, **kw):
    if "chunk_size" not in kw:
        kw["chunk_size"] = max(size, 1)
    cs = kw["chunk_size"]
    total = kw.get("total_chunks", (size + cs - 1) // cs)
    kw["total_chunks"] = total
    out = {"total_size": size, "chunk_size": cs, "total_chunks": total}
    out.update(kw)
    return out


def create_upload(client, tid, size, *, request_key=None, expect=201, **kw):
    headers = {"X-Tenant-ID": tid}
    if request_key is not None:
        headers["X-Idempotency-Key"] = request_key
    r = client.post("/uploads", json=create_payload(size, **kw), headers=headers)
    assert r.status_code == expect, r.text
    return r


def put_chunk(client, tid, uid, index, body, digest=None):
    return client.put(
        f"/uploads/{uid}/chunks/{index}",
        content=body,
        headers={
            "X-Tenant-ID": tid,
            "X-Chunk-SHA256": digest or sha256(body),
        },
    )


def upload_single(client, tid, data=b"x", **kw):
    """Create + upload the single chunk + complete -> job queued."""
    r = create_upload(client, tid, len(data), **kw)
    uid = r.json()["upload_id"]
    assert put_chunk(client, tid, uid, 0, data).status_code == 200
    r = client.post(f"/uploads/{uid}/complete", headers={"X-Tenant-ID": tid})
    assert r.status_code == 202, r.text
    return uid


def merge_now(app, uid):
    return run_merge(app.state.session_factory, app.state.storage, uid)


def occupancy(app, tid):
    with app.state.session_factory() as s:
        return accounting.occupancy(s, tid)


def claim(app, worker="w1"):
    return jobs.claim_next(app.state.session_factory, worker, lease_seconds=120)


def status(app, tid):
    client = TestClient(app)
    return client.get(f"/tenants/{tid}/status", headers={"X-Tenant-ID": tid}).json()


def seal_one(app, client, tid, data=b"x", **kw):
    uid = upload_single(client, tid, data, **kw)
    assert claim(app) == uid
    assert merge_now(app, uid)["status"] == "sealed"
    return uid


# --------------------------------------------------------------------------- baseline flow


def test_multi_tenant_happy_path_and_isolation(env):
    client, app, _ = env
    make_tenant(client, "a", capacity=10_000)
    make_tenant(client, "b", capacity=10_000)
    data = os.urandom(5000)

    r = create_upload(client, "a", len(data), chunk_size=1024)
    uid = r.json()["upload_id"]
    assert r.json()["policy_version"] == 1

    # tenant isolation: tenant b cannot see a's upload
    assert client.get(f"/uploads/{uid}", headers={"X-Tenant-ID": "b"}).status_code == 404
    assert client.get(f"/uploads/{uid}").status_code == 400  # tenant header required

    parts = [data[i : i + 1024] for i in range(0, len(data), 1024)]
    for i, p in enumerate(parts):
        assert put_chunk(client, "a", uid, i, p).status_code == 200

    r = client.post(f"/uploads/{uid}/complete", headers={"X-Tenant-ID": "a"})
    assert r.status_code == 202 and r.json()["status"] == "queued"

    # durable DB queue (no redis): claim from the scheduler
    assert claim(app) == uid
    assert merge_now(app, uid)["status"] == "sealed"

    st = client.get(f"/uploads/{uid}", headers={"X-Tenant-ID": "a"}).json()
    vid = st["version"]["version_id"]
    r = client.get(f"/versions/{vid}/content", headers={"X-Tenant-ID": "a"})
    assert r.status_code == 200 and r.content == data
    # b can't download a's object
    assert client.get(f"/versions/{vid}/content", headers={"X-Tenant-ID": "b"}).status_code == 404

    reserved, used = occupancy(app, "a")
    assert (reserved, used) == (0, len(data))
    with app.state.session_factory() as s:
        assert s.scalar(select(func.count()).select_from(MergeJob).where(
            MergeJob.upload_id == uid, MergeJob.status == "done")) == 1


# --------------------------------------------------------------------------- 1. concurrent capacity


def test_concurrent_creates_never_exceed_cap(env):
    client, app, _ = env
    cap = 1000
    make_tenant(client, "t", capacity=cap, parallel=4, weight=1)
    size = 128
    n_threads = 20  # 20*128 = 2560 > 1000

    results = []
    barrier = threading.Barrier(n_threads)

    def create(i):
        barrier.wait()
        headers = {"X-Tenant-ID": "t", "X-Idempotency-Key": f"k{i}"}
        r = client.post("/uploads", json=create_payload(size), headers=headers)
        return r.status_code

    with ThreadPoolExecutor(max_workers=n_threads) as pool:
        results = list(pool.map(create, range(n_threads)))

    ok = [code for code in results if code == 201]
    rejected = [code for code in results if code == 507]
    assert len(ok) == cap // size  # exactly 7 reservations fit
    assert len(rejected) == n_threads - len(ok)
    assert set(results) <= {201, 507}

    reserved, used = occupancy(app, "t")
    assert reserved + used == cap - cap % size  # 896, never over cap
    assert reserved <= cap

    # ledger rows: exactly one reserve per successful upload
    with app.state.session_factory() as s:
        reserves = s.scalar(
            select(func.count()).select_from(CapacityLedger).where(
                CapacityLedger.tenant_id == "t", CapacityLedger.event_type == "reserve"
            )
        )
        assert reserves == len(ok)
        total = s.scalar(
            select(func.coalesce(func.sum(CapacityLedger.bytes_delta), 0))
            .select_from(CapacityLedger).where(CapacityLedger.tenant_id == "t")
        )
        assert total == reserved + used


def test_oversized_single_upload_rejected_and_reserves_nothing(env):
    client, app, _ = env
    make_tenant(client, "t", capacity=100, parallel=1, weight=1)
    r = create_upload(client, "t", 101, expect=507)
    assert r.json()["detail"]["error"] == "capacity_exceeded"
    assert occupancy(app, "t") == (0, 0)


# --------------------------------------------------------------------------- 2. idempotency / conflict


def test_idempotent_retry_returns_same_upload(env):
    client, app, _ = env
    make_tenant(client, "t", capacity=10_000)
    payload = create_payload(500)
    h = {"X-Tenant-ID": "t", "X-Idempotency-Key": "abc"}

    r1 = client.post("/uploads", json=payload, headers=h)
    r2 = client.post("/uploads", json=payload, headers=h)
    r3 = client.post("/uploads", json=payload, headers=h)
    assert r1.status_code == 201 and r1.json()["replayed"] is False
    assert r2.status_code == 201 and r2.json()["replayed"] is True
    assert r1.json()["upload_id"] == r2.json()["upload_id"] == r3.json()["upload_id"]

    with app.state.session_factory() as s:
        n_uploads = s.scalar(select(func.count()).select_from(Upload).where(Upload.tenant_id == "t"))
        n_reserves = s.scalar(select(func.count()).select_from(CapacityLedger))
        assert n_uploads == 1 and n_reserves == 1  # billed once despite 3 calls


def test_same_key_different_params_conflicts(env):
    client, app, _ = env
    make_tenant(client, "t", capacity=10_000)
    h = {"X-Tenant-ID": "t", "X-Idempotency-Key": "dup"}
    r1 = client.post("/uploads", json=create_payload(500), headers=h)
    assert r1.status_code == 201
    original = r1.json()["upload_id"]

    r2 = client.post("/uploads", json=create_payload(600), headers=h)
    assert r2.status_code == 409
    detail = r2.json()["detail"]
    assert detail["error"] == "request_key_reused_with_different_parameters"
    assert detail["original_upload_id"] == original

    # keys are scoped per tenant: same key in another tenant is independent
    make_tenant(client, "u", capacity=10_000)
    r3 = client.post(
        "/uploads", json=create_payload(600), headers={"X-Tenant-ID": "u", "X-Idempotency-Key": "dup"}
    )
    assert r3.status_code == 201 and r3.json()["upload_id"] != original


def test_concurrent_same_key_collapses_to_one(env):
    client, app, _ = env
    make_tenant(client, "t", capacity=10_000)
    n = 10
    barrier = threading.Barrier(n)

    def create():
        barrier.wait()
        return client.post(
            "/uploads",
            json=create_payload(100),
            headers={"X-Tenant-ID": "t", "X-Idempotency-Key": "hot"},
        )

    with ThreadPoolExecutor(max_workers=n) as pool:
        results = list(pool.map(lambda _: create(), range(n)))
    ids = {r.json()["upload_id"] for r in results}
    assert all(r.status_code == 201 for r in results)
    assert len(ids) == 1
    with app.state.session_factory() as s:
        assert s.scalar(select(func.count()).select_from(CapacityLedger)) == 1


# --------------------------------------------------------------------------- 3. exactly-once transitions


def test_abort_releases_reservation_exactly_once(env):
    client, app, _ = env
    make_tenant(client, "t", capacity=1000)
    uid = create_upload(client, "t", 300).json()["upload_id"]
    assert occupancy(app, "t") == (300, 0)

    assert client.delete(f"/uploads/{uid}", headers={"X-Tenant-ID": "t"}).status_code == 200
    assert client.delete(f"/uploads/{uid}", headers={"X-Tenant-ID": "t"}).status_code == 200
    assert occupancy(app, "t") == (0, 0)
    with app.state.session_factory() as s:
        assert s.scalar(
            select(func.count()).select_from(CapacityLedger).where(
                CapacityLedger.upload_id == uid, CapacityLedger.event_type == "release")
        ) == 1
        assert s.get(Upload, uid).status == "aborted"


def test_expiry_releases_reservation_exactly_once(env):
    client, app, _ = env
    make_tenant(client, "t", capacity=1000)
    uid = create_upload(client, "t", 400, ttl_seconds=60).json()["upload_id"]
    assert occupancy(app, "t") == (400, 0)

    future = utcnow() + timedelta(seconds=120)
    assert sweep_expired(app.state.session_factory, app.state.storage, now=future) == [uid]
    assert sweep_expired(app.state.session_factory, app.state.storage, now=future) == []
    assert occupancy(app, "t") == (0, 0)
    assert client.get(f"/uploads/{uid}", headers={"X-Tenant-ID": "t"}).json()["status"] == "expired"


def test_successful_merge_converts_reserved_to_used_exactly_once(env):
    client, app, _ = env
    make_tenant(client, "t", capacity=1000)
    data = b"hello" * 100  # 500 bytes, single chunk
    uid = upload_single(client, "t", data)
    assert occupancy(app, "t") == (500, 0)

    assert claim(app) == uid
    assert merge_now(app, uid)["status"] == "sealed"
    # replaying the merge changes nothing
    assert merge_now(app, uid)["status"] == "sealed"
    assert client.post(f"/uploads/{uid}/complete", headers={"X-Tenant-ID": "t"}).json()["status"] == "sealed"

    reserved, used = occupancy(app, "t")
    assert (reserved, used) == (0, 500)
    with app.state.session_factory() as s:
        assert s.scalar(select(func.count()).select_from(CapacityLedger).where(
            CapacityLedger.upload_id == uid, CapacityLedger.event_type == "commit_used")) == 1
        assert s.scalar(select(func.count()).select_from(CapacityLedger).where(
            CapacityLedger.upload_id == uid, CapacityLedger.event_type == "release")) == 1
        assert s.scalar(select(func.count()).select_from(Version)) == 1


# --------------------------------------------------------------------------- 4. weighted fairness


def test_weighted_fair_scheduling_ratio_and_no_starvation(env):
    client, app, _ = env
    make_tenant(client, "hi", capacity=1_000_000, parallel=10, weight=3)
    make_tenant(client, "lo", capacity=1_000_000, parallel=10, weight=1)

    n = 12
    for i in range(n):
        upload_single(client, "hi", bytes([i % 256]))
        upload_single(client, "lo", bytes([(i + 100) % 256]))

    order = []
    while True:
        uid = claim(app)
        if uid is None:
            break
        with app.state.session_factory() as s:
            tid = s.get(Upload, uid).tenant_id
        order.append(tid)
        # complete the merge so the parallel bucket frees (bucket counts running jobs)
        merge_now(app, uid)

    assert len(order) == 2 * n
    hi = order.count("hi")
    lo = order.count("lo")
    assert (hi, lo) == (n, n)  # every job runs, low weight not starved

    # Ratio on the first 12 picks should be ~3:1 (9 hi / 3 lo with these tags).
    head = order[:12]
    assert head.count("hi") == 9 and head.count("lo") == 3, head
    # Sequence of tenants, head only: hi,hi,hi interleaving per tags 1/3.. etc.
    assert head[0] == "hi"
    assert "lo" in head


def test_fifo_within_tenant(env):
    client, app, _ = env
    make_tenant(client, "t", capacity=1_000_000, parallel=10, weight=1)
    uids = [upload_single(client, "t", bytes([i])) for i in range(6)]
    claimed = []
    for _ in range(6):
        uid = claim(app)
        claimed.append(uid)
        merge_now(app, uid)
    assert claimed == uids


# --------------------------------------------------------------------------- 5. parallel cap


def test_parallel_cap_blocks_same_tenant_but_not_others(env):
    client, app, _ = env
    make_tenant(client, "a", capacity=1_000_000, parallel=1, weight=1)
    make_tenant(client, "b", capacity=1_000_000, parallel=2, weight=1)

    a_jobs = [upload_single(client, "a", b"a") for _ in range(3)]
    b_jobs = [upload_single(client, "b", b"b") for _ in range(3)]

    first_a = claim(app)
    assert first_a == a_jobs[0]  # a's single slot taken

    # next claim must skip a's queued jobs and serve b
    got_b = claim(app)
    assert got_b == b_jobs[0]
    got_b2 = claim(app)
    assert got_b2 == b_jobs[1]  # b has parallel=2
    assert claim(app) is None  # a blocked, b at cap -> nothing eligible

    # wait-reason for a's head job is parallel_limit
    st = status(app, "a")
    assert st["queue"][0]["upload_id"] == a_jobs[1]
    assert st["queue"][0]["reason"] == "parallel_limit"
    assert st["running_merges"] == 1 and st["queued_merges"] == 2

    # finish a's running merge -> a's next becomes eligible
    merge_now(app, first_a)
    assert claim(app) == a_jobs[1]

    # finishing b's frees b too
    merge_now(app, got_b)
    merge_now(app, got_b2)
    assert claim(app) == b_jobs[2]


# --------------------------------------------------------------------------- 6. cap lowered below usage


def test_lower_cap_keeps_objects_and_blocks_new_reserves(env):
    client, app, _ = env
    make_tenant(client, "t", capacity=1000, parallel=1, weight=1)
    data = b"q" * 500
    uid = seal_one(app, client, "t", data)
    vid = client.get(f"/uploads/{uid}", headers={"X-Tenant-ID": "t"}).json()["version"]["version_id"]

    # lower the cap below current usage (used=500)
    put_policy(client, "t", capacity_bytes=100)
    st = status(app, "t")
    assert st["used_bytes"] == 500 and st["capacity_bytes"] == 100
    assert st["available_bytes"] == 0

    # existing object still downloadable, upload rows untouched
    r = client.get(f"/versions/{vid}/content", headers={"X-Tenant-ID": "t"})
    assert r.status_code == 200 and r.content == data

    # any new reservation rejected
    assert create_upload(client, "t", 1, expect=507).status_code == 507

    # raising it again allows new uploads
    put_policy(client, "t", capacity_bytes=2000)
    assert create_upload(client, "t", 100).status_code == 201


# --------------------------------------------------------------------------- policy versions


def test_policy_change_only_affects_new_uploads(env):
    client, app, _ = env
    make_tenant(client, "t", capacity=1000, parallel=10, weight=1)
    uid_old = upload_single(client, "t", b"old")

    put_policy(client, "t", capacity=1000, max_parallel_merges=10, weight=5)
    uid_new = upload_single(client, "t", b"new")

    with app.state.session_factory() as s:
        jo = s.get(MergeJob, uid_old)
        jn = s.get(MergeJob, uid_new)
        assert jo.weight == 1 and jo.policy_version == 1
        assert jn.weight == 5 and jn.policy_version == 2

    # new policy's larger capacity still can't exceed old uploads' reservations
    reserved, _ = occupancy(app, "t")
    assert reserved == 6  # 3+3


# --------------------------------------------------------------------------- wait reasons


def test_wait_reason_scheduling_order(env):
    client, app, _ = env
    make_tenant(client, "a", capacity=1_000_000, parallel=10, weight=1)
    make_tenant(client, "b", capacity=1_000_000, parallel=10, weight=10)
    a_uid = upload_single(client, "a", b"a")
    for _ in range(3):  # b weight 10 -> its jobs have much smaller vtags
        upload_single(client, "b", b"b")
    # a waits its scheduling turn behind them
    st = status(app, "a")
    assert st["queue"][0]["upload_id"] == a_uid
    assert st["queue"][0]["reason"] == "scheduling_order"


def test_wait_reason_capacity_when_over_cap(env):
    client, app, _ = env
    make_tenant(client, "t", capacity=1000, parallel=10, weight=1)
    # one sealed (used=300) + two queued reservations of 150 each => occupancy 600
    seal_one(app, client, "t", b"x" * 300)
    job1 = upload_single(client, "t", b"y" * 150)  # FIFO head
    job2 = upload_single(client, "t", b"z" * 150)
    # existing uploads keep their v1 snapshot; the CURRENT cap is lowered below
    # occupancy (600) — objects stay, new reserves blocked, and the capacity gate
    # parks queued jobs except the FIFO head (sealing is occupancy-neutral).
    put_policy(client, "t", capacity_bytes=400)

    assert create_upload(client, "t", 1, expect=507).status_code == 507

    st = status(app, "t")
    by_id = {q["upload_id"]: q for q in st["queue"]}
    assert by_id[job2]["reason"] == "capacity"
    assert by_id[job1]["reason"] != "capacity"
    # only the head is dispatchable
    assert claim(app) == job1


# --------------------------------------------------------------------------- 7. crash recovery


def test_crash_during_create_commits_nothing_or_everything(env, monkeypatch):
    client, app, _ = env
    make_tenant(client, "t", capacity=1000)
    sf = app.state.session_factory

    # Simulate a process kill at the single create commit: the first commit that
    # would persist a CapacityLedger 'reserve' row raises. BEGIN IMMEDIATE + one
    # commit => the whole reservation is all-or-nothing.
    import sqlalchemy.orm.session as sess_mod
    orig_commit = sess_mod.Session.commit
    crashed = {"done": False}

    def crashing_commit(self):
        if not crashed["done"]:
            has_reserve = any(
                isinstance(o, CapacityLedger) and o.event_type == accounting.RESERVE
                for o in self.new
            )
            if has_reserve:
                crashed["done"] = True
                raise RuntimeError("SIMULATED CRASH during reserve commit")
        return orig_commit(self)

    monkeypatch.setattr(sess_mod.Session, "commit", crashing_commit)
    with pytest.raises(RuntimeError):
        client.post(
            "/uploads", json=create_payload(200),
            headers={"X-Tenant-ID": "t", "X-Idempotency-Key": "k1"},
        )
    monkeypatch.undo()

    # a fresh process (normal commits again) sees a clean slate
    assert occupancy(app, "t") == (0, 0)
    with sf() as s:
        assert s.scalar(select(func.count()).select_from(Upload)) == 0
        assert s.scalar(select(func.count()).select_from(CapacityLedger)) == 0
        assert s.scalar(select(func.count()).select_from(MergeJob)) == 0

    # retry with the same key is treated as first use, bills once
    r = client.post(
        "/uploads", json=create_payload(200),
        headers={"X-Tenant-ID": "t", "X-Idempotency-Key": "k1"},
    )
    assert r.status_code == 201 and r.json()["replayed"] is False
    assert occupancy(app, "t") == (200, 0)


def test_crash_after_enqueue_claim_recovers_running_to_queued_once(env):
    client, app, _ = env
    make_tenant(client, "t", capacity=10_000, parallel=2, weight=1)
    uids = [upload_single(client, "t", bytes([i])) for i in range(3)]

    # worker claims two (parallel=2), then crashes before sealing
    j1 = claim(app, "worker-1")
    j2 = claim(app, "worker-1")
    assert {j1, j2} == set(uids[:2])
    with app.state.session_factory() as s:
        running = s.scalars(select(MergeJob.upload_id).where(MergeJob.status == "running")).all()
        assert set(running) == set(uids[:2])

    # new worker starts immediately: leases still valid -> jobs not reclaimable,
    # but the third job can be claimed up to the parallel cap... cap already full
    assert claim(app, "worker-2") is None

    # after lease expiry (simulated), both crashed jobs return to the queue and
    # are claimed in original FIFO order; nothing duplicated
    old = utcnow() - timedelta(seconds=1000)
    with app.state.session_factory() as s:
        from sqlalchemy import update
        s.execute(update(MergeJob).where(MergeJob.status == "running").values(lease_expires_at=old))
        s.commit()

    recovered = []
    for _ in range(3):
        uid = claim(app, "worker-2")
        recovered.append(uid)
        merge_now(app, uid)
    assert set(recovered) == set(uids)
    with app.state.session_factory() as s:
        assert s.scalar(select(func.count()).select_from(MergeJob).where(MergeJob.status == "done")) == 3
        # each upload sealed once, billed once
        assert s.scalar(select(func.count()).select_from(Version)) == 3
        assert s.scalar(
            select(func.count()).select_from(CapacityLedger).where(
                CapacityLedger.event_type == "commit_used")
        ) == 3


def test_crash_between_seal_and_commit_retries_without_double_billing(env, monkeypatch):
    client, app, _ = env
    make_tenant(client, "t", capacity=10_000)
    data = b"recover me" * 50
    uid = upload_single(client, "t", data)
    claim(app)

    sf = app.state.session_factory
    crashed = {"done": False}

    # Kill the process at the final atomic seal commit: the before_flush hook
    # fires while the new Version is still pending, right before any row is
    # written. The sealed object file exists but the DB commit never lands.
    import sqlalchemy.orm.session as sess_mod
    from sqlalchemy import event

    def before_flush(session, flush_context, instances):
        if not crashed["done"] and any(isinstance(o, Version) for o in session.new):
            crashed["done"] = True
            raise RuntimeError("SIMULATED CRASH after seal, before commit")

    event.listen(sess_mod.Session, "before_flush", before_flush)
    try:
        with pytest.raises(RuntimeError):
            run_merge(sf, app.state.storage, uid)
    finally:
        event.remove(sess_mod.Session, "before_flush", before_flush)

    # object file exists on disk (deterministic path) but no committed version,
    # status still merging, reservation untouched
    with sf() as s:
        assert s.scalar(select(func.count()).select_from(Version)) == 0
        assert s.get(Upload, uid).status == "merging"
    assert occupancy(app, "t") == (len(data), 0)

    # retry: adopts the orphan object via deterministic version id, commits once
    assert run_merge(sf, app.state.storage, uid)["status"] == "sealed"
    assert run_merge(sf, app.state.storage, uid)["status"] == "sealed"
    assert occupancy(app, "t") == (0, len(data))
    with sf() as s:
        assert s.scalar(select(func.count()).select_from(Version)) == 1
        assert s.scalar(select(func.count()).select_from(CapacityLedger).where(
            CapacityLedger.event_type == "commit_used")) == 1


def test_crash_during_enqueue_retry_does_not_duplicate_queue_row(env):
    """complete() is idempotent: a crash between status flip and enqueue commit is
    impossible (same tx); even two /complete calls yield one job row."""
    client, app, _ = env
    make_tenant(client, "t", capacity=10_000)
    r = create_upload(client, "t", 5)
    uid = r.json()["upload_id"]
    put_chunk(client, "t", uid, 0, b"hello")
    for _ in range(3):
        rr = client.post(f"/uploads/{uid}/complete", headers={"X-Tenant-ID": "t"})
        assert rr.status_code == 202
    with app.state.session_factory() as s:
        assert s.scalar(select(func.count()).select_from(MergeJob)) == 1


# --------------------------------------------------------------------------- durable restart


def test_full_restart_rebuilds_state_from_db(tmp_path):
    settings = Settings(
        data_dir=str(tmp_path / "data"),
        database_url=f"sqlite:///{tmp_path}/restart.db",
        upload_ttl_seconds=3600,
        merge_lease_seconds=120,
    )
    app1 = create_app(settings)
    c1 = TestClient(app1)
    make_tenant(c1, "t", capacity=10_000, parallel=1, weight=1)
    uid = upload_single(c1, "t", b"persist" * 10)
    # process 1 claims then "dies"; its lease is expired below
    jobs.claim_next(app1.state.session_factory, "dead-worker", lease_seconds=120)

    # brand-new process: same DB file + data volume
    settings2 = Settings(
        data_dir=str(tmp_path / "data"),
        database_url=f"sqlite:///{tmp_path}/restart.db",
        upload_ttl_seconds=3600,
        merge_lease_seconds=0,  # immediate reclaim for the test
    )
    app2 = create_app(settings2)
    from sqlalchemy import update
    with app2.state.session_factory() as s:
        s.execute(update(MergeJob).where(MergeJob.status == "running").values(
            lease_expires_at=utcnow() - timedelta(seconds=1)))
        s.commit()

    recovered = jobs.claim_next(app2.state.session_factory, "new-worker", lease_seconds=0)
    assert recovered == uid
    result = run_merge(app2.state.session_factory, app2.state.storage, uid)
    assert result["status"] == "sealed"
    with app2.state.session_factory() as s:
        assert s.scalar(select(func.count()).select_from(Version)) == 1
        # no duplicate queue row
        assert s.scalar(select(func.count()).select_from(MergeJob)) == 1


# --------------------------------------------------------------------------- status endpoint


def test_tenant_status_counters(env):
    client, app, _ = env
    make_tenant(client, "t", capacity=10_000, parallel=1, weight=1)
    create_upload(client, "t", 100)                          # uploading (reserved 100)
    seal_one(app, client, "t", b"z" * 250)                   # sealed used=250
    upload_single(client, "t", b"q" * 50)                    # queued (reserved 50)

    st = status(app, "t")
    assert st["reserved_bytes"] == 150
    assert st["used_bytes"] == 250
    assert st["queued_merges"] == 1
    assert st["running_merges"] == 0
    assert st["sealed_objects"] == 1
    assert st["active_uploads"] == 2
