from __future__ import annotations

import hashlib
import os
from datetime import timedelta

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import func, select

from app.config import Settings
from app.db import Chunk, Upload, Version, utcnow
from app.main import create_app
from app.merge import MergeError, run_merge
from app.sweeper import requeue_stale_merges, sweep_expired


@pytest.fixture()
def env(tmp_path):
    settings = Settings(
        data_dir=str(tmp_path / "data"),
        database_url=f"sqlite:///{tmp_path}/test.db",
        redis_url="fake://",
        upload_ttl_seconds=3600,
    )
    app = create_app(settings)
    app.state.queue.r.flushall()  # fake:// server is shared process-wide
    client = TestClient(app)
    return client, app, settings


def sha256(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def create_upload(client, data: bytes, chunk_size: int, **kw):
    total_chunks = (len(data) + chunk_size - 1) // chunk_size
    payload = {
        "total_size": len(data),
        "chunk_size": chunk_size,
        "total_chunks": total_chunks,
        "expected_sha256": sha256(data),
    }
    payload.update(kw)
    r = client.post("/uploads", json=payload)
    assert r.status_code == 201, r.text
    return r.json()["upload_id"]


def put_chunk(client, uid: str, index: int, body: bytes, digest: str | None = None):
    return client.put(
        f"/uploads/{uid}/chunks/{index}",
        content=body,
        headers={"X-Chunk-SHA256": digest or sha256(body)},
    )


def chunks_of(data: bytes, chunk_size: int) -> list[bytes]:
    return [data[i : i + chunk_size] for i in range(0, len(data), chunk_size)]


def merge_now(app, uid: str) -> dict:
    return run_merge(app.state.session_factory, app.state.storage, uid)


# --- 1. happy path: out-of-order + duplicate upload, verifiable digest, one version ---


def test_out_of_order_and_duplicate_upload_seals_exactly_once(env):
    client, app, _ = env
    data = os.urandom(5000)
    uid = create_upload(client, data, 1024)
    parts = chunks_of(data, 1024)
    assert len(parts) == 5

    for i in [3, 0, 4, 1, 2]:  # out of order
        r = put_chunk(client, uid, i, parts[i])
        assert r.status_code == 200, r.text
        assert r.json()["duplicate"] is False

    r = put_chunk(client, uid, 1, parts[1])  # duplicate retry
    assert r.status_code == 200 and r.json()["duplicate"] is True

    st = client.get(f"/uploads/{uid}").json()
    assert st["status"] == "uploading" and st["received"] == 5 and st["missing_count"] == 0

    r = client.post(f"/uploads/{uid}/complete")
    assert r.status_code == 202 and r.json()["status"] == "merging"

    result = merge_now(app, uid)
    assert result["status"] == "sealed"

    st = client.get(f"/uploads/{uid}").json()
    assert st["status"] == "sealed"
    vid = st["version"]["version_id"]
    assert st["version"]["sha256"] == sha256(data)
    assert st["version"]["size"] == len(data)

    # retrying complete / merge yields the same single version
    assert client.post(f"/uploads/{uid}/complete").json()["version_id"] == vid
    assert merge_now(app, uid)["version_id"] == vid
    with app.state.session_factory() as s:
        assert s.scalar(select(func.count()).select_from(Version)) == 1

    # sealed content is byte-identical and carries a verifiable digest header
    r = client.get(f"/versions/{vid}/content")
    assert r.status_code == 200
    assert r.content == data
    assert r.headers["X-Content-SHA256"] == sha256(data)

    # staging cleaned up, object is read-only
    assert not app.state.storage.staging_dir(uid).exists()
    assert not os.access(app.state.storage.object_path(vid), os.W_OK)


# --- 2. checksum failure is reported, tracked, and recoverable ---


def test_checksum_failure_tracked_and_recoverable(env):
    client, app, _ = env
    data = os.urandom(2048)
    uid = create_upload(client, data, 1024)
    parts = chunks_of(data, 1024)

    r = put_chunk(client, uid, 0, parts[0], digest="0" * 64)  # wrong digest
    assert r.status_code == 422

    st = client.get(f"/uploads/{uid}").json()
    assert st["checksum_failed"] == [0]
    assert st["received"] == 0

    r = put_chunk(client, uid, 0, parts[0])  # correct retry
    assert r.status_code == 200
    st = client.get(f"/uploads/{uid}").json()
    assert st["checksum_failed"] == [] and st["received"] == 1

    # a bad re-upload must not clobber the good stored chunk
    r = put_chunk(client, uid, 0, b"garbage" + parts[0][7:], digest=sha256(b"garbage" + parts[0][7:]))
    assert r.status_code == 200  # self-consistent chunk accepted (last writer wins)
    r = put_chunk(client, uid, 0, parts[0])
    assert r.status_code == 200

    # wrong size rejected
    r = put_chunk(client, uid, 1, parts[1] + b"x")
    assert r.status_code == 422

    assert put_chunk(client, uid, 1, parts[1]).status_code == 200
    assert client.post(f"/uploads/{uid}/complete").status_code == 202
    assert merge_now(app, uid)["status"] == "sealed"


# --- 3. resume: complete with missing chunks -> 409, then finish after re-upload ---


def test_resume_after_disconnect(env):
    client, app, _ = env
    data = os.urandom(3000)
    uid = create_upload(client, data, 1024)
    parts = chunks_of(data, 1024)

    put_chunk(client, uid, 0, parts[0])
    put_chunk(client, uid, 2, parts[2])  # "disconnect" before chunk 1

    r = client.post(f"/uploads/{uid}/complete")
    assert r.status_code == 409
    assert r.json()["detail"]["missing"] == [1]

    put_chunk(client, uid, 1, parts[1])  # resume: only the missing chunk
    assert client.post(f"/uploads/{uid}/complete").status_code == 202
    assert merge_now(app, uid)["status"] == "sealed"
    vid = client.get(f"/uploads/{uid}").json()["version"]["version_id"]
    assert client.get(f"/versions/{vid}/content").content == data


# --- 4. crash / unwritable storage mid-merge: nothing half-visible, retry seals once ---


def test_crash_mid_merge_leaves_nothing_visible_and_retry_seals(env, monkeypatch):
    client, app, _ = env
    data = os.urandom(2048)
    uid = create_upload(client, data, 1024)
    for i, p in enumerate(chunks_of(data, 1024)):
        put_chunk(client, uid, i, p)
    client.post(f"/uploads/{uid}/complete")

    storage = app.state.storage
    real_seal = storage.seal_object
    calls = {"n": 0}

    def flaky_seal(tmp_path, version_id, manifest):
        calls["n"] += 1
        if calls["n"] == 1:
            raise OSError("No space left on device")  # storage temporarily unwritable
        return real_seal(tmp_path, version_id, manifest)

    monkeypatch.setattr(storage, "seal_object", flaky_seal)
    with pytest.raises(MergeError):
        merge_now(app, uid)

    # no half-finished object is visible anywhere
    assert list(storage.objects.iterdir()) == []
    with app.state.session_factory() as s:
        assert s.scalar(select(func.count()).select_from(Version)) == 0
        assert s.get(Upload, uid).status == "merging"  # retryable

    monkeypatch.setattr(storage, "seal_object", real_seal)
    assert merge_now(app, uid)["status"] == "sealed"
    with app.state.session_factory() as s:
        assert s.scalar(select(func.count()).select_from(Version)) == 1


def test_crash_before_version_commit_discards_orphan_object(env, monkeypatch):
    client, app, _ = env
    data = os.urandom(1024)
    uid = create_upload(client, data, 1024)
    put_chunk(client, uid, 0, data)
    client.post(f"/uploads/{uid}/complete")

    storage = app.state.storage
    sf = app.state.session_factory

    # simulate a process crash right after seal_object but before the DB commit:
    # run the merge with a session factory whose commits fail once
    real_commit = None
    import app.merge as merge_mod

    orig_session_factory = sf
    crashed = {"done": False}

    class CrashOnce:
        def __call__(self):
            s = orig_session_factory()
            if not crashed["done"]:
                crashed["done"] = True
                real = s.commit

                def boom():
                    raise RuntimeError("process crashed")

                s.commit = boom
            return s

    with pytest.raises(RuntimeError):
        run_merge(CrashOnce(), storage, uid)

    # object file may exist on disk but NO version row -> not visible via API
    with sf() as s:
        assert s.scalar(select(func.count()).select_from(Version)) == 0
    assert client.get(f"/uploads/{uid}").json()["status"] == "merging"

    # retry seals exactly one version
    assert merge_now(app, uid)["status"] == "sealed"
    with sf() as s:
        assert s.scalar(select(func.count()).select_from(Version)) == 1


# --- 5. final digest mismatch -> failed, no version, client can fix and retry ---


def test_final_digest_mismatch_marks_failed(env):
    client, app, _ = env
    data = os.urandom(1024)
    uid = create_upload(client, data, 1024, expected_sha256="f" * 64)
    put_chunk(client, uid, 0, data)
    client.post(f"/uploads/{uid}/complete")

    assert merge_now(app, uid)["status"] == "failed"
    st = client.get(f"/uploads/{uid}").json()
    assert st["status"] == "failed" and "final digest" in st["error"]
    with app.state.session_factory() as s:
        assert s.scalar(select(func.count()).select_from(Version)) == 0


# --- 6. TTL sweeper expires incomplete uploads and cleans staging ---


def test_sweeper_expires_stale_uploads(env):
    client, app, _ = env
    data = os.urandom(2048)
    uid = create_upload(client, data, 1024, ttl_seconds=60)
    put_chunk(client, uid, 0, data[:1024])
    assert app.state.storage.staging_dir(uid).exists()

    expired = sweep_expired(
        app.state.session_factory, app.state.storage, now=utcnow() + timedelta(seconds=120)
    )
    assert expired == [uid]
    st = client.get(f"/uploads/{uid}").json()
    assert st["status"] == "expired"
    assert not app.state.storage.staging_dir(uid).exists()

    # expired uploads reject chunks and complete
    assert put_chunk(client, uid, 1, data[1024:]).status_code == 409
    assert client.post(f"/uploads/{uid}/complete").status_code == 410

    # sealed uploads are never expired
    uid2 = create_upload(client, data[:1024], 1024, ttl_seconds=60)
    put_chunk(client, uid2, 0, data[:1024])
    client.post(f"/uploads/{uid2}/complete")
    merge_now(app, uid2)
    assert sweep_expired(
        app.state.session_factory, app.state.storage, now=utcnow() + timedelta(days=365)
    ) == []
    assert client.get(f"/uploads/{uid2}").json()["status"] == "sealed"


# --- 7. queue recovery: stale merges get re-enqueued ---


def test_stale_merge_requeue(env):
    client, app, _ = env
    data = os.urandom(1024)
    uid = create_upload(client, data, 1024)
    put_chunk(client, uid, 0, data)
    client.post(f"/uploads/{uid}/complete")

    queue = app.state.queue
    queue.dequeue(timeout=1)  # worker takes it into processing...
    assert queue.recover() == 1  # ...crashes; startup recovery puts it back
    assert queue.dequeue(timeout=1) == uid

    n = requeue_stale_merges(app.state.session_factory, queue, stale_seconds=0)
    assert n == 1  # stuck 'merging' upload re-enqueued


# --- 8. validation guards ---


def test_validation_guards(env):
    client, app, _ = env
    data = os.urandom(1024)

    r = client.post("/uploads", json={"total_size": 100, "chunk_size": 64, "total_chunks": 1})
    assert r.status_code == 400  # 100 > 64*1, inconsistent

    uid = create_upload(client, data, 1024)
    assert put_chunk(client, uid, 5, data).status_code == 400  # index out of range
    r = client.put(f"/uploads/{uid}/chunks/0", content=data)  # missing digest header
    assert r.status_code == 400

    assert client.get("/uploads/nonexistent").status_code == 404

    # abort removes staging and freezes the upload
    assert client.delete(f"/uploads/{uid}").json()["status"] == "aborted"
    assert put_chunk(client, uid, 0, data).status_code == 409

    # sealed uploads cannot be aborted
    uid2 = create_upload(client, data, 1024)
    put_chunk(client, uid2, 0, data)
    client.post(f"/uploads/{uid2}/complete")
    merge_now(app, uid2)
    assert client.delete(f"/uploads/{uid2}").status_code == 409
