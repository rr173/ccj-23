from __future__ import annotations

import hashlib
import json
import logging
import os
import re
from datetime import timedelta

from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field
from sqlalchemy import delete, func, select
from sqlalchemy.exc import IntegrityError

from . import accounting, jobs, tenants
from .archive.api import build_router as build_archive_router
from .archive.models import (  # noqa: F401  (register archive tables on Base.metadata)
    ArchiveAuditEvent,
    ArchiveDeleteOp,
    ArchivedVersion,
    ArchivePolicy,
    ContentBlob,
    DeletionCertificate,
    LegalHold,
    ObjectPin,
)
from .archive.service import ArchiveService
from .config import Settings
from .db import (
    Chunk,
    IdempotentRequest,
    MergeJob,
    Tenant,
    Upload,
    Version,
    init_db,
    make_engine,
    make_session_factory,
    new_id,
    utcnow,
)
from .derive.api import build_router as build_derive_router
from .derive.models import (  # noqa: F401  (register derivation tables on Base.metadata)
    DerivationJob,
    DerivationLedger,
    DerivationProtection,
    DerivationRequest,
    DerivationSegment,
)
from .derive.service import DerivationService
from .storage import Storage

log = logging.getLogger("ingest.api")

SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
MISSING_PREVIEW = 100

MERGEABLE_STATUSES = ("uploading", "merging", "failed", "queued")


class CreateUploadRequest(BaseModel):
    total_size: int = Field(gt=0)
    chunk_size: int = Field(gt=0)
    total_chunks: int = Field(ge=1)
    expected_sha256: str | None = None
    ttl_seconds: int | None = None
    # Client-supplied idempotency key; may also be sent as X-Idempotency-Key.
    request_key: str | None = Field(default=None, max_length=128)


class TenantRequest(BaseModel):
    capacity_bytes: int = Field(gt=0)
    max_parallel_merges: int = Field(ge=1)
    weight: int = Field(ge=1)


def create_app(settings: Settings) -> FastAPI:
    engine = make_engine(settings.database_url)
    init_db(engine)
    sf = make_session_factory(engine)
    storage = Storage(settings.data_dir)

    app = FastAPI(title="media-ingest", version="2.0.0")
    app.state.settings = settings
    app.state.session_factory = sf
    app.state.storage = storage

    # Archive subsystem: retention/holds/dedup/provable deletion over sealed
    # objects. Physical blobs live in <data_dir>/archive/content.
    archive = ArchiveService(
        settings.database_url,
        os.path.join(settings.data_dir, "archive"),
        proof_key=settings.archive_proof_key,
        init_schema=False,  # init_db above already created all tables
    )
    # Finish any delete interrupted by an abnormal exit before serving traffic.
    archive.resume()
    app.state.archive = archive
    app.include_router(build_archive_router(archive))

    # Derivation subsystem: new immutable versions assembled from pinned ranges
    # of existing sealed versions. Staging lives in <data_dir>/derive and is
    # never downloadable; results seal into the shared archive content store.
    derive = DerivationService(
        settings.database_url,
        os.path.join(settings.data_dir, "derive"),
        content_dir=os.path.join(settings.data_dir, "archive"),
        default_capacity_bytes=settings.default_capacity_bytes,
        lease_seconds=settings.derive_lease_seconds,
        init_schema=False,  # init_db above already created all tables
    )
    # Finish published-but-unbilled jobs after a crash; the API is typically
    # single-process so force-expire all stale worker leases too.
    derive.resume(force=True)
    app.state.derive = derive
    app.include_router(build_derive_router(derive))

    def require_tenant(x_tenant_id: str | None) -> str:
        if not x_tenant_id:
            raise HTTPException(400, "X-Tenant-ID header is required")
        return x_tenant_id

    def record_failure(tenant_id: str, upload_id: str, index: int) -> None:
        """Track a checksum/size failure without downgrading an already-stored chunk."""
        with sf() as s:
            row = s.get(Chunk, (upload_id, index))
            if row is None:
                s.add(Chunk(upload_id=upload_id, index=index, state="failed", failures=1, updated_at=utcnow()))
            else:
                row.failures += 1
                if row.state != "stored":
                    row.state = "failed"
                row.updated_at = utcnow()
            s.commit()

    @app.get("/healthz")
    def healthz():
        return {"ok": True}

    # ---------------------------------------------------------------- tenants

    @app.post("/tenants/{tenant_id}", status_code=201)
    def create_tenant(tenant_id: str, req: TenantRequest):
        try:
            return tenants.create_tenant(
                sf,
                tenant_id,
                capacity_bytes=req.capacity_bytes,
                max_parallel_merges=req.max_parallel_merges,
                weight=req.weight,
            )
        except ValueError as exc:
            raise HTTPException(409, str(exc))

    @app.put("/tenants/{tenant_id}/policy")
    def put_policy(tenant_id: str, req: TenantRequest):
        try:
            return tenants.put_policy(
                sf,
                tenant_id,
                capacity_bytes=req.capacity_bytes,
                max_parallel_merges=req.max_parallel_merges,
                weight=req.weight,
            )
        except tenants.TenantNotFound:
            raise HTTPException(404, "tenant not found")

    @app.get("/tenants/{tenant_id}")
    def get_tenant(tenant_id: str):
        try:
            return tenants.get_tenant(sf, tenant_id)
        except tenants.TenantNotFound:
            raise HTTPException(404, "tenant not found")

    @app.get("/tenants/{tenant_id}/status")
    def tenant_status(tenant_id: str):
        """Reservation/usage counters, queue depths and, for each queued job, why
        it cannot currently run (capacity / parallel_limit / scheduling_order)."""
        with sf() as s:
            t = s.get(Tenant, tenant_id)
            if t is None:
                raise HTTPException(404, "tenant not found")
            pv = tenants.current_policy(s, tenant_id)
            reserved, used = accounting.occupancy(s, tenant_id)
            counts = jobs.tenant_queue_counts(s, tenant_id)
            running_uploads = s.scalar(
                select(func.count()).select_from(Upload).where(
                    Upload.tenant_id == tenant_id, Upload.status == "merging"
                )
            )
            active_uploads = s.scalar(
                select(func.count()).select_from(Upload).where(
                    Upload.tenant_id == tenant_id,
                    Upload.status.in_(("uploading", "failed", "queued", "merging")),
                )
            )
            queued = jobs.list_queued(s, tenant_id)
        return {
            "tenant_id": tenant_id,
            "policy_version": pv.version,
            "capacity_bytes": pv.capacity_bytes,
            "max_parallel_merges": pv.max_parallel_merges,
            "weight": pv.weight,
            "reserved_bytes": reserved,
            "used_bytes": used,
            "available_bytes": max(0, pv.capacity_bytes - reserved - used),
            "running_merges": counts["running"],
            "queued_merges": counts["queued"],
            "active_uploads": int(active_uploads),
            "uploading_sessions": int(active_uploads) - counts["running"] - counts["queued"],
            "sealed_objects": counts["sealed_jobs"],
            "merging_uploads": int(running_uploads),
            "queue": queued,
        }

    # ---------------------------------------------------------------- uploads

    @app.post("/uploads", status_code=201)
    def create_upload(
        req: CreateUploadRequest,
        x_tenant_id: str | None = Header(default=None),
        x_idempotency_key: str | None = Header(default=None),
    ):
        tenant_id = require_tenant(x_tenant_id)
        request_key = req.request_key or x_idempotency_key

        if req.total_chunks > settings.max_total_chunks:
            raise HTTPException(400, f"total_chunks exceeds limit {settings.max_total_chunks}")
        # Chunks must exactly tile the object: all but the last are chunk_size bytes.
        if not (req.chunk_size * (req.total_chunks - 1) < req.total_size <= req.chunk_size * req.total_chunks):
            raise HTTPException(400, "total_size inconsistent with chunk_size * total_chunks")
        if req.expected_sha256 is not None and not SHA256_RE.match(req.expected_sha256):
            raise HTTPException(400, "expected_sha256 must be 64 lowercase hex chars")

        fingerprint_payload = {
            "total_size": req.total_size,
            "chunk_size": req.chunk_size,
            "total_chunks": req.total_chunks,
            "expected_sha256": req.expected_sha256,
            "ttl_seconds": req.ttl_seconds,
        }
        fingerprint = hashlib.sha256(
            json.dumps(fingerprint_payload, sort_keys=True).encode()
        ).hexdigest()

        ttl = req.ttl_seconds if req.ttl_seconds is not None else settings.upload_ttl_seconds
        now = utcnow()

        # Exclusive tenant write transaction: idempotency lookup, capacity check,
        # upload row and reserve ledger row are one atomic commit. Concurrent
        # creates serialize here, so the cap cannot be exceeded.
        with sf() as s:
            pv = tenants.lock_or_bootstrap(
                s,
                tenant_id,
                default_capacity=settings.default_capacity_bytes,
                default_parallel=settings.default_max_parallel_merges,
                default_weight=settings.default_weight,
            )

            existing = None
            if request_key is not None:
                existing = s.get(IdempotentRequest, (tenant_id, request_key))
            if existing is not None:
                if existing.fingerprint != fingerprint:
                    conflict = s.get(Upload, existing.upload_id)
                    raise HTTPException(
                        409,
                        detail={
                            "error": "request_key_reused_with_different_parameters",
                            "request_key": request_key,
                            "original_upload_id": existing.upload_id,
                            "original_status": conflict.status if conflict else None,
                        },
                    )
                original = s.get(Upload, existing.upload_id)
                response = {
                    "upload_id": original.id,
                    "status": original.status,
                    "chunk_size": original.chunk_size,
                    "total_chunks": original.total_chunks,
                    "policy_version": original.policy_version,
                    "expires_at": original.expires_at.isoformat() + "Z",
                    "replayed": True,
                }
                s.commit()
                return response

            # First use of this key (or no key): policy is resolved/bootstrapped
            # above under the tenant lock.
            reserved, used = accounting.occupancy(s, tenant_id)
            if reserved + used + req.total_size > pv.capacity_bytes:
                raise HTTPException(
                    507,
                    detail={
                        "error": "capacity_exceeded",
                        "tenant_id": tenant_id,
                        "requested_bytes": req.total_size,
                        "reserved_bytes": reserved,
                        "used_bytes": used,
                        "capacity_bytes": pv.capacity_bytes,
                    },
                )

            upload_id = new_id()
            upload = Upload(
                id=upload_id,
                tenant_id=tenant_id,
                status="uploading",
                total_size=req.total_size,
                chunk_size=req.chunk_size,
                total_chunks=req.total_chunks,
                expected_sha256=req.expected_sha256,
                created_at=now,
                updated_at=now,
                expires_at=now + timedelta(seconds=ttl),
                policy_version=pv.version,
                policy_capacity_bytes=pv.capacity_bytes,
                policy_max_parallel_merges=pv.max_parallel_merges,
                policy_weight=pv.weight,
                request_key=request_key,
            )
            s.add(upload)
            s.flush()
            # Reserve atomically with the upload row — same transaction, same
            # tenant lock, so the cap cannot be exceeded by concurrent creates.
            s.add(
                accounting.CapacityLedger(
                    tenant_id=tenant_id,
                    upload_id=upload_id,
                    event_type=accounting.RESERVE,
                    bytes_delta=req.total_size,
                )
            )

            if request_key is not None:
                s.add(
                    IdempotentRequest(
                        tenant_id=tenant_id,
                        request_key=request_key,
                        upload_id=upload_id,
                        fingerprint=fingerprint,
                        created_at=now,
                    )
                )
            try:
                s.commit()
            except IntegrityError:
                # Concurrent transaction committed the same (tenant, key) first.
                s.rollback()
                with sf() as s2:
                    racer = s2.get(IdempotentRequest, (tenant_id, request_key))
                    if racer is not None:
                        original = s2.get(Upload, racer.upload_id)
                        if racer.fingerprint == fingerprint:
                            return {
                                "upload_id": original.id,
                                "status": original.status,
                                "chunk_size": original.chunk_size,
                                "total_chunks": original.total_chunks,
                                "policy_version": original.policy_version,
                                "expires_at": original.expires_at.isoformat() + "Z",
                                "replayed": True,
                            }
                        raise HTTPException(
                            409,
                            detail={
                                "error": "request_key_reused_with_different_parameters",
                                "request_key": request_key,
                                "original_upload_id": racer.upload_id,
                            },
                        )
                raise

        return {
            "upload_id": upload_id,
            "status": "uploading",
            "chunk_size": req.chunk_size,
            "total_chunks": req.total_chunks,
            "policy_version": pv.version,
            "expires_at": upload.expires_at.isoformat() + "Z",
            "replayed": False,
        }

    def _load_upload(s, tenant_id: str, upload_id: str) -> Upload:
        u = s.get(Upload, upload_id)
        if u is None or u.tenant_id != tenant_id:
            raise HTTPException(404, "upload not found")
        return u

    @app.put("/uploads/{upload_id}/chunks/{index}")
    async def put_chunk(
        upload_id: str,
        index: int,
        request: Request,
        x_tenant_id: str | None = Header(default=None),
        x_chunk_sha256: str | None = Header(default=None),
    ):
        tenant_id = require_tenant(x_tenant_id)
        with sf() as s:
            u = _load_upload(s, tenant_id, upload_id)
            if u.status not in ("uploading", "failed"):
                raise HTTPException(409, f"upload is {u.status}; chunks no longer accepted")
            if not (0 <= index < u.total_chunks):
                raise HTTPException(400, "chunk index out of range")
            expected_size = (
                u.chunk_size if index < u.total_chunks - 1 else u.total_size - u.chunk_size * (u.total_chunks - 1)
            )
        if x_chunk_sha256 is None or not SHA256_RE.match(x_chunk_sha256):
            raise HTTPException(400, "X-Chunk-SHA256 header (64 lowercase hex) is required")
        content_length = request.headers.get("content-length")
        if content_length is not None and int(content_length) != expected_size:
            record_failure(tenant_id, upload_id, index)
            raise HTTPException(422, f"chunk {index} must be exactly {expected_size} bytes")

        tmp = storage.new_tmp("chunk")
        h = hashlib.sha256()
        size = 0
        try:
            with open(tmp, "wb") as f:
                async for part in request.stream():
                    size += len(part)
                    if size > expected_size:
                        raise HTTPException(422, f"chunk {index} exceeds {expected_size} bytes")
                    h.update(part)
                    f.write(part)
                f.flush()
                os.fsync(f.fileno())
        except HTTPException:
            tmp.unlink(missing_ok=True)
            record_failure(tenant_id, upload_id, index)
            raise
        except Exception:
            tmp.unlink(missing_ok=True)
            raise

        digest = h.hexdigest()
        if size != expected_size:
            tmp.unlink(missing_ok=True)
            record_failure(tenant_id, upload_id, index)
            raise HTTPException(422, f"chunk {index} must be exactly {expected_size} bytes, got {size}")
        if digest != x_chunk_sha256:
            tmp.unlink(missing_ok=True)
            record_failure(tenant_id, upload_id, index)
            raise HTTPException(422, f"chunk {index} digest {digest} != X-Chunk-SHA256 {x_chunk_sha256}")

        storage.commit_chunk(tmp, upload_id, index)
        with sf() as s:
            row = s.get(Chunk, (upload_id, index))
            duplicate = row is not None and row.state == "stored" and row.sha256 == digest
            if row is None:
                s.add(
                    Chunk(
                        upload_id=upload_id,
                        index=index,
                        state="stored",
                        size=size,
                        sha256=digest,
                        failures=0,
                        updated_at=utcnow(),
                    )
                )
            else:
                row.state = "stored"
                row.size = size
                row.sha256 = digest
                row.updated_at = utcnow()
            s.commit()
        return {"index": index, "size": size, "sha256": digest, "duplicate": duplicate}

    @app.get("/uploads/{upload_id}")
    def upload_status(upload_id: str, x_tenant_id: str | None = Header(default=None)):
        tenant_id = require_tenant(x_tenant_id)
        with sf() as s:
            u = _load_upload(s, tenant_id, upload_id)
            chunks = s.execute(select(Chunk).where(Chunk.upload_id == upload_id)).scalars().all()
            version = s.get(Version, u.version_id) if u.version_id else None
            job = s.get(MergeJob, upload_id)
            wait = None
            if job is not None and job.status == jobs.QUEUED:
                wait = jobs.explain_wait(s, job)

        stored = sorted(c.index for c in chunks if c.state == "stored")
        failed = sorted(c.index for c in chunks if c.state == "failed")
        stored_set = set(stored)
        missing = [i for i in range(u.total_chunks) if i not in stored_set]
        resp = {
            "upload_id": u.id,
            "tenant_id": u.tenant_id,
            "status": u.status,
            "total_size": u.total_size,
            "chunk_size": u.chunk_size,
            "total_chunks": u.total_chunks,
            "policy_version": u.policy_version,
            "received": len(stored),
            "received_bytes": sum(c.size for c in chunks if c.state == "stored"),
            "checksum_failed": failed,
            "missing_count": len(missing),
            "missing": missing[:MISSING_PREVIEW],
            "error": u.error,
            "expires_at": u.expires_at.isoformat() + "Z",
        }
        if wait is not None:
            resp["queue"] = {
                "state": "queued",
                "reason": wait["reason"],
                "reason_detail": wait["detail"],
            }
        elif job is not None:
            resp["queue"] = {"state": job.status}
        if u.status == "sealed" and version is not None:
            resp["version"] = {
                "version_id": version.id,
                "sha256": version.sha256,
                "size": version.size,
                "created_at": version.created_at.isoformat() + "Z",
                "download_url": f"/versions/{version.id}/content",
            }
        return resp

    @app.post("/uploads/{upload_id}/complete", status_code=202)
    def complete(upload_id: str, x_tenant_id: str | None = Header(default=None)):
        tenant_id = require_tenant(x_tenant_id)
        # Atomic: validate chunks, flip status and insert the durable queue row
        # (UNIQUE upload_id => enqueue exactly once) in one tenant-locked tx.
        with accounting.tenant_write_tx(sf, tenant_id) as s:
            u = _load_upload(s, tenant_id, upload_id)
            if u.status == "sealed":
                return {"upload_id": u.id, "status": "sealed", "version_id": u.version_id}
            if u.status in ("expired", "aborted"):
                raise HTTPException(410, f"upload is {u.status}")
            job = s.get(MergeJob, upload_id)
            if job is not None and job.status in (jobs.QUEUED, jobs.RUNNING):
                # Idempotent /complete while already scheduled.
                u.status = "queued" if job.status == jobs.QUEUED else "merging"
                return {"upload_id": u.id, "status": u.status}
            stored = s.scalar(
                select(func.count()).select_from(Chunk).where(
                    Chunk.upload_id == upload_id, Chunk.state == "stored"
                )
            )
            if stored < u.total_chunks:
                rows = s.execute(
                    select(Chunk.index).where(Chunk.upload_id == upload_id, Chunk.state == "stored")
                ).scalars()
                have = set(rows)
                missing = [i for i in range(u.total_chunks) if i not in have]
                raise HTTPException(
                    409,
                    detail={
                        "error": "chunks missing",
                        "missing_count": u.total_chunks - stored,
                        "missing": missing[:MISSING_PREVIEW],
                    },
                )
            u.status = "queued"
            u.error = None
            jobs.enqueue(s, u)
        return {"upload_id": upload_id, "status": "queued"}

    @app.get("/versions/{version_id}")
    def version_info(version_id: str, x_tenant_id: str | None = Header(default=None)):
        tenant_id = require_tenant(x_tenant_id)
        with sf() as s:
            v = s.get(Version, version_id)
            if v is None or v.tenant_id != tenant_id:
                raise HTTPException(404, "version not found")
        return {
            "version_id": v.id,
            "upload_id": v.upload_id,
            "tenant_id": v.tenant_id,
            "sha256": v.sha256,
            "size": v.size,
            "created_at": v.created_at.isoformat() + "Z",
            "download_url": f"/versions/{v.id}/content",
        }

    @app.get("/versions/{version_id}/content")
    def version_content(version_id: str, x_tenant_id: str | None = Header(default=None)):
        tenant_id = require_tenant(x_tenant_id)
        with sf() as s:
            v = s.get(Version, version_id)
            if v is None or v.tenant_id != tenant_id:
                raise HTTPException(404, "version not found")
        path = storage.object_path(version_id)
        if not path.exists():
            raise HTTPException(500, "object missing from store")
        return FileResponse(
            path,
            media_type="application/octet-stream",
            headers={
                "X-Content-SHA256": v.sha256,
                "ETag": f'"{v.sha256}"',
                "Cache-Control": "immutable",
            },
        )

    @app.delete("/uploads/{upload_id}")
    def abort(upload_id: str, x_tenant_id: str | None = Header(default=None)):
        tenant_id = require_tenant(x_tenant_id)
        # Abort and the one-and-only reservation release must commit together;
        # sealed uploads are immutable and keep their converted "used" bytes.
        with accounting.tenant_write_tx(sf, tenant_id) as s:
            u = _load_upload(s, tenant_id, upload_id)
            if u.status == "sealed":
                raise HTTPException(409, "sealed uploads are immutable")
            job = s.get(MergeJob, upload_id)
            if job is not None and job.status == jobs.RUNNING:
                raise HTTPException(409, "merge is running; wait for it to finish or fail")
            released = accounting.release_once(s, u)
            if released:
                u.status = "aborted"
                jobs.cancel_queued(s, u.id)
                s.execute(delete(Chunk).where(Chunk.upload_id == u.id))
        storage.remove_staging(upload_id)
        return {"upload_id": upload_id, "status": "aborted"}

    return app


def build_app() -> FastAPI:
    """Uvicorn factory: `uvicorn app.main:build_app --factory`."""
    return create_app(Settings.from_env())
