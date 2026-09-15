from __future__ import annotations

import hashlib
import logging
import os
import re
from datetime import timedelta

from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field
from sqlalchemy import delete, func, select, update

from .config import Settings
from .db import Chunk, Upload, Version, init_db, make_engine, make_session_factory, utcnow
from .queue import MergeQueue
from .storage import Storage

log = logging.getLogger("ingest.api")

SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
MISSING_PREVIEW = 100


class CreateUploadRequest(BaseModel):
    total_size: int = Field(gt=0)
    chunk_size: int = Field(gt=0)
    total_chunks: int = Field(ge=1)
    expected_sha256: str | None = None
    ttl_seconds: int | None = None


def create_app(settings: Settings) -> FastAPI:
    engine = make_engine(settings.database_url)
    init_db(engine)
    sf = make_session_factory(engine)
    storage = Storage(settings.data_dir)
    queue = MergeQueue(settings.redis_url)

    app = FastAPI(title="media-ingest", version="1.0.0")
    app.state.settings = settings
    app.state.session_factory = sf
    app.state.storage = storage
    app.state.queue = queue

    def record_failure(upload_id: str, index: int) -> None:
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

    @app.post("/uploads", status_code=201)
    def create_upload(req: CreateUploadRequest):
        if req.total_chunks > settings.max_total_chunks:
            raise HTTPException(400, f"total_chunks exceeds limit {settings.max_total_chunks}")
        # Chunks must exactly tile the object: all but the last are chunk_size bytes.
        if not (req.chunk_size * (req.total_chunks - 1) < req.total_size <= req.chunk_size * req.total_chunks):
            raise HTTPException(400, "total_size inconsistent with chunk_size * total_chunks")
        if req.expected_sha256 is not None and not SHA256_RE.match(req.expected_sha256):
            raise HTTPException(400, "expected_sha256 must be 64 lowercase hex chars")
        ttl = req.ttl_seconds if req.ttl_seconds is not None else settings.upload_ttl_seconds
        now = utcnow()
        upload = Upload(
            status="uploading",
            total_size=req.total_size,
            chunk_size=req.chunk_size,
            total_chunks=req.total_chunks,
            expected_sha256=req.expected_sha256,
            created_at=now,
            updated_at=now,
            expires_at=now + timedelta(seconds=ttl),
        )
        with sf() as s:
            s.add(upload)
            s.commit()
            s.refresh(upload)
        return {
            "upload_id": upload.id,
            "status": upload.status,
            "chunk_size": upload.chunk_size,
            "total_chunks": upload.total_chunks,
            "expires_at": upload.expires_at.isoformat() + "Z",
        }

    @app.put("/uploads/{upload_id}/chunks/{index}")
    async def put_chunk(
        upload_id: str,
        index: int,
        request: Request,
        x_chunk_sha256: str | None = Header(default=None),
    ):
        with sf() as s:
            u = s.get(Upload, upload_id)
            if u is None:
                raise HTTPException(404, "upload not found")
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
            record_failure(upload_id, index)
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
            record_failure(upload_id, index)
            raise
        except Exception:
            tmp.unlink(missing_ok=True)
            raise

        digest = h.hexdigest()
        if size != expected_size:
            tmp.unlink(missing_ok=True)
            record_failure(upload_id, index)
            raise HTTPException(422, f"chunk {index} must be exactly {expected_size} bytes, got {size}")
        if digest != x_chunk_sha256:
            tmp.unlink(missing_ok=True)
            record_failure(upload_id, index)
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
    def upload_status(upload_id: str):
        with sf() as s:
            u = s.get(Upload, upload_id)
            if u is None:
                raise HTTPException(404, "upload not found")
            chunks = s.execute(select(Chunk).where(Chunk.upload_id == upload_id)).scalars().all()
            version = s.get(Version, u.version_id) if u.version_id else None

        stored = sorted(c.index for c in chunks if c.state == "stored")
        failed = sorted(c.index for c in chunks if c.state == "failed")
        stored_set = set(stored)
        missing = [i for i in range(u.total_chunks) if i not in stored_set]
        resp = {
            "upload_id": u.id,
            "status": u.status,
            "total_size": u.total_size,
            "chunk_size": u.chunk_size,
            "total_chunks": u.total_chunks,
            "received": len(stored),
            "received_bytes": sum(c.size for c in chunks if c.state == "stored"),
            "checksum_failed": failed,
            "missing_count": len(missing),
            "missing": missing[:MISSING_PREVIEW],
            "error": u.error,
            "expires_at": u.expires_at.isoformat() + "Z",
        }
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
    def complete(upload_id: str):
        with sf() as s:
            u = s.get(Upload, upload_id)
            if u is None:
                raise HTTPException(404, "upload not found")
            if u.status == "sealed":
                return {"upload_id": u.id, "status": "sealed", "version_id": u.version_id}
            if u.status in ("expired", "aborted"):
                raise HTTPException(410, f"upload is {u.status}")
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
            s.execute(
                update(Upload)
                .where(
                    Upload.id == upload_id,
                    Upload.status.in_(("uploading", "merging", "failed")),
                    Upload.version_id.is_(None),
                )
                .values(status="merging", error=None)
            )
            s.commit()
        # Idempotent: duplicates in the queue collapse to a single version.
        queue.enqueue(upload_id)
        return {"upload_id": upload_id, "status": "merging"}

    @app.get("/versions/{version_id}")
    def version_info(version_id: str):
        with sf() as s:
            v = s.get(Version, version_id)
            if v is None:
                raise HTTPException(404, "version not found")
        return {
            "version_id": v.id,
            "upload_id": v.upload_id,
            "sha256": v.sha256,
            "size": v.size,
            "created_at": v.created_at.isoformat() + "Z",
            "download_url": f"/versions/{v.id}/content",
        }

    @app.get("/versions/{version_id}/content")
    def version_content(version_id: str):
        with sf() as s:
            v = s.get(Version, version_id)
            if v is None:
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
    def abort(upload_id: str):
        with sf() as s:
            u = s.get(Upload, upload_id)
            if u is None:
                raise HTTPException(404, "upload not found")
            if u.status == "sealed":
                raise HTTPException(409, "sealed uploads are immutable")
            u.status = "aborted"
            s.execute(delete(Chunk).where(Chunk.upload_id == upload_id))
            s.commit()
        storage.remove_staging(upload_id)
        return {"upload_id": upload_id, "status": "aborted"}

    return app


def build_app() -> FastAPI:
    """Uvicorn factory: `uvicorn app.main:build_app --factory`."""
    return create_app(Settings.from_env())
