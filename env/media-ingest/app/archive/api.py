"""HTTP surface for the archive subsystem.

Mounted under ``/archive`` by main.py; every route requires ``X-Tenant-ID``.
The router is a thin layer over ArchiveService — all invariants live there.
"""

from __future__ import annotations

from collections.abc import Callable

from fastapi import APIRouter, Header, HTTPException, Request
from fastapi.responses import Response
from pydantic import BaseModel, Field

from .errors import (
    ArchiveError,
    DeletionBlocked,
    NotFound,
    RequestKeyConflict,
    RetentionPolicyMissing,
    VersionExists,
)
from .service import ArchiveService


class PolicyRequest(BaseModel):
    retention_seconds: int = Field(ge=0)
    description: str | None = None


class HoldRequest(BaseModel):
    hold_key: str = Field(min_length=1, max_length=128)
    reason: str | None = None


class PinRequest(BaseModel):
    pin_key: str = Field(min_length=1, max_length=128)
    kind: str = "reference"
    reason: str | None = None


def _map_error(exc: Exception) -> HTTPException:
    if isinstance(exc, NotFound):
        return HTTPException(404, str(exc))
    if isinstance(exc, RetentionPolicyMissing):
        return HTTPException(409, f"retention policy missing: {exc}")
    if isinstance(exc, (VersionExists, RequestKeyConflict)):
        return HTTPException(409, str(exc))
    if isinstance(exc, DeletionBlocked):
        return HTTPException(
            409,
            detail={
                "error": "deletion_blocked",
                "blockers": exc.blockers,
                "primary_reason": exc.blockers[0]["reason"] if exc.blockers else None,
            },
        )
    if isinstance(exc, ValueError):
        return HTTPException(400, str(exc))
    # Non-domain exception: let the framework turn it into a 500.
    raise exc


def build_router(svc: ArchiveService) -> APIRouter:
    router = APIRouter(prefix="/archive", tags=["archive"])

    def tenant(x_tenant_id: str | None) -> str:
        if not x_tenant_id:
            raise HTTPException(400, "X-Tenant-ID header is required")
        return x_tenant_id

    def call(fn: Callable[[], dict]):
        try:
            return fn()
        except (ArchiveError, ValueError) as exc:
            raise _map_error(exc)

    async def read_body(request: Request) -> bytes:
        data = await request.body()
        if not data:
            raise HTTPException(400, "empty object body")
        return data

    # ------------------------------------------------------------ policy

    @router.put("/tenants/{tenant_id}/policy", status_code=201)
    def publish_policy(tenant_id: str, req: PolicyRequest):
        return call(
            lambda: svc.publish_policy(
                tenant_id, req.retention_seconds, description=req.description
            )
        )

    @router.get("/tenants/{tenant_id}/policy")
    def get_policy(tenant_id: str, version: int | None = None):
        return call(lambda: svc.get_policy(tenant_id, version))

    @router.get("/tenants/{tenant_id}/policies")
    def list_policies(tenant_id: str):
        return {"policies": svc.list_policies(tenant_id)}

    # ------------------------------------------------------------ objects

    @router.put("/objects/{object_id}/versions/{version}")
    async def seal_version(
        object_id: str,
        version: int,
        request: Request,
        x_tenant_id: str | None = Header(default=None),
    ):
        t = tenant(x_tenant_id)
        data = await read_body(request)
        return call(lambda: svc.seal_version(t, object_id, data, version=version))

    @router.post("/objects/{object_id}/versions")
    async def seal_next_version(
        object_id: str,
        request: Request,
        x_tenant_id: str | None = Header(default=None),
    ):
        t = tenant(x_tenant_id)
        data = await read_body(request)
        return call(lambda: svc.seal_version(t, object_id, data))

    @router.get("/objects/{object_id}/versions/{version}")
    def version_info(
        object_id: str,
        version: int,
        x_tenant_id: str | None = Header(default=None),
    ):
        t = tenant(x_tenant_id)
        return call(lambda: svc.get_version(t, object_id, version))

    @router.get("/objects/{object_id}/versions/{version}/content")
    def download(
        object_id: str,
        version: int,
        x_tenant_id: str | None = Header(default=None),
    ):
        t = tenant(x_tenant_id)
        try:
            data, meta = svc.download(t, object_id, version)
        except NotFound as exc:
            raise _map_error(exc)
        return Response(
            content=data,
            media_type="application/octet-stream",
            headers={
                "X-Content-SHA256": meta["content_sha256"],
                "X-Policy-Version": str(meta["policy_version"]),
                "ETag": f'"{meta["content_sha256"]}"',
                "Cache-Control": "immutable",
            },
        )

    @router.get("/objects/{object_id}/versions/{version}/eligibility")
    def eligibility(
        object_id: str,
        version: int,
        x_tenant_id: str | None = Header(default=None),
    ):
        t = tenant(x_tenant_id)
        return call(lambda: svc.eligibility(t, object_id, version))

    @router.delete("/objects/{object_id}/versions/{version}")
    def delete_version(
        object_id: str,
        version: int,
        x_tenant_id: str | None = Header(default=None),
        x_idempotency_key: str | None = Header(default=None),
    ):
        t = tenant(x_tenant_id)
        return call(
            lambda: svc.delete_version(
                t, object_id, version, request_key=x_idempotency_key
            )
        )

    # ------------------------------------------------------------ legal holds

    @router.put("/objects/{object_id}/holds/{hold_key}", status_code=201)
    def add_hold(
        object_id: str,
        hold_key: str,
        req: HoldRequest | None = None,
        x_tenant_id: str | None = Header(default=None),
    ):
        t = tenant(x_tenant_id)
        reason = req.reason if req else None
        return call(lambda: svc.add_hold(t, object_id, hold_key, reason=reason))

    @router.delete("/objects/{object_id}/holds/{hold_key}")
    def release_hold(
        object_id: str,
        hold_key: str,
        x_tenant_id: str | None = Header(default=None),
    ):
        t = tenant(x_tenant_id)
        return svc.release_hold(t, object_id, hold_key)

    @router.get("/objects/{object_id}/holds")
    def list_holds(
        object_id: str,
        x_tenant_id: str | None = Header(default=None),
    ):
        t = tenant(x_tenant_id)
        return {"holds": svc.list_holds(t, object_id)}

    # ------------------------------------------------------------ other references

    @router.put(
        "/objects/{object_id}/versions/{version}/pins/{pin_key}", status_code=201
    )
    def add_pin(
        object_id: str,
        version: int,
        pin_key: str,
        req: PinRequest | None = None,
        x_tenant_id: str | None = Header(default=None),
    ):
        t = tenant(x_tenant_id)
        kind = req.kind if req else "reference"
        reason = req.reason if req else None
        return call(
            lambda: svc.add_pin(t, object_id, version, pin_key, kind=kind, reason=reason)
        )

    @router.delete("/objects/{object_id}/versions/{version}/pins/{pin_key}")
    def remove_pin(
        object_id: str,
        version: int,
        pin_key: str,
        x_tenant_id: str | None = Header(default=None),
    ):
        t = tenant(x_tenant_id)
        return svc.remove_pin(t, object_id, version, pin_key)

    # ------------------------------------------------------------ proofs

    @router.get("/certificates/{certificate_id}")
    def get_certificate(
        certificate_id: str,
        x_tenant_id: str | None = Header(default=None),
    ):
        t = tenant(x_tenant_id)
        return call(lambda: svc.get_certificate(certificate_id, tenant_id=t))

    @router.get("/objects/{object_id}/versions/{version}/certificate")
    def get_certificate_for_object(
        object_id: str,
        version: int,
        x_tenant_id: str | None = Header(default=None),
    ):
        t = tenant(x_tenant_id)
        return call(lambda: svc.get_certificate_for_object(t, object_id, version))

    @router.get("/certificates")
    def list_certificates(
        x_tenant_id: str | None = Header(default=None),
        limit: int = 100,
        offset: int = 0,
    ):
        t = tenant(x_tenant_id)
        return {"certificates": svc.list_certificates(t, limit=limit, offset=offset)}

    return router
