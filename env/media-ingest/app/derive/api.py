"""HTTP surface for the derivation subsystem.

Mounted under ``/derive`` by main.py; every route requires ``X-Tenant-ID``.
Thin layer over DerivationService — all invariants live there.
"""

from __future__ import annotations

from fastapi import APIRouter, Header, HTTPException
from pydantic import BaseModel, Field

from .errors import (
    CapacityExceeded,
    DeriveError,
    NotCancellable,
    NotFound,
    ReferenceRejected,
    RequestKeyConflict,
)
from .recipe import RecipeError
from .service import DerivationService


class DeriveRequest(BaseModel):
    # Free-form recipe object; validated/canonicalized by parse_recipe.
    recipe: dict
    request_key: str | None = Field(default=None, max_length=128)
    output_object_id: str = Field(min_length=1, max_length=256)
    output_version: int | None = Field(default=None, ge=1)


def _map_error(exc: Exception) -> HTTPException:
    if isinstance(exc, (RecipeError, ValueError)):
        return HTTPException(400, str(exc))
    if isinstance(exc, NotFound):
        return HTTPException(404, str(exc))
    if isinstance(exc, RequestKeyConflict):
        return HTTPException(
            409,
            detail={
                "error": "request_key_reused_with_different_recipe",
                "request_key": exc.request_key,
                "existing_job_id": exc.existing_job_id,
                "existing_recipe_digest": exc.existing_digest,
            },
        )
    if isinstance(exc, NotCancellable):
        return HTTPException(409, str(exc))
    if isinstance(exc, CapacityExceeded):
        return HTTPException(
            507,
            detail={
                "error": "capacity_exceeded",
                "tenant_id": exc.tenant_id,
                "requested_bytes": exc.requested,
                "reserved_bytes": exc.reserved,
                "used_bytes": exc.used,
                "capacity_bytes": exc.cap,
            },
        )
    if isinstance(exc, ReferenceRejected):
        return HTTPException(
            422,
            detail={
                "error": "recipe_rejected",
                "reason": exc.code,
                "segment": exc.segment,
                "detail": exc.detail,
            },
        )
    raise exc


def build_router(svc: DerivationService) -> APIRouter:
    router = APIRouter(prefix="/derive", tags=["derive"])

    def tenant(x_tenant_id: str | None) -> str:
        if not x_tenant_id:
            raise HTTPException(400, "X-Tenant-ID header is required")
        return x_tenant_id

    def call(fn):
        try:
            return fn()
        except DeriveError as exc:
            raise _map_error(exc)
        except RecipeError as exc:
            raise _map_error(exc)

    @router.post("/jobs", status_code=201)
    def submit(
        req: DeriveRequest,
        x_tenant_id: str | None = Header(default=None),
        x_idempotency_key: str | None = Header(default=None),
    ):
        t = tenant(x_tenant_id)
        request_key = req.request_key or x_idempotency_key
        if not request_key:
            raise HTTPException(
                400, "request_key (body) or X-Idempotency-Key header is required"
            )
        try:
            res = svc.submit(
                t,
                req.recipe,
                request_key,
                output_object_id=req.output_object_id,
                output_version=req.output_version,
            )
        except (DeriveError, RecipeError) as exc:
            raise _map_error(exc)
        # Replayed requests are not a new resource: 200, same task reference.
        return res

    @router.get("/jobs/{job_id}")
    def get_job(job_id: str, x_tenant_id: str | None = Header(default=None)):
        t = tenant(x_tenant_id)
        try:
            return svc.get_job(t, job_id)
        except NotFound as exc:
            raise _map_error(exc)

    @router.get("/jobs")
    def list_jobs(x_tenant_id: str | None = Header(default=None), limit: int = 100):
        t = tenant(x_tenant_id)
        return {"jobs": svc.list_jobs(t, limit=limit)}

    @router.post("/jobs/{job_id}/cancel")
    def cancel(job_id: str, x_tenant_id: str | None = Header(default=None)):
        t = tenant(x_tenant_id)
        try:
            return svc.cancel(t, job_id)
        except (NotFound, NotCancellable) as exc:
            raise _map_error(exc)

    @router.get("/capacity")
    def capacity(x_tenant_id: str | None = Header(default=None)):
        t = tenant(x_tenant_id)
        return svc.capacity(t)

    return router
