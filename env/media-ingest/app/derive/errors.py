"""Domain errors for the derivation subsystem."""

from __future__ import annotations


class DeriveError(Exception):
    """Base class for derivation errors."""


class NotFound(DeriveError):
    """Job does not exist within this tenant (tenant-isolated lookups)."""


class ReferenceRejected(DeriveError):
    """Acceptance rejected: a source version is missing, foreign-tenant,
    already in a delete flow, out of range, or its digest mismatches.

    The response never distinguishes 'missing' from 'foreign tenant' — both are
    ``source_not_found`` so other tenants' object existence cannot be probed.
    The whole request is rejected atomically: no job, no protection, no
    reservation row is left behind."""

    def __init__(self, code: str, detail: str, *, segment: int | None = None):
        self.code = code
        self.detail = detail
        self.segment = segment
        super().__init__(f"{code}: {detail}")


class RequestKeyConflict(DeriveError):
    """Same (tenant, request_key) was already used with a different recipe."""

    def __init__(self, request_key: str, existing_job_id: str, existing_digest: str):
        self.request_key = request_key
        self.existing_job_id = existing_job_id
        self.existing_digest = existing_digest
        super().__init__(
            f"request key {request_key!r} already used by job {existing_job_id} "
            f"with recipe {existing_digest}"
        )


class CapacityExceeded(DeriveError):
    def __init__(self, tenant_id: str, requested: int, reserved: int, used: int, cap: int):
        self.tenant_id = tenant_id
        self.requested = requested
        self.reserved = reserved
        self.used = used
        self.cap = cap
        super().__init__(
            f"tenant {tenant_id}: derived reserve {requested} would exceed cap "
            f"{cap} (reserved={reserved}, used={used})"
        )


class NotCancellable(DeriveError):
    """The job has already published/billed/failed; cancel is no longer legal."""
