"""Persistence model for the derivation subsystem.

A *derivation* is a background job that reads fixed byte ranges of several
already-sealed versions of the SAME tenant, verifies each range against a
digest declared in the recipe, concatenates the ranges in recipe order and
finally publishes a brand-new immutable ``archived_versions`` row.

Tables
------

* ``derivation_requests``  — idempotency ledger: (tenant_id, request_key) maps
                             one request to one job and fingerprints the
                             canonical recipe. Same recipe retries replay; a
                             reused key with another recipe is a 409 conflict.
* ``derivation_jobs``      — the durable task state machine + lease/fence.
* ``derivation_segments``  — one row per recipe segment: waiting / processing /
                             verified / failed, with the failure reason.
* ``derivation_protections`` — a pin from a live job onto one exact source
                             version. While non-released it appears as a
                             ``reference`` deletion blocker in the archive
                             subsystem, so a source can never disappear while a
                             job may still need it. Released exactly once.
* ``derivation_ledger``    — append-only per-tenant capacity ledger for derived
                             bytes: one reserve at acceptance, exactly one
                             release (cancel/fail) or release+commit_used pair
                             (publish billing). UNIQUE(job_id, event_type) is
                             the hard exactly-once guard.
"""

from __future__ import annotations

from sqlalchemy import (
    BigInteger,
    Boolean,
    Float,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.orm import Mapped, mapped_column

from ..db import Base

# Job state machine:
#   queued -> processing -> assembling -> published -> billed   (success path)
#   queued|processing|assembling -> canceled                    (user cancel)
#   processing|assembling        -> failed                      (deterministic)
#
# Invariants by state:
#   queued       protections + reserve exist; nothing on disk
#   processing   some segments verified; per-segment part files in staging
#   assembling   all segments verified and a full concatenation staged
#   published    the new ArchivedVersion row exists and is downloadable, but
#                billing has NOT been recorded yet and protections/reserve are
#                still held (crash window between publish and bill)
#   billed       terminal: committed once, protections released, staging gone
#   canceled     terminal: reservation released once, protections released,
#                staging gone; no archived version was ever created
#   failed       terminal: same cleanup as canceled; segment rows carry reasons
JOB_QUEUED = "queued"
JOB_PROCESSING = "processing"
JOB_ASSEMBLING = "assembling"
JOB_PUBLISHED = "published"
JOB_BILLED = "billed"
JOB_CANCELED = "canceled"
JOB_FAILED = "failed"

# States from which a user cancel / a deterministic failure can still terminate.
JOB_LIVE_STATES = (JOB_QUEUED, JOB_PROCESSING, JOB_ASSEMBLING)
# States a crash-recovery pass may have to finish.
JOB_RECOVERABLE_STATES = (JOB_PROCESSING, JOB_ASSEMBLING, JOB_PUBLISHED)
JOB_TERMINAL_STATES = (JOB_BILLED, JOB_CANCELED, JOB_FAILED)

# Segment state machine:
#   waiting -> processing -> verified
#   processing -> failed          (source unreadable / digest mismatch)
SEG_WAITING = "waiting"
SEG_PROCESSING = "processing"
SEG_VERIFIED = "verified"
SEG_FAILED = "failed"

# Capacity ledger events (append-only; semantics mirror app.accounting).
EV_RESERVE = "reserve"
EV_RELEASE = "release"
EV_COMMIT_USED = "commit_used"


class DerivationRequest(Base):
    """Idempotency record. (tenant_id, request_key) is the primary key; the
    fingerprint is the canonical recipe digest. Repeating the key with a
    different fingerprint is an explicit request-key conflict."""

    __tablename__ = "derivation_requests"

    tenant_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    request_key: Mapped[str] = mapped_column(String(128), primary_key=True)
    job_id: Mapped[str] = mapped_column(String(32), index=True)
    recipe_digest: Mapped[str] = mapped_column(String(64))
    created_at: Mapped[float] = mapped_column(Float)


class DerivationJob(Base):
    __tablename__ = "derivation_jobs"

    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String(64), index=True)
    request_key: Mapped[str | None] = mapped_column(String(128), nullable=True)
    # Canonical recipe (JSON) and its SHA-256 digest.
    recipe_json: Mapped[str] = mapped_column(Text)
    recipe_digest: Mapped[str] = mapped_column(String(64), index=True)
    # Where the result is sealed (only populated on the publish transition).
    output_object_id: Mapped[str] = mapped_column(String(256))
    output_version: Mapped[int | None] = mapped_column(Integer, nullable=True)
    result_object_id: Mapped[str | None] = mapped_column(String(256), nullable=True)
    result_version: Mapped[int | None] = mapped_column(Integer, nullable=True)
    result_sha256: Mapped[str | None] = mapped_column(String(64), nullable=True)
    result_size: Mapped[int | None] = mapped_column(BigInteger, nullable=True)

    status: Mapped[str] = mapped_column(String(16), default=JOB_QUEUED, index=True)
    cancel_requested: Mapped[bool] = mapped_column(Boolean, default=False)
    total_size: Mapped[int] = mapped_column(BigInteger)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)

    # Lease: only the owning worker may advance the job while the lease is live.
    lease_owner: Mapped[str | None] = mapped_column(String(64), nullable=True)
    lease_expires_at: Mapped[float | None] = mapped_column(Float, nullable=True)
    # Fence token: bumped on every (re)claim. Conditional updates carry the
    # fence the worker saw, so a worker that lost its lease cannot write.
    fence: Mapped[int] = mapped_column(Integer, default=0)
    attempts: Mapped[int] = mapped_column(Integer, default=0)

    created_at: Mapped[float] = mapped_column(Float)
    started_at: Mapped[float | None] = mapped_column(Float, nullable=True)
    assembled_at: Mapped[float | None] = mapped_column(Float, nullable=True)
    published_at: Mapped[float | None] = mapped_column(Float, nullable=True)
    billed_at: Mapped[float | None] = mapped_column(Float, nullable=True)
    finished_at: Mapped[float | None] = mapped_column(Float, nullable=True)

    __table_args__ = (
        Index("ix_derivation_jobs_tenant_created", "tenant_id", "created_at"),
        Index("ix_derivation_jobs_lease", "status", "lease_expires_at"),
    )


class DerivationSegment(Base):
    """One recipe segment, pinned to exact source coordinates at acceptance.
    State is durable, so a crashed worker resumes per segment without rewriting
    verified ranges; ``failure_reason`` backs the per-segment failed status."""

    __tablename__ = "derivation_segments"

    job_id: Mapped[str] = mapped_column(
        String(32), primary_key=True
    )
    index: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=False)
    tenant_id: Mapped[str] = mapped_column(String(64), index=True)
    source_object_id: Mapped[str] = mapped_column(String(256))
    source_version: Mapped[int] = mapped_column(Integer)
    range_start: Mapped[int] = mapped_column(BigInteger)
    range_end: Mapped[int] = mapped_column(BigInteger)  # exclusive
    size: Mapped[int] = mapped_column(BigInteger)
    expected_sha256: Mapped[str] = mapped_column(String(64))
    actual_sha256: Mapped[str | None] = mapped_column(String(64), nullable=True)
    state: Mapped[str] = mapped_column(String(16), default=SEG_WAITING)
    failure_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    updated_at: Mapped[float] = mapped_column(Float)


class DerivationProtection(Base):
    """A live job's pin onto one exact source version. One row per
    (job, version): the same source used by two segments of one job pins it
    once; two jobs each pin it independently. Rows are never deleted — the
    release is recorded via ``released_at`` (exactly-once audit)."""

    __tablename__ = "derivation_protections"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    job_id: Mapped[str] = mapped_column(String(32), index=True)
    tenant_id: Mapped[str] = mapped_column(String(64), index=True)
    object_id: Mapped[str] = mapped_column(String(256))
    version: Mapped[int] = mapped_column(Integer)
    content_sha256: Mapped[str] = mapped_column(String(64))
    created_at: Mapped[float] = mapped_column(Float)
    released_at: Mapped[float | None] = mapped_column(Float, nullable=True, index=True)

    __table_args__ = (
        UniqueConstraint(
            "job_id",
            "tenant_id",
            "object_id",
            "version",
            name="uq_derivation_protections_job_version",
        ),
        # The archive delete flow asks: does anything still pin THIS version?
        Index(
            "ix_derivation_protections_ref",
            "tenant_id",
            "object_id",
            "version",
            "released_at",
        ),
        Index(
            "ix_derivation_protections_job_open",
            "job_id",
            unique=False,
            sqlite_where=text("released_at IS NULL"),
        ),
    )


class DerivationLedger(Base):
    """Append-only capacity ledger for derived bytes, keyed by the job id.

      reserve      +size   acceptance, atomically with job/protections
      release      -size   cancel/fail (once) or paired with commit at billing
      commit_used  +size   successful publish: reservation converts to used

    UNIQUE(job_id, event_type) makes release/commit exactly-once across retries,
    crashes and concurrent workers; tenant used bytes are the SUM of deltas."""

    __tablename__ = "derivation_ledger"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    tenant_id: Mapped[str] = mapped_column(String(64), index=True)
    job_id: Mapped[str] = mapped_column(String(32), index=True)
    event_type: Mapped[str] = mapped_column(String(16))
    bytes_delta: Mapped[int] = mapped_column(BigInteger)
    created_at: Mapped[float] = mapped_column(Float)

    __table_args__ = (
        UniqueConstraint(
            "job_id", "event_type", name="uq_derivation_ledger_job_event"
        ),
    )
