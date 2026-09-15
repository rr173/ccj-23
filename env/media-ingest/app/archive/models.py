"""Persistence model for the archive subsystem.

Everything that governs a sealed object's lifetime lives here:

* ``archive_policies``       — versioned, immutable retention policies; every
                               sealed version snapshots the revision in force at
                               seal time and never changes it.
* ``archived_versions``      — one row per (tenant, object, version). Permissions
                               and lifecycle are per row, even when two rows point
                               at the same physical content.
* ``content_blobs``          — content-addressed physical storage (SHA-256). A
                               refcount of active version rows backs dedup.
* ``legal_holds``            — append/release legal holds; any active hold blocks
                               logical deletion regardless of retention expiry.
* ``object_pins``            — other persistent references (external links,
                               snapshots). They are an explicit third blocker.
* ``archive_delete_ops``     — the durable state machine of one delete request.
* ``deletion_certificates``  — tamper-evident, hash-chained proofs of deletion.
* ``archive_audit_events``   — append-only audit log (holds, pins, delete flow).
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
    text,
)
from sqlalchemy.orm import Mapped, mapped_column

from ..db import Base

# Blob lifecycle:
#   active        file present, referenced by >= 1 active version
#   pending_purge file present, refcount just hit 0; waiting for physical unlink
#   purged        file unlinked; content provably gone
#   retained_shared never materialises as a state: the blob simply stays active.
BLOB_ACTIVE = "active"
BLOB_PENDING_PURGE = "pending_purge"
BLOB_PURGED = "purged"

# Delete operation state machine (each transition is its own transaction):
#   logical_deleted -> refs_released -> finalized
OP_LOGICAL_DELETED = "logical_deleted"
OP_REFS_RELEASED = "refs_released"
OP_FINALIZED = "finalized"


class ArchivePolicy(Base):
    """Immutable retention policy revision. Publishing creates a new row with an
    incremented version; sealed versions keep the revision they were sealed under,
    so policy changes only affect objects sealed afterwards."""

    __tablename__ = "archive_policies"

    tenant_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    version: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=False)
    retention_seconds: Mapped[int] = mapped_column(BigInteger)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    published_at: Mapped[float] = mapped_column(Float)


class ArchivedVersion(Base):
    """A sealed (tenant, object, version). A version is either downloadable
    (state='active') or an irreversible tombstone (state='tombstoned').

    ``content_sha256`` may be shared across any number of rows — including rows of
    other tenants — while access control (tenant_id) and lifecycle (state, holds,
    retention snapshot) stay per row. ``refs_released_at`` is the exactly-once
    marker for the physical-refcount release phase of a delete."""

    __tablename__ = "archived_versions"

    tenant_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    object_id: Mapped[str] = mapped_column(String(256), primary_key=True)
    version: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=False)
    size: Mapped[int] = mapped_column(BigInteger)
    content_sha256: Mapped[str] = mapped_column(String(64), index=True)
    # Retention policy snapshot taken at seal time — never updated afterwards.
    policy_version: Mapped[int] = mapped_column(Integer)
    retention_seconds: Mapped[int] = mapped_column(BigInteger)
    sealed_at: Mapped[float] = mapped_column(Float)
    state: Mapped[str] = mapped_column(String(16), default="active", index=True)
    # Id of the delete operation that tombstoned the row (NULL while active).
    delete_op_id: Mapped[str | None] = mapped_column(String(32), nullable=True, index=True)
    tombstoned_at: Mapped[float | None] = mapped_column(Float, nullable=True)
    # Set exactly once, when this version's physical reference is released.
    refs_released_at: Mapped[float | None] = mapped_column(Float, nullable=True)

    __table_args__ = (
        Index("ix_archived_versions_blob", "content_sha256", "state"),
    )


class ContentBlob(Base):
    """One physical copy of a byte sequence, identified by its SHA-256. ``refcount``
    is the number of *active* version rows referring to it; it must never be
    negative because releases only happen through a conditional decrement that
    also marks the row's refs_released_at exactly once."""

    __tablename__ = "content_blobs"

    sha256: Mapped[str] = mapped_column(String(64), primary_key=True)
    size: Mapped[int] = mapped_column(BigInteger)
    path: Mapped[str] = mapped_column(Text)
    refcount: Mapped[int] = mapped_column(Integer, default=0)
    state: Mapped[str] = mapped_column(String(16), default=BLOB_ACTIVE)
    created_at: Mapped[float] = mapped_column(Float)
    purged_at: Mapped[float | None] = mapped_column(Float, nullable=True)


class LegalHold(Base):
    """A legal hold. Multiple named holds may coexist; the object stays blocked
    until the LAST active hold is released. Rows are never deleted: releasing
    appends released_at so the full hold-state history is provable."""

    __tablename__ = "legal_holds"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    tenant_id: Mapped[str] = mapped_column(String(64), index=True)
    object_id: Mapped[str] = mapped_column(String(256), index=True)
    hold_key: Mapped[str] = mapped_column(String(128))
    reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    placed_at: Mapped[float] = mapped_column(Float)
    released_at: Mapped[float | None] = mapped_column(Float, nullable=True, index=True)

    __table_args__ = (
        Index(
            "uq_legal_holds_active_key",
            "tenant_id",
            "object_id",
            "hold_key",
            unique=True,
            sqlite_where=text("released_at IS NULL"),
        ),
        Index("ix_legal_holds_object", "tenant_id", "object_id", "released_at"),
    )


class ObjectPin(Base):
    """Any persistent reference to an archived version other than a legal hold
    (external catalog links, snapshots, derived-artifact links...). Like holds
    they are named and block logical deletion; unlike content dedup sharing,
    this is a same-object reference the owner must drop explicitly."""

    __tablename__ = "object_pins"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    tenant_id: Mapped[str] = mapped_column(String(64), index=True)
    object_id: Mapped[str] = mapped_column(String(256), index=True)
    version: Mapped[int] = mapped_column(Integer)
    pin_key: Mapped[str] = mapped_column(String(128))
    kind: Mapped[str] = mapped_column(String(64), default="reference")
    reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[float] = mapped_column(Float)
    removed_at: Mapped[float | None] = mapped_column(Float, nullable=True, index=True)

    __table_args__ = (
        Index(
            "uq_object_pins_active_key",
            "tenant_id",
            "object_id",
            "version",
            "pin_key",
            unique=True,
            sqlite_where=text("removed_at IS NULL"),
        ),
    )


class ArchiveDeleteOp(Base):
    """Durable state machine record for one delete request. The request is
    idempotent on (tenant, object, version, request_key): concurrent or retried
    deletes all converge on this single op, hence a single refcount change and a
    single certificate. Blob transitions are recorded in-place so crash recovery
    can resume exactly where the process died."""

    __tablename__ = "archive_delete_ops"

    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String(64), index=True)
    object_id: Mapped[str] = mapped_column(String(256), index=True)
    version: Mapped[int] = mapped_column(Integer)
    request_key: Mapped[str | None] = mapped_column(String(128), nullable=True)
    state: Mapped[str] = mapped_column(String(24), default=OP_LOGICAL_DELETED, index=True)
    content_sha256: Mapped[str] = mapped_column(String(64))
    policy_version: Mapped[int] = mapped_column(Integer)
    retention_seconds: Mapped[int] = mapped_column(BigInteger)
    sealed_at: Mapped[float] = mapped_column(Float)
    retention_expires_at: Mapped[float] = mapped_column(Float)
    # Snapshot of hold/pin state at logical-delete time.
    active_holds: Mapped[str] = mapped_column(Text, default="[]")
    active_pins: Mapped[str] = mapped_column(Text, default="[]")
    logical_deleted_at: Mapped[float] = mapped_column(Float)
    refs_released_at: Mapped[float | None] = mapped_column(Float, nullable=True)
    # Physical outcome of the shared blob: retained (other refs live) or purged.
    blob_physical_result: Mapped[str | None] = mapped_column(String(32), nullable=True)
    blob_purged_at: Mapped[float | None] = mapped_column(Float, nullable=True)
    remaining_active_refs: Mapped[int | None] = mapped_column(Integer, nullable=True)
    finalized_at: Mapped[float | None] = mapped_column(Float, nullable=True)
    certificate_id: Mapped[str | None] = mapped_column(String(32), nullable=True)
    # De-duplicates the refcount release: guards the conditional decrement.
    release_attempted: Mapped[bool] = mapped_column(Boolean, default=False)

    __table_args__ = (
        Index(
            "uq_archive_delete_request",
            "tenant_id",
            "object_id",
            "version",
            "request_key",
            unique=True,
            sqlite_where=text("request_key IS NOT NULL"),
        ),
    )


class DeletionCertificate(Base):
    """Immutable proof of deletion. Rows are inserted once and never updated or
    deleted. ``record_hash`` chains each certificate to the previous one (global
    append order) and ``signature`` is HMAC-SHA256 over the canonical payload, so
    any retroactive edit — row content or chain order — is detectable."""

    __tablename__ = "deletion_certificates"

    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    seq: Mapped[int] = mapped_column(Integer, unique=True, autoincrement=False)
    tenant_id: Mapped[str] = mapped_column(String(64), index=True)
    object_id: Mapped[str] = mapped_column(String(256), index=True)
    version: Mapped[int] = mapped_column(Integer)
    delete_op_id: Mapped[str] = mapped_column(String(32), unique=True, index=True)
    payload_json: Mapped[str] = mapped_column(Text)
    prev_record_hash: Mapped[str | None] = mapped_column(String(64), nullable=True)
    record_hash: Mapped[str] = mapped_column(String(64), unique=True)
    signature: Mapped[str] = mapped_column(String(64))
    created_at: Mapped[float] = mapped_column(Float, index=True)


class ArchiveAuditEvent(Base):
    """Append-only audit trail for the archive subsystem."""

    __tablename__ = "archive_audit_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    tenant_id: Mapped[str] = mapped_column(String(64), index=True)
    object_id: Mapped[str | None] = mapped_column(String(256), nullable=True, index=True)
    version: Mapped[int | None] = mapped_column(Integer, nullable=True)
    event_type: Mapped[str] = mapped_column(String(48))
    detail_json: Mapped[str] = mapped_column(Text, default="{}")
    created_at: Mapped[float] = mapped_column(Float)
