"""Archive service: retention, legal holds, content-dedup references and
provable deletion.

Design in one screen
--------------------

* **Policy versions.** Publishing a retention policy appends an immutable row.
  seal_version() snapshots (policy_version, retention_seconds) onto the version
  row; old objects are governed by the revision they were sealed under forever.

* **Deletion blockers.** A version is deletable iff (1) its retention window has
  elapsed, (2) it has no active legal hold, (3) it has no active pin (other
  persistent reference). eligibility() returns every blocker with its reason, so
  callers can see exactly what is in the way.

* **Dedup references.** Bytes are content-addressed (content_blobs keyed by
  SHA-256); any number of versions — across tenants — share one physical copy.
  Deleting a version only drops *its* reference: the blob is physically purged
  when the last active reference goes, and never before. Cross-version sharing is
  therefore unaffected by deleting one reference.

* **Multi-phase, crash-safe delete.** The delete state machine persists each
  phase in its own transaction:

      1. logical delete     version -> tombstoned (downloads die at once)
      2. release references exactly-once conditional refcount decrement; the
                            last reference moves the blob -> pending_purge
      3. purge + finalize   unlink blob when it was the last reference, then mint
                            the certificate

  resume() on startup continues any non-finalized op and purges any
  pending_purge blob. A tombstoned row is never reverted, so downloads can never
  resurrect; a blob with remaining references is never purged.

* **Concurrency.** Writers take a process lock plus BEGIN IMMEDIATE (one global
  writer for SQLite). Concurrent/duplicate deletes converge on the same
  archive_delete_ops row, so there is exactly one refcount change and one proof.

* **Proofs.** Certificates are append-only and hash-chained (each row signs the
  previous hash) with HMAC-SHA256 over canonical JSON — tampering is detectable.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import threading
import time
import uuid
from collections.abc import Callable

from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from ..db import make_engine, make_session_factory
from . import models as m
from .content_store import ContentStore
from .errors import (
    DeletionBlocked,
    NotFound,
    RequestKeyConflict,
    RetentionPolicyMissing,
    VersionExists,
)


def _new_id() -> str:
    return uuid.uuid4().hex


class ArchiveService:
    def __init__(
        self,
        database_url: str,
        data_dir: str,
        *,
        proof_key: bytes | str = b"dev-proof-key",
        clock: Callable[[], float] = time.time,
        init_schema: bool = True,
    ):
        self.engine = make_engine(database_url)
        if init_schema:
            from ..db import init_db

            init_db(self.engine)
        self.sf = make_session_factory(self.engine)
        self.store = ContentStore(data_dir)
        if isinstance(proof_key, str):
            proof_key = proof_key.encode()
        self.proof_key = proof_key
        self.clock = clock
        # Serialises in-process writers; BEGIN IMMEDIATE serialises across
        # processes. A re-entrant lock lets helpers call helpers.
        self._lock = threading.RLock()
        # Test hook: crash(when) is called between durable phases. ``when`` is a
        # dotted string such as "delete.after_logical_delete". A hook raising /
        # os._exit() simulates an abnormal process exit with everything committed
        # up to that crash point.
        self.crash: Callable[[str], None] | None = None

    # ---------------------------------------------------------------- helpers

    def _crash(self, when: str) -> None:
        if self.crash is not None:
            self.crash(when)

    def _audit(
        self,
        s: Session,
        tenant_id: str,
        event_type: str,
        *,
        object_id: str | None = None,
        version: int | None = None,
        detail: dict | None = None,
    ) -> None:
        s.add(
            m.ArchiveAuditEvent(
                tenant_id=tenant_id,
                object_id=object_id,
                version=version,
                event_type=event_type,
                detail_json=json.dumps(detail or {}, sort_keys=True),
                created_at=self.clock(),
            )
        )

    def _get_version(
        self, s: Session, tenant_id: str, object_id: str, version: int
    ) -> m.ArchivedVersion:
        v = s.get(m.ArchivedVersion, (tenant_id, object_id, version))
        if v is None:
            raise NotFound(f"{tenant_id}/{object_id}/v{version}")
        return v

    def _active_holds(self, s: Session, tenant_id: str, object_id: str) -> list[m.LegalHold]:
        return list(
            s.scalars(
                select(m.LegalHold).where(
                    m.LegalHold.tenant_id == tenant_id,
                    m.LegalHold.object_id == object_id,
                    m.LegalHold.released_at.is_(None),
                )
            )
        )

    def _active_pins(
        self, s: Session, tenant_id: str, object_id: str, version: int
    ) -> list[m.ObjectPin]:
        return list(
            s.scalars(
                select(m.ObjectPin).where(
                    m.ObjectPin.tenant_id == tenant_id,
                    m.ObjectPin.object_id == object_id,
                    m.ObjectPin.version == version,
                    m.ObjectPin.removed_at.is_(None),
                )
            )
        )

    def _blockers(self, s: Session, v: m.ArchivedVersion, now: float) -> list[dict]:
        """Evaluate every reason that currently blocks logical deletion."""
        blockers: list[dict] = []
        expires_at = v.sealed_at + v.retention_seconds
        if now < expires_at:
            blockers.append(
                {
                    "reason": "retention",
                    "detail": "retention period has not elapsed",
                    "policy_version": v.policy_version,
                    "sealed_at": v.sealed_at,
                    "retention_seconds": v.retention_seconds,
                    "expires_at": expires_at,
                    "now": now,
                    "remaining_seconds": expires_at - now,
                }
            )
        holds = self._active_holds(s, v.tenant_id, v.object_id)
        if holds:
            blockers.append(
                {
                    "reason": "legal_hold",
                    "detail": f"{len(holds)} active legal hold(s)",
                    "holds": [
                        {"hold_key": h.hold_key, "reason": h.reason, "placed_at": h.placed_at}
                        for h in holds
                    ],
                }
            )
        pins = self._active_pins(s, v.tenant_id, v.object_id, v.version)
        if pins:
            blockers.append(
                {
                    "reason": "reference",
                    "detail": f"{len(pins)} active reference(s) still pin this version",
                    "pins": [
                        {"pin_key": p.pin_key, "kind": p.kind, "reason": p.reason}
                        for p in pins
                    ],
                }
            )

        # Active derivation jobs pin exact source versions for the whole life
        # of the task (acceptance -> billing/cancel/fail). A delete must fail
        # while any job may still need the bytes; the blocker names the jobs.
        # The derivation tables are optional: a standalone archive-only schema
        # (subsystem tests, minimal deployments) simply has no such pins.
        # NOTE: the existence probe must run on the session's OWN connection —
        # opening a second pooled connection here would deadlock against the
        # BEGIN IMMEDIATE write lock this session already holds on SQLite.
        if self._has_table(s, "derivation_protections"):
            from ..derive import models as dm

            protections = list(
                s.scalars(
                    select(dm.DerivationProtection).where(
                        dm.DerivationProtection.tenant_id == v.tenant_id,
                        dm.DerivationProtection.object_id == v.object_id,
                        dm.DerivationProtection.version == v.version,
                        dm.DerivationProtection.released_at.is_(None),
                    )
                )
            )
            if protections:
                blockers.append(
                    {
                        "reason": "reference",
                        "detail": (
                            f"{len(protections)} active derivation job(s) reference "
                            "this version as a fixed source"
                        ),
                        "derivations": [
                            {"job_id": p.job_id, "pinned_at": p.created_at}
                            for p in protections
                        ],
                    }
                )
        return blockers

    def _has_table(self, s, table_name: str) -> bool:
        """Schema-existence probe that reuses the session's connection (so it
        cannot deadlock against the session's own write lock). Result is cached
        per engine: the schema never disappears within a process."""
        engine = s.bind
        cache = getattr(self, "_table_presence", None)
        if cache is None:
            cache = self._table_presence = {}
        key = (engine.url.render_as_string(hide_password=False), table_name)
        if key in cache:
            return cache[key]
        conn = s.connection()
        if engine.dialect.name == "sqlite":
            row = conn.exec_driver_sql(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                (table_name,),
            ).first()
        else:
            row = conn.exec_driver_sql(
                "SELECT 1 FROM information_schema.tables WHERE table_name=%s",
                (table_name,),
            ).first()
        present = row is not None
        cache[key] = present
        return present

    # ---------------------------------------------------------------- policy

    def publish_policy(
        self,
        tenant_id: str,
        retention_seconds: int,
        *,
        description: str | None = None,
    ) -> dict:
        """Append a new immutable retention policy revision for a tenant."""
        if retention_seconds < 0:
            raise ValueError("retention_seconds must be >= 0")
        with self._lock, self.sf() as s:
            current = s.scalar(
                select(func.max(m.ArchivePolicy.version)).where(
                    m.ArchivePolicy.tenant_id == tenant_id
                )
            )
            version = (current or 0) + 1
            s.add(
                m.ArchivePolicy(
                    tenant_id=tenant_id,
                    version=version,
                    retention_seconds=retention_seconds,
                    description=description,
                    published_at=self.clock(),
                )
            )
            self._audit(
                s,
                tenant_id,
                "policy.published",
                detail={"version": version, "retention_seconds": retention_seconds},
            )
            s.commit()
            return {
                "tenant_id": tenant_id,
                "version": version,
                "retention_seconds": retention_seconds,
                "description": description,
            }

    def get_policy(self, tenant_id: str, version: int | None = None) -> dict:
        with self.sf() as s:
            p = self._load_policy(s, tenant_id, version)
            return {
                "tenant_id": tenant_id,
                "version": p.version,
                "retention_seconds": p.retention_seconds,
                "description": p.description,
                "published_at": p.published_at,
            }

    def _load_policy(
        self, s: Session, tenant_id: str, version: int | None = None
    ) -> m.ArchivePolicy:
        if version is not None:
            p = s.get(m.ArchivePolicy, (tenant_id, version))
            if p is None:
                raise RetentionPolicyMissing(
                    f"tenant {tenant_id} policy v{version} not found"
                )
            return p
        current = s.scalars(
            select(m.ArchivePolicy)
            .where(m.ArchivePolicy.tenant_id == tenant_id)
            .order_by(m.ArchivePolicy.version.desc())
            .limit(1)
        ).first()
        if current is None:
            raise RetentionPolicyMissing(f"tenant {tenant_id} has no retention policy")
        return current

    def list_policies(self, tenant_id: str) -> list[dict]:
        with self.sf() as s:
            rows = s.scalars(
                select(m.ArchivePolicy)
                .where(m.ArchivePolicy.tenant_id == tenant_id)
                .order_by(m.ArchivePolicy.version)
            ).all()
            return [
                {
                    "version": p.version,
                    "retention_seconds": p.retention_seconds,
                    "description": p.description,
                    "published_at": p.published_at,
                }
                for p in rows
            ]

    # ---------------------------------------------------------------- sealing

    def seal_version(
        self,
        tenant_id: str,
        object_id: str,
        data: bytes,
        *,
        version: int | None = None,
    ) -> dict:
        """Seal one immutable (tenant, object, version) under the policy revision
        currently in force. Identical content is physically stored once; the blob
        gains one reference per sealed version."""
        sha256, size, _ = self.store.put(data)
        with self._lock, self.sf() as s:
            policy = self._load_policy(s, tenant_id)
            now = self.clock()
            if version is None:
                latest = s.scalar(
                    select(func.max(m.ArchivedVersion.version)).where(
                        m.ArchivedVersion.tenant_id == tenant_id,
                        m.ArchivedVersion.object_id == object_id,
                    )
                )
                version = (latest or 0) + 1
            else:
                existing = s.get(m.ArchivedVersion, (tenant_id, object_id, version))
                if existing is not None:
                    raise VersionExists(f"{tenant_id}/{object_id}/v{version}")

            blob = s.get(m.ContentBlob, sha256)
            if blob is None:
                # First physical reference anywhere (any tenant).
                blob = m.ContentBlob(
                    sha256=sha256,
                    size=size,
                    path=str(self.store.blob_path(sha256)),
                    refcount=1,
                    state=m.BLOB_ACTIVE,
                    created_at=now,
                )
                s.add(blob)
            else:
                # Content identical to an existing blob: share the physical copy.
                # A purged (content-addressed) row can only exist if an old
                # tombstoned history lingered; new content with the same hash
                # simply resurrects it as a fresh physical blob.
                if blob.state == m.BLOB_PURGED:
                    blob.state = m.BLOB_ACTIVE
                    blob.purged_at = None
                    blob.path = str(self.store.blob_path(sha256))
                blob.refcount += 1

            v = m.ArchivedVersion(
                tenant_id=tenant_id,
                object_id=object_id,
                version=version,
                size=size,
                content_sha256=sha256,
                policy_version=policy.version,
                retention_seconds=policy.retention_seconds,
                sealed_at=now,
                state="active",
            )
            s.add(v)
            self._audit(
                s,
                tenant_id,
                "version.sealed",
                object_id=object_id,
                version=version,
                detail={
                    "content_sha256": sha256,
                    "size": size,
                    "policy_version": policy.version,
                    "refcount": blob.refcount,
                },
            )
            s.commit()
            return {
                "tenant_id": tenant_id,
                "object_id": object_id,
                "version": version,
                "content_sha256": sha256,
                "size": size,
                "policy_version": policy.version,
                "retention_seconds": policy.retention_seconds,
                "refcount": blob.refcount,
                "sealed_at": now,
            }

    def download(
        self, tenant_id: str, object_id: str, version: int
    ) -> tuple[bytes, dict]:
        """Return bytes for an active version. Tombstones and foreign-tenant rows
        are indistinguishable 404s; a mid-delete crash leaves the row tombstoned,
        so the object can never come back online."""
        with self.sf() as s:
            v = s.get(m.ArchivedVersion, (tenant_id, object_id, version))
            if v is None or v.state != "active":
                raise NotFound(f"{tenant_id}/{object_id}/v{version}")
            sha = v.content_sha256
            meta = {
                "content_sha256": sha,
                "size": v.size,
                "policy_version": v.policy_version,
            }
        try:
            return self.store.read(sha), meta
        except FileNotFoundError:
            # Metadata says active but the physical copy is gone. That must not
            # happen (a shared blob is purged only after the last reference), so
            # surface it loudly rather than returning torn data.
            raise RuntimeError("physical blob missing while version is active")

    def get_version(self, tenant_id: str, object_id: str, version: int) -> dict:
        with self.sf() as s:
            v = self._get_version(s, tenant_id, object_id, version)
            blob = s.get(m.ContentBlob, v.content_sha256)
            return {
                "tenant_id": tenant_id,
                "object_id": object_id,
                "version": version,
                "state": v.state,
                "size": v.size,
                "content_sha256": v.content_sha256,
                "policy_version": v.policy_version,
                "retention_seconds": v.retention_seconds,
                "sealed_at": v.sealed_at,
                "retention_expires_at": v.sealed_at + v.retention_seconds,
                "tombstoned_at": v.tombstoned_at,
                "blob_state": blob.state if blob else None,
                "blob_refcount": blob.refcount if blob else None,
            }

    # ---------------------------------------------------------------- holds / refs

    def add_hold(
        self,
        tenant_id: str,
        object_id: str,
        hold_key: str,
        *,
        reason: str | None = None,
    ) -> dict:
        """Place a named legal hold. Holds are object-wide across all versions.
        Re-adding an active hold key is idempotent; adding a released key name
        again creates a new active hold (the history row stays)."""
        with self._lock, self.sf() as s:
            # The object must have at least one version (any state: a hold may be
            # relevant for the history too).
            present = s.scalar(
                select(func.count()).select_from(m.ArchivedVersion).where(
                    m.ArchivedVersion.tenant_id == tenant_id,
                    m.ArchivedVersion.object_id == object_id,
                )
            )
            if not present:
                raise NotFound(f"{tenant_id}/{object_id}")
            existing = s.scalars(
                select(m.LegalHold).where(
                    m.LegalHold.tenant_id == tenant_id,
                    m.LegalHold.object_id == object_id,
                    m.LegalHold.hold_key == hold_key,
                )
            ).all()
            active = next((h for h in existing if h.released_at is None), None)
            if active is not None:
                s.commit()
                return {
                    "hold_key": hold_key,
                    "state": "active",
                    "replayed": True,
                    "placed_at": active.placed_at,
                }
            now = self.clock()
            s.add(
                m.LegalHold(
                    tenant_id=tenant_id,
                    object_id=object_id,
                    hold_key=hold_key,
                    reason=reason,
                    placed_at=now,
                )
            )
            self._audit(
                s,
                tenant_id,
                "hold.placed",
                object_id=object_id,
                detail={"hold_key": hold_key, "reason": reason},
            )
            s.commit()
            return {"hold_key": hold_key, "state": "active", "placed_at": now}

    def release_hold(self, tenant_id: str, object_id: str, hold_key: str) -> dict:
        """Release one legal hold. The object becomes deletable only once the
        LAST active hold is gone. Releasing an already-released (or unknown) key
        is idempotent."""
        with self._lock, self.sf() as s:
            active = s.scalars(
                select(m.LegalHold).where(
                    m.LegalHold.tenant_id == tenant_id,
                    m.LegalHold.object_id == object_id,
                    m.LegalHold.hold_key == hold_key,
                    m.LegalHold.released_at.is_(None),
                )
            ).first()
            remaining = len(self._active_holds(s, tenant_id, object_id))
            if active is None:
                s.commit()
                return {
                    "hold_key": hold_key,
                    "state": "released",
                    "replayed": True,
                    "active_holds_remaining": remaining,
                }
            now = self.clock()
            active.released_at = now
            self._audit(
                s,
                tenant_id,
                "hold.released",
                object_id=object_id,
                detail={"hold_key": hold_key},
            )
            s.commit()
            return {
                "hold_key": hold_key,
                "state": "released",
                "released_at": now,
                "active_holds_remaining": remaining - 1,
            }

    def list_holds(self, tenant_id: str, object_id: str) -> list[dict]:
        with self.sf() as s:
            rows = s.scalars(
                select(m.LegalHold)
                .where(
                    m.LegalHold.tenant_id == tenant_id,
                    m.LegalHold.object_id == object_id,
                )
                .order_by(m.LegalHold.id)
            ).all()
            return [
                {
                    "hold_key": h.hold_key,
                    "reason": h.reason,
                    "placed_at": h.placed_at,
                    "released_at": h.released_at,
                    "state": "active" if h.released_at is None else "released",
                }
                for h in rows
            ]

    def add_pin(
        self,
        tenant_id: str,
        object_id: str,
        version: int,
        pin_key: str,
        *,
        kind: str = "reference",
        reason: str | None = None,
    ) -> dict:
        with self._lock, self.sf() as s:
            self._get_version(s, tenant_id, object_id, version)
            existing = s.scalars(
                select(m.ObjectPin).where(
                    m.ObjectPin.tenant_id == tenant_id,
                    m.ObjectPin.object_id == object_id,
                    m.ObjectPin.version == version,
                    m.ObjectPin.pin_key == pin_key,
                )
            ).all()
            active = next((p for p in existing if p.removed_at is None), None)
            if active is not None:
                s.commit()
                return {"pin_key": pin_key, "state": "active", "replayed": True}
            now = self.clock()
            s.add(
                m.ObjectPin(
                    tenant_id=tenant_id,
                    object_id=object_id,
                    version=version,
                    pin_key=pin_key,
                    kind=kind,
                    reason=reason,
                    created_at=now,
                )
            )
            self._audit(
                s,
                tenant_id,
                "pin.added",
                object_id=object_id,
                version=version,
                detail={"pin_key": pin_key, "kind": kind},
            )
            s.commit()
            return {"pin_key": pin_key, "state": "active"}

    def remove_pin(
        self, tenant_id: str, object_id: str, version: int, pin_key: str
    ) -> dict:
        with self._lock, self.sf() as s:
            active = s.scalars(
                select(m.ObjectPin).where(
                    m.ObjectPin.tenant_id == tenant_id,
                    m.ObjectPin.object_id == object_id,
                    m.ObjectPin.version == version,
                    m.ObjectPin.pin_key == pin_key,
                    m.ObjectPin.removed_at.is_(None),
                )
            ).first()
            if active is None:
                s.commit()
                return {"pin_key": pin_key, "state": "removed", "replayed": True}
            now = self.clock()
            active.removed_at = now
            self._audit(
                s,
                tenant_id,
                "pin.removed",
                object_id=object_id,
                version=version,
                detail={"pin_key": pin_key},
            )
            s.commit()
            return {"pin_key": pin_key, "state": "removed"}

    # ---------------------------------------------------------------- eligibility

    def eligibility(self, tenant_id: str, object_id: str, version: int) -> dict:
        """Deletion-eligibility query. The response names exactly what blocks
        deletion — retention, legal_hold, reference — and reports shared-content
        references separately: those never block the *logical* delete, they only
        mean the physical blob is retained when this version goes."""
        with self.sf() as s:
            v = self._get_version(s, tenant_id, object_id, version)
            now = self.clock()
            if v.state != "active":
                return {
                    "eligible": False,
                    "state": v.state,
                    "blockers": [
                        {"reason": "already_deleted", "detail": "version is a tombstone"}
                    ],
                    "delete_op_id": v.delete_op_id,
                }
            blockers = self._blockers(s, v, now)
            blob = s.get(m.ContentBlob, v.content_sha256)
            # Other active versions sharing this physical blob.
            others = s.scalar(
                select(func.count()).select_from(m.ArchivedVersion).where(
                    m.ArchivedVersion.content_sha256 == v.content_sha256,
                    m.ArchivedVersion.state == "active",
                    m.ArchivedVersion.tenant_id != v.tenant_id,
                )
            )
            same_tenant_other = s.scalar(
                select(func.count()).select_from(m.ArchivedVersion).where(
                    m.ArchivedVersion.content_sha256 == v.content_sha256,
                    m.ArchivedVersion.state == "active",
                    m.ArchivedVersion.tenant_id == v.tenant_id,
                    m.ArchivedVersion.object_id != v.object_id,
                )
            )
            other_versions = (blob.refcount - 1) if blob else 0
            return {
                "eligible": not blockers,
                "state": "active",
                "blockers": blockers,
                "primary_reason": blockers[0]["reason"] if blockers else None,
                "retention": {
                    "policy_version": v.policy_version,
                    "retention_seconds": v.retention_seconds,
                    "sealed_at": v.sealed_at,
                    "expires_at": v.sealed_at + v.retention_seconds,
                    "expired": now >= v.sealed_at + v.retention_seconds,
                },
                "active_holds": len(
                    self._active_holds(s, tenant_id, object_id)
                ),
                "active_pins": len(self._active_pins(s, tenant_id, object_id, version)),
                "shared_content": {
                    "content_sha256": v.content_sha256,
                    "total_active_refs": blob.refcount if blob else None,
                    "other_active_refs": max(0, other_versions),
                    "other_tenants_active_refs": int(others),
                    "other_objects_same_tenant_active_refs": int(same_tenant_other),
                    "physical_blob_deleted_on_this_delete": other_versions == 0,
                },
            }

    # ---------------------------------------------------------------- delete

    def delete_version(
        self,
        tenant_id: str,
        object_id: str,
        version: int,
        *,
        request_key: str | None = None,
    ) -> dict:
        """Delete one version, idempotently and crash-safely.

        Returns the certificate once the whole flow (incl. physical cleanup when
        applicable) has finished. Concurrent or repeated calls share one
        archive_delete_ops row and one certificate.
        """
        with self._lock:
            op_id = self._begin_delete(
                tenant_id, object_id, version, request_key=request_key
            )
            return self.advance_delete(op_id)

    def _find_existing_request(
        self,
        s: Session,
        tenant_id: str,
        object_id: str,
        version: int,
        request_key: str,
    ) -> m.ArchiveDeleteOp | None:
        return s.scalars(
            select(m.ArchiveDeleteOp).where(
                m.ArchiveDeleteOp.tenant_id == tenant_id,
                m.ArchiveDeleteOp.object_id == object_id,
                m.ArchiveDeleteOp.version == version,
                m.ArchiveDeleteOp.request_key == request_key,
            )
        ).first()

    def _begin_delete(
        self,
        tenant_id: str,
        object_id: str,
        version: int,
        *,
        request_key: str | None,
    ) -> str:
        """Phase 1: validate eligibility and flip the version to a tombstone in a
        single transaction. Returns the delete-op id. A tombstone is permanent."""
        now = self.clock()
        with self.sf() as s:
            v = self._get_version(s, tenant_id, object_id, version)

            if request_key is not None:
                prior = self._find_existing_request(
                    s, tenant_id, object_id, version, request_key
                )
                if prior is not None:
                    s.commit()
                    return prior.id

            if v.state != "active":
                # Already deleting/deleted: converge on the existing op.
                if v.delete_op_id is None:
                    raise RuntimeError("tombstone without delete op")
                if request_key is not None:
                    # A different explicit key on an already-deleted version would
                    # violate the unique request replay contract.
                    other = self._find_existing_request(
                        s, tenant_id, object_id, version, request_key
                    )
                    if other is None:
                        raise RequestKeyConflict(
                            "version already deleted under a different request"
                        )
                s.commit()
                return v.delete_op_id

            blockers = self._blockers(s, v, now)
            if blockers:
                raise DeletionBlocked(blockers)

            holds = self._active_holds(s, tenant_id, object_id)
            pins = self._active_pins(s, tenant_id, object_id, version)
            op = m.ArchiveDeleteOp(
                id=_new_id(),
                tenant_id=tenant_id,
                object_id=object_id,
                version=version,
                request_key=request_key,
                state=m.OP_LOGICAL_DELETED,
                content_sha256=v.content_sha256,
                policy_version=v.policy_version,
                retention_seconds=v.retention_seconds,
                sealed_at=v.sealed_at,
                retention_expires_at=v.sealed_at + v.retention_seconds,
                active_holds=json.dumps(
                    [
                        {"hold_key": h.hold_key, "reason": h.reason, "placed_at": h.placed_at}
                        for h in holds
                    ],
                    sort_keys=True,
                ),
                active_pins=json.dumps(
                    [
                        {"pin_key": p.pin_key, "kind": p.kind, "reason": p.reason}
                        for p in pins
                    ],
                    sort_keys=True,
                ),
                logical_deleted_at=now,
            )
            s.add(op)
            s.flush()  # assign op.id before referencing it

            v.state = "tombstoned"
            v.delete_op_id = op.id
            v.tombstoned_at = now

            self._audit(
                s,
                tenant_id,
                "delete.logical",
                object_id=object_id,
                version=version,
                detail={"op_id": op.id, "request_key": request_key},
            )
            try:
                s.commit()
            except IntegrityError:
                # A concurrent request with the same key won the unique index.
                s.rollback()
                with self.sf() as s2:
                    racer = self._find_existing_request(
                        s2, tenant_id, object_id, version, request_key
                    )
                    if racer is not None:
                        return racer.id
                raise
            return op.id

    def advance_delete(self, op_id: str) -> dict:
        """Run phases 2 and 3 to completion, resuming idempotently. Each phase is
        one transaction; the physical unlink sits between durable state flips."""
        self._release_refs(op_id)
        self._crash("delete.after_refs_released")
        return self._purge_and_finalize(op_id)

    def _release_refs(self, op_id: str) -> None:
        """Phase 2: release this version's one physical reference exactly once.

        The decrement is a single conditional UPDATE guarded by the version's
        refs_released_at marker: it cannot fire twice under retries, crashes or
        concurrent duplicate deletes, and it can never drive refcount negative."""
        with self.sf() as s:
            op = s.get(m.ArchiveDeleteOp, op_id)
            if op is None:
                raise NotFound(f"delete op {op_id}")
            if op.state in (m.OP_REFS_RELEASED, m.OP_FINALIZED):
                s.commit()
                return

            v = self._get_version(s, op.tenant_id, op.object_id, op.version)
            now = self.clock()
            if v.refs_released_at is None:
                # Conditional, guarded decrement: only an active blob with > 0
                # refs is decremented. rowcount != 1 means invariants are broken.
                cur = s.execute(
                    m.ContentBlob.__table__.update()
                    .where(
                        m.ContentBlob.sha256 == op.content_sha256,
                        m.ContentBlob.state == m.BLOB_ACTIVE,
                        m.ContentBlob.refcount > 0,
                    )
                    .values(refcount=m.ContentBlob.refcount - 1)
                )
                if cur.rowcount != 1:
                    raise RuntimeError(
                        f"cannot release reference for blob {op.content_sha256}: "
                        "not active or refcount already 0"
                    )
                v.refs_released_at = now
                s.flush()
                blob = s.get(m.ContentBlob, op.content_sha256)
                if blob.refcount == 0:
                    # Last valid reference: the blob becomes unlinkable to new
                    # state changes; physical unlink happens in phase 3 (and is
                    # resumed if the process dies first).
                    blob.state = m.BLOB_PENDING_PURGE
                self._audit(
                    s,
                    op.tenant_id,
                    "delete.refs_released",
                    object_id=op.object_id,
                    version=op.version,
                    detail={
                        "op_id": op.id,
                        "content_sha256": op.content_sha256,
                        "refcount_after": blob.refcount,
                        "pending_purge": blob.refcount == 0,
                    },
                )

            op.state = m.OP_REFS_RELEASED
            op.refs_released_at = now
            op.release_attempted = True
            s.commit()

    def _purge_and_finalize(self, op_id: str) -> dict:
        """Phase 3: physical cleanup of a last-reference blob, then the proof."""
        with self.sf() as s:
            op = s.get(m.ArchiveDeleteOp, op_id)
            if op is None:
                raise NotFound(f"delete op {op_id}")
            if op.state == m.OP_LOGICAL_DELETED:
                s.commit()
                self._release_refs(op_id)
                return self._purge_and_finalize(op_id)
            if op.state == m.OP_FINALIZED:
                cert = s.get(m.DeletionCertificate, op.certificate_id)
                s.commit()
                return self._cert_summary(cert, already_finalized=True)

            blob = s.get(m.ContentBlob, op.content_sha256)
            now = self.clock()

            if blob.state == m.BLOB_PENDING_PURGE:
                # Durably claim the unlink so that even a crash between the DB
                # flip and os.unlink is recoverable: resume() retries the unlink.
                blob.state = "purging"
                s.commit()
                self._crash("delete.before_physical_unlink")
                self.store.purge(op.content_sha256)
                self._crash("delete.after_physical_unlink")
                with self.sf() as s2:
                    blob2 = s2.get(m.ContentBlob, op.content_sha256)
                    blob2.state = m.BLOB_PURGED
                    blob2.purged_at = now
                    op2 = s2.get(m.ArchiveDeleteOp, op_id)
                    op2.blob_physical_result = "purged"
                    op2.blob_purged_at = now
                    op2.remaining_active_refs = 0
                    self._audit(
                        s2,
                        op2.tenant_id,
                        "delete.physical_purged",
                        object_id=op2.object_id,
                        version=op2.version,
                        detail={"op_id": op_id, "content_sha256": op2.content_sha256},
                    )
                    s2.commit()
                return self._finalize_certificate(
                    op_id, physical_result="purged", remaining_refs=0
                )

            if blob.state == "purging":
                # Crashed between claiming and confirming the unlink: finish it.
                s.commit()
                self.store.purge(op.content_sha256)
                with self.sf() as s2:
                    blob2 = s2.get(m.ContentBlob, op.content_sha256)
                    blob2.state = m.BLOB_PURGED
                    blob2.purged_at = blob2.purged_at or now
                    op2 = s2.get(m.ArchiveDeleteOp, op_id)
                    op2.blob_physical_result = "purged"
                    op2.blob_purged_at = blob2.purged_at
                    op2.remaining_active_refs = 0
                    s2.commit()
                return self._finalize_certificate(
                    op_id, physical_result="purged", remaining_refs=0
                )

            if blob.state == m.BLOB_PURGED:
                s.commit()
                return self._finalize_certificate(
                    op_id, physical_result="purged", remaining_refs=0
                )

            # state == active: other versions still share the content. The
            # physical copy is retained; record the honest "retained_shared"
            # outcome and mint the proof.
            remaining = blob.refcount
            op.blob_physical_result = "retained_shared"
            op.remaining_active_refs = remaining
            s.commit()
            return self._finalize_certificate(
                op_id,
                physical_result="retained_shared",
                remaining_refs=remaining,
            )

    # ---------------------------------------------------------------- proofs

    def _finalize_certificate(
        self, op_id: str, *, physical_result: str, remaining_refs: int
    ) -> dict:
        """Insert the immutable, hash-chained, HMAC-signed certificate and mark
        the op finalized. Exactly-once: a finalized op already has cert id."""
        with self.sf() as s:
            op = s.get(m.ArchiveDeleteOp, op_id)
            if op.state == m.OP_FINALIZED:
                cert = s.get(m.DeletionCertificate, op.certificate_id)
                s.commit()
                return self._cert_summary(cert, already_finalized=True)
            v = self._get_version(s, op.tenant_id, op.object_id, op.version)
            now = self.clock()

            last = s.scalars(
                select(m.DeletionCertificate).order_by(
                    m.DeletionCertificate.seq.desc()
                ).limit(1)
            ).first()
            seq = (last.seq + 1) if last else 1
            prev_hash = last.record_hash if last else None

            payload = {
                "object": {
                    "tenant_id": op.tenant_id,
                    "object_id": op.object_id,
                    "version": op.version,
                    "content_sha256": op.content_sha256,
                    "size": v.size,
                },
                "policy": {
                    "policy_version": op.policy_version,
                    "retention_seconds": op.retention_seconds,
                    "sealed_at": op.sealed_at,
                    "retention_expires_at": op.retention_expires_at,
                },
                "holds_at_deletion": json.loads(op.active_holds),
                "pins_at_deletion": json.loads(op.active_pins),
                "logical_deletion": {
                    "deleted_at": op.logical_deleted_at,
                    "refs_released_at": op.refs_released_at,
                },
                "physical_cleanup": {
                    "result": physical_result,
                    "remaining_active_refs": remaining_refs,
                    "purged_at": op.blob_purged_at,
                },
                "finalized_at": now,
                "delete_op_id": op.id,
                "seq": seq,
                "prev_record_hash": prev_hash,
            }
            canonical = json.dumps(
                payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False
            ).encode("utf-8")
            record_hash = hashlib.sha256(prev_hash.encode() + canonical if prev_hash else canonical).hexdigest()
            signature = hmac.new(
                self.proof_key, record_hash.encode(), hashlib.sha256
            ).hexdigest()

            cert = m.DeletionCertificate(
                id=_new_id(),
                seq=seq,
                tenant_id=op.tenant_id,
                object_id=op.object_id,
                version=op.version,
                delete_op_id=op.id,
                payload_json=canonical.decode("utf-8"),
                prev_record_hash=prev_hash,
                record_hash=record_hash,
                signature=signature,
                created_at=now,
            )
            s.add(cert)
            s.flush()
            op.state = m.OP_FINALIZED
            op.finalized_at = now
            op.certificate_id = cert.id
            self._audit(
                s,
                op.tenant_id,
                "delete.certificate",
                object_id=op.object_id,
                version=op.version,
                detail={
                    "op_id": op.id,
                    "certificate_id": cert.id,
                    "physical_result": physical_result,
                },
            )
            s.commit()
            return self._cert_summary(cert)

    def _cert_summary(self, cert: m.DeletionCertificate, *, already_finalized: bool = False) -> dict:
        return {
            "certificate_id": cert.id,
            "seq": cert.seq,
            "tenant_id": cert.tenant_id,
            "object_id": cert.object_id,
            "version": cert.version,
            "delete_op_id": cert.delete_op_id,
            "record_hash": cert.record_hash,
            "created_at": cert.created_at,
            "physical_result": json.loads(cert.payload_json)["physical_cleanup"]["result"],
            "replayed": already_finalized,
        }

    def get_certificate(self, certificate_id: str, *, tenant_id: str | None = None) -> dict:
        with self.sf() as s:
            cert = s.get(m.DeletionCertificate, certificate_id)
            if cert is None:
                raise NotFound(f"certificate {certificate_id}")
            if tenant_id is not None and cert.tenant_id != tenant_id:
                # Certificates of other tenants do not exist for you.
                raise NotFound(f"certificate {certificate_id}")
            return self._cert_dict(s, cert)

    def get_certificate_for_object(
        self, tenant_id: str, object_id: str, version: int
    ) -> dict:
        with self.sf() as s:
            cert = s.scalars(
                select(m.DeletionCertificate).where(
                    m.DeletionCertificate.tenant_id == tenant_id,
                    m.DeletionCertificate.object_id == object_id,
                    m.DeletionCertificate.version == version,
                )
            ).first()
            if cert is None:
                raise NotFound(f"certificate for {tenant_id}/{object_id}/v{version}")
            return self._cert_dict(s, cert)

    def _cert_dict(self, s: Session, cert: m.DeletionCertificate) -> dict:
        payload = json.loads(cert.payload_json)
        return {
            "certificate_id": cert.id,
            "seq": cert.seq,
            "tenant_id": cert.tenant_id,
            "object_id": cert.object_id,
            "version": cert.version,
            "delete_op_id": cert.delete_op_id,
            "payload": payload,
            "prev_record_hash": cert.prev_record_hash,
            "record_hash": cert.record_hash,
            "signature": cert.signature,
            "created_at": cert.created_at,
            "chain_valid": self._verify_cert(s, cert),
        }

    def list_certificates(
        self, tenant_id: str, *, limit: int = 100, offset: int = 0
    ) -> list[dict]:
        with self.sf() as s:
            rows = s.scalars(
                select(m.DeletionCertificate)
                .where(m.DeletionCertificate.tenant_id == tenant_id)
                .order_by(m.DeletionCertificate.seq)
                .offset(offset)
                .limit(limit)
            ).all()
            return [self._cert_dict(s, c) for c in rows]

    def verify_chain(self, *, tenant_id: str | None = None) -> dict:
        """Verify the tamper-evident chain (optionally one tenant's slice). Every
        stored signature must verify and every link must hash to the next record."""
        with self.sf() as s:
            q = select(m.DeletionCertificate).order_by(m.DeletionCertificate.seq)
            if tenant_id is not None:
                q = q.where(m.DeletionCertificate.tenant_id == tenant_id)
            certs = list(s.scalars(q))
            verified = 0
            for c in certs:
                if not self._verify_cert(s, c):
                    return {"valid": False, "failed_at_seq": c.seq, "verified": verified}
                verified += 1
            return {"valid": True, "verified": verified}

    def _verify_cert(self, s: Session, cert: m.DeletionCertificate) -> bool:
        expected_sig = hmac.new(
            self.proof_key, cert.record_hash.encode(), hashlib.sha256
        ).hexdigest()
        if not hmac.compare_digest(expected_sig, cert.signature):
            return False
        payload = json.loads(cert.payload_json)
        canonical = json.dumps(
            payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False
        ).encode("utf-8")
        prev = cert.prev_record_hash.encode() if cert.prev_record_hash else b""
        expected_hash = hashlib.sha256(prev + canonical).hexdigest()
        if not hmac.compare_digest(expected_hash, cert.record_hash):
            return False
        # The embedded link must point at the real previous global record.
        if cert.prev_record_hash is not None:
            prev_cert = s.scalars(
                select(m.DeletionCertificate).where(
                    m.DeletionCertificate.seq == cert.seq - 1
                )
            ).first()
            if prev_cert is None or prev_cert.record_hash != cert.prev_record_hash:
                return False
        return True

    # ---------------------------------------------------------------- recovery

    def resume(self) -> dict:
        """Recover after an abnormal exit. Safe to run on every startup and to
        call repeatedly.

        Order matters: first finish any blob stuck in 'purging' (the unlink may
        have happened or not), then advance every non-finalized delete op. Nothing
        here can resurrect a tombstone or purge a blob that still has references.
        """
        purged, resumed, finalized = 0, 0, 0
        with self._lock:
            with self.sf() as s:
                stuck = list(s.scalars(
                    select(m.ContentBlob).where(m.ContentBlob.state == "purging")
                ))
            for blob in stuck:
                self.store.purge(blob.sha256)
                with self.sf() as s2:
                    b = s2.get(m.ContentBlob, blob.sha256)
                    if b is not None and b.state == "purging":
                        b.state = m.BLOB_PURGED
                        b.purged_at = b.purged_at or self.clock()
                        # Any op waiting on this unlink records the outcome.
                        for op in s2.scalars(
                            select(m.ArchiveDeleteOp).where(
                                m.ArchiveDeleteOp.content_sha256 == b.sha256,
                                m.ArchiveDeleteOp.state != m.OP_FINALIZED,
                            )
                        ):
                            op.blob_physical_result = "purged"
                            op.blob_purged_at = b.purged_at
                            op.remaining_active_refs = 0
                        s2.commit()
                purged += 1

            # Pending-purge blobs whose owning op died before phase 3.
            with self.sf() as s:
                pending = list(s.scalars(
                    select(m.ArchiveDeleteOp).where(
                        m.ArchiveDeleteOp.state != m.OP_FINALIZED
                    ).order_by(m.ArchiveDeleteOp.logical_deleted_at)
                ))
            for op in pending:
                self.advance_delete(op.id)
                resumed += 1
                finalized += 1

        return {"purged_blobs": purged, "resumed_ops": resumed, "finalized_ops": finalized}
