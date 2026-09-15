"""Tenant capacity accounting on an append-only ledger.

Ledger events (capacity_ledger rows):

    reserve      +size   creation time, atomically with the upload row
    release      -size   upload aborted / expired — reservation given back once
    commit_used  +size   merge sealed — the reservation is converted to used bytes

A seal appends BOTH a release (-size) and a commit_used (+size) in one
transaction: net occupancy is unchanged, reserved drops and used rises by the
exact same amount. UNIQUE(upload_id, event_type) makes every state transition
exactly-once even with retries or crashes mid-commit.

Invariant:  occupancy = SUM(bytes_delta) = reserved_bytes + used_bytes
"""
from __future__ import annotations

from contextlib import contextmanager

from sqlalchemy import func, select

from .db import CapacityLedger, Tenant, Upload, ensure_tx_started

RESERVE = "reserve"
RELEASE = "release"
COMMIT_USED = "commit_used"

# Statuses that still hold a byte reservation (sealed moved the bytes to "used";
# aborted/expired already released).
RESERVATION_STATUSES = ("uploading", "failed", "merging", "queued")


class CapacityExceeded(Exception):
    def __init__(self, tenant_id: str, requested: int, reserved: int, used: int, cap: int):
        self.tenant_id = tenant_id
        self.requested = requested
        self.reserved = reserved
        self.used = used
        self.cap = cap
        super().__init__(
            f"tenant {tenant_id}: reserve {requested} would exceed cap {cap} "
            f"(reserved={reserved}, used={used})"
        )


def lock_tenant(session, tenant_id: str) -> Tenant:
    """Serialize writers of one tenant for the read-check-insert reserve dance.

    SQLite: acquire the database write lock immediately (BEGIN IMMEDIATE), which
    turns concurrent create transactions into a strict queue.
    Postgres: row-lock the tenant with SELECT ... FOR UPDATE.
    """
    ensure_tx_started(session)
    dialect = session.bind.dialect.name
    if dialect == "sqlite":
        t = session.get(Tenant, tenant_id)
    else:
        t = session.scalar(select(Tenant).where(Tenant.id == tenant_id).with_for_update())
    if t is None:
        raise KeyError(tenant_id)
    return t


@contextmanager
def tenant_write_tx(session_factory, tenant_id: str):
    """Open a session holding an exclusive tenant write lock; commits on success."""
    with session_factory() as s:
        lock_tenant(s, tenant_id)
        yield s
        s.commit()


def occupancy(session, tenant_id: str) -> tuple[int, int]:
    """Return (reserved_bytes, used_bytes)."""
    rows = session.execute(
        select(CapacityLedger.event_type, func.coalesce(func.sum(CapacityLedger.bytes_delta), 0))
        .where(CapacityLedger.tenant_id == tenant_id)
        .group_by(CapacityLedger.event_type)
    ).all()
    totals = {event: int(total) for event, total in rows}
    used = totals.get(COMMIT_USED, 0)
    reserves = totals.get(RESERVE, 0)
    release_rows = totals.get(RELEASE, 0)  # sum of negative deltas (<= 0)
    # Derivation:
    #   aborted/expired of size A produce one release row  -A  (never used)
    #   sealed size used produce a release row -used AND commit_used +used
    # so release_rows == -(A + used), and active reservations are
    #   reserves - A - used = reserves + release_rows
    # (sealed rows cancel to zero; permanent used bytes come from COMMIT_USED).
    reserved = reserves + release_rows
    return reserved, used


def release_once(session, upload: Upload) -> bool:
    """Append the release row for an upload that still holds a reservation.
    Returns False when the reservation was already released/committed.

    The UNIQUE(upload_id, 'release') constraint is the hard exactly-once guard;
    the status check is the fast path. Caller must keep the transaction alive
    (the unique constraint is the final backstop for concurrent attempts)."""
    if upload.status in ("sealed", "aborted", "expired"):
        return False
    already = session.scalar(
        select(func.count())
        .select_from(CapacityLedger)
        .where(CapacityLedger.upload_id == upload.id, CapacityLedger.event_type == RELEASE)
    )
    if already:
        return False
    session.add(
        CapacityLedger(
            tenant_id=upload.tenant_id,
            upload_id=upload.id,
            event_type=RELEASE,
            bytes_delta=-upload.total_size,
        )
    )
    session.flush()
    return True


def commit_used(session, upload: Upload) -> bool:
    """Convert a reservation into used bytes: release (-size) + commit_used (+size)
    atomically. Idempotent — a repeated call after a crash is a no-op. Returns
    True when this call performed the conversion.

    The existence check runs with autoflush disabled: the seal path calls this
    right after s.add(Version(...)), and an autoflushed SELECT would otherwise
    write the pending Version before the atomic final commit boundary."""
    with session.no_autoflush:
        existing = session.scalar(
            select(func.count())
            .select_from(CapacityLedger)
            .where(CapacityLedger.upload_id == upload.id, CapacityLedger.event_type == COMMIT_USED)
        )
        if existing:
            return False
        session.add(
            CapacityLedger(
                tenant_id=upload.tenant_id,
                upload_id=upload.id,
                event_type=RELEASE,
                bytes_delta=-upload.total_size,
            )
        )
        session.add(
            CapacityLedger(
                tenant_id=upload.tenant_id,
                upload_id=upload.id,
                event_type=COMMIT_USED,
                bytes_delta=upload.total_size,
            )
        )
    return True
