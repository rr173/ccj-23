"""Durable merge queue and the weighted-fair scheduler.

The queue is a database table (merge_jobs), not an in-memory broker: every queue
state is persisted, enqueue is exactly-once (UNIQUE upload_id), and a worker
crash simply leaves rows in 'running' with an expired lease that the next
scheduling pass reclaims to 'queued'. Nothing is ever duplicated on restart.

Scheduling model
----------------
Each job gets, at enqueue time:
  seq  — per-tenant monotonic counter, so a tenant is always served FIFO
  vtag — weighted virtual tag = seq / weight. The globally smallest eligible
         vtag is dispatched first. Backlogged tenants therefore receive service
         proportional to their weights (weight 3 picks ~3x as often as weight 1),
         while every tenant's vtag grows without bound, so a continuously
         backlogged low-weight tenant is never starved.

Eligibility (checked at dispatch and used for wait-reason explanations):
  * retry backoff: not_before in the future
  * parallel cap: running merges under the job's (tenant, policy_version) bucket
    < that revision's max_parallel_merges
  * capacity gate: a tenant at/over its CURRENT cap gets no NEW processing slots,
    except its FIFO head always passes — sealing is occupancy-neutral
    (reservation converts to used), and accepted uploads must keep making
    progress even after the cap was lowered beneath current occupancy
"""
from __future__ import annotations

from datetime import timedelta

from sqlalchemy import func, select, update

from . import accounting
from .db import MergeJob, Tenant, Upload, ensure_tx_started, utcnow
from .tenants import current_policy

QUEUED = "queued"
RUNNING = "running"
DONE = "done"
CANCELED = "canceled"


# ---------------------------------------------------------------- enqueue


def enqueue(session, upload: Upload, now=None) -> str:
    """Attach a merge job for an upload, exactly once.

    Must run while holding the tenant write lock (the /complete path uses
    accounting.tenant_write_tx). Returns the resulting job status:
      queued   — new (or reopened after a permanent failure), queued or running
                 on an idempotent retry
      sealed   — upload already sealed; nothing to do
    """
    now = now or utcnow()
    existing = session.get(MergeJob, upload.id)
    if existing is not None:
        if existing.status == DONE and upload.status != "sealed":
            # Client fixed chunks after a permanent failure and called /complete
            # again: reopen the same job.
            existing.status = QUEUED
            existing.lease_owner = None
            existing.lease_expires_at = None
            existing.not_before = None
            existing.started_at = None
            existing.finished_at = None
            existing.result = None
            existing.error = None
            session.flush()
            return QUEUED
        return existing.status if existing.status != DONE else "sealed"

    n = (
        session.scalar(
            select(func.count()).select_from(MergeJob).where(MergeJob.tenant_id == upload.tenant_id)
        )
        + 1
    )
    job = MergeJob(
        upload_id=upload.id,
        tenant_id=upload.tenant_id,
        weight=upload.policy_weight,
        max_parallel_merges=upload.policy_max_parallel_merges,
        policy_version=upload.policy_version,
        status=QUEUED,
        seq=n,
        vtag=n / max(upload.policy_weight, 1),
        not_before=None,
        enqueued_at=now,
    )
    session.add(job)
    session.flush()
    return QUEUED


def cancel_queued(session, upload_id: str) -> bool:
    """Cancel a job that never started (upload aborted while waiting)."""
    job = session.get(MergeJob, upload_id)
    if job is not None and job.status == QUEUED:
        job.status = CANCELED
        job.finished_at = utcnow()
        return True
    return False


# ---------------------------------------------------------------- lease/reclaim


def reclaim_expired(session, now=None) -> list[str]:
    """Move running jobs whose lease has expired back to the queue. The job row is
    the single source of truth, so a crashed worker can never cause a duplicate
    or a lost job — once the lease expires the job is claimable again, while the
    old owner's merge result collapses against the UNIQUE constraints if it was
    in fact still alive."""
    now = now or utcnow()
    ids = session.scalars(
        select(MergeJob.upload_id).where(
            MergeJob.status == RUNNING,
            MergeJob.lease_expires_at < now,
        )
    ).all()
    if ids:
        session.execute(
            update(MergeJob)
            .where(MergeJob.upload_id.in_(ids))
            .values(status=QUEUED, lease_owner=None, lease_expires_at=None)
        )
    return list(ids)


def heartbeat(session, upload_id: str, owner: str, until) -> bool:
    """Extend the lease while a long merge is still making progress."""
    res = session.execute(
        update(MergeJob)
        .where(MergeJob.upload_id == upload_id, MergeJob.lease_owner == owner)
        .values(lease_expires_at=until)
    )
    session.commit()
    return res.rowcount > 0


def mark_done(session, upload_id: str, result: str, error: str | None = None, now=None) -> None:
    now = now or utcnow()
    session.execute(
        update(MergeJob)
        .where(MergeJob.upload_id == upload_id)
        .values(status=DONE, result=result, error=error, finished_at=now,
                lease_owner=None, lease_expires_at=None)
    )


def requeue_failed(session, upload_id: str, error: str, not_before, now=None) -> None:
    """Transient merge failure: back to the queue with a backoff, same position
    policy (vtag unchanged)."""
    now = now or utcnow()
    session.execute(
        update(MergeJob)
        .where(MergeJob.upload_id == upload_id)
        .values(
            status=QUEUED,
            lease_owner=None,
            lease_expires_at=None,
            not_before=not_before,
            error=error,
        )
    )


# ---------------------------------------------------------------- scheduling


def _lock_tenants_for_schedule(session, tenant_ids) -> None:
    """Serialize schedulers that touch overlapping tenants.

    Postgres: ordered row locks on the tenants (released at commit).
    SQLite: a writer transaction (BEGIN IMMEDIATE) already serializes everyone.
    """
    if not tenant_ids:
        return
    ensure_tx_started(session)
    if session.bind.dialect.name == "sqlite":
        return  # the BEGIN IMMEDIATE transaction already holds the global write lock
    session.scalars(
        select(Tenant.id)
        .where(Tenant.id.in_(tenant_ids))
        .order_by(Tenant.id)
        .with_for_update()
    ).all()


def claim_next(session_factory, worker_id: str, lease_seconds: int, now=None) -> str | None:
    """One scheduling+claim pass. Returns the upload_id now leased to this worker,
    or None when nothing is eligible. Inter-worker races collapse on the
    conditional UPDATE (status='queued'), verified by rowcount."""
    now = now or utcnow()
    lease_until = now + timedelta(seconds=lease_seconds)
    with session_factory() as s:
        # On SQLite the engine "begin" hook makes this BEGIN IMMEDIATE, so the
        # read-check-claim sequence below is serialized with other workers and
        # with the API's reservation/enqueue writes.
        ensure_tx_started(s)

        # Always reclaim crashed workers' leases first — the reclaimed jobs may
        # be the only ready work in the queue.
        reclaim_expired(s, now)

        ready_tenants = s.scalars(
            select(MergeJob.tenant_id)
            .where(
                MergeJob.status == QUEUED,
                (MergeJob.not_before.is_(None)) | (MergeJob.not_before <= now),
            )
            .distinct()
        ).all()
        if not ready_tenants:
            s.commit()
            return None

        _lock_tenants_for_schedule(s, ready_tenants)

        ready = s.scalars(
            select(MergeJob)
            .where(
                MergeJob.status == QUEUED,
                (MergeJob.not_before.is_(None)) | (MergeJob.not_before <= now),
            )
            .order_by(MergeJob.vtag, MergeJob.tenant_id, MergeJob.seq)
        ).all()
        if not ready:
            s.commit()
            return None

        running = s.scalars(
            select(MergeJob).where(MergeJob.status == RUNNING)
        ).all()
        running_per_bucket: dict[tuple[str, int], int] = {}
        for j in running:
            running_per_bucket[(j.tenant_id, j.policy_version)] = (
                running_per_bucket.get((j.tenant_id, j.policy_version), 0) + 1
            )

        # Current caps / occupancy for the capacity gate.
        caps: dict[str, int] = {}
        occupancy: dict[str, int] = {}
        head_seq: dict[str, int] = {}
        for j in ready:
            if j.tenant_id not in caps:
                pv = current_policy(s, j.tenant_id)
                caps[j.tenant_id] = pv.capacity_bytes
                reserved, used = accounting.occupancy(s, j.tenant_id)
                occupancy[j.tenant_id] = reserved + used
                head_seq[j.tenant_id] = j.seq  # ready list is vtag/seq ordered

        chosen: MergeJob | None = None
        for j in ready:
            bucket = running_per_bucket.get((j.tenant_id, j.policy_version), 0)
            if bucket >= j.max_parallel_merges:
                continue
            over_cap = occupancy[j.tenant_id] >= caps[j.tenant_id]
            if over_cap and j.seq != head_seq[j.tenant_id]:
                continue
            chosen = j
            break

        if chosen is None:
            s.commit()
            return None

        res = s.execute(
            update(MergeJob)
            .where(MergeJob.upload_id == chosen.upload_id, MergeJob.status == QUEUED)
            .values(
                status=RUNNING,
                lease_owner=worker_id,
                lease_expires_at=lease_until,
                started_at=now,
                attempts=MergeJob.attempts + 1,
                not_before=None,
            )
        )
        if res.rowcount == 0:  # lost a race against another worker
            s.rollback()
            return claim_next(session_factory, worker_id, lease_seconds, now)
        s.commit()
        return chosen.upload_id


# ---------------------------------------------------------------- status / explain


def tenant_queue_counts(session, tenant_id: str) -> dict:
    running = session.scalar(
        select(func.count()).select_from(MergeJob).where(
            MergeJob.tenant_id == tenant_id, MergeJob.status == RUNNING
        )
    )
    queued = session.scalar(
        select(func.count()).select_from(MergeJob).where(
            MergeJob.tenant_id == tenant_id, MergeJob.status == QUEUED
        )
    )
    sealed = session.scalar(
        select(func.count()).select_from(MergeJob).where(
            MergeJob.tenant_id == tenant_id, MergeJob.status == DONE,
            MergeJob.result == "sealed",
        )
    )
    return {"running": int(running), "queued": int(queued), "sealed_jobs": int(sealed)}


def explain_wait(session, job: MergeJob, now=None) -> dict:
    """Explain why a queued job cannot run right now:
    capacity | parallel_limit | scheduling_order."""
    now = now or utcnow()

    if job.not_before is not None and job.not_before > now:
        return {
            "reason": "scheduling_order",
            "detail": f"retry backoff until {job.not_before.isoformat()}Z",
        }

    bucket_running = session.scalar(
        select(func.count()).select_from(MergeJob).where(
            MergeJob.status == RUNNING,
            MergeJob.tenant_id == job.tenant_id,
            MergeJob.policy_version == job.policy_version,
        )
    )
    if bucket_running >= job.max_parallel_merges:
        return {
            "reason": "parallel_limit",
            "detail": (
                f"{bucket_running} running merge(es) already use the "
                f"max_parallel_merges={job.max_parallel_merges} of policy "
                f"version {job.policy_version}"
            ),
        }

    pv = current_policy(session, job.tenant_id)
    reserved, used = accounting.occupancy(session, job.tenant_id)
    occ = reserved + used
    head = session.scalars(
        select(MergeJob)
        .where(
            MergeJob.tenant_id == job.tenant_id,
            MergeJob.status == QUEUED,
            (MergeJob.not_before.is_(None)) | (MergeJob.not_before <= now),
        )
        .order_by(MergeJob.seq)
        .limit(1)
    ).first()
    if occ >= pv.capacity_bytes and (head is None or head.upload_id != job.upload_id):
        return {
            "reason": "capacity",
            "detail": (
                f"tenant occupancy {occ} is at/over capacity_bytes={pv.capacity_bytes} "
                f"(reserved={reserved}, used={used}); the FIFO head still proceeds "
                f"because sealing is occupancy-neutral"
            ),
        }

    ahead = session.scalar(
        select(func.count())
        .select_from(MergeJob)
        .where(
            MergeJob.status == QUEUED,
            (MergeJob.not_before.is_(None)) | (MergeJob.not_before <= now),
            MergeJob.vtag < job.vtag,
        )
    )
    return {
        "reason": "scheduling_order",
        "detail": f"{ahead} eligible job(s) with smaller weighted virtual tags run first",
    }


def list_queued(session, tenant_id: str, now=None) -> list[dict]:
    now = now or utcnow()
    jobs = session.scalars(
        select(MergeJob)
        .where(MergeJob.tenant_id == tenant_id, MergeJob.status == QUEUED)
        .order_by(MergeJob.seq)
    ).all()
    out = []
    for j in jobs:
        why = explain_wait(session, j, now)
        out.append(
            {
                "upload_id": j.upload_id,
                "seq": j.seq,
                "vtag": j.vtag,
                "weight": j.weight,
                "policy_version": j.policy_version,
                "attempts": j.attempts,
                "backoff_until": j.not_before.isoformat() + "Z" if j.not_before else None,
                "reason": why["reason"],
                "reason_detail": why["detail"],
            }
        )
    return out
