"""Derivation service: immutable objects derived from pinned source versions.

Lifecycle in one screen
-----------------------

1. **Acceptance (``submit``) is all-or-nothing, in one write transaction.**

   * canonicalize the recipe -> stable ``recipe_digest``;
   * idempotency on (tenant, request_key): same recipe replays the original
     job, a reused key with another recipe is an explicit 409;
   * pin EVERY source version: it must exist, belong to the tenant, be
     ``active`` (not in a delete flow), and the declared half-open range must
     fit the sealed size. Missing and foreign-tenant versions return the SAME
     error, so other tenants' object existence cannot be probed;
   * atomically insert job + segments + protection rows + ONE capacity
     reservation. Any failure rolls everything back — no partial task exists.

2. **Source protection.** An open ``derivation_protections`` row appears as a
   ``reference`` blocker in the archive delete flow. It is released exactly
   once at terminal cleanup (billing, cancel or failure). Until then a source
   delete is rejected with a reason that names the deriving job.

3. **Exactly-one worker.** ``claim_next`` leases a job and bumps a fence token;
   every later conditional update carries that fence. A crashed worker's lease
   expires and the job is reclaimed; the loser's writes collapse on the fence.

4. **Publish then bill, in two transactions.** The assembled bytes are stored
   content-addressed and the new ``archived_versions`` row + blob refcount +
   job->published commit atomically (downloadable only from this point). A
   second transaction converts the reservation into used bytes ONCE
   (UNIQUE(job_id,'commit_used')) and releases protections. A crash in between
   is recovered by finalizing billing; a crash before publish is recovered by
   resuming extraction from the per-segment durable states.

5. **Cancel / failure / crashes never leak.** Staging is invisible output and
   removed, the reservation is released once, protections are released once;
   nothing is ever billed twice. Published (or billed) jobs can't be cancelled.
"""

from __future__ import annotations

import os
import threading
import time
import uuid
from collections.abc import Callable

from sqlalchemy import func, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from ..archive.content_store import ContentStore
from ..archive.models import ArchivedVersion, ArchivePolicy, ContentBlob
from ..db import PolicyVersion, make_engine, make_session_factory
from . import models as dm
from .errors import (
    CapacityExceeded,
    NotCancellable,
    NotFound,
    ReferenceRejected,
    RequestKeyConflict,
)
from .recipe import Recipe, parse_recipe
from .storage import DerivationStore


def _new_id() -> str:
    return uuid.uuid4().hex


class DerivationService:
    def __init__(
        self,
        database_url: str,
        data_dir: str,
        *,
        content_dir: str | None = None,
        default_capacity_bytes: int | None = None,
        lease_seconds: float = 120.0,
        clock: Callable[[], float] = time.time,
        init_schema: bool = True,
    ):
        self.engine = make_engine(database_url)
        if init_schema:
            from ..db import init_db

            init_db(self.engine)
        self.sf = make_session_factory(self.engine)
        # Private, never-downloadable per-job scratch space (segments,
        # assembled output). The final result seals into the SAME
        # content-addressed tree the archive subsystem uses, so content_dir
        # points at the archive store root; by convention it is the sibling
        # "archive" directory next to the derivation staging root.
        self.store = DerivationStore(data_dir)
        self.content = ContentStore(content_dir or os.path.join(os.path.dirname(data_dir.rstrip("/")), "archive"))
        self.default_capacity_bytes = default_capacity_bytes
        self.lease_seconds = lease_seconds
        self.clock = clock
        self._lock = threading.RLock()
        # Test hook: called at the three documented crash points (and a few
        # internal ones). An os._exit inside simulates a hard crash with all
        # previously committed transactions durable.
        self.crash: Callable[[str], None] | None = None

    def _crash(self, when: str) -> None:
        if self.crash is not None:
            self.crash(when)

    # =============================================================== helpers

    def _capacity_cap(self, s: Session, tenant_id: str) -> int | None:
        """Capacity cap for derived bytes. A tenant that already exists in the
        ingest subsystem shares its capacity_bytes; otherwise the service-wide
        default (if configured) applies; None means unlimited."""
        pv = s.scalars(
            select(PolicyVersion)
            .where(PolicyVersion.tenant_id == tenant_id)
            .order_by(PolicyVersion.version.desc())
            .limit(1)
        ).first()
        if pv is not None:
            return pv.capacity_bytes
        return self.default_capacity_bytes

    def _occupancy(self, s: Session, tenant_id: str) -> tuple[int, int]:
        """(reserved, used) derived bytes for a tenant.

        Same algebra as app.accounting: reserve +size; cancel/fail appends a
        release -size; success appends release -size AND commit_used +size, so
        the SUM of reserve/release deltas is exactly the live reservation and
        commit_used is permanent usage."""
        rows = s.execute(
            select(dm.DerivationLedger.event_type, func.coalesce(func.sum(dm.DerivationLedger.bytes_delta), 0))
            .where(dm.DerivationLedger.tenant_id == tenant_id)
            .group_by(dm.DerivationLedger.event_type)
        ).all()
        totals = {event: int(total) for event, total in rows}
        reserved = totals.get(dm.EV_RESERVE, 0) + totals.get(dm.EV_RELEASE, 0)
        used = totals.get(dm.EV_COMMIT_USED, 0)
        return reserved, used

    def _get_job_tenant(self, s: Session, job_id: str, tenant_id: str) -> dm.DerivationJob:
        job = s.get(dm.DerivationJob, job_id)
        if job is None or job.tenant_id != tenant_id:
            # Same shape for unknown id and foreign tenant: do not reveal it.
            raise NotFound(f"derivation job {job_id}")
        return job

    @staticmethod
    def _job_dict(job: dm.DerivationJob) -> dict:
        return {
            "job_id": job.id,
            "tenant_id": job.tenant_id,
            "status": job.status,
            "recipe_digest": job.recipe_digest,
            "cancel_requested": job.cancel_requested,
            "total_size": job.total_size,
            "error": job.error,
            "output_object_id": job.result_object_id or job.output_object_id,
            "output_version": job.result_version,
            "result_sha256": job.result_sha256,
            "result_size": job.result_size,
            "created_at": job.created_at,
            "started_at": job.started_at,
            "published_at": job.published_at,
            "billed_at": job.billed_at,
            "finished_at": job.finished_at,
        }

    # =============================================================== submit

    def submit(
        self,
        tenant_id: str,
        recipe: dict | str | Recipe,
        request_key: str,
        *,
        output_object_id: str,
        output_version: int | None = None,
    ) -> dict:
        """Accept a derivation request atomically and return the job view.

        Raises RecipeError for malformed recipes, ReferenceRejected when any
        source cannot be pinned, RequestKeyConflict on a reused key with a
        different recipe, CapacityExceeded against the tenant cap."""
        parsed = parse_recipe(recipe)
        digest = parsed.digest()
        if not request_key:
            raise ValueError("request_key is required for derivation idempotency")
        if not output_object_id:
            raise ValueError("output_object_id is required")

        now = self.clock()
        with self._lock, self.sf() as s:
            # Idempotency first: an identical retry must never re-validate or
            # create anything.
            prior = s.get(dm.DerivationRequest, (tenant_id, request_key))
            if prior is not None:
                if prior.recipe_digest != digest:
                    raise RequestKeyConflict(
                        request_key, prior.job_id, prior.recipe_digest
                    )
                job = s.get(dm.DerivationJob, prior.job_id)
                out = self._job_dict(job)
                out["replayed"] = True
                s.commit()
                return out

            # Validate and pin every source BEFORE inserting anything.
            pinned: list[tuple[ArchivedVersion, int, int, str]] = []
            for i, seg in enumerate(parsed.segments):
                v = s.get(
                    ArchivedVersion, (tenant_id, seg.object_id, seg.version)
                )
                # Missing and foreign-tenant rows are indistinguishable; note
                # the composite key already scopes the lookup to this tenant.
                if v is None:
                    raise ReferenceRejected(
                        "source_not_found",
                        f"segment {i}: object {seg.object_id!r} version "
                        f"{seg.version} does not exist",
                        segment=i,
                    )
                if v.state != "active":
                    raise ReferenceRejected(
                        "source_deleting",
                        f"segment {i}: object {seg.object_id!r} version "
                        f"{seg.version} is in a delete flow ({v.state})",
                        segment=i,
                    )
                if seg.end > v.size:
                    raise ReferenceRejected(
                        "range_out_of_bounds",
                        f"segment {i}: range [{seg.start},{seg.end}) exceeds "
                        f"source size {v.size}",
                        segment=i,
                    )
                # Re-hash the exact range now: a wrong declared digest rejects
                # the whole acceptance, before any protection is taken.
                try:
                    actual, got = self.content.hash_range(
                        v.content_sha256, seg.start, seg.end
                    )
                except FileNotFoundError as exc:
                    raise RuntimeError(
                        "physical blob missing while source version is active"
                    ) from exc
                if got != seg.size:
                    raise ReferenceRejected(
                        "range_out_of_bounds",
                        f"segment {i}: source yielded {got} bytes, expected {seg.size}",
                        segment=i,
                    )
                if actual != seg.sha256:
                    raise ReferenceRejected(
                        "digest_mismatch",
                        f"segment {i}: declared digest {seg.sha256} does not "
                        f"match source bytes {actual}",
                        segment=i,
                    )
                pinned.append((v, seg.start, seg.end, seg.sha256))

            # Capacity gate (best-effort; the hard cap is the ingest create
            # path, derived output is reserved here up front).
            cap = self._capacity_cap(s, tenant_id)
            if cap is not None:
                reserved, used = self._occupancy(s, tenant_id)
                if reserved + used + parsed.total_size > cap:
                    raise CapacityExceeded(
                        tenant_id, parsed.total_size, reserved, used, cap
                    )

            # Output version numbering: explicit version must not exist; the
            # implicit next version counts only same-object rows.
            if output_version is not None:
                existing = s.get(
                    ArchivedVersion, (tenant_id, output_object_id, output_version)
                )
                if existing is not None:
                    raise ReferenceRejected(
                        "output_version_exists",
                        f"{tenant_id}/{output_object_id}/v{output_version} already exists",
                    )
            else:
                latest = s.scalar(
                    select(func.max(ArchivedVersion.version)).where(
                        ArchivedVersion.tenant_id == tenant_id,
                        ArchivedVersion.object_id == output_object_id,
                    )
                )
                output_version = (latest or 0) + 1

            job_id = _new_id()
            job = dm.DerivationJob(
                id=job_id,
                tenant_id=tenant_id,
                request_key=request_key,
                recipe_json=parsed.canonical_json(),
                recipe_digest=digest,
                output_object_id=output_object_id,
                output_version=output_version,
                status=dm.JOB_QUEUED,
                total_size=parsed.total_size,
                created_at=now,
            )
            s.add(job)
            s.flush()

            for i, (v, start, end, sha) in enumerate(pinned):
                seg = parsed.segments[i]
                s.add(
                    dm.DerivationSegment(
                        job_id=job_id,
                        index=i,
                        tenant_id=tenant_id,
                        source_object_id=seg.object_id,
                        source_version=seg.version,
                        range_start=start,
                        range_end=end,
                        size=end - start,
                        expected_sha256=sha,
                        state=dm.SEG_WAITING,
                        updated_at=now,
                    )
                )

            # One pin per distinct exact source version: two segments reading
            # the same object/version must not raise the unique constraint and
            # must not double-block the source.
            for object_id, version, content_sha in {
                (seg.object_id, seg.version, ver.content_sha256)
                for seg, (ver, _, _, _) in zip(parsed.segments, pinned)
            }:
                s.add(
                    dm.DerivationProtection(
                        job_id=job_id,
                        tenant_id=tenant_id,
                        object_id=object_id,
                        version=version,
                        content_sha256=content_sha,
                        created_at=now,
                    )
                )

            # The one-and-only reservation, in the same atomic commit.
            s.add(
                dm.DerivationLedger(
                    tenant_id=tenant_id,
                    job_id=job_id,
                    event_type=dm.EV_RESERVE,
                    bytes_delta=parsed.total_size,
                    created_at=now,
                )
            )
            s.add(
                dm.DerivationRequest(
                    tenant_id=tenant_id,
                    request_key=request_key,
                    job_id=job_id,
                    recipe_digest=digest,
                    created_at=now,
                )
            )
            try:
                s.commit()
            except IntegrityError:
                # Concurrent identical (tenant, request_key) submission won the
                # primary-key race (Postgres; SQLite serializes via BEGIN
                # IMMEDIATE). Re-read: replay on the same recipe, conflict on a
                # different one — the loser leaves no task behind.
                s.rollback()
                with self.sf() as s2:
                    winner = s2.get(dm.DerivationRequest, (tenant_id, request_key))
                    if winner is None:
                        raise
                    if winner.recipe_digest != digest:
                        raise RequestKeyConflict(
                            request_key, winner.job_id, winner.recipe_digest
                        )
                    out = self._job_dict(s2.get(dm.DerivationJob, winner.job_id))
                out["replayed"] = True
                return out

        out = self._job_dict(job)
        out["replayed"] = False
        return out

    # =============================================================== status

    def get_job(self, tenant_id: str, job_id: str) -> dict:
        with self.sf() as s:
            job = self._get_job_tenant(s, job_id, tenant_id)
            segs = list(
                s.scalars(
                    select(dm.DerivationSegment)
                    .where(dm.DerivationSegment.job_id == job_id)
                    .order_by(dm.DerivationSegment.index)
                )
            )
            protections = list(
                s.scalars(
                    select(dm.DerivationProtection).where(
                        dm.DerivationProtection.job_id == job_id,
                        dm.DerivationProtection.released_at.is_(None),
                    )
                )
            )
            out = self._job_dict(job)
            out["segments"] = [self._segment_view(job, seg) for seg in segs]
            out["open_protections"] = len(protections)
            s.commit()
            return out

    def _segment_view(self, job: dm.DerivationJob, seg: dm.DerivationSegment) -> dict:
        """Map durable segment state onto waiting/processing/verified/failed and
        attach a machine-readable block reason for non-verified states."""
        if seg.state == dm.SEG_FAILED:
            return {
                "index": seg.index,
                "state": "failed",
                "blocked_reason": seg.failure_reason or "segment failed",
                "source": {
                    "object_id": seg.source_object_id,
                    "version": seg.source_version,
                    "range": [seg.range_start, seg.range_end],
                },
                "expected_sha256": seg.expected_sha256,
                "actual_sha256": seg.actual_sha256,
            }
        if seg.state == dm.SEG_VERIFIED:
            state = "verified"
            reason = None
        elif seg.state == dm.SEG_PROCESSING:
            state = "processing"
            reason = "segment bytes are being extracted and verified"
        else:
            # waiting — the reason depends on what the job is doing overall.
            state = "waiting"
            if job.status == dm.JOB_FAILED:
                reason = f"job failed before this segment: {job.error or 'unknown error'}"
            elif job.status == dm.JOB_CANCELED:
                reason = "job was canceled before this segment"
            elif job.status == dm.JOB_QUEUED:
                reason = "waiting_for_worker"
            elif job.cancel_requested:
                reason = "cancel_requested"
            else:
                reason = "waiting_for_earlier_segments"
        return {
            "index": seg.index,
            "state": state,
            "blocked_reason": reason,
            "source": {
                "object_id": seg.source_object_id,
                "version": seg.source_version,
                "range": [seg.range_start, seg.range_end],
            },
            "expected_sha256": seg.expected_sha256,
            "actual_sha256": seg.actual_sha256,
        }

    def list_jobs(self, tenant_id: str, *, limit: int = 100) -> list[dict]:
        with self.sf() as s:
            rows = s.scalars(
                select(dm.DerivationJob)
                .where(dm.DerivationJob.tenant_id == tenant_id)
                .order_by(dm.DerivationJob.created_at.desc())
                .limit(limit)
            ).all()
            return [self._job_dict(j) for j in rows]

    def capacity(self, tenant_id: str) -> dict:
        with self.sf() as s:
            reserved, used = self._occupancy(s, tenant_id)
            cap = self._capacity_cap(s, tenant_id)
            return {
                "tenant_id": tenant_id,
                "capacity_bytes": cap,
                "reserved_bytes": reserved,
                "used_bytes": used,
                "available_bytes": None if cap is None else max(0, cap - reserved - used),
            }

    # =============================================================== cancel

    def cancel(self, tenant_id: str, job_id: str) -> dict:
        """Cancel a queued/processing/assembling job.

        * queued jobs terminate synchronously here (cleanup + one release +
          release of all protections in one transaction);
        * in-flight jobs only get cancel_requested: the lease-holding worker
          (or recovery) performs the same terminal cleanup at the next safe
          boundary. Either path is guarded so release happens exactly once;
        * published/billed/failed/canceled jobs can no longer be cancelled.
        """
        now = self.clock()
        with self._lock, self.sf() as s:
            job = self._get_job_tenant(s, job_id, tenant_id)
            if job.status in (dm.JOB_PUBLISHED, dm.JOB_BILLED):
                raise NotCancellable(
                    f"job {job_id} already published as "
                    f"{job.result_object_id or job.output_object_id}/v{job.result_version or job.output_version}"
                )
            if job.status in (dm.JOB_CANCELED, dm.JOB_FAILED):
                out = self._job_dict(job)
                out["replayed"] = True
                s.commit()
                return out

            if job.status == dm.JOB_QUEUED:
                self._terminate(s, job, dm.JOB_CANCELED, "canceled while queued", now)
                s.commit()
                # Staging output never existed for a queued job, but run the
                # idempotent cleanup anyway (a claim may have raced us).
                self.store.cleanup_job(job_id)
                self._crash("derive.after_cancel_cleanup")
                return self._job_dict(job)

            # processing / assembling: ask the owner to stop at a boundary.
            job.cancel_requested = True
            s.commit()
            return self._job_dict(job)

    # =============================================================== worker

    def claim_next(self, worker_id: str, *, now: float | None = None) -> str | None:
        """Lease one live job to this worker.

        Eligible:
          * queued jobs with no live lease (never started), or
          * processing/assembling/published jobs whose lease expired (crashed
            owner).

        A queued job flips to ``processing`` AT the claim, so it cannot match a
        second worker's predicate: one task is ever advanced by one owner. The
        claim also bumps a fence token; every later conditional write requires
        that token, so even an expired-lease reclaim fences off a zombie owner
        that was merely paused."""
        now = self.clock() if now is None else now
        until = now + self.lease_seconds
        with self.sf() as s:
            reclaimable = list(
                s.scalars(
                    select(dm.DerivationJob).where(
                        dm.DerivationJob.status.in_(dm.JOB_RECOVERABLE_STATES),
                        dm.DerivationJob.lease_expires_at.is_not(None),
                        dm.DerivationJob.lease_expires_at <= now,
                    )
                )
            )
            candidates = list(
                s.scalars(
                    select(dm.DerivationJob)
                    .where(
                        dm.DerivationJob.status == dm.JOB_QUEUED,
                        (dm.DerivationJob.lease_expires_at.is_(None))
                        | (dm.DerivationJob.lease_expires_at <= now),
                    )
                    .order_by(dm.DerivationJob.created_at)
                )
            )
            # Expired leases first (a half-finished job should resume), then FIFO.
            reclaimable.sort(key=lambda j: j.lease_expires_at or 0)
            chosen = (reclaimable + candidates)[0] if (reclaimable or candidates) else None
            if chosen is None:
                s.commit()
                return None

            if chosen.status == dm.JOB_QUEUED:
                res = s.execute(
                    update(dm.DerivationJob)
                    .where(
                        dm.DerivationJob.id == chosen.id,
                        dm.DerivationJob.status == dm.JOB_QUEUED,
                        (dm.DerivationJob.lease_expires_at.is_(None))
                        | (dm.DerivationJob.lease_expires_at <= now),
                    )
                    .values(
                        status=dm.JOB_PROCESSING,
                        lease_owner=worker_id,
                        lease_expires_at=until,
                        fence=dm.DerivationJob.fence + 1,
                        attempts=dm.DerivationJob.attempts + 1,
                        started_at=func.coalesce(dm.DerivationJob.started_at, now),
                    )
                )
            else:
                res = s.execute(
                    update(dm.DerivationJob)
                    .where(
                        dm.DerivationJob.id == chosen.id,
                        dm.DerivationJob.status.in_(dm.JOB_RECOVERABLE_STATES),
                        dm.DerivationJob.lease_expires_at <= now,
                    )
                    .values(
                        lease_owner=worker_id,
                        lease_expires_at=until,
                        fence=dm.DerivationJob.fence + 1,
                        attempts=dm.DerivationJob.attempts + 1,
                        started_at=func.coalesce(dm.DerivationJob.started_at, now),
                    )
                )
            if res.rowcount == 0:
                # Lost a race against another worker; back off (the caller
                # retries on the next tick).
                s.rollback()
                return None
            s.commit()
            return chosen.id

    def reclaim_stale(self, *, now: float | None = None) -> list[str]:
        """Drop leases of crashed owners so the jobs become claimable again.
        Called by a worker on startup and before each scheduling tick."""
        now = self.clock() if now is None else now
        with self.sf() as s:
            ids = s.scalars(
                select(dm.DerivationJob.id).where(
                    dm.DerivationJob.status.in_(dm.JOB_RECOVERABLE_STATES),
                    dm.DerivationJob.lease_expires_at.is_not(None),
                    dm.DerivationJob.lease_expires_at <= now,
                )
            ).all()
            s.execute(
                update(dm.DerivationJob)
                .where(dm.DerivationJob.id.in_(ids))
                .values(lease_owner=None, lease_expires_at=None)
            )
            s.commit()
            return list(ids)

    def force_reclaim_all(self) -> int:
        """Test/ops helper: expire ALL live leases (e.g. right after a restart in
        a single-process deployment). Leases are set to the epoch rather than
        NULL so the jobs match the ordinary expired-lease claim predicate."""
        with self.sf() as s:
            ids = s.scalars(
                select(dm.DerivationJob.id).where(
                    dm.DerivationJob.status.in_(
                        dm.JOB_LIVE_STATES + (dm.JOB_PUBLISHED,)
                    )
                )
            ).all()
            s.execute(
                update(dm.DerivationJob)
                .where(dm.DerivationJob.id.in_(ids))
                .values(lease_owner=None, lease_expires_at=0.0)
            )
            s.commit()
            return len(ids)

    def _renew_lease(self, job_id: str, worker_id: str, fence: int) -> bool:
        until = self.clock() + self.lease_seconds
        with self.sf() as s:
            res = s.execute(
                update(dm.DerivationJob)
                .where(
                    dm.DerivationJob.id == job_id,
                    dm.DerivationJob.lease_owner == worker_id,
                    dm.DerivationJob.fence == fence,
                )
                .values(lease_expires_at=until)
            )
            s.commit()
            return res.rowcount == 1

    # -------------------------------------------------- terminal cleanup

    def _release_protections_once(self, s: Session, job: dm.DerivationJob, now: float) -> int:
        """Mark still-open protections released. The open-predicate makes this
        idempotent across crashes and duplicate attempts."""
        rows = s.scalars(
            select(dm.DerivationProtection).where(
                dm.DerivationProtection.job_id == job.id,
                dm.DerivationProtection.released_at.is_(None),
            )
        ).all()
        for p in rows:
            p.released_at = now
        return len(rows)

    def _release_reservation_once(self, s: Session, job: dm.DerivationJob, now: float) -> bool:
        """Append the single release row (-size). UNIQUE(job, event) is the hard
        backstop; the existence check keeps IntegrityError out of the happy path."""
        already = s.scalar(
            select(func.count())
            .select_from(dm.DerivationLedger)
            .where(
                dm.DerivationLedger.job_id == job.id,
                dm.DerivationLedger.event_type == dm.EV_RELEASE,
            )
        )
        if already:
            return False
        s.add(
            dm.DerivationLedger(
                tenant_id=job.tenant_id,
                job_id=job.id,
                event_type=dm.EV_RELEASE,
                bytes_delta=-job.total_size,
                created_at=now,
            )
        )
        s.flush()
        return True

    def _terminate(
        self, s: Session, job: dm.DerivationJob, status: str, error: str | None, now: float
    ) -> None:
        """Shared terminal transition for cancel/failure: flip every non-verified
        segment appropriately, release reservation once and all protections,
        detach the lease. Caller owns the transaction and the staging cleanup
        (files are removed after commit by the caller)."""
        if job.status not in dm.JOB_LIVE_STATES:
            return
        for seg in s.scalars(
            select(dm.DerivationSegment).where(dm.DerivationSegment.job_id == job.id)
        ):
            if status == dm.JOB_FAILED and seg.state != dm.SEG_VERIFIED:
                seg.state = dm.SEG_FAILED
                seg.failure_reason = seg.failure_reason or error
                seg.updated_at = now
            elif status == dm.JOB_CANCELED and seg.state in (dm.SEG_WAITING, dm.SEG_PROCESSING):
                # Cancel leaves no failed segment: they simply stop waiting.
                seg.state = dm.SEG_WAITING
                seg.failure_reason = None
                seg.updated_at = now
        self._release_reservation_once(s, job, now)
        self._release_protections_once(s, job, now)
        job.status = status
        job.error = error
        job.cancel_requested = False
        job.lease_owner = None
        job.lease_expires_at = None
        job.finished_at = now

    def _fail_job(self, job_id: str, fence: int, reason: str, segment_index: int | None = None) -> None:
        """Deterministic failure terminal transition (digest/source problems
        found while processing). Exactly-once release semantics."""
        now = self.clock()
        with self._lock, self.sf() as s:
            job = s.get(dm.DerivationJob, job_id)
            if job is None or job.fence != fence or job.status not in dm.JOB_LIVE_STATES:
                s.rollback()
                return
            if segment_index is not None:
                seg = s.get(dm.DerivationSegment, (job_id, segment_index))
                if seg is not None and seg.state != dm.SEG_VERIFIED:
                    seg.state = dm.SEG_FAILED
                    seg.failure_reason = reason
                    seg.updated_at = now
            self._terminate(s, job, dm.JOB_FAILED, reason, now)
            s.commit()
        self.store.cleanup_job(job_id)

    # =============================================================== processing

    def process_job(self, job_id: str, worker_id: str, fence: int) -> dict:
        """Drive one claimed job forward to a terminal state (or until the lease
        is lost / a cancel is observed). Every disk+state step is independently
        crash-safe and idempotent; recovery simply re-enters this method."""
        job = self._reconcile(job_id, worker_id, fence)
        if job is None:
            return {"job_id": job_id, "state": "lost"}
        if job.status in (dm.JOB_BILLED, dm.JOB_CANCELED, dm.JOB_FAILED):
            return {"job_id": job_id, "state": job.status}
        if job.status == dm.JOB_PUBLISHED:
            return self._finalize_billing(job_id, worker_id, fence)
        return self._process_live(job_id, worker_id, fence)

    def _mine(self, s: Session, job_id: str, worker_id: str, fence: int) -> dm.DerivationJob | None:
        job = s.get(dm.DerivationJob, job_id)
        if job is None:
            return None
        if job.lease_owner != worker_id or job.fence != fence:
            return None
        return job

    def _reconcile(self, job_id: str, worker_id: str, fence: int) -> dm.DerivationJob | None:
        """Resolve durable state vs staging files after (possibly) a crash.

        * a cancel requested at a safe boundary terminates the job;
        * a processing/assembling job whose segment file exists and matches the
          declared digest is marked verified without rewriting it;
        * an existing assembled.bin that matches is trusted; otherwise it is
          rebuilt from verified segment parts.
        """
        now = self.clock()
        with self.sf() as s:
            job = self._mine(s, job_id, worker_id, fence)
            if job is None:
                s.rollback()
                return None

            if job.status in dm.JOB_LIVE_STATES and job.cancel_requested:
                self._terminate(s, job, dm.JOB_CANCELED, "canceled while processing", now)
                s.commit()
                self.store.cleanup_job(job_id)
                return s.get(dm.DerivationJob, job_id)

            if job.status in (dm.JOB_PROCESSING, dm.JOB_ASSEMBLING):
                changed = False
                for seg in s.scalars(
                    select(dm.DerivationSegment)
                    .where(dm.DerivationSegment.job_id == job_id)
                    .order_by(dm.DerivationSegment.index)
                ):
                    if seg.state == dm.SEG_VERIFIED:
                        continue
                    path = self.store.seg_path(job_id, seg.index)
                    if path.exists() and os_path_size(path) == seg.size:
                        actual = file_sha256(path)
                        if actual == seg.expected_sha256:
                            seg.state = dm.SEG_VERIFIED
                            seg.actual_sha256 = actual
                            seg.failure_reason = None
                            seg.updated_at = now
                            changed = True
                if changed:
                    s.commit()
                    job = s.get(dm.DerivationJob, job_id)
            s.commit()
            return s.get(dm.DerivationJob, job_id)

    def _process_live(self, job_id: str, worker_id: str, fence: int) -> dict:
        now = self.clock()
        with self.sf() as s:
            job = self._mine(s, job_id, worker_id, fence)
            if job is None:
                s.rollback()
                return {"job_id": job_id, "state": "lost"}
            if job.status == dm.JOB_QUEUED:
                job.status = dm.JOB_PROCESSING
                job.started_at = job.started_at or now
                s.commit()
            segments = list(
                s.scalars(
                    select(dm.DerivationSegment)
                    .where(dm.DerivationSegment.job_id == job_id)
                    .order_by(dm.DerivationSegment.index)
                )
            )
            # Source snapshot keyed by segment (re-checked at use time).
            sources = {
                seg.index: s.get(
                    ArchivedVersion,
                    (job.tenant_id, seg.source_object_id, seg.source_version),
                )
                for seg in segments
            }

        for seg in segments:
            # Cancel boundary between segments.
            with self.sf() as s:
                job = self._mine(s, job_id, worker_id, fence)
                if job is None:
                    return {"job_id": job_id, "state": "lost"}
                if job.cancel_requested:
                    self._terminate(s, job, dm.JOB_CANCELED, "canceled while processing", self.clock())
                    s.commit()
                    self.store.cleanup_job(job_id)
                    return {"job_id": job_id, "state": dm.JOB_CANCELED}
                s.commit()

            with self.sf() as s:
                db_seg = s.get(dm.DerivationSegment, (job_id, seg.index))
                if db_seg.state == dm.SEG_VERIFIED and self.store.has_segment(job_id, seg.index):
                    continue
                v = sources[seg.index]
                if v is None or v.state != "active":
                    reason = (
                        f"segment {seg.index}: source vanished or is in a delete flow"
                    )
                    s.commit()
                    self._fail_job(job_id, fence, reason, seg.index)
                    return {"job_id": job_id, "state": dm.JOB_FAILED}
                blob_sha = v.content_sha256
                db_seg.state = dm.SEG_PROCESSING
                db_seg.updated_at = self.clock()
                s.commit()

            # Extract bytes outside any DB transaction (streaming IO).
            try:
                src_path = self.content.path_for(blob_sha)
                actual, got = self.store.extract_segment(
                    job_id, seg.index, src_path, seg.range_start, seg.range_end
                )
            except FileNotFoundError:
                self._fail_job(
                    job_id, fence,
                    f"segment {seg.index}: physical source blob missing",
                    seg.index,
                )
                return {"job_id": job_id, "state": dm.JOB_FAILED}

            # ---- CRASH POINT 1: part file durable, not yet marked verified.
            self._crash("derive.after_segment_written")

            if got != seg.size or actual != seg.expected_sha256:
                reason = (
                    f"segment {seg.index}: digest mismatch after extraction "
                    f"(got {actual}, expected {seg.expected_sha256})"
                )
                self._fail_job(job_id, fence, reason, seg.index)
                return {"job_id": job_id, "state": dm.JOB_FAILED}

            with self.sf() as s:
                mine = s.get(dm.DerivationJob, job_id)
                if mine is None or mine.lease_owner != worker_id or mine.fence != fence:
                    s.rollback()
                    return {"job_id": job_id, "state": "lost"}
                db_seg = s.get(dm.DerivationSegment, (job_id, seg.index))
                if db_seg.state != dm.SEG_VERIFIED:
                    db_seg.state = dm.SEG_VERIFIED
                    db_seg.actual_sha256 = actual
                    db_seg.failure_reason = None
                    db_seg.updated_at = self.clock()
                s.commit()

        # All segments verified -> assemble.
        return self._assemble_and_publish(job_id, worker_id, fence)

    def _assemble_and_publish(self, job_id: str, worker_id: str, fence: int) -> dict:
        now = self.clock()
        with self.sf() as s:
            job = self._mine(s, job_id, worker_id, fence)
            if job is None:
                s.rollback()
                return {"job_id": job_id, "state": "lost"}
            if job.cancel_requested:
                self._terminate(s, job, dm.JOB_CANCELED, "canceled while assembling", now)
                s.commit()
                self.store.cleanup_job(job_id)
                return {"job_id": job_id, "state": dm.JOB_CANCELED}
            count = s.scalar(
                select(func.count()).select_from(dm.DerivationSegment).where(
                    dm.DerivationSegment.job_id == job_id,
                    dm.DerivationSegment.state == dm.SEG_VERIFIED,
                )
            )
            total = s.scalar(
                select(func.count()).select_from(dm.DerivationSegment).where(
                    dm.DerivationSegment.job_id == job_id
                )
            )
            if count != total:
                s.rollback()
                # A crash/failure raced us; re-enter processing.
                return self._process_live(job_id, worker_id, fence)
            job.status = dm.JOB_ASSEMBLING
            s.commit()

        assembled_sha, assembled_size = self.store.assemble(job_id, total)

        # ---- CRASH POINT 2: full concatenation staged, not yet published.
        self._crash("derive.after_assembled")

        with self.sf() as s:
            job = self._mine(s, job_id, worker_id, fence)
            if job is None:
                s.rollback()
                return {"job_id": job_id, "state": "lost"}
            expected_size = job.total_size
            # Re-verify every source is still pinned & active (protections were
            # held throughout, so this is defense in depth).
            for seg in s.scalars(
                select(dm.DerivationSegment).where(dm.DerivationSegment.job_id == job_id)
            ):
                v = s.get(
                    ArchivedVersion,
                    (job.tenant_id, seg.source_object_id, seg.source_version),
                )
                if v is None or v.state != "active":
                    reason = f"segment {seg.index}: source no longer available at publish"
                    s.commit()
                    self._fail_job(job_id, fence, reason, seg.index)
                    return {"job_id": job_id, "state": dm.JOB_FAILED}
            s.commit()

        if assembled_size != expected_size:
            self._fail_job(
                job_id, fence,
                f"assembled size {assembled_size} != expected {expected_size}",
            )
            return {"job_id": job_id, "state": dm.JOB_FAILED}

        # Publish the immutable result through the content-addressed store. The
        # bytes only become downloadable once the archive row exists.
        data = self.store.read_assembled(job_id)
        sha256, size, _ = self.content.put(data)
        if sha256 != assembled_sha or size != expected_size:
            self._fail_job(job_id, fence, "assembled content digest inconsistent")
            return {"job_id": job_id, "state": dm.JOB_FAILED}

        with self.sf() as s:
            job = self._mine(s, job_id, worker_id, fence)
            if job is None:
                s.rollback()
                return {"job_id": job_id, "state": "lost"}

            # A cancel that landed after assembly must still win: no result
            # version is ever published for a canceled job.
            if job.cancel_requested:
                self._terminate(s, job, dm.JOB_CANCELED, "canceled before publish", self.clock())
                s.commit()
                self.store.cleanup_job(job_id)
                return {"job_id": job_id, "state": dm.JOB_CANCELED}

            # The result version must still be free (another derivation could
            # target the same explicit object/version).
            out_v = job.output_version
            existing = s.get(ArchivedVersion, (job.tenant_id, job.output_object_id, out_v))
            if existing is not None:
                # Converge safely only if it is exactly our content; otherwise
                # this job cannot publish and fails deterministically.
                if existing.content_sha256 != sha256 or existing.size != size:
                    reason = (
                        f"output {job.output_object_id}/v{out_v} was taken by "
                        "different content"
                    )
                    self._terminate(s, job, dm.JOB_FAILED, reason, self.clock())
                    s.commit()
                    self.store.cleanup_job(job_id)
                    return {"job_id": job_id, "state": dm.JOB_FAILED}
                published = True
            else:
                published = False

            if not published:
                blob = s.get(ContentBlob, sha256)
                now = self.clock()
                if blob is None:
                    blob = ContentBlob(
                        sha256=sha256,
                        size=size,
                        path=str(self.content.blob_path(sha256)),
                        refcount=1,
                        state="active",
                        created_at=now,
                    )
                    s.add(blob)
                else:
                    if blob.state == "purged":
                        blob.state = "active"
                        blob.purged_at = None
                        blob.path = str(self.content.blob_path(sha256))
                    blob.refcount += 1

                # Derived rows seal under the tenant's current archive policy;
                # every source version implies one exists, and it cannot be
                # deleted retroactively (policies are append-only).
                policy = s.scalars(
                    select(ArchivePolicy)
                    .where(ArchivePolicy.tenant_id == job.tenant_id)
                    .order_by(ArchivePolicy.version.desc())
                    .limit(1)
                ).first()
                if policy is None:
                    raise RuntimeError("archive policy vanished before publish")

                s.add(
                    ArchivedVersion(
                        tenant_id=job.tenant_id,
                        object_id=job.output_object_id,
                        version=out_v,
                        size=size,
                        content_sha256=sha256,
                        policy_version=policy.version,
                        retention_seconds=policy.retention_seconds,
                        sealed_at=now,
                        state="active",
                    )
                )

            # ---- CRASH POINT 3 boundary: publish row + job->published are one
            # atomic commit; billing happens in the NEXT transaction.
            job.status = dm.JOB_PUBLISHED
            job.assembled_at = job.assembled_at or self.clock()
            job.published_at = job.published_at or self.clock()
            job.result_object_id = job.output_object_id
            job.result_version = out_v
            job.result_sha256 = sha256
            job.result_size = size
            job.error = None
            s.commit()

        # ---- CRASH POINT 3: published (downloadable result exists) but the
        # billing transaction has not run yet; protections still held.
        self._crash("derive.before_billing")

        # Point 3 crash window: published but not billed. Billing is idempotent.
        return self._finalize_billing(job_id, worker_id, fence)

    def _finalize_billing(self, job_id: str, worker_id: str, fence: int) -> dict:
        """Convert the reservation into used bytes exactly once, release all
        source protections once, and detach the lease. Staging is removed after
        the durable commit. Safe to re-enter after a crash at point 3."""
        now = self.clock()
        with self._lock, self.sf() as s:
            job = s.get(dm.DerivationJob, job_id)
            if job is None:
                s.rollback()
                return {"job_id": job_id, "state": "lost"}
            if job.status == dm.JOB_BILLED:
                s.commit()
                # Defensive: staging should already be gone.
                self.store.cleanup_job(job_id)
                return {"job_id": job_id, "state": dm.JOB_BILLED, "replayed": True}
            if job.status != dm.JOB_PUBLISHED:
                s.rollback()
                return {"job_id": job_id, "state": job.status}

            # Exactly-once billing: UNIQUE(job, 'commit_used'). Both ledger rows
            # land in this one transaction with the protection releases.
            committed = s.scalar(
                select(func.count())
                .select_from(dm.DerivationLedger)
                .where(
                    dm.DerivationLedger.job_id == job_id,
                    dm.DerivationLedger.event_type == dm.EV_COMMIT_USED,
                )
            )
            if not committed:
                self._release_reservation_once(s, job, now)
                s.add(
                    dm.DerivationLedger(
                        tenant_id=job.tenant_id,
                        job_id=job_id,
                        event_type=dm.EV_COMMIT_USED,
                        bytes_delta=job.total_size,
                        created_at=now,
                    )
                )
            self._release_protections_once(s, job, now)
            job.status = dm.JOB_BILLED
            job.billed_at = job.billed_at or now
            job.finished_at = now
            job.lease_owner = None
            job.lease_expires_at = None
            job.cancel_requested = False
            try:
                s.commit()
            except IntegrityError:
                # On Postgres two finalizers of the same published job can race
                # on UNIQUE(job_id, event_type); the winner billed, converge.
                s.rollback()
                with self.sf() as s2:
                    winner = s2.get(dm.DerivationJob, job_id)
                    already_billed = winner is not None and winner.status == dm.JOB_BILLED
                if already_billed:
                    self.store.cleanup_job(job_id)
                    return {"job_id": job_id, "state": dm.JOB_BILLED, "replayed": True}
                raise

        # Output has been sealed into the content tree; staging is invisible
        # scratch space and is removed now.
        self.store.cleanup_job(job_id)
        return {"job_id": job_id, "state": dm.JOB_BILLED}

    # =============================================================== recovery

    def resume(self, *, force: bool = False, worker_id: str = "recovery") -> dict:
        """Recover after an abnormal exit.

        With force=True (single-process deployment / tests) every live lease is
        expired immediately and each unfinished job is driven forward in this
        process; otherwise crashed owners' leases simply expire and jobs are
        picked up by the ordinary claim path.

        Published-but-unbilled jobs are always finalized, even without force:
        they hold source protections and a reservation that must not linger,
        and the billing transition is fully idempotent."""
        if force:
            n = self.force_reclaim_all()
        else:
            n = len(self.reclaim_stale())

        finalized, resumed = 0, 0
        with self.sf() as s:
            # Anything published but not billed is recoverable without a lease:
            # billing only appends guarded ledger rows.
            pending_bill = list(
                s.scalars(
                    select(dm.DerivationJob)
                    .where(dm.DerivationJob.status == dm.JOB_PUBLISHED)
                    .order_by(dm.DerivationJob.published_at)
                )
            )
            claimable = list(
                s.scalars(
                    select(dm.DerivationJob)
                    .where(
                        dm.DerivationJob.status.in_(dm.JOB_LIVE_STATES),
                        dm.DerivationJob.lease_expires_at.is_not(None),
                        dm.DerivationJob.lease_expires_at <= self.clock(),
                    )
                    .order_by(dm.DerivationJob.created_at)
                )
            )

        for job in pending_bill:
            self._finalize_billing(job.id, "recovery", job.fence)
            finalized += 1

        if force:
            for job in claimable:
                job_id = self.claim_next(worker_id, now=self.clock())
                if job_id is None:
                    break
                with self.sf() as s:
                    claimed = s.get(dm.DerivationJob, job_id)
                    fence = claimed.fence
                self.process_job(job_id, worker_id, fence)
                resumed += 1

        self.store.sweep_tmp()
        return {"leases_expired": n, "billing_finalized": finalized, "jobs_resumed": resumed}


# ---------------------------------------------------------------- small utils

def os_path_size(path) -> int:
    import os

    return os.path.getsize(path)


def file_sha256(path) -> str:
    import hashlib

    h = hashlib.sha256()
    with open(path, "rb") as f:
        for buf in iter(lambda: f.read(1024 * 1024), b""):
            h.update(buf)
    return h.hexdigest()
