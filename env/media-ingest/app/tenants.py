"""Tenant and policy management.

Each tenant has three tunable knobs:
  capacity_bytes        — upper bound on reserved+used bytes (checked at creation
                          against the CURRENT revision; lowering it never deletes
                          existing objects/sealed bytes, only blocks new reserves)
  max_parallel_merges   — per-tenant cap on concurrently running merges
  weight                — scheduling share; service between tenants is
                          proportional to weight while FIFO holds within a tenant

Every PUT writes a NEW immutable PolicyVersion row and bumps the tenant's current
version pointer. Uploads snapshot the revision they were created under, so policy
changes only affect uploads created afterwards.
"""
from __future__ import annotations

from sqlalchemy import select

from .db import PolicyVersion, Tenant


class TenantNotFound(Exception):
    pass


def create_tenant(session_factory, tenant_id: str, capacity_bytes: int,
                  max_parallel_merges: int, weight: int) -> dict:
    """Create tenant with policy version 1. Returns current policy."""
    with session_factory() as s:
        existing = s.get(Tenant, tenant_id)
        if existing is not None:
            raise ValueError(f"tenant {tenant_id!r} already exists")
        s.add(Tenant(id=tenant_id, current_policy_version=1))
        s.add(
            PolicyVersion(
                tenant_id=tenant_id,
                version=1,
                capacity_bytes=capacity_bytes,
                max_parallel_merges=max_parallel_merges,
                weight=weight,
            )
        )
        s.commit()
    return {
        "tenant_id": tenant_id,
        "version": 1,
        "capacity_bytes": capacity_bytes,
        "max_parallel_merges": max_parallel_merges,
        "weight": weight,
    }


def get_or_bootstrap_tenant(session, tenant_id: str, default_capacity: int,
                            default_parallel: int, default_weight: int) -> Tenant:
    """Return the tenant, transparently bootstrapping it with defaults on first use.
    Used by the test/demo path; explicit POST /tenants is the production path."""
    t = session.get(Tenant, tenant_id)
    if t is None:
        t = Tenant(id=tenant_id, current_policy_version=1)
        session.add(t)
        session.add(
            PolicyVersion(
                tenant_id=tenant_id,
                version=1,
                capacity_bytes=default_capacity,
                max_parallel_merges=default_parallel,
                weight=default_weight,
            )
        )
        session.flush()
    return t


def lock_or_bootstrap(session, tenant_id: str, default_capacity: int,
                      default_parallel: int, default_weight: int) -> PolicyVersion:
    """Like accounting.lock_tenant, but also auto-creates an unknown tenant.

    Must be the first thing in the transaction (SQLite begins IMMEDIATE via the
    engine hook; Postgres takes a FOR UPDATE row lock when the row exists). A
    concurrent first-create on Postgres collapses on the tenants primary key.
    """
    from .db import ensure_tx_started

    ensure_tx_started(session)
    dialect = session.bind.dialect.name
    if dialect != "sqlite":
        t = session.scalar(select(Tenant).where(Tenant.id == tenant_id).with_for_update())
    else:
        t = session.get(Tenant, tenant_id)
    if t is None:
        t = get_or_bootstrap_tenant(
            session, tenant_id, default_capacity, default_parallel, default_weight
        )
    return session.get(PolicyVersion, (tenant_id, t.current_policy_version))


def current_policy(session, tenant_id: str) -> PolicyVersion:
    t = session.get(Tenant, tenant_id)
    if t is None:
        raise TenantNotFound(tenant_id)
    pv = session.get(PolicyVersion, (tenant_id, t.current_policy_version))
    return pv


def put_policy(session_factory, tenant_id: str, capacity_bytes: int,
               max_parallel_merges: int, weight: int) -> dict:
    """Append a new policy revision. Does not touch running/queued uploads — they
    keep scheduling under the revision snapshotted on their row."""
    with session_factory() as s:
        t = s.get(Tenant, tenant_id)
        if t is None:
            raise TenantNotFound(tenant_id)
        next_version = t.current_policy_version + 1
        s.add(
            PolicyVersion(
                tenant_id=tenant_id,
                version=next_version,
                capacity_bytes=capacity_bytes,
                max_parallel_merges=max_parallel_merges,
                weight=weight,
            )
        )
        t.current_policy_version = next_version
        s.commit()
    return {
        "tenant_id": tenant_id,
        "version": next_version,
        "capacity_bytes": capacity_bytes,
        "max_parallel_merges": max_parallel_merges,
        "weight": weight,
    }


def get_tenant(session_factory, tenant_id: str) -> dict:
    with session_factory() as s:
        t = s.get(Tenant, tenant_id)
        if t is None:
            raise TenantNotFound(tenant_id)
        pv = s.get(PolicyVersion, (tenant_id, t.current_policy_version))
        return {
            "tenant_id": t.id,
            "version": pv.version,
            "capacity_bytes": pv.capacity_bytes,
            "max_parallel_merges": pv.max_parallel_merges,
            "weight": pv.weight,
            "created_at": t.created_at,
        }
