from __future__ import annotations

import uuid
from datetime import datetime, timezone

from sqlalchemy import (
    BigInteger,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    create_engine,
    event,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker


def utcnow() -> datetime:
    """Naive UTC — portable across SQLite and Postgres."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


def new_id() -> str:
    return uuid.uuid4().hex


class Base(DeclarativeBase):
    pass


class Tenant(Base):
    """A tenant. The mutable knobs (capacity cap / parallel cap / weight) live in
    PolicyVersion rows; tenants only points at the current version."""

    __tablename__ = "tenants"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    current_policy_version: Mapped[int] = mapped_column(Integer, default=1)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, onupdate=utcnow)


class PolicyVersion(Base):
    """Immutable policy revision. Uploads snapshot (policy_version, capacity_bytes,
    max_parallel_merges, weight) at creation time and keep using that revision for
    scheduling forever; policy changes only affect later uploads."""

    __tablename__ = "policy_versions"

    tenant_id: Mapped[str] = mapped_column(
        ForeignKey("tenants.id"), primary_key=True
    )
    version: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=False)
    capacity_bytes: Mapped[int] = mapped_column(BigInteger)
    max_parallel_merges: Mapped[int] = mapped_column(Integer)
    weight: Mapped[int] = mapped_column(Integer)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)


# Upload.status state machine:
#   uploading --(all chunks stored, /complete)--> merging --(merge ok)--> sealed
#   uploading/merging/failed --(TTL sweeper)-----> expired
#   uploading/failed/queued --(client DELETE)----> aborted
#   merging --(final digest mismatch)------------> failed --(client fixes chunks, /complete)--> merging
class Upload(Base):
    __tablename__ = "uploads"

    id: Mapped[str] = mapped_column(String(32), primary_key=True, default=new_id)
    tenant_id: Mapped[str] = mapped_column(String(64), index=True)
    status: Mapped[str] = mapped_column(String(16), default="uploading", index=True)
    total_size: Mapped[int] = mapped_column(BigInteger)
    chunk_size: Mapped[int] = mapped_column(BigInteger)
    total_chunks: Mapped[int] = mapped_column(Integer)
    expected_sha256: Mapped[str | None] = mapped_column(String(64), nullable=True)
    version_id: Mapped[str | None] = mapped_column(String(32), nullable=True)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    # Policy snapshot: the upload is forever scheduled/limited under this revision.
    policy_version: Mapped[int] = mapped_column(Integer, default=1)
    policy_capacity_bytes: Mapped[int] = mapped_column(BigInteger)
    policy_max_parallel_merges: Mapped[int] = mapped_column(Integer)
    policy_weight: Mapped[int] = mapped_column(Integer)
    # Idempotency request key (unique per tenant together with tenant_id).
    request_key: Mapped[str | None] = mapped_column(String(128), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, onupdate=utcnow)
    expires_at: Mapped[datetime] = mapped_column(DateTime)

    __table_args__ = (
        Index("uq_uploads_tenant_request_key", "tenant_id", "request_key", unique=True),
    )


class Chunk(Base):
    __tablename__ = "chunks"

    upload_id: Mapped[str] = mapped_column(ForeignKey("uploads.id"), primary_key=True)
    index: Mapped[int] = mapped_column(Integer, primary_key=True)
    state: Mapped[str] = mapped_column(String(8))  # stored | failed
    size: Mapped[int] = mapped_column(BigInteger, default=0)
    sha256: Mapped[str | None] = mapped_column(String(64), nullable=True)
    failures: Mapped[int] = mapped_column(Integer, default=0)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, onupdate=utcnow)


class Version(Base):
    """A sealed, immutable object. UNIQUE(upload_id) is the exactly-once guarantee:
    any number of merge retries/concurrent runs collapse to a single row."""

    __tablename__ = "versions"

    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    upload_id: Mapped[str] = mapped_column(String(32), unique=True, index=True)
    tenant_id: Mapped[str] = mapped_column(String(64), index=True)
    sha256: Mapped[str] = mapped_column(String(64))
    size: Mapped[int] = mapped_column(BigInteger)
    path: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)


class IdempotentRequest(Base):
    """Idempotency record: (tenant_id, request_key) maps to an upload and stores a
    fingerprint of the creation parameters. Same params replay; different params
    on a reused key are an explicit 409 conflict."""

    __tablename__ = "idempotent_requests"

    tenant_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    request_key: Mapped[str] = mapped_column(String(128), primary_key=True)
    upload_id: Mapped[str] = mapped_column(String(32), index=True)
    fingerprint: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)


class CapacityLedger(Base):
    """Append-only tenant capacity ledger. Every upload gets exactly one 'reserve'
    row at creation, and later exactly one terminal row:

      reserve      +bytes  (creation; capacity check under the CURRENT policy cap)
      release      -bytes  (abort / expiry; reservation given back, never used)
      commit_used  ±0 net   (seal: reservation turns into permanent used bytes)

    The UNIQUE(upload_id, event_type) constraint makes release/commit exactly-once
    even under retries and crashes mid-commit. Tenant occupancy is the SUM of all
    rows, so it can never double-count."""

    __tablename__ = "capacity_ledger"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    tenant_id: Mapped[str] = mapped_column(String(64), index=True)
    upload_id: Mapped[str] = mapped_column(String(32), index=True)
    event_type: Mapped[str] = mapped_column(String(16))  # reserve | release | commit_used
    bytes_delta: Mapped[int] = mapped_column(BigInteger)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)

    __table_args__ = (
        Index(
            "uq_ledger_upload_event",
            "upload_id",
            "event_type",
            unique=True,
        ),
    )


class MergeJob(Base):
    """Durable merge queue. One row per upload (UNIQUE upload_id) — enqueue is
    therefore exactly-once across /complete retries and process crashes.

    Ordering:
      seq       per-tenant monotonic FIFO position (same tenant stays FIFO)
      vtag      weighted-fair virtual tag: the k-th job of a tenant with weight w
                gets vtag = k / w (floating). The scheduler runs the globally
                smallest eligible vtag, so service is proportional to weight and
                a continuously backlogged low-weight tenant still advances.

    Lifecycle: queued -> running -> done (sealed or permanently failed) /
    canceled (upload aborted while queued). A lease guards running jobs so a
    crashed worker's jobs are reclaimed once their lease expires."""

    __tablename__ = "merge_jobs"

    upload_id: Mapped[str] = mapped_column(String(32), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String(64), index=True)
    # Scheduling knobs are snapshotted from the upload's policy revision.
    weight: Mapped[int] = mapped_column(Integer)
    max_parallel_merges: Mapped[int] = mapped_column(Integer)
    policy_version: Mapped[int] = mapped_column(Integer)
    status: Mapped[str] = mapped_column(String(16), default="queued", index=True)
    seq: Mapped[int] = mapped_column(Integer)
    vtag: Mapped[float] = mapped_column(Float, index=True)
    attempts: Mapped[int] = mapped_column(Integer, default=0)
    not_before: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    lease_owner: Mapped[str | None] = mapped_column(String(64), nullable=True)
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    enqueued_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)
    started_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    result: Mapped[str | None] = mapped_column(String(16), nullable=True)  # sealed | failed
    error: Mapped[str | None] = mapped_column(Text, nullable=True)

    __table_args__ = (
        # FIFO within a tenant; ties on vtag break deterministically.
        Index("ix_merge_jobs_tenant_seq", "tenant_id", "seq"),
        Index("ix_merge_jobs_ready", "status", "vtag"),
    )


def make_engine(url: str):
    kwargs = {}
    if url.startswith("sqlite"):
        kwargs["connect_args"] = {"check_same_thread": False, "timeout": 30}

    engine = create_engine(url, pool_pre_ping=True, **kwargs)

    if url.startswith("sqlite"):
        # WAL allows the API and worker to run in separate processes sharing the
        # file; busy_timeout lets writers wait instead of failing immediately.
        @event.listens_for(engine, "connect")
        def _sqlite_connect(dbapi_conn, _record):
            cur = dbapi_conn.cursor()
            cur.execute("PRAGMA journal_mode=WAL")
            cur.execute("PRAGMA busy_timeout=30000")
            cur.execute("PRAGMA synchronous=NORMAL")
            cur.close()
            # Take transaction control away from the driver so the "begin" event
            # can choose the transaction kind (official pysqlite concurrency
            # recipe): every transaction becomes BEGIN IMMEDIATE, i.e. one global
            # writer at a time — concurrent capacity checks queue instead of
            # racing, and readers never see a torn reserve.
            dbapi_conn.isolation_level = None

        @event.listens_for(engine, "begin")
        def _sqlite_begin_immediate(conn):
            conn.exec_driver_sql("BEGIN IMMEDIATE")

    return engine


def ensure_tx_started(session) -> None:
    """Force the transaction to begin now. On SQLite the engine-wide "begin"
    hook turns this into BEGIN IMMEDIATE (exclusive writer lock); on Postgres
    callers additionally take row locks with SELECT ... FOR UPDATE."""
    session.connection()


def init_db(engine, attempts: int = 10) -> None:
    """create_all that tolerates api and worker starting concurrently."""
    import time

    from sqlalchemy import inspect

    for i in range(attempts):
        try:
            Base.metadata.create_all(engine)
            return
        except Exception:
            try:
                with engine.connect() as conn:
                    if inspect(conn).has_table("uploads"):
                        return  # another process won the race
            except Exception:
                pass
            if i == attempts - 1:
                raise
            time.sleep(0.2 * (i + 1))


def make_session_factory(engine) -> sessionmaker:
    return sessionmaker(engine, expire_on_commit=False)
