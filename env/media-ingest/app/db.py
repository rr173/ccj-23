from __future__ import annotations

import uuid
from datetime import datetime, timezone

from sqlalchemy import BigInteger, DateTime, ForeignKey, Integer, String, Text, create_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker


def utcnow() -> datetime:
    """Naive UTC — portable across SQLite and Postgres."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


def new_id() -> str:
    return uuid.uuid4().hex


class Base(DeclarativeBase):
    pass


# Upload.status state machine:
#   uploading --(all chunks stored, /complete)--> merging --(merge ok)--> sealed
#   uploading/merging/failed --(TTL sweeper)-----> expired
#   uploading/failed --(client DELETE)-----------> aborted
#   merging --(final digest mismatch)------------> failed --(client fixes chunks, /complete)--> merging
class Upload(Base):
    __tablename__ = "uploads"

    id: Mapped[str] = mapped_column(String(32), primary_key=True, default=new_id)
    status: Mapped[str] = mapped_column(String(16), default="uploading", index=True)
    total_size: Mapped[int] = mapped_column(BigInteger)
    chunk_size: Mapped[int] = mapped_column(BigInteger)
    total_chunks: Mapped[int] = mapped_column(Integer)
    expected_sha256: Mapped[str | None] = mapped_column(String(64), nullable=True)
    version_id: Mapped[str | None] = mapped_column(String(32), nullable=True)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, onupdate=utcnow)
    expires_at: Mapped[datetime] = mapped_column(DateTime)


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

    id: Mapped[str] = mapped_column(String(32), primary_key=True, default=new_id)
    upload_id: Mapped[str] = mapped_column(String(32), unique=True, index=True)
    sha256: Mapped[str] = mapped_column(String(64))
    size: Mapped[int] = mapped_column(BigInteger)
    path: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)


def make_engine(url: str):
    kwargs = {}
    if url.startswith("sqlite"):
        kwargs["connect_args"] = {"check_same_thread": False}
    return create_engine(url, pool_pre_ping=True, **kwargs)


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
