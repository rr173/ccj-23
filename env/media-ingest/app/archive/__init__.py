"""Archive subsystem: retention, legal holds, content dedup and provable deletion.

Public surface::

    ArchiveService       the service (use one instance per process)
    ContentStore        content-addressed physical blob store
    error types         NotFound, DeletionBlocked, VersionDeleted, ...
"""

from __future__ import annotations

from .content_store import ContentStore
from .errors import (
    ArchiveError,
    DeletionBlocked,
    HoldConflict,
    NotFound,
    RequestKeyConflict,
    RetentionPolicyMissing,
    VersionDeleted,
    VersionExists,
)
from .service import ArchiveService

__all__ = [
    "ArchiveError",
    "ArchiveService",
    "ContentStore",
    "DeletionBlocked",
    "HoldConflict",
    "NotFound",
    "RequestKeyConflict",
    "RetentionPolicyMissing",
    "VersionDeleted",
    "VersionExists",
]
