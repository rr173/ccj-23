from __future__ import annotations


class ArchiveError(Exception):
    """Base class for archive subsystem errors."""


class NotFound(ArchiveError):
    """Tenant/object/version does not exist (also used to enforce tenant isolation)."""


class RetentionPolicyMissing(ArchiveError):
    """No retention policy has ever been published for this tenant."""


class VersionExists(ArchiveError):
    """An object version with this number already exists (versions are immutable)."""


class VersionDeleted(ArchiveError):
    """The version is a tombstone: logical deletion is irreversible."""


class DeletionBlocked(ArchiveError):
    """Deletion is currently not allowed; ``blockers`` says exactly why."""

    def __init__(self, blockers: list[dict]):
        self.blockers = blockers
        primary = blockers[0]["reason"] if blockers else "unknown"
        super().__init__(f"deletion blocked: {primary}")


class HoldConflict(ArchiveError):
    """A hold/reference with the same key already exists in a conflicting state."""


class RequestKeyConflict(ArchiveError):
    """The same idempotency key was reused for a different delete request."""
