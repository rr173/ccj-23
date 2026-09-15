"""Derivation subsystem: build new immutable objects from pinned ranges of
existing sealed versions.

Public surface::

    DerivationService   the service (one instance per process)
    DerivationStore     private per-job staging files
    parse_recipe        validate/canonicalize a recipe dict/JSON
    error types         RecipeError, ReferenceRejected, RequestKeyConflict, ...
"""

from __future__ import annotations

from .errors import (
    CapacityExceeded,
    DeriveError,
    NotCancellable,
    NotFound,
    ReferenceRejected,
    RequestKeyConflict,
)
from .recipe import Recipe, RecipeError, SegmentSpec, parse_recipe
from .service import DerivationService
from .storage import DerivationStore

__all__ = [
    "CapacityExceeded",
    "DerivationService",
    "DerivationStore",
    "DeriveError",
    "NotCancellable",
    "NotFound",
    "Recipe",
    "RecipeError",
    "ReferenceRejected",
    "RequestKeyConflict",
    "SegmentSpec",
    "parse_recipe",
]
