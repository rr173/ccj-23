"""Derivation recipe parsing, validation and canonicalization.

A recipe references fixed versions of sealed objects of ONE tenant and selects
a half-open byte range ``[start, end)`` plus an expected SHA-256 digest for
each segment::

    {
      "segments": [
        {"object_id": "a", "version": 1,
         "range": [0, 10], "sha256": "..."},
        {"object_id": "b", "version": 3,
         "start": 5, "end": 25, "expected_sha256": "..."},
        ...
      ]
    }

Semantically identical recipes — segments written in different key orders,
ranges written as a list/tuple ``[s, e]`` vs ``{"start": s, "end": e}`` vs a
string ``"s-e"``, digest hex in upper case — normalize to the SAME canonical
JSON and therefore the same digest. Order of segments is semantic (it defines
concatenation order), so it is NOT reordered.

The digest declared per segment is over the selected RANGE bytes only.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass

SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


class RecipeError(ValueError):
    """The recipe payload is malformed (distinct from rejected references)."""


@dataclass(frozen=True)
class SegmentSpec:
    object_id: str
    version: int
    start: int
    end: int  # exclusive
    sha256: str  # lowercase hex, digest of the selected range bytes

    @property
    def size(self) -> int:
        return self.end - self.start

    def canonical(self) -> dict:
        return {
            "object_id": self.object_id,
            "range": [self.start, self.end],
            "sha256": self.sha256,
            "version": self.version,
        }


@dataclass(frozen=True)
class Recipe:
    segments: tuple[SegmentSpec, ...]

    @property
    def total_size(self) -> int:
        return sum(seg.size for seg in self.segments)

    def canonical_json(self) -> str:
        """Deterministic serialization: sorted keys, compact separators."""
        payload = {"segments": [seg.canonical() for seg in self.segments]}
        return json.dumps(
            payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False
        )

    def digest(self) -> str:
        return hashlib.sha256(self.canonical_json().encode("utf-8")).hexdigest()


def _parse_range(raw) -> tuple[int, int]:
    """Accept [s, e] / (s, e), {'start': s, 'end': e} or the string 's-e'."""
    if isinstance(raw, str):
        parts = raw.split("-")
        if len(parts) != 2:
            raise RecipeError(f"invalid range {raw!r}: expected 'start-end'")
        try:
            start, end = int(parts[0]), int(parts[1])
        except ValueError as exc:
            raise RecipeError(f"invalid range {raw!r}: bounds must be integers") from exc
    elif isinstance(raw, dict):
        if "start" not in raw or "end" not in raw:
            raise RecipeError("range object requires 'start' and 'end'")
        start, end = raw["start"], raw["end"]
    elif isinstance(raw, (list, tuple)):
        if len(raw) != 2:
            raise RecipeError("range list must contain exactly [start, end]")
        start, end = raw[0], raw[1]
    else:
        raise RecipeError(f"unsupported range form: {type(raw).__name__}")

    if isinstance(start, bool) or isinstance(end, bool):
        raise RecipeError("range bounds must be integers")
    if not isinstance(start, int) or not isinstance(end, int):
        raise RecipeError("range bounds must be integers")
    if start < 0 or end < 0:
        raise RecipeError("range bounds must be >= 0")
    if end < start:
        raise RecipeError(f"range end {end} precedes start {start}")
    if end == start:
        raise RecipeError("empty ranges are not allowed")
    return start, end


def parse_recipe(raw: dict | str | Recipe) -> Recipe:
    """Validate user input and produce the normalized Recipe.

    Raises RecipeError for any malformed field. Unknown fields are ignored so
    adding metadata never changes the digest; semantic order is preserved.
    """
    if isinstance(raw, Recipe):
        return raw
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise RecipeError(f"recipe is not valid JSON: {exc}") from exc
    if not isinstance(raw, dict):
        raise RecipeError("recipe must be an object with 'segments'")

    raw_segments = raw.get("segments")
    if not isinstance(raw_segments, list) or not raw_segments:
        raise RecipeError("recipe requires a non-empty 'segments' list")

    specs: list[SegmentSpec] = []
    for i, seg in enumerate(raw_segments):
        if not isinstance(seg, dict):
            raise RecipeError(f"segment {i} must be an object")

        object_id = seg.get("object_id") or seg.get("object")
        if not isinstance(object_id, str) or not object_id:
            raise RecipeError(f"segment {i}: object_id is required")

        version = seg.get("version")
        if isinstance(version, bool) or not isinstance(version, int) or version < 1:
            raise RecipeError(f"segment {i}: version must be a positive integer")

        # Range may be nested under "range" or given as flat start/end fields.
        if "range" in seg and seg["range"] is not None:
            start, end = _parse_range(seg["range"])
        elif "start" in seg or "end" in seg:
            start, end = _parse_range(
                {"start": seg.get("start"), "end": seg.get("end")}
            )
        else:
            raise RecipeError(f"segment {i}: a 'range' or start/end is required")

        digest = (
            seg.get("sha256")
            or seg.get("expected_sha256")
            or seg.get("digest")
        )
        if not isinstance(digest, str):
            raise RecipeError(f"segment {i}: sha256 is required")
        digest = digest.strip().lower()
        if not SHA256_RE.match(digest):
            raise RecipeError(f"segment {i}: sha256 must be 64 hex characters")

        specs.append(
            SegmentSpec(
                object_id=object_id,
                version=version,
                start=start,
                end=end,
                sha256=digest,
            )
        )

    return Recipe(segments=tuple(specs))
