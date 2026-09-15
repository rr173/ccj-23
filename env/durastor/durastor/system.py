"""Durable replica management for sealed media versions.

A single SQLite database persists all control-plane state (nodes, policies,
placement plans, replicas, tasks, quarantine records).  Replica payloads live
one-file-per-version under each node's storage directory; the sealed master
copy lives in the origin directory.

Key invariants enforced here:

* Placement is atomic: capacity is reserved on every planned node inside one
  transaction, or not at all.  A failed plan leaves zero reservations.
* Capacity is settled exactly once -- at reservation time.  Confirming a
  replica never touches capacity again; releasing happens exactly once when a
  location transitions to ``removed`` (guarded UPDATE).
* Only ``verified`` replicas ever serve reads or act as repair/migration
  sources.  Data copied from a source is re-checked against the sealed
  checksum before the target becomes readable, so corruption can never be
  propagated into a new valid replica.
* All crash windows (file written but unconfirmed, quarantine recorded but
  repair task missing, replacement confirmed but capacity not released) are
  closed by :meth:`DurabilitySystem.recover`.
"""

from __future__ import annotations

import hashlib
import json
import os
import random
import sqlite3
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable, Dict, List, Optional

from .errors import DurabilityError, PlacementBlocked, UnreadableError

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

NODE_ACTIVE = "active"
NODE_DRAINING = "draining"
NODE_OFFLINE = "offline"
NODE_STATUSES = (NODE_ACTIVE, NODE_DRAINING, NODE_OFFLINE)

LOC_PENDING = "pending"          # 等待
LOC_COPYING = "copying"          # 复制中
LOC_VERIFIED = "verified"        # 已校验
LOC_QUARANTINED = "quarantined"  # 隔离
LOC_REPAIRING = "repairing"      # 修复中
LOC_REMOVED = "removed"          # 已移除
LOCATION_STATES = (
    LOC_PENDING, LOC_COPYING, LOC_VERIFIED,
    LOC_QUARANTINED, LOC_REPAIRING, LOC_REMOVED,
)

TASK_PENDING = "pending"
TASK_CLAIMED = "claimed"
TASK_DONE = "done"
TASK_BLOCKED = "blocked"
TASK_CANCELLED = "cancelled"

T_REPLICATE = "replicate"
T_REPAIR = "repair"
T_MIGRATE = "migrate"

HEALTHY = "healthy"
DEGRADED = "degraded"
UNREADABLE = "unreadable"

SCHEMA = """
CREATE TABLE IF NOT EXISTS nodes (
    node_id        TEXT PRIMARY KEY,
    capacity_total INTEGER NOT NULL,
    capacity_used  INTEGER NOT NULL DEFAULT 0,
    fault_domain   TEXT NOT NULL,
    status         TEXT NOT NULL DEFAULT 'active',
    created_at     TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS policies (
    policy_id       TEXT PRIMARY KEY,
    tenant_id       TEXT NOT NULL,
    version         INTEGER NOT NULL,
    target_replicas INTEGER NOT NULL,
    min_readable    INTEGER NOT NULL,
    allowed_nodes   TEXT,           -- JSON array of node ids, NULL = all nodes
    created_at      TEXT NOT NULL,
    UNIQUE (tenant_id, version)
);

CREATE TABLE IF NOT EXISTS media_versions (
    version_id       TEXT PRIMARY KEY,
    tenant_id        TEXT NOT NULL,
    size_bytes       INTEGER NOT NULL,
    checksum         TEXT NOT NULL,  -- sha256 of the full sealed content
    block_size       INTEGER NOT NULL,
    block_checksums  TEXT NOT NULL,  -- JSON array of per-block sha256 (scrub manifest)
    policy_id        TEXT NOT NULL,  -- policy version pinned at seal time
    state            TEXT NOT NULL,  -- sealed | placed | placement_blocked
    blocked_reasons  TEXT,           -- JSON array when placement_blocked
    created_at       TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS plan_locations (
    location_id    TEXT PRIMARY KEY,  -- "<version_id>:<node_id>"
    version_id     TEXT NOT NULL,
    node_id        TEXT NOT NULL,
    state          TEXT NOT NULL,
    reserved_bytes INTEGER NOT NULL,
    checksum       TEXT,
    created_at     TEXT NOT NULL,
    updated_at     TEXT NOT NULL,
    UNIQUE (version_id, node_id)
);
CREATE INDEX IF NOT EXISTS idx_locations_version ON plan_locations(version_id);
CREATE INDEX IF NOT EXISTS idx_locations_node ON plan_locations(node_id);

CREATE TABLE IF NOT EXISTS tasks (
    task_id             TEXT PRIMARY KEY,
    type                TEXT NOT NULL,  -- replicate | repair | migrate
    version_id          TEXT NOT NULL,
    location_id         TEXT,           -- replicate / repair target location
    src_location_id     TEXT,           -- migrate: location on the draining node
    dst_location_id     TEXT,           -- migrate: replacement location
    phase               TEXT,           -- migrate: planned | replacement_confirmed | done
    status              TEXT NOT NULL,  -- pending | claimed | done | blocked | cancelled
    blocked_reason      TEXT,
    claim_owner         TEXT,
    claim_lease_expires REAL,
    created_at          TEXT NOT NULL,
    updated_at          TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status);

CREATE TABLE IF NOT EXISTS quarantine_records (
    quarantine_id TEXT PRIMARY KEY,
    version_id    TEXT NOT NULL,
    location_id   TEXT NOT NULL,
    node_id       TEXT NOT NULL,
    reason        TEXT NOT NULL,
    created_at    TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS scrub_records (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    version_id   TEXT NOT NULL,
    location_id  TEXT NOT NULL,
    byte_offset  INTEGER NOT NULL,
    length       INTEGER NOT NULL,
    result       TEXT NOT NULL,  -- ok | mismatch | missing
    created_at   TEXT NOT NULL
);
"""


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _write_file_atomic(path: str, chunks) -> str:
    """Write *chunks* to *path* via temp-file + rename; return sha256 written."""
    tmp = "{}.tmp-{}".format(path, uuid.uuid4().hex)
    h = hashlib.sha256()
    with open(tmp, "wb") as fh:
        for chunk in chunks:
            fh.write(chunk)
            h.update(chunk)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)
    return h.hexdigest()


@dataclass
class SealResult:
    version_id: str
    ok: bool
    policy_id: Optional[str] = None
    reasons: List[str] = field(default_factory=list)


class DurabilitySystem:
    """Control plane + worker for durable replica management."""

    def __init__(
        self,
        root_dir: str,
        block_size: int = 4096,
        lease_seconds: float = 30.0,
        hooks: Optional[Dict[str, Callable]] = None,
        clock: Optional[Callable[[], float]] = None,
    ):
        self.root_dir = root_dir
        self.db_path = os.path.join(root_dir, "durastor.db")
        self.origin_dir = os.path.join(root_dir, "origin")
        self.node_root = os.path.join(root_dir, "nodes")
        self.block_size = block_size
        self.lease_seconds = lease_seconds
        # Test/recovery hooks, fired at the exact crash windows:
        #   after_replica_file_written, after_quarantine_recorded,
        #   after_replacement_confirmed
        self.hooks = hooks or {}
        self.clock = clock or time.time
        os.makedirs(self.origin_dir, exist_ok=True)
        os.makedirs(self.node_root, exist_ok=True)
        self._init_db()

    # ------------------------------------------------------------------
    # infrastructure
    # ------------------------------------------------------------------

    def _init_db(self) -> None:
        conn = sqlite3.connect(self.db_path)
        try:
            conn.executescript(SCHEMA)
            conn.commit()
        finally:
            conn.close()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=60)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=60000")
        return conn

    @contextmanager
    def _tx(self):
        """One atomic write transaction (BEGIN IMMEDIATE, always closed)."""
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            yield conn
            conn.commit()
        except BaseException:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _query(self, sql: str, args=()) -> List[sqlite3.Row]:
        conn = self._connect()
        try:
            return conn.execute(sql, args).fetchall()
        finally:
            conn.close()

    def _query_one(self, sql: str, args=()) -> Optional[sqlite3.Row]:
        rows = self._query(sql, args)
        return rows[0] if rows else None

    def _fire_hook(self, name: str, ctx: dict) -> None:
        hook = self.hooks.get(name)
        if hook is not None:
            hook(ctx)

    # path helpers ------------------------------------------------------

    def _origin_path(self, version_id: str) -> str:
        return os.path.join(self.origin_dir, version_id)

    def _node_dir(self, node_id: str) -> str:
        return os.path.join(self.node_root, node_id)

    def _replica_path(self, node_id: str, version_id: str) -> str:
        return os.path.join(self._node_dir(node_id), version_id)

    # ------------------------------------------------------------------
    # admin API
    # ------------------------------------------------------------------

    def register_node(self, node_id: str, capacity: int, fault_domain: str,
                      status: str = NODE_ACTIVE) -> None:
        """Register (or update capacity/fault-domain of) a storage node."""
        if status not in NODE_STATUSES:
            raise ValueError("invalid node status: %r" % (status,))
        ts = _utcnow_iso()
        with self._tx() as conn:
            conn.execute(
                """INSERT INTO nodes(node_id, capacity_total, capacity_used,
                                     fault_domain, status, created_at)
                   VALUES(?,?,0,?,?,?)
                   ON CONFLICT(node_id) DO UPDATE SET
                       capacity_total=excluded.capacity_total,
                       fault_domain=excluded.fault_domain""",
                (node_id, capacity, fault_domain, status, ts),
            )
        os.makedirs(self._node_dir(node_id), exist_ok=True)

    def set_node_status(self, node_id: str, status: str) -> None:
        """Change a node's service status.

        ``draining`` starts safe migration of every replica still counted
        toward durability.  Re-activating a node never resurrects quarantined
        replicas -- only a successful repair can make them readable again.
        """
        if status not in NODE_STATUSES:
            raise ValueError("invalid node status: %r" % (status,))
        if status == NODE_DRAINING:
            self._drain_node(node_id)
        else:
            with self._tx() as conn:
                cur = conn.execute("UPDATE nodes SET status=? WHERE node_id=?",
                                   (status, node_id))
                if cur.rowcount == 0:
                    raise DurabilityError("unknown node %r" % (node_id,))

    def publish_policy(self, tenant_id: str, target_replicas: int,
                       min_readable: int,
                       allowed_nodes: Optional[List[str]] = None) -> str:
        """Publish a new, immutable policy version for a tenant."""
        if target_replicas < 1:
            raise ValueError("target_replicas must be >= 1")
        if not 1 <= min_readable <= target_replicas:
            raise ValueError("min_readable must be within [1, target_replicas]")
        with self._tx() as conn:
            row = conn.execute(
                "SELECT MAX(version) AS v FROM policies WHERE tenant_id=?",
                (tenant_id,)).fetchone()
            version = (row["v"] or 0) + 1
            policy_id = "%s:v%d" % (tenant_id, version)
            conn.execute(
                """INSERT INTO policies(policy_id, tenant_id, version,
                                        target_replicas, min_readable,
                                        allowed_nodes, created_at)
                   VALUES(?,?,?,?,?,?,?)""",
                (policy_id, tenant_id, version, target_replicas, min_readable,
                 json.dumps(allowed_nodes) if allowed_nodes else None,
                 _utcnow_iso()),
            )
        return policy_id

    def seal_version(self, tenant_id: str, version_id: str,
                     content: bytes) -> SealResult:
        """Seal a media version and build its placement plan.

        Pins the tenant's latest policy version at this moment.  If placement
        is impossible the version is kept (state ``placement_blocked``) with
        the blocking reasons, and no capacity is reserved anywhere.
        """
        if isinstance(content, str):
            content = content.encode()
        policy = self._query_one(
            "SELECT * FROM policies WHERE tenant_id=? ORDER BY version DESC LIMIT 1",
            (tenant_id,))
        if policy is None:
            raise DurabilityError(
                "no durability policy published for tenant %r" % (tenant_id,))

        _write_file_atomic(self._origin_path(version_id), [content])
        checksum = _sha256_bytes(content)
        blocks = [
            _sha256_bytes(content[i:i + self.block_size])
            for i in range(0, len(content), self.block_size)
        ]
        try:
            with self._tx() as conn:
                conn.execute(
                    """INSERT INTO media_versions(version_id, tenant_id, size_bytes,
                                                  checksum, block_size, block_checksums,
                                                  policy_id, state, created_at)
                       VALUES(?,?,?,?,?,?,?,?,?)""",
                    (version_id, tenant_id, len(content), checksum,
                     self.block_size, json.dumps(blocks), policy["policy_id"],
                     "sealed", _utcnow_iso()),
                )
        except sqlite3.IntegrityError:
            raise DurabilityError("media version %r already exists" % (version_id,))
        return self._place(version_id)

    def replan(self, version_id: str) -> SealResult:
        """Retry placement for a version whose plan was previously blocked."""
        if self._query_one("SELECT version_id FROM media_versions WHERE version_id=?",
                           (version_id,)) is None:
            raise DurabilityError("unknown media version %r" % (version_id,))
        return self._place(version_id)

    # ------------------------------------------------------------------
    # placement
    # ------------------------------------------------------------------

    def _place(self, version_id: str) -> SealResult:
        try:
            with self._tx() as conn:
                self._create_plan(conn, version_id)
                conn.execute(
                    "UPDATE media_versions SET state='placed', blocked_reasons=NULL "
                    "WHERE version_id=?", (version_id,))
            v = self._query_one(
                "SELECT policy_id FROM media_versions WHERE version_id=?",
                (version_id,))
            return SealResult(version_id=version_id, ok=True,
                              policy_id=v["policy_id"])
        except PlacementBlocked as exc:
            with self._tx() as conn:
                conn.execute(
                    "UPDATE media_versions SET state='placement_blocked', "
                    "blocked_reasons=? WHERE version_id=?",
                    (json.dumps(exc.reasons), version_id))
            return SealResult(version_id=version_id, ok=False, reasons=exc.reasons)

    def _create_plan(self, conn: sqlite3.Connection, version_id: str) -> None:
        """Form a placement plan and atomically reserve capacity.

        Runs inside the caller's transaction: either every location is
        planned and every reservation made, or the transaction rolls back
        and nothing is left behind.
        """
        v = conn.execute("SELECT * FROM media_versions WHERE version_id=?",
                         (version_id,)).fetchone()
        policy = conn.execute("SELECT * FROM policies WHERE policy_id=?",
                              (v["policy_id"],)).fetchone()
        size = v["size_bytes"]
        existing = conn.execute(
            "SELECT * FROM plan_locations WHERE version_id=? AND state != 'removed'",
            (version_id,)).fetchall()
        used_nodes = {loc["node_id"] for loc in existing}
        needed = policy["target_replicas"] - len(existing)
        if needed <= 0:
            return

        allowed = (json.loads(policy["allowed_nodes"])
                   if policy["allowed_nodes"] else None)
        nodes = conn.execute("SELECT * FROM nodes").fetchall()

        def free(n):
            return n["capacity_total"] - n["capacity_used"]

        eligible, unavailable = [], []
        for n in nodes:
            if allowed is not None and n["node_id"] not in allowed:
                continue
            if n["node_id"] in used_nodes:
                continue
            if n["status"] != NODE_ACTIVE:
                unavailable.append(n)
                continue
            eligible.append(n)

        blocking = []
        if len(eligible) < needed:
            msg = ("not enough eligible nodes: need %d, have %d active allowed "
                   "node(s)" % (needed, len(eligible)))
            if unavailable:
                msg += "; excluded (unavailable): " + ", ".join(
                    "%s(status=%s)" % (n["node_id"], n["status"])
                    for n in unavailable)
            blocking.append(msg)

        with_cap = [n for n in eligible if free(n) >= size]
        if len(with_cap) < needed:
            lacking = [n for n in eligible if free(n) < size]
            msg = ("insufficient capacity: need %d node(s) with >= %d bytes "
                   "free, only %d eligible node(s) qualify"
                   % (needed, size, len(with_cap)))
            if lacking:
                msg += "; nodes without enough free capacity: " + ", ".join(
                    "%s(free=%d < %d)" % (n["node_id"], free(n), size)
                    for n in lacking)
            blocking.append(msg)

        fds_available = {n["fault_domain"] for n in with_cap}
        required_fds = min(2, policy["target_replicas"])
        if len(fds_available) < required_fds:
            blocking.append(
                "insufficient fault domains: need at least %d distinct fault "
                "domain(s), only %d available among eligible nodes with "
                "capacity: %s" % (required_fds, len(fds_available),
                                  sorted(fds_available) or "none"))

        if blocking:
            raise PlacementBlocked(blocking)

        selected = self._spread_select(with_cap, free, needed)
        ts = _utcnow_iso()
        for n in selected:
            cur = conn.execute(
                "UPDATE nodes SET capacity_used = capacity_used + ? "
                "WHERE node_id=? AND capacity_total - capacity_used >= ?",
                (size, n["node_id"], size))
            if cur.rowcount == 0:
                # Lost a capacity race: rolling back releases every
                # reservation made so far in this transaction.
                raise PlacementBlocked(
                    ["capacity reservation failed on node %s (concurrent "
                     "change); no reservations were kept" % n["node_id"]])
            loc_id = "%s:%s" % (version_id, n["node_id"])
            conn.execute(
                """INSERT INTO plan_locations(location_id, version_id, node_id,
                                              state, reserved_bytes,
                                              created_at, updated_at)
                   VALUES(?,?,?,?,?,?,?)""",
                (loc_id, version_id, n["node_id"], LOC_PENDING, size, ts, ts))
            conn.execute(
                """INSERT INTO tasks(task_id, type, version_id, location_id,
                                     status, created_at, updated_at)
                   VALUES(?,?,?,?,?,?,?)""",
                ("replicate:%s" % loc_id, T_REPLICATE, version_id, loc_id,
                 TASK_PENDING, ts, ts))

    @staticmethod
    def _spread_select(candidates, free, count):
        """Pick *count* nodes, preferring fault domains not yet used."""
        selected = []
        fd_count: Dict[str, int] = {}
        pool = list(candidates)
        while pool and len(selected) < count:
            best = min(pool, key=lambda n: (fd_count.get(n["fault_domain"], 0),
                                            -free(n), n["node_id"]))
            pool.remove(best)
            selected.append(best)
            fd_count[best["fault_domain"]] = fd_count.get(best["fault_domain"], 0) + 1
        return selected

    # ------------------------------------------------------------------
    # worker
    # ------------------------------------------------------------------

    def run_worker(self, worker_id: str, max_tasks: Optional[int] = None) -> int:
        """Claim and execute tasks until none remain; returns tasks executed."""
        self.recover()
        self.retry_blocked_tasks()
        executed = 0
        while max_tasks is None or executed < max_tasks:
            task = self._claim_next_task(worker_id)
            if task is None:
                break
            self._execute_task(dict(task), worker_id)
            executed += 1
        return executed

    def _claim_next_task(self, worker_id: str) -> Optional[sqlite3.Row]:
        now = self.clock()
        lease = now + self.lease_seconds
        with self._tx() as conn:
            row = conn.execute(
                """SELECT * FROM tasks
                   WHERE status='pending'
                      OR (status='claimed' AND claim_lease_expires < ?)
                   ORDER BY rowid LIMIT 1""", (now,)).fetchone()
            if row is None:
                return None
            cur = conn.execute(
                """UPDATE tasks SET status='claimed', claim_owner=?,
                                   claim_lease_expires=?, updated_at=?
                   WHERE task_id=?
                     AND (status='pending'
                          OR (status='claimed' AND claim_lease_expires < ?))""",
                (worker_id, lease, _utcnow_iso(), row["task_id"], now))
            if cur.rowcount == 0:
                return None
            return conn.execute("SELECT * FROM tasks WHERE task_id=?",
                                (row["task_id"],)).fetchone()

    def _execute_task(self, task: dict, worker_id: str) -> None:
        if task["type"] == T_REPLICATE:
            self._exec_replicate(task, worker_id)
        elif task["type"] == T_REPAIR:
            self._exec_repair(task, worker_id)
        elif task["type"] == T_MIGRATE:
            self._exec_migrate(task, worker_id)

    def _finish_task(self, task_id: str) -> None:
        with self._tx() as conn:
            conn.execute("UPDATE tasks SET status='done', updated_at=? "
                         "WHERE task_id=?", (_utcnow_iso(), task_id))

    def _block_task(self, task_id: str, reason: str) -> None:
        with self._tx() as conn:
            conn.execute("UPDATE tasks SET status='blocked', blocked_reason=?, "
                         "updated_at=? WHERE task_id=?",
                         (reason, _utcnow_iso(), task_id))

    # -- replicate ------------------------------------------------------

    def _exec_replicate(self, task: dict, worker_id: str) -> None:
        loc = self._query_one("SELECT * FROM plan_locations WHERE location_id=?",
                              (task["location_id"],))
        if loc is None or loc["state"] in (LOC_REMOVED, LOC_VERIFIED):
            self._finish_task(task["task_id"])
            return
        v = self._query_one("SELECT * FROM media_versions WHERE version_id=?",
                            (task["version_id"],))
        dst = self._replica_path(loc["node_id"], v["version_id"])
        ts = _utcnow_iso()
        with self._tx() as conn:
            conn.execute(
                "UPDATE plan_locations SET state=?, updated_at=? "
                "WHERE location_id=? AND state IN (?,?)",
                (LOC_COPYING, ts, loc["location_id"], LOC_PENDING, LOC_COPYING))
        if not (os.path.exists(dst) and _sha256_file(dst) == v["checksum"]):
            os.makedirs(os.path.dirname(dst), exist_ok=True)
            with open(self._origin_path(v["version_id"]), "rb") as src:
                digest = _write_file_atomic(
                    dst, iter(lambda: src.read(1 << 20), b""))
            if digest != v["checksum"]:
                raise DurabilityError(
                    "origin content for %r does not match its sealed checksum"
                    % v["version_id"])
        # Crash window A: file written but replica not yet confirmed.
        self._fire_hook("after_replica_file_written",
                        {"task": task, "location": dict(loc)})
        with self._tx() as conn:
            conn.execute(
                "UPDATE plan_locations SET state=?, checksum=?, updated_at=? "
                "WHERE location_id=? AND state=?",
                (LOC_VERIFIED, v["checksum"], _utcnow_iso(),
                 loc["location_id"], LOC_COPYING))
            conn.execute("UPDATE tasks SET status='done', updated_at=? "
                         "WHERE task_id=?", (_utcnow_iso(), task["task_id"]))

    # -- repair ---------------------------------------------------------

    def _exec_repair(self, task: dict, worker_id: str) -> None:
        loc = self._query_one("SELECT * FROM plan_locations WHERE location_id=?",
                              (task["location_id"],))
        if loc is None or loc["state"] in (LOC_REMOVED, LOC_VERIFIED):
            self._finish_task(task["task_id"])
            return
        v = self._query_one("SELECT * FROM media_versions WHERE version_id=?",
                            (task["version_id"],))
        ts = _utcnow_iso()
        with self._tx() as conn:
            conn.execute(
                "UPDATE plan_locations SET state=?, updated_at=? "
                "WHERE location_id=? AND state IN (?,?)",
                (LOC_REPAIRING, ts, loc["location_id"],
                 LOC_QUARANTINED, LOC_REPAIRING))
        dst = self._replica_path(loc["node_id"], v["version_id"])
        # Idempotency: a previous attempt may have written good bytes already.
        if os.path.exists(dst) and _sha256_file(dst) == v["checksum"]:
            with self._tx() as conn:
                conn.execute(
                    "UPDATE plan_locations SET state=?, checksum=?, updated_at=? "
                    "WHERE location_id=? AND state=?",
                    (LOC_VERIFIED, v["checksum"], _utcnow_iso(),
                     loc["location_id"], LOC_REPAIRING))
                conn.execute("UPDATE tasks SET status='done', updated_at=? "
                             "WHERE task_id=?", (_utcnow_iso(), task["task_id"]))
            return
        candidates = self._query(
            "SELECT * FROM plan_locations WHERE version_id=? AND state='verified' "
            "AND node_id != ? ORDER BY node_id",
            (v["version_id"], loc["node_id"]))
        ok = bool(candidates) and self._copy_from_verified_source(v, candidates, dst)
        if ok:
            with self._tx() as conn:
                conn.execute(
                    "UPDATE plan_locations SET state=?, checksum=?, updated_at=? "
                    "WHERE location_id=? AND state=?",
                    (LOC_VERIFIED, v["checksum"], _utcnow_iso(),
                     loc["location_id"], LOC_REPAIRING))
                conn.execute("UPDATE tasks SET status='done', updated_at=? "
                             "WHERE task_id=?", (_utcnow_iso(), task["task_id"]))
        else:
            reason = ("no trusted replica available as repair source"
                      if not candidates else
                      "all candidate repair sources failed verification")
            with self._tx() as conn:
                conn.execute(
                    "UPDATE plan_locations SET state=?, updated_at=? "
                    "WHERE location_id=? AND state=?",
                    (LOC_QUARANTINED, _utcnow_iso(), loc["location_id"],
                     LOC_REPAIRING))
                conn.execute(
                    "UPDATE tasks SET status='blocked', blocked_reason=?, "
                    "updated_at=? WHERE task_id=?",
                    (reason, _utcnow_iso(), task["task_id"]))

    def _copy_from_verified_source(self, version, candidates, dst) -> bool:
        """Copy from the first candidate whose bytes match the sealed checksum.

        A candidate whose bytes fail verification is quarantined -- corrupt
        data is never installed as a valid replica and never propagated.
        """
        for cand in candidates:
            src = self._replica_path(cand["node_id"], version["version_id"])
            if not os.path.exists(src):
                self._quarantine(cand["location_id"],
                                 "repair/migrate: source replica file missing")
                continue
            if _sha256_file(src) != version["checksum"]:
                self._quarantine(
                    cand["location_id"],
                    "repair/migrate: source replica failed checksum verification")
                continue
            os.makedirs(os.path.dirname(dst), exist_ok=True)
            with open(src, "rb") as fh:
                digest = _write_file_atomic(
                    dst, iter(lambda: fh.read(1 << 20), b""))
            if digest != version["checksum"]:
                raise DurabilityError(
                    "local write to %s failed verification" % dst)
            return True
        return False

    # -- migrate (node drain) --------------------------------------------

    def _exec_migrate(self, task: dict, worker_id: str) -> None:
        if task["status"] == TASK_DONE:
            return
        src_loc = self._query_one("SELECT * FROM plan_locations WHERE location_id=?",
                                  (task["src_location_id"],))
        dst_loc = (self._query_one("SELECT * FROM plan_locations WHERE location_id=?",
                                   (task["dst_location_id"],))
                   if task["dst_location_id"] else None)
        v = self._query_one("SELECT * FROM media_versions WHERE version_id=?",
                            (task["version_id"],))
        if dst_loc is None:
            self._block_task(task["task_id"],
                             "migration has no target location yet")
            return
        if dst_loc["state"] != LOC_VERIFIED:
            dst_path = self._replica_path(dst_loc["node_id"], v["version_id"])
            if not (os.path.exists(dst_path)
                    and _sha256_file(dst_path) == v["checksum"]):
                candidates = []
                if src_loc is not None and src_loc["state"] == LOC_VERIFIED:
                    candidates.append(src_loc)
                candidates.extend(self._query(
                    "SELECT * FROM plan_locations WHERE version_id=? "
                    "AND state='verified' AND node_id NOT IN (?,?) "
                    "ORDER BY node_id",
                    (v["version_id"],
                     src_loc["node_id"] if src_loc else "",
                     dst_loc["node_id"])))
                if not candidates:
                    self._block_task(
                        task["task_id"],
                        "no verified replica available as migration source; "
                        "original replica is kept")
                    return
                if not self._copy_from_verified_source(v, candidates, dst_path):
                    self._block_task(
                        task["task_id"],
                        "all migration sources failed verification; "
                        "original replica is kept")
                    return
            with self._tx() as conn:
                conn.execute(
                    "UPDATE plan_locations SET state=?, checksum=?, updated_at=? "
                    "WHERE location_id=? AND state IN (?,?)",
                    (LOC_VERIFIED, v["checksum"], _utcnow_iso(),
                     dst_loc["location_id"], LOC_PENDING, LOC_COPYING))
                conn.execute(
                    "UPDATE tasks SET phase='replacement_confirmed', updated_at=? "
                    "WHERE task_id=?", (_utcnow_iso(), task["task_id"]))
            # Crash window C: replacement confirmed but original capacity
            # not yet released.
            self._fire_hook("after_replacement_confirmed", {"task": task})
        self._finalize_migration(task["task_id"])

    def _finalize_migration(self, task_id: str) -> None:
        """Release the source location of a confirmed migration (idempotent)."""
        with self._tx() as conn:
            task = conn.execute("SELECT * FROM tasks WHERE task_id=?",
                                (task_id,)).fetchone()
            if task is None or task["status"] == TASK_DONE:
                return
            src = conn.execute("SELECT * FROM plan_locations WHERE location_id=?",
                               (task["src_location_id"],)).fetchone()
            v = conn.execute("SELECT * FROM media_versions WHERE version_id=?",
                             (task["version_id"],)).fetchone()
            policy = conn.execute("SELECT * FROM policies WHERE policy_id=?",
                                  (v["policy_id"],)).fetchone()
            if src is not None and src["state"] != LOC_REMOVED:
                verified = conn.execute(
                    "SELECT COUNT(*) AS c FROM plan_locations "
                    "WHERE version_id=? AND state='verified'",
                    (v["version_id"],)).fetchone()["c"]
                if verified < policy["min_readable"]:
                    # Never drop below the minimum readable count: keep the
                    # original replica until enough verified copies exist.
                    return
                cur = conn.execute(
                    "UPDATE plan_locations SET state='removed', updated_at=? "
                    "WHERE location_id=? AND state != 'removed'",
                    (_utcnow_iso(), src["location_id"]))
                if cur.rowcount:
                    conn.execute(
                        "UPDATE nodes SET capacity_used = MAX(0, capacity_used - ?) "
                        "WHERE node_id=?",
                        (src["reserved_bytes"], src["node_id"]))
            conn.execute(
                "UPDATE tasks SET status='done', phase='done', updated_at=? "
                "WHERE task_id=?", (_utcnow_iso(), task_id))

    # ------------------------------------------------------------------
    # recovery
    # ------------------------------------------------------------------

    def recover(self) -> None:
        """Close every crash window left by a previous process.

        Idempotent; safe to run on every worker start.  Only touches work
        items whose lease has expired, so live workers are never disturbed.
        """
        now = self.clock()
        ts = _utcnow_iso()

        # 1. Expired task claims become claimable again.
        with self._tx() as conn:
            conn.execute(
                "UPDATE tasks SET status='pending', claim_owner=NULL, "
                "claim_lease_expires=NULL, updated_at=? "
                "WHERE status='claimed' AND claim_lease_expires < ?", (ts, now))

        # 2. Crash window A: locations stuck in 'copying' with no live claim.
        rows = self._query(
            """SELECT l.* FROM plan_locations l
               WHERE l.state='copying' AND NOT EXISTS (
                   SELECT 1 FROM tasks t
                   WHERE t.type='replicate' AND t.location_id=l.location_id
                     AND t.status='claimed' AND t.claim_lease_expires >= ?)""",
            (now,))
        for loc in rows:
            v = self._query_one("SELECT * FROM media_versions WHERE version_id=?",
                                (loc["version_id"],))
            path = self._replica_path(loc["node_id"], loc["version_id"])
            if os.path.exists(path) and _sha256_file(path) == v["checksum"]:
                # The bytes were fully written: confirm them in place instead
                # of copying again -- no new reservation, no second replica.
                with self._tx() as conn:
                    conn.execute(
                        "UPDATE plan_locations SET state='verified', checksum=?, "
                        "updated_at=? WHERE location_id=? AND state='copying'",
                        (v["checksum"], ts, loc["location_id"]))
                    conn.execute(
                        "UPDATE tasks SET status='done', updated_at=? "
                        "WHERE type='replicate' AND location_id=? "
                        "AND status != 'done'", (ts, loc["location_id"]))
            else:
                # Torn/partial write: discard and let a worker re-copy.
                if os.path.exists(path):
                    os.remove(path)
                with self._tx() as conn:
                    conn.execute(
                        "UPDATE plan_locations SET state='pending', updated_at=? "
                        "WHERE location_id=? AND state='copying'",
                        (ts, loc["location_id"]))

        # 3. Repairs interrupted mid-flight return to quarantine.
        with self._tx() as conn:
            conn.execute(
                """UPDATE plan_locations SET state='quarantined', updated_at=?
                   WHERE state='repairing' AND NOT EXISTS (
                       SELECT 1 FROM tasks t
                       WHERE t.type='repair'
                         AND t.location_id=plan_locations.location_id
                         AND t.status='claimed' AND t.claim_lease_expires >= ?)""",
                (ts, now))

        # 4. Crash window B: quarantine records without a repair task.
        for q in self._query("SELECT * FROM quarantine_records"):
            task_id = "repair:%s:%s" % (q["location_id"], q["quarantine_id"])
            with self._tx() as conn:
                conn.execute(
                    """INSERT OR IGNORE INTO tasks(task_id, type, version_id,
                                                   location_id, status,
                                                   created_at, updated_at)
                       VALUES(?,?,?,?,?,?,?)""",
                    (task_id, T_REPAIR, q["version_id"], q["location_id"],
                     TASK_PENDING, ts, ts))

        # 5. Crash window C: migrations confirmed but not finalized.
        for row in self._query(
                "SELECT task_id FROM tasks WHERE type='migrate' "
                "AND phase='replacement_confirmed' AND status != 'done'"):
            self._finalize_migration(row["task_id"])

    def retry_blocked_tasks(self) -> None:
        """Re-arm blocked tasks whose blocking condition may have cleared."""
        ts = _utcnow_iso()
        for t in self._query("SELECT * FROM tasks WHERE status='blocked'"):
            if t["type"] == T_REPAIR:
                loc = self._query_one(
                    "SELECT * FROM plan_locations WHERE location_id=?",
                    (t["location_id"],))
                if loc is None or loc["state"] == LOC_REMOVED:
                    self._finish_task(t["task_id"])
                    continue
                src = self._query_one(
                    "SELECT 1 AS x FROM plan_locations WHERE version_id=? "
                    "AND state='verified' AND node_id != ? LIMIT 1",
                    (t["version_id"], loc["node_id"]))
                if src:
                    with self._tx() as conn:
                        conn.execute(
                            "UPDATE tasks SET status='pending', blocked_reason=NULL, "
                            "updated_at=? WHERE task_id=? AND status='blocked'",
                            (ts, t["task_id"]))
            elif t["type"] == T_MIGRATE:
                if t["dst_location_id"] is None:
                    self._replan_blocked_migration(t)
                else:
                    src = self._query_one(
                        "SELECT 1 AS x FROM plan_locations WHERE version_id=? "
                        "AND state='verified' LIMIT 1", (t["version_id"],))
                    if src:
                        with self._tx() as conn:
                            conn.execute(
                                "UPDATE tasks SET status='pending', "
                                "blocked_reason=NULL, updated_at=? "
                                "WHERE task_id=? AND status='blocked'",
                                (ts, t["task_id"]))

    def _replan_blocked_migration(self, task) -> None:
        ts = _utcnow_iso()
        with self._tx() as conn:
            src_loc = conn.execute("SELECT * FROM plan_locations WHERE location_id=?",
                                   (task["src_location_id"],)).fetchone()
            if src_loc is None or src_loc["state"] == LOC_REMOVED:
                conn.execute("UPDATE tasks SET status='done', phase='done', "
                             "updated_at=? WHERE task_id=?", (ts, task["task_id"]))
                return
            v = conn.execute("SELECT * FROM media_versions WHERE version_id=?",
                             (task["version_id"],)).fetchone()
            policy = conn.execute("SELECT * FROM policies WHERE policy_id=?",
                                  (v["policy_id"],)).fetchone()
            dst = self._pick_migration_target(conn, v, policy,
                                              exclude_node=src_loc["node_id"])
            if dst is None:
                conn.execute(
                    "UPDATE tasks SET blocked_reason=?, updated_at=? WHERE task_id=?",
                    ("still no legal target node: no active allowed node with "
                     "enough free capacity outside the draining node",
                     ts, task["task_id"]))
                return
            cur = conn.execute(
                "UPDATE nodes SET capacity_used = capacity_used + ? "
                "WHERE node_id=? AND capacity_total - capacity_used >= ?",
                (src_loc["reserved_bytes"], dst["node_id"],
                 src_loc["reserved_bytes"]))
            if cur.rowcount == 0:
                return  # stay blocked; capacity raced away
            dst_loc_id = "%s:%s" % (v["version_id"], dst["node_id"])
            conn.execute(
                """INSERT INTO plan_locations(location_id, version_id, node_id,
                                              state, reserved_bytes,
                                              created_at, updated_at)
                   VALUES(?,?,?,?,?,?,?)""",
                (dst_loc_id, v["version_id"], dst["node_id"], LOC_PENDING,
                 src_loc["reserved_bytes"], ts, ts))
            conn.execute(
                "UPDATE tasks SET dst_location_id=?, status='pending', "
                "blocked_reason=NULL, updated_at=? WHERE task_id=?",
                (dst_loc_id, ts, task["task_id"]))

    # ------------------------------------------------------------------
    # node drain
    # ------------------------------------------------------------------

    def _drain_node(self, node_id: str) -> None:
        with self._tx() as conn:
            node = conn.execute("SELECT * FROM nodes WHERE node_id=?",
                                (node_id,)).fetchone()
            if node is None:
                raise DurabilityError("unknown node %r" % (node_id,))
            if node["status"] == NODE_DRAINING:
                return
            conn.execute("UPDATE nodes SET status=? WHERE node_id=?",
                         (NODE_DRAINING, node_id))
            locs = conn.execute(
                "SELECT * FROM plan_locations WHERE node_id=? AND state != 'removed'",
                (node_id,)).fetchall()
            ts = _utcnow_iso()
            for loc in locs:
                if loc["state"] != LOC_VERIFIED:
                    # Not counted toward durability: cancel and release the
                    # reservation immediately.
                    cur = conn.execute(
                        "UPDATE plan_locations SET state='removed', updated_at=? "
                        "WHERE location_id=? AND state != 'removed'",
                        (ts, loc["location_id"]))
                    if cur.rowcount:
                        conn.execute(
                            "UPDATE nodes SET capacity_used = "
                            "MAX(0, capacity_used - ?) WHERE node_id=?",
                            (loc["reserved_bytes"], node_id))
                    conn.execute(
                        "UPDATE tasks SET status='cancelled', updated_at=? "
                        "WHERE (location_id=? OR dst_location_id=?) "
                        "AND status IN ('pending','claimed','blocked')",
                        (ts, loc["location_id"], loc["location_id"]))
                    continue
                # Verified replica counted toward durability: plan a migration.
                v = conn.execute("SELECT * FROM media_versions WHERE version_id=?",
                                 (loc["version_id"],)).fetchone()
                policy = conn.execute("SELECT * FROM policies WHERE policy_id=?",
                                      (v["policy_id"],)).fetchone()
                task_id = "migrate:%s" % loc["location_id"]
                if conn.execute("SELECT 1 FROM tasks WHERE task_id=?",
                                (task_id,)).fetchone():
                    continue
                dst = self._pick_migration_target(conn, v, policy,
                                                  exclude_node=node_id)
                if dst is not None:
                    cur = conn.execute(
                        "UPDATE nodes SET capacity_used = capacity_used + ? "
                        "WHERE node_id=? AND capacity_total - capacity_used >= ?",
                        (loc["reserved_bytes"], dst["node_id"],
                         loc["reserved_bytes"]))
                    if cur.rowcount == 0:
                        dst = None
                if dst is None:
                    conn.execute(
                        """INSERT INTO tasks(task_id, type, version_id,
                                             src_location_id, phase, status,
                                             blocked_reason, created_at, updated_at)
                           VALUES(?,?,?,?,?,?,?,?,?)""",
                        (task_id, T_MIGRATE, loc["version_id"],
                         loc["location_id"], "planned", TASK_BLOCKED,
                         "no legal target node: no active allowed node with "
                         "enough free capacity outside the draining node; "
                         "original replica is kept", ts, ts))
                else:
                    dst_loc_id = "%s:%s" % (loc["version_id"], dst["node_id"])
                    conn.execute(
                        """INSERT INTO plan_locations(location_id, version_id,
                                                      node_id, state,
                                                      reserved_bytes,
                                                      created_at, updated_at)
                           VALUES(?,?,?,?,?,?,?)""",
                        (dst_loc_id, loc["version_id"], dst["node_id"],
                         LOC_PENDING, loc["reserved_bytes"], ts, ts))
                    conn.execute(
                        """INSERT INTO tasks(task_id, type, version_id,
                                             src_location_id, dst_location_id,
                                             phase, status, created_at, updated_at)
                           VALUES(?,?,?,?,?,?,?,?,?)""",
                        (task_id, T_MIGRATE, loc["version_id"],
                         loc["location_id"], dst_loc_id, "planned",
                         TASK_PENDING, ts, ts))

    def _pick_migration_target(self, conn, v, policy, exclude_node):
        used = {r["node_id"] for r in conn.execute(
            "SELECT node_id FROM plan_locations WHERE version_id=? "
            "AND state != 'removed'", (v["version_id"],))}
        used.add(exclude_node)
        allowed = (json.loads(policy["allowed_nodes"])
                   if policy["allowed_nodes"] else None)
        fd_count = {}
        for r in conn.execute(
                "SELECT n.fault_domain AS fd, COUNT(*) AS c "
                "FROM plan_locations l JOIN nodes n ON n.node_id = l.node_id "
                "WHERE l.version_id=? AND l.state != 'removed' "
                "GROUP BY n.fault_domain", (v["version_id"],)):
            fd_count[r["fd"]] = r["c"]
        best, best_key = None, None
        for n in conn.execute("SELECT * FROM nodes WHERE status='active'").fetchall():
            if n["node_id"] in used:
                continue
            if allowed is not None and n["node_id"] not in allowed:
                continue
            free = n["capacity_total"] - n["capacity_used"]
            if free < v["size_bytes"]:
                continue
            key = (fd_count.get(n["fault_domain"], 0), -free, n["node_id"])
            if best is None or key < best_key:
                best, best_key = n, key
        return best

    # ------------------------------------------------------------------
    # scrub & quarantine
    # ------------------------------------------------------------------

    def scrub_version(self, version_id: str, block_indices=None,
                      num_blocks: int = 1, full: bool = False, rng=None) -> list:
        """Integrity-scrub every verified replica of a version.

        By default samples random byte ranges (per-block); ``full=True``
        checks every block.  A missing file or checksum mismatch quarantines
        the replica immediately and schedules a repair.
        """
        rng = rng or random
        v = self._query_one("SELECT * FROM media_versions WHERE version_id=?",
                            (version_id,))
        if v is None:
            raise DurabilityError("unknown media version %r" % (version_id,))
        manifest = json.loads(v["block_checksums"])
        bs = v["block_size"]
        results = []
        locs = self._query(
            "SELECT * FROM plan_locations WHERE version_id=? AND state='verified' "
            "ORDER BY node_id", (version_id,))
        for loc in locs:
            path = self._replica_path(loc["node_id"], version_id)
            if not os.path.exists(path):
                self._record_scrub(version_id, loc["location_id"], -1, 0, "missing")
                self._quarantine(loc["location_id"], "scrub: replica file missing")
                results.append({"location_id": loc["location_id"],
                                "node_id": loc["node_id"], "result": "missing"})
                continue
            if full:
                indices = list(range(len(manifest)))
            elif block_indices is not None:
                indices = list(block_indices)
            elif manifest:
                indices = rng.sample(range(len(manifest)),
                                     min(num_blocks, len(manifest)))
            else:
                indices = []
            outcome = "ok"
            for bi in indices:
                off = bi * bs
                length = min(bs, v["size_bytes"] - off)
                with open(path, "rb") as fh:
                    fh.seek(off)
                    data = fh.read(length)
                ok = _sha256_bytes(data) == manifest[bi]
                self._record_scrub(version_id, loc["location_id"], off, length,
                                   "ok" if ok else "mismatch")
                if not ok:
                    self._quarantine(
                        loc["location_id"],
                        "scrub: checksum mismatch at offset %d (block %d)"
                        % (off, bi))
                    outcome = "mismatch"
                    break
            results.append({"location_id": loc["location_id"],
                            "node_id": loc["node_id"], "result": outcome})
        return results

    def _record_scrub(self, version_id, location_id, offset, length, result):
        with self._tx() as conn:
            conn.execute(
                """INSERT INTO scrub_records(version_id, location_id, byte_offset,
                                             length, result, created_at)
                   VALUES(?,?,?,?,?,?)""",
                (version_id, location_id, offset, length, result, _utcnow_iso()))

    def _quarantine(self, location_id: str, reason: str) -> Optional[str]:
        """Quarantine a verified replica, then schedule its repair.

        The quarantine record and the repair task are separate steps on
        purpose; recovery closes the window if the process dies in between.
        """
        qid = uuid.uuid4().hex
        ts = _utcnow_iso()
        with self._tx() as conn:
            cur = conn.execute(
                "UPDATE plan_locations SET state='quarantined', updated_at=? "
                "WHERE location_id=? AND state='verified'", (ts, location_id))
            if cur.rowcount == 0:
                return None  # no longer verified; nothing to quarantine
            loc = conn.execute("SELECT * FROM plan_locations WHERE location_id=?",
                               (location_id,)).fetchone()
            conn.execute(
                """INSERT INTO quarantine_records(quarantine_id, version_id,
                                                  location_id, node_id, reason,
                                                  created_at)
                   VALUES(?,?,?,?,?,?)""",
                (qid, loc["version_id"], location_id, loc["node_id"], reason, ts))
        # Crash window B: quarantine recorded but repair task not yet created.
        self._fire_hook("after_quarantine_recorded",
                        {"location_id": location_id, "quarantine_id": qid,
                         "reason": reason})
        with self._tx() as conn:
            conn.execute(
                """INSERT OR IGNORE INTO tasks(task_id, type, version_id,
                                               location_id, status,
                                               created_at, updated_at)
                   VALUES(?,?,?,?,?,?,?)""",
                ("repair:%s:%s" % (location_id, qid), T_REPAIR,
                 loc["version_id"], location_id, TASK_PENDING, ts, ts))
        return qid

    # ------------------------------------------------------------------
    # read & status
    # ------------------------------------------------------------------

    def read(self, version_id: str) -> bytes:
        """Read content from a verified replica only.

        Quarantined or incomplete replicas never participate.  A replica that
        fails verification while serving is quarantined on the spot.
        """
        v = self._query_one("SELECT * FROM media_versions WHERE version_id=?",
                            (version_id,))
        if v is None:
            raise DurabilityError("unknown media version %r" % (version_id,))
        locs = list(self._query(
            "SELECT * FROM plan_locations WHERE version_id=? AND state='verified'",
            (version_id,)))
        random.shuffle(locs)
        if not locs:
            raise UnreadableError(
                "version %r is unreadable: no verified replicas available"
                % (version_id,))
        for loc in locs:
            path = self._replica_path(loc["node_id"], version_id)
            if not os.path.exists(path):
                self._quarantine(loc["location_id"], "read: replica file missing")
                continue
            with open(path, "rb") as fh:
                data = fh.read()
            if _sha256_bytes(data) != v["checksum"]:
                self._quarantine(loc["location_id"], "read: checksum mismatch")
                continue
            return data
        raise UnreadableError(
            "version %r is unreadable: every verified replica failed its "
            "integrity check" % (version_id,))

    def version_status(self, version_id: str) -> dict:
        v = self._query_one("SELECT * FROM media_versions WHERE version_id=?",
                            (version_id,))
        if v is None:
            raise DurabilityError("unknown media version %r" % (version_id,))
        policy = self._query_one("SELECT * FROM policies WHERE policy_id=?",
                                 (v["policy_id"],))
        locs = self._query(
            """SELECT l.*, n.fault_domain, n.status AS node_status
               FROM plan_locations l JOIN nodes n ON n.node_id = l.node_id
               WHERE l.version_id=? ORDER BY l.location_id""", (version_id,))
        verified = sum(1 for loc in locs if loc["state"] == LOC_VERIFIED)
        target = policy["target_replicas"]
        min_readable = policy["min_readable"]
        if verified >= target:
            health = HEALTHY
        elif verified >= min_readable:
            health = DEGRADED
        else:
            health = UNREADABLE
        tasks = self._query(
            "SELECT task_id, type, status, blocked_reason, phase "
            "FROM tasks WHERE version_id=? ORDER BY rowid", (version_id,))
        return {
            "version_id": version_id,
            "tenant_id": v["tenant_id"],
            "state": v["state"],
            "policy": {
                "policy_id": policy["policy_id"],
                "version": policy["version"],
                "target_replicas": target,
                "min_readable": min_readable,
            },
            "verified_replicas": verified,
            "health": health,
            "blocked_reasons": (json.loads(v["blocked_reasons"])
                                if v["blocked_reasons"] else []),
            "locations": [
                {
                    "location_id": loc["location_id"],
                    "node_id": loc["node_id"],
                    "fault_domain": loc["fault_domain"],
                    "node_status": loc["node_status"],
                    "state": loc["state"],
                    "reserved_bytes": loc["reserved_bytes"],
                    "checksum": loc["checksum"],
                }
                for loc in locs
            ],
            "tasks": [dict(t) for t in tasks],
        }

    def node_info(self, node_id: str) -> dict:
        row = self._query_one("SELECT * FROM nodes WHERE node_id=?", (node_id,))
        if row is None:
            raise DurabilityError("unknown node %r" % (node_id,))
        return {
            "node_id": row["node_id"],
            "capacity_total": row["capacity_total"],
            "capacity_used": row["capacity_used"],
            "capacity_free": row["capacity_total"] - row["capacity_used"],
            "fault_domain": row["fault_domain"],
            "status": row["status"],
        }

    def list_tasks(self, version_id: Optional[str] = None) -> list:
        if version_id is not None:
            rows = self._query("SELECT * FROM tasks WHERE version_id=? "
                               "ORDER BY rowid", (version_id,))
        else:
            rows = self._query("SELECT * FROM tasks ORDER BY rowid")
        return [dict(r) for r in rows]

    def list_quarantines(self, version_id: Optional[str] = None) -> list:
        if version_id is not None:
            rows = self._query("SELECT * FROM quarantine_records "
                               "WHERE version_id=? ORDER BY rowid", (version_id,))
        else:
            rows = self._query("SELECT * FROM quarantine_records ORDER BY rowid")
        return [dict(r) for r in rows]
