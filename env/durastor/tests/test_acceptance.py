"""Acceptance tests for the durable replica management system."""

import os
import shutil
import sys
import tempfile
import threading
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from durastor import DurabilitySystem, SimulatedCrash, UnreadableError


class FakeClock:
    def __init__(self, t=1_000_000.0):
        self.t = t

    def __call__(self):
        return self.t

    def advance(self, dt):
        self.t += dt


class CrashOnce:
    """Hook that raises SimulatedCrash the first time it fires."""

    def __init__(self):
        self.fired = False

    def __call__(self, ctx):
        if not self.fired:
            self.fired = True
            raise SimulatedCrash("simulated crash at hook")


def corrupt_file(path, offset=10):
    with open(path, "r+b") as fh:
        fh.seek(offset)
        b = fh.read(1)
        fh.seek(offset)
        fh.write(bytes([b[0] ^ 0xFF]))


class DurabilityTestCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="durastor-test-")
        self.root = os.path.join(self.tmp, "sys")
        self.addCleanup(shutil.rmtree, self.tmp, True)

    def make_system(self, **kw):
        return DurabilitySystem(self.root, **kw)

    def replica_path(self, node_id, version_id):
        return os.path.join(self.root, "nodes", node_id, version_id)

    # ------------------------------------------------------------------
    # 1. Placement: three nodes across two fault domains, policy pinned
    # ------------------------------------------------------------------

    def test_placement_three_nodes_two_fault_domains(self):
        s = self.make_system()
        s.register_node("n1", 1_000_000, "fd-a")
        s.register_node("n2", 1_000_000, "fd-a")
        s.register_node("n3", 1_000_000, "fd-b")
        s.publish_policy("tenant1", target_replicas=3, min_readable=2)

        content = os.urandom(5000)
        res = s.seal_version("tenant1", "v1", content)
        self.assertTrue(res.ok, res.reasons)

        st = s.version_status("v1")
        self.assertEqual(len(st["locations"]), 3)
        self.assertEqual({l["node_id"] for l in st["locations"]},
                         {"n1", "n2", "n3"})
        # spread across both fault domains (2 + 1)
        fds = [l["fault_domain"] for l in st["locations"]]
        self.assertEqual(sorted(fds), ["fd-a", "fd-a", "fd-b"])
        # all locations start pending; capacity reserved exactly once per node
        self.assertTrue(all(l["state"] == "pending" for l in st["locations"]))
        for nid in ("n1", "n2", "n3"):
            self.assertEqual(s.node_info(nid)["capacity_used"], len(content))

        # the version pinned the policy version in effect at seal time
        s.publish_policy("tenant1", target_replicas=2, min_readable=2)
        st = s.version_status("v1")
        self.assertEqual(st["policy"]["version"], 1)
        self.assertEqual(st["policy"]["target_replicas"], 3)

        s.run_worker("w1")
        st = s.version_status("v1")
        self.assertEqual(st["health"], "healthy")
        self.assertEqual(st["verified_replicas"], 3)
        self.assertTrue(all(l["state"] == "verified" for l in st["locations"]))
        self.assertEqual(s.read("v1"), content)

    # ------------------------------------------------------------------
    # 2. Blocked placement leaves no partial plan or reservation
    # ------------------------------------------------------------------

    def test_insufficient_capacity_leaves_no_partial_reservation(self):
        s = self.make_system()
        s.register_node("n1", 5000, "fd-a")
        s.register_node("n2", 5000, "fd-b")
        s.register_node("n3", 100, "fd-c")  # too small
        s.publish_policy("tenant1", target_replicas=3, min_readable=2)

        res = s.seal_version("tenant1", "v1", os.urandom(5000))
        self.assertFalse(res.ok)
        self.assertTrue(any("insufficient capacity" in r for r in res.reasons),
                        res.reasons)
        self.assertTrue(any("n3" in r for r in res.reasons), res.reasons)

        # no partial plan, no partial reservation, no tasks
        for nid in ("n1", "n2", "n3"):
            self.assertEqual(s.node_info(nid)["capacity_used"], 0)
        st = s.version_status("v1")
        self.assertEqual(st["state"], "placement_blocked")
        self.assertEqual(st["locations"], [])
        self.assertEqual(s.list_tasks("v1"), [])

        # after fixing capacity the same version can be placed
        s.register_node("n3", 5000, "fd-c")
        res2 = s.replan("v1")
        self.assertTrue(res2.ok, res2.reasons)
        s.run_worker("w1")
        self.assertEqual(s.version_status("v1")["health"], "healthy")

    def test_insufficient_fault_domains_blocked(self):
        s = self.make_system()
        s.register_node("n1", 1_000_000, "fd-a")
        s.register_node("n2", 1_000_000, "fd-a")
        s.register_node("n3", 1_000_000, "fd-a")
        s.publish_policy("tenant1", target_replicas=3, min_readable=2)

        res = s.seal_version("tenant1", "v1", os.urandom(1000))
        self.assertFalse(res.ok)
        self.assertTrue(any("fault domain" in r for r in res.reasons),
                        res.reasons)
        for nid in ("n1", "n2", "n3"):
            self.assertEqual(s.node_info(nid)["capacity_used"], 0)
        self.assertEqual(s.list_tasks("v1"), [])

    def test_draining_nodes_excluded_from_placement(self):
        s = self.make_system()
        s.register_node("n1", 1_000_000, "fd-a")
        s.register_node("n2", 1_000_000, "fd-b")
        s.register_node("n3", 1_000_000, "fd-c")
        s.set_node_status("n3", "draining")
        s.publish_policy("tenant1", target_replicas=3, min_readable=2)

        res = s.seal_version("tenant1", "v1", os.urandom(1000))
        self.assertFalse(res.ok)
        self.assertTrue(any("draining" in r for r in res.reasons), res.reasons)
        for nid in ("n1", "n2", "n3"):
            self.assertEqual(s.node_info(nid)["capacity_used"], 0)

    # ------------------------------------------------------------------
    # 3. Concurrent workers: one replica per node, capacity settled once
    # ------------------------------------------------------------------

    def test_concurrent_workers_single_replica_single_settlement(self):
        s = self.make_system()
        for nid, fd in (("n1", "fd-a"), ("n2", "fd-a"), ("n3", "fd-b")):
            s.register_node(nid, 1_000_000, fd)
        s.publish_policy("tenant1", target_replicas=3, min_readable=2)
        content = os.urandom(5000)
        self.assertTrue(s.seal_version("tenant1", "v1", content).ok)

        errors = []

        def work(wid):
            try:
                s.run_worker(wid)
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)

        threads = [threading.Thread(target=work, args=("w%d" % i,))
                   for i in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(errors, [])

        st = s.version_status("v1")
        self.assertEqual(st["health"], "healthy")
        self.assertEqual(st["verified_replicas"], 3)
        # exactly one valid replica per node, capacity settled exactly once
        for nid in ("n1", "n2", "n3"):
            self.assertEqual(s.node_info(nid)["capacity_used"], len(content))
            self.assertEqual(os.listdir(os.path.join(self.root, "nodes", nid)),
                             ["v1"])
        # all tasks done; re-running workers changes nothing
        self.assertTrue(all(t["status"] == "done" for t in s.list_tasks("v1")))
        self.assertEqual(s.run_worker("w9"), 0)
        for nid in ("n1", "n2", "n3"):
            self.assertEqual(s.node_info(nid)["capacity_used"], len(content))
        self.assertEqual(s.read("v1"), content)

    # ------------------------------------------------------------------
    # 4. Scrub detects corruption; reads avoid quarantine; repair heals
    # ------------------------------------------------------------------

    def test_scrub_quarantines_and_repair_heals(self):
        s = self.make_system()
        for nid, fd in (("n1", "fd-a"), ("n2", "fd-a"), ("n3", "fd-b")):
            s.register_node(nid, 1_000_000, fd)
        s.publish_policy("tenant1", target_replicas=3, min_readable=2)
        content = os.urandom(5000)
        self.assertTrue(s.seal_version("tenant1", "v1", content).ok)
        s.run_worker("w1")
        self.assertEqual(s.version_status("v1")["health"], "healthy")

        corrupt_file(self.replica_path("n1", "v1"))
        results = s.scrub_version("v1", full=True)
        by_node = {r["node_id"]: r["result"] for r in results}
        self.assertEqual(by_node["n1"], "mismatch")
        self.assertEqual(by_node["n2"], "ok")
        self.assertEqual(by_node["n3"], "ok")

        st = s.version_status("v1")
        loc_by_node = {l["node_id"]: l["state"] for l in st["locations"]}
        self.assertEqual(loc_by_node["n1"], "quarantined")
        self.assertEqual(st["health"], "degraded")  # 2 verified >= min_readable
        self.assertEqual(len(s.list_quarantines("v1")), 1)

        # reads never touch the quarantined replica and stay correct
        for _ in range(10):
            self.assertEqual(s.read("v1"), content)

        s.run_worker("w2")
        st = s.version_status("v1")
        self.assertEqual(st["health"], "healthy")
        self.assertTrue(all(l["state"] == "verified" for l in st["locations"]))
        with open(self.replica_path("n1", "v1"), "rb") as fh:
            self.assertEqual(fh.read(), content)

    def test_repair_never_propagates_corruption(self):
        s = self.make_system()
        for nid, fd in (("n1", "fd-a"), ("n2", "fd-a"), ("n3", "fd-b")):
            s.register_node(nid, 1_000_000, fd)
        s.publish_policy("tenant1", target_replicas=3, min_readable=2)
        content = os.urandom(5000)
        self.assertTrue(s.seal_version("tenant1", "v1", content).ok)
        s.run_worker("w1")

        # n1 is scrubbed out; n2 is corrupted *after* the scrub, so it is
        # still (wrongly) marked verified when the repair runs.
        corrupt_file(self.replica_path("n1", "v1"))
        s.scrub_version("v1", full=True)
        corrupt_file(self.replica_path("n2", "v1"))

        s.run_worker("w2")
        st = s.version_status("v1")
        # the corrupt "verified" source was detected during repair and
        # quarantined; every replica was ultimately rebuilt from good data
        self.assertEqual(st["health"], "healthy")
        self.assertTrue(all(l["state"] == "verified" for l in st["locations"]))
        for nid in ("n1", "n2", "n3"):
            with open(self.replica_path(nid, "v1"), "rb") as fh:
                self.assertEqual(fh.read(), content)
        reasons = [q["reason"] for q in s.list_quarantines("v1")]
        self.assertTrue(any("source" in r for r in reasons), reasons)

    # ------------------------------------------------------------------
    # 5. All trusted replicas lost: stays unreadable, nothing propagates
    # ------------------------------------------------------------------

    def test_all_trusted_replicas_lost_stays_unreadable(self):
        s = self.make_system()
        for nid, fd in (("n1", "fd-a"), ("n2", "fd-a"), ("n3", "fd-b")):
            s.register_node(nid, 1_000_000, fd)
        s.publish_policy("tenant1", target_replicas=3, min_readable=2)
        content = os.urandom(5000)
        self.assertTrue(s.seal_version("tenant1", "v1", content).ok)
        s.run_worker("w1")

        for nid in ("n1", "n2", "n3"):
            corrupt_file(self.replica_path(nid, "v1"))
        s.scrub_version("v1", full=True)

        st = s.version_status("v1")
        self.assertEqual(st["health"], "unreadable")
        self.assertEqual(st["verified_replicas"], 0)
        self.assertTrue(
            all(l["state"] == "quarantined" for l in st["locations"]))
        with self.assertRaises(UnreadableError):
            s.read("v1")

        # repairs stay blocked with a clear reason; nothing becomes readable
        s.run_worker("w2")
        s.run_worker("w3")
        st = s.version_status("v1")
        self.assertEqual(st["health"], "unreadable")
        self.assertEqual(st["verified_replicas"], 0)
        repair_tasks = [t for t in s.list_tasks("v1") if t["type"] == "repair"]
        self.assertEqual(len(repair_tasks), 3)
        self.assertTrue(all(t["status"] == "blocked" for t in repair_tasks))
        self.assertTrue(all("no trusted replica" in t["blocked_reason"]
                            for t in repair_tasks))
        with self.assertRaises(UnreadableError):
            s.read("v1")

    # ------------------------------------------------------------------
    # 6. Draining a node migrates safely, then releases capacity
    # ------------------------------------------------------------------

    def _healthy_four_node_system(self):
        s = self.make_system()
        s.register_node("n1", 1_000_000, "fd-a")
        s.register_node("n2", 1_000_000, "fd-a")
        s.register_node("n3", 1_000_000, "fd-b")
        s.register_node("n4", 1_000_000, "fd-b")
        s.publish_policy("tenant1", target_replicas=3, min_readable=2)
        content = os.urandom(5000)
        self.assertTrue(s.seal_version("tenant1", "v1", content).ok)
        s.run_worker("w1")
        self.assertEqual(s.version_status("v1")["health"], "healthy")
        return s, content

    def test_drain_migrates_then_releases_capacity(self):
        s, content = self._healthy_four_node_system()
        used = {l["node_id"] for l in s.version_status("v1")["locations"]}
        self.assertEqual(used, {"n1", "n2", "n3"})
        victim = "n1"
        spare = "n4"
        self.assertEqual(s.node_info(victim)["capacity_used"], len(content))

        s.set_node_status(victim, "draining")
        # the original replica is still counted until its replacement exists
        st = s.version_status("v1")
        self.assertEqual(st["verified_replicas"], 3)
        self.assertEqual(s.node_info(victim)["capacity_used"], len(content))
        self.assertEqual(s.read("v1"), content)

        s.run_worker("w2")
        st = s.version_status("v1")
        self.assertEqual(st["health"], "healthy")
        self.assertEqual(st["verified_replicas"], 3)
        loc_by_node = {l["node_id"]: l["state"] for l in st["locations"]}
        self.assertEqual(loc_by_node[victim], "removed")
        self.assertEqual(loc_by_node[spare], "verified")
        # capacity released exactly once on the drained node
        self.assertEqual(s.node_info(victim)["capacity_used"], 0)
        self.assertEqual(s.node_info(spare)["capacity_used"], len(content))
        self.assertEqual(s.read("v1"), content)

        # the draining node is excluded from future placement
        res = s.seal_version("tenant1", "v2", os.urandom(1000))
        self.assertTrue(res.ok, res.reasons)
        nodes = {l["node_id"] for l in s.version_status("v2")["locations"]}
        self.assertNotIn(victim, nodes)

    def test_drain_blocked_without_legal_target_keeps_replica(self):
        s = self.make_system()
        for nid, fd in (("n1", "fd-a"), ("n2", "fd-a"), ("n3", "fd-b")):
            s.register_node(nid, 1_000_000, fd)
        s.publish_policy("tenant1", target_replicas=3, min_readable=2)
        content = os.urandom(5000)
        self.assertTrue(s.seal_version("tenant1", "v1", content).ok)
        s.run_worker("w1")

        s.set_node_status("n1", "draining")  # no spare node exists
        st = s.version_status("v1")
        loc_by_node = {l["node_id"]: l["state"] for l in st["locations"]}
        self.assertEqual(loc_by_node["n1"], "verified")  # still counted
        self.assertEqual(st["verified_replicas"], 3)
        self.assertEqual(s.node_info("n1")["capacity_used"], len(content))
        migrate = [t for t in s.list_tasks("v1") if t["type"] == "migrate"]
        self.assertEqual(len(migrate), 1)
        self.assertEqual(migrate[0]["status"], "blocked")
        self.assertIn("no legal target node", migrate[0]["blocked_reason"])

        # once a legal target appears the migration proceeds
        s.register_node("n4", 1_000_000, "fd-b")
        s.run_worker("w2")
        st = s.version_status("v1")
        self.assertEqual(st["health"], "healthy")
        loc_by_node = {l["node_id"]: l["state"] for l in st["locations"]}
        self.assertEqual(loc_by_node["n1"], "removed")
        self.assertEqual(loc_by_node["n4"], "verified")
        self.assertEqual(s.node_info("n1")["capacity_used"], 0)

    def test_reonline_does_not_restore_quarantined_replica(self):
        s = self.make_system()
        for nid, fd in (("n1", "fd-a"), ("n2", "fd-a"), ("n3", "fd-b")):
            s.register_node(nid, 1_000_000, fd)
        s.publish_policy("tenant1", target_replicas=3, min_readable=2)
        content = os.urandom(5000)
        self.assertTrue(s.seal_version("tenant1", "v1", content).ok)
        s.run_worker("w1")

        corrupt_file(self.replica_path("n1", "v1"))
        s.scrub_version("v1", full=True)
        loc_by_node = {l["node_id"]: l["state"]
                       for l in s.version_status("v1")["locations"]}
        self.assertEqual(loc_by_node["n1"], "quarantined")

        # node flaps offline -> active: quarantine must survive
        s.set_node_status("n1", "offline")
        s.set_node_status("n1", "active")
        loc_by_node = {l["node_id"]: l["state"]
                       for l in s.version_status("v1")["locations"]}
        self.assertEqual(loc_by_node["n1"], "quarantined")
        self.assertEqual(s.version_status("v1")["health"], "degraded")
        self.assertEqual(s.read("v1"), content)

        # only a real repair makes it readable again
        s.run_worker("w2")
        self.assertEqual(s.version_status("v1")["health"], "healthy")

    # ------------------------------------------------------------------
    # 7. Crash recovery at the three crash windows
    # ------------------------------------------------------------------

    def test_crash_after_replica_write_before_confirm(self):
        clock = FakeClock()
        hook = CrashOnce()
        s1 = self.make_system(hooks={"after_replica_file_written": hook},
                              clock=clock)
        for nid, fd in (("n1", "fd-a"), ("n2", "fd-a"), ("n3", "fd-b")):
            s1.register_node(nid, 1_000_000, fd)
        s1.publish_policy("tenant1", target_replicas=3, min_readable=2)
        content = os.urandom(5000)
        self.assertTrue(s1.seal_version("tenant1", "v1", content).ok)

        with self.assertRaises(SimulatedCrash):
            s1.run_worker("w1")
        self.assertTrue(hook.fired)
        copying = [l for l in s1.version_status("v1")["locations"]
                   if l["state"] == "copying"]
        self.assertEqual(len(copying), 1)

        # "restart": leases held by the dead process expire
        clock.advance(3600)
        s2 = self.make_system(clock=clock)
        s2.recover()
        s2.run_worker("w2")

        st = s2.version_status("v1")
        self.assertEqual(st["health"], "healthy")
        self.assertEqual(st["verified_replicas"], 3)
        for nid in ("n1", "n2", "n3"):
            # capacity settled exactly once, exactly one replica file
            self.assertEqual(s2.node_info(nid)["capacity_used"], len(content))
            self.assertEqual(os.listdir(os.path.join(self.root, "nodes", nid)),
                             ["v1"])
        self.assertEqual(s2.read("v1"), content)

    def test_recovery_discards_torn_replica(self):
        clock = FakeClock()
        root = self.root

        class CorruptThenCrash:
            fired = False

            def __call__(self, ctx):
                if self.fired:
                    return
                self.fired = True
                path = os.path.join(root, "nodes", ctx["location"]["node_id"],
                                    ctx["task"]["version_id"])
                with open(path, "r+b") as fh:
                    fh.write(b"GARBAGE-TORN-WRITE")
                raise SimulatedCrash("torn write")

        hook = CorruptThenCrash()
        s1 = self.make_system(hooks={"after_replica_file_written": hook},
                              clock=clock)
        for nid, fd in (("n1", "fd-a"), ("n2", "fd-a"), ("n3", "fd-b")):
            s1.register_node(nid, 1_000_000, fd)
        s1.publish_policy("tenant1", target_replicas=3, min_readable=2)
        content = os.urandom(5000)
        self.assertTrue(s1.seal_version("tenant1", "v1", content).ok)
        with self.assertRaises(SimulatedCrash):
            s1.run_worker("w1")

        clock.advance(3600)
        s2 = self.make_system(clock=clock)
        s2.run_worker("w2")  # recover() runs inside run_worker
        st = s2.version_status("v1")
        self.assertEqual(st["health"], "healthy")
        for nid in ("n1", "n2", "n3"):
            with open(self.replica_path(nid, "v1"), "rb") as fh:
                self.assertEqual(fh.read(), content)
            self.assertEqual(s2.node_info(nid)["capacity_used"], len(content))

    def test_crash_after_quarantine_before_repair_task(self):
        clock = FakeClock()
        s1 = self.make_system(clock=clock)
        for nid, fd in (("n1", "fd-a"), ("n2", "fd-a"), ("n3", "fd-b")):
            s1.register_node(nid, 1_000_000, fd)
        s1.publish_policy("tenant1", target_replicas=3, min_readable=2)
        content = os.urandom(5000)
        self.assertTrue(s1.seal_version("tenant1", "v1", content).ok)
        s1.run_worker("w1")

        corrupt_file(self.replica_path("n1", "v1"))
        hook = CrashOnce()
        s_crash = self.make_system(
            hooks={"after_quarantine_recorded": hook}, clock=clock)
        with self.assertRaises(SimulatedCrash):
            s_crash.scrub_version("v1", full=True)

        # quarantine persisted, repair task missing
        self.assertEqual(len(s1.list_quarantines("v1")), 1)
        self.assertEqual([t for t in s1.list_tasks("v1")
                          if t["type"] == "repair"], [])
        loc_by_node = {l["node_id"]: l["state"]
                       for l in s1.version_status("v1")["locations"]}
        self.assertEqual(loc_by_node["n1"], "quarantined")

        clock.advance(3600)
        s2 = self.make_system(clock=clock)
        s2.recover()
        s2.recover()  # idempotent
        repairs = [t for t in s2.list_tasks("v1") if t["type"] == "repair"]
        self.assertEqual(len(repairs), 1)
        self.assertEqual(repairs[0]["status"], "pending")

        s2.run_worker("w2")
        st = s2.version_status("v1")
        self.assertEqual(st["health"], "healthy")
        self.assertEqual(s2.read("v1"), content)
        for nid in ("n1", "n2", "n3"):
            self.assertEqual(s2.node_info(nid)["capacity_used"], len(content))

    def test_crash_after_replacement_confirmed_before_release(self):
        clock = FakeClock()
        s1, content = self._healthy_four_node_system()
        s1.set_node_status("n1", "draining")

        hook = CrashOnce()
        s_crash = self.make_system(
            hooks={"after_replacement_confirmed": hook}, clock=clock)
        with self.assertRaises(SimulatedCrash):
            s_crash.run_worker("w1")

        # replacement confirmed, original not yet released
        st = s1.version_status("v1")
        self.assertEqual(st["verified_replicas"], 4)
        loc_by_node = {l["node_id"]: l["state"] for l in st["locations"]}
        self.assertEqual(loc_by_node["n1"], "verified")
        self.assertEqual(loc_by_node["n4"], "verified")
        self.assertEqual(s1.node_info("n1")["capacity_used"], len(content))

        clock.advance(3600)
        s2 = self.make_system(clock=clock)
        s2.recover()
        s2.recover()  # idempotent: capacity released exactly once
        st = s2.version_status("v1")
        self.assertEqual(st["health"], "healthy")
        self.assertEqual(st["verified_replicas"], 3)
        loc_by_node = {l["node_id"]: l["state"] for l in st["locations"]}
        self.assertEqual(loc_by_node["n1"], "removed")
        self.assertEqual(s2.node_info("n1")["capacity_used"], 0)
        self.assertEqual(s2.node_info("n4")["capacity_used"], len(content))
        migrate = [t for t in s2.list_tasks("v1") if t["type"] == "migrate"]
        self.assertEqual(migrate[0]["status"], "done")
        self.assertEqual(s2.read("v1"), content)


if __name__ == "__main__":
    unittest.main()
