import asyncio
from dataclasses import asdict
import os
import random
import unittest
from unittest.mock import patch

from node.algo.gossip import Gossip


class StubNode:
    def __init__(self, name="node1", peers=None):
        self.id = name
        self.peers = peers if peers is not None else [peer for peer in ("node1", "node2", "node3") if peer != name]
        self.rng = random.Random(42)
        self.events = []

    def log(self, event, **fields):
        self.events.append((event, fields))


class GossipTest(unittest.IsolatedAsyncioTestCase):
    def test_environment_defaults_and_explicit_overrides(self):
        with patch.dict(os.environ, {}, clear=True):
            default = Gossip(StubNode())
            self.assertEqual((default.fanout, default.interval_s, default.data_items), (1, .5, 5))
        with patch.dict(os.environ, {"GOSSIP_FANOUT": "2", "GOSSIP_INTERVAL": "0.125", "DATA_ITEMS": "3"}, clear=True):
            configured = Gossip(StubNode())
            self.assertEqual((configured.fanout, configured.interval_s, configured.data_items), (2, .125, 3))
            explicit = Gossip(StubNode(), fanout=1, interval_s=.02, data_items=0)
            self.assertEqual((explicit.fanout, explicit.interval_s, explicit.data_items), (1, .02, 0))

    def test_invalid_configuration_is_rejected_at_construction(self):
        with patch.dict(os.environ, {}, clear=True):
            for kwargs in ({"fanout": 0}, {"fanout": .5}, {"fanout": True},
                           {"interval_s": 0}, {"interval_s": float("nan")},
                           {"interval_s": float("inf")}, {"data_items": -1}):
                with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                    Gossip(StubNode(), **kwargs)

    def test_versioned_exchange_preserves_records_and_ignores_stale_snapshots(self):
        a, b = Gossip(StubNode()), Gossip(StubNode("node2"))
        first = {"job_id": "first", "computer": "node1"}
        second = {"job_id": "second", "computer": "node2"}
        a.merge([first])
        old = asdict(a.store["node1"])
        b.merge([second])
        reply = b._handle_gossip("node1", {"version_control": a._version_control(),
            "entries": {owner: asdict(entry) for owner, entry in a.store.items()}})
        self.assertEqual(set(reply["entries"]), {"node2"})
        a._merge(reply["entries"], via="pull", peer="node2")
        self.assertEqual(a.values(), b.values())
        self.assertEqual(a.state()["record_count"], 2)
        self.assertEqual(a.state()["records"], a.values())
        # Uma atualização de dados de exemplo não pode apagar impressões.
        a.update_local([42])
        a.merge([{"job_id": "third", "computer": "node1"}])
        latest = asdict(a.store["node1"])
        b._merge({"node1": latest}, via="push", peer="node1")
        b._merge({"node1": old}, via="push", peer="node1")
        self.assertEqual(b.store["node1"].data, [42])
        self.assertEqual(len(b.values()), 3)
        self.assertEqual(b.version_of("node1"), a.own_version)
        events = [fields["job_id"] for event, fields in b.node.events if event == "record_learned"]
        self.assertEqual(sorted(events), ["first", "second", "third"])

    def test_records_and_example_data_are_not_aliased(self):
        gossip = Gossip(StubNode())
        record = {"job_id": "a", "computer": "node1", "details": {"pages": 1}}
        data = [10]
        gossip.update_local(data)
        version = gossip.own_version
        self.assertEqual(gossip.merge([record, record]), 1)
        self.assertEqual(gossip.own_version, version + 1)
        self.assertEqual(gossip.merge([record]), 0)
        data.append(20)
        record["details"]["pages"] = 99
        snapshot = gossip.state()
        snapshot["records"][0]["details"]["pages"] = 50
        snapshot["store"]["node1"]["records"].clear()
        self.assertEqual(gossip.store["node1"].data, [10])
        self.assertEqual(gossip.values()[0]["details"]["pages"], 1)
        self.assertEqual(len(gossip.store["node1"].records), 1)

    def test_invalid_snapshot_is_atomic_and_cannot_remove_or_change_records(self):
        gossip = Gossip(StubNode())
        record = {"job_id": "a", "computer": "node2"}
        gossip._merge({"node2": {"version": 2, "data": [4], "records": [record]}}, "push", "node2")
        before = gossip.state()
        for records in ([], [{**record, "pages": 100}]):
            with self.subTest(records=records), self.assertRaises(ValueError):
                gossip._merge({
                    "node3": {"version": 1, "data": [9], "records": []},
                    "node2": {"version": 3, "data": [5], "records": records},
                }, "push", "node2")
            self.assertEqual(gossip.state(), before)
        with self.assertRaises(ValueError):
            gossip._merge({"node2": {"version": 2, "data": [5], "records": [record]}}, "push", "node2")
        self.assertEqual(gossip.state(), before)
        with self.assertRaises(ValueError):
            gossip.merge([record])  # Novos registros locais precisam pertencer a node1.

    async def test_fanout_is_bounded_and_stop_drains_inflight_exchanges(self):
        node = StubNode(peers=["node2", "node3", "node4"])
        active = peak = 0
        ready = asyncio.Event()
        never = asyncio.Event()

        async def send(*args):
            nonlocal active, peak
            active += 1
            peak = max(peak, active)
            if active == 2:
                ready.set()
            try:
                await never.wait()
            finally:
                active -= 1

        node.send = send
        gossip = Gossip(node, fanout=2, interval_s=.001)
        await gossip.start()
        task = gossip._rounds_task
        try:
            await gossip.start()
            self.assertIs(gossip._rounds_task, task)
            await asyncio.wait_for(ready.wait(), 1)
            await asyncio.sleep(.02)
            self.assertEqual(peak, 2)
        finally:
            await gossip.stop()
        self.assertEqual(active, 0)
        self.assertTrue(task.done())
        self.assertEqual(gossip.requests_sent, 2)
        await gossip.stop()

    async def test_failed_exchange_does_not_kill_future_rounds(self):
        node = StubNode(peers=["node2"])
        calls = 0
        recovered = asyncio.Event()

        async def send(*args):
            nonlocal calls
            calls += 1
            if calls == 1:
                return {"entries": []}  # Resposta inválida não encerra o loop.
            recovered.set()
            return {"entries": {"node2": {"version": 1, "data": [4]}}}

        node.send = send
        gossip = Gossip(node, interval_s=.001)
        await gossip.start()
        try:
            await asyncio.wait_for(recovered.wait(), 1)
            self.assertIn("node2", gossip.store)
            self.assertEqual(gossip.requests_failed, 1)
            self.assertTrue(any(event == "gossip_error" for event, _ in node.events))
        finally:
            await gossip.stop()

    async def test_lost_rpc_is_counted_without_changing_history(self):
        node = StubNode()

        async def send(*args):
            return None

        node.send = send
        gossip = Gossip(node)
        await gossip.exchange("node2")
        self.assertEqual((gossip.requests_sent, gossip.requests_failed), (1, 1))
        self.assertEqual(gossip.values(), [])
