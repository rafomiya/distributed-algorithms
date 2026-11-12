import asyncio
import copy
import random
import unittest

from node.algo import Gossip, RicartAgrawala


class Endpoint:
    def __init__(self, name, network):
        self.id = name
        self.boot_id = name
        self.peers = [peer for peer in network.names if peer != name]
        self.rng = random.Random(name)
        self.network = network
        self.ra = RicartAgrawala(self, ["printer1", "printer2"], 0.005)
        self.gossip = Gossip(self, 0.005)
        self.events = []

    def log(self, event, **fields):
        self.events.append((event, fields))

    async def send(self, peer, kind, payload):
        return await self.network.send(self.id, peer, kind, payload)


class Network:
    def __init__(self, n=3):
        self.names = [f"node{i}" for i in range(1, n + 1)]
        self.nodes = {name: Endpoint(name, self) for name in self.names}
        self.blocked = set()
        self.drop_first = set()
        self.duplicate = False

    async def send(self, sender, peer, kind, payload):
        await asyncio.sleep(0.001)
        key = (sender, peer, kind)
        if (sender, peer) in self.blocked:
            return None
        if key in self.drop_first:
            self.drop_first.remove(key)
            return None
        algo = self.nodes[peer].gossip if kind == "gossip" else self.nodes[peer].ra
        msg = {"from": sender, "type": kind, "payload": copy.deepcopy(payload)}
        result = await algo.handle(msg)
        if self.duplicate:
            await algo.handle(msg)
        return result

    async def start(self):
        for node in self.nodes.values():
            await node.ra.start()

    async def stop(self):
        for node in self.nodes.values():
            await node.ra.stop()


class AlgorithmsTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.net = Network(5)
        await self.net.start()

    async def asyncTearDown(self):
        await self.net.stop()

    async def test_concurrent_requests_loss_duplicates_and_local_queue(self):
        active = 0
        completed = []
        self.net.duplicate = True
        self.net.drop_first = {(a, b, kind) for a in self.net.names
            for b in self.net.names if a != b for kind in ("ra_request", "ra_reply")}

        async def work(node):
            nonlocal active
            async with node.ra.hold("printer1"):
                active += 1
                self.assertEqual(active, 1, "duas seções críticas simultâneas")
                await asyncio.sleep(0.003)
                completed.append(node.id)
                active -= 1

        await asyncio.wait_for(asyncio.gather(*(
            work(node) for _ in range(3) for node in self.net.nodes.values())), 5)
        self.assertEqual(len(completed), 15)
        self.assertEqual(completed[:5], self.net.names, "desempate dos pedidos iniciais")

    async def test_independent_printers_can_run_concurrently(self):
        entered = [asyncio.Event(), asyncio.Event()]

        async def work(node, printer, index):
            async with node.ra.hold(printer):
                entered[index].set()
                await asyncio.wait_for(entered[1 - index].wait(), 1)

        await asyncio.gather(work(self.net.nodes["node1"], "printer1", 0),
                             work(self.net.nodes["node2"], "printer2", 1))

    async def test_partition_blocks_until_healed(self):
        node = self.net.nodes["node1"]
        self.net.blocked = {("node1", "node5"), ("node5", "node1")}
        entered = asyncio.Event()

        async def work():
            async with node.ra.hold("printer1"):
                entered.set()

        task = asyncio.create_task(work())
        await asyncio.sleep(0.08)
        self.assertFalse(entered.is_set())
        self.assertIn("node5", node.ra.resources["printer1"].waiting)
        self.net.blocked.clear()
        await asyncio.wait_for(task, 1)
        self.assertTrue(entered.is_set())

    async def test_transport_ack_and_stale_reply_do_not_grant_permission(self):
        node = self.net.nodes["node1"]
        original = node.send

        async def ack_only(*args):
            return {"ok": True}

        node.send = ack_only
        entered = asyncio.Event()

        async def work():
            async with node.ra.hold("printer1"):
                entered.set()

        task = asyncio.create_task(work())
        await asyncio.sleep(0.02)
        for peer in node.peers:
            await node.ra.handle({"from": peer, "type": "ra_reply", "payload": {
                "resource": "printer1", "request_id": "pedido-antigo", "clock": 10}})
        await asyncio.sleep(0.02)
        self.assertFalse(entered.is_set())
        node.send = original
        await asyncio.wait_for(task, 1)

    async def test_gossip_union_idempotence_and_partition_recovery(self):
        a, b, c = (self.net.nodes[f"node{i}"].gossip for i in (1, 2, 3))
        first = {"job_id": "a", "computer": "node1"}
        second = {"job_id": "b", "computer": "node2"}
        a.merge([first, first])
        b.merge([second])
        self.net.blocked = {("node1", "node3")}
        await a.exchange("node3")
        self.assertEqual(c.values(), [])
        self.net.blocked.clear()
        await a.exchange("node2")
        await b.exchange("node3")
        self.assertEqual(a.values(), b.values())
        self.assertEqual(b.values(), c.values())
        first["computer"] = "mutated"
        self.assertEqual(a.values()[0]["computer"], "node1")
        with self.assertRaises(ValueError):
            a.merge([{"job_id": "a", "computer": "conflict"}])


if __name__ == "__main__":
    unittest.main()
