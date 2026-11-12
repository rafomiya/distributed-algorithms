import asyncio
import tempfile
import unittest

from scripts.cluster import Cluster, assert_safety, wait_until
from scripts.verify import verify


class IntegrationTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.cluster = Cluster(output=self.tmp.name)
        await self.cluster.__aenter__()

    async def asyncTearDown(self):
        await self.cluster.__aexit__(None, None, None)
        self.tmp.cleanup()

    async def test_printing_contention_replication_and_idempotent_printer(self):
        jobs = await asyncio.gather(*(self.cluster.submit(node=f"node{i}",
            printer=f"printer{1 + (j % 2)}") for j in range(2) for i in range(1, 4)))
        await self.cluster.completed(6)
        states = await self.cluster.converged([job["job_id"] for job in jobs])
        assert_safety(states)
        for name in ("printer1", "printer2"):
            record = states[name]["jobs"][0]
            again = await self.cluster.request(name, "/print", record)
            self.assertEqual(again, record)
            self.assertEqual(len((await self.cluster.request(name, "/state"))["jobs"]), 3)
        result = verify(self.cluster.log_dir)
        self.assertEqual(result["cs_entries"], 6)
        self.assertEqual(result["printed_jobs"], 6)
        self.assertEqual(result["open_cs"], [])

    async def test_partition_blocks_ra_then_recovers(self):
        await self.cluster.partition(["node1", "node2"], ["node3"])
        jobs = await asyncio.gather(self.cluster.submit("node1"), self.cluster.submit("node3"))
        await asyncio.sleep(0.3)
        states = await self.cluster.states()
        self.assertEqual(states["printer1"]["jobs"], [])
        self.assertEqual(states["node1"]["ra"]["resources"]["printer1"]["status"], "WANTED")
        await self.cluster.heal()
        await self.cluster.completed(2)
        states = await self.cluster.converged([job["job_id"] for job in jobs])
        assert_safety(states)
        self.assertEqual(verify(self.cluster.log_dir)["printed_jobs"], 2)

    async def test_gossip_partition_and_crash_stop_consequence(self):
        await self.cluster.partition(["node1", "node2"], ["node3"], types=["gossip"])
        job = await self.cluster.submit("node1")
        await self.cluster.completed(1)
        await asyncio.sleep(0.2)
        self.assertEqual(await self.cluster.request("node3", "/records"), [])
        await self.cluster.heal()
        await self.cluster.converged([job["job_id"]])
        await self.cluster.crash("node3")
        await self.cluster.submit("node1")
        await asyncio.sleep(0.3)
        states = await self.cluster.states()
        self.assertEqual(len(states["printer1"]["jobs"]), 1)
        self.assertIn("node3", states["node1"]["ra"]["resources"]["printer1"]["waiting"])
        await self.cluster.converged([job["job_id"]])
        assert_safety(states)

    async def test_loss_and_delay_retransmission(self):
        for node in self.cluster.computers:
            await self.cluster.request(node, "/fault", {"loss": 0.3, "delay_ms": 5})
        jobs = await asyncio.gather(*(self.cluster.submit(node) for node in self.cluster.computers))
        await self.cluster.completed(3)
        states = await self.cluster.converged([job["job_id"] for job in jobs])
        assert_safety(states)
        self.assertGreater(sum(state.get("metrics", {}).get("ra_request_dropped", 0)
                               for state in states.values()), 0)

    async def test_printer_outage_holds_critical_section_until_return(self):
        process = self.cluster.processes["printer1"]
        import signal
        process.send_signal(signal.SIGSTOP)
        try:
            first = await self.cluster.submit("node1")
            async def held():
                state = await self.cluster.request("node1", "/state")
                return state["ra"]["resources"]["printer1"]["status"] == "HELD"
            await wait_until(held)
            second = await self.cluster.submit("node2")
            await asyncio.sleep(0.2)
            state = await self.cluster.request("node2", "/state")
            self.assertEqual(state["ra"]["resources"]["printer1"]["status"], "WANTED")
        finally:
            process.send_signal(signal.SIGCONT)
        await self.cluster.completed(2)
        states = await self.cluster.converged([first["job_id"], second["job_id"]])
        assert_safety(states)
