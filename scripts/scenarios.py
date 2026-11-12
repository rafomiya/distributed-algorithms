"""Demonstração verificável: impressão, gossip isolado, partição total e crash."""
import argparse
import asyncio
import json
from pathlib import Path

from cluster import Cluster, assert_safety
from verify import verify


async def run(args):
    async with Cluster(count=3, seed=42, output=args.output, backend=args.backend) as cluster:
        ids = []
        evidence = {}
        print("1. Três computadores disputam printer1; printer2 funciona em paralelo.", flush=True)
        jobs = await asyncio.gather(*(cluster.submit(node) for node in cluster.computers),
                                    cluster.submit("node1", "printer2"))
        ids.extend(job["job_id"] for job in jobs)
        await cluster.completed(4)
        evidence["normal"] = await cluster.converged(ids)
        assert_safety(evidence["normal"])

        print("2. Partição só do gossip: impressão continua e históricos divergem.", flush=True)
        await cluster.partition(["node1", "node2"], ["node3"], types=["gossip"])
        job = await cluster.submit("node1")
        ids.append(job["job_id"])
        await cluster.completed(5)
        evidence["gossip_partition"] = await cluster.states()
        assert job["job_id"] not in {record["job_id"] for record in
            evidence["gossip_partition"]["node3"]["gossip"]["records"]}
        await cluster.heal()
        evidence["gossip_healed"] = await cluster.converged(ids)

        print("3. Partição total: pedidos aguardam; após heal, todos terminam.", flush=True)
        await cluster.partition(["node1", "node2"], ["node3"])
        jobs = await asyncio.gather(cluster.submit("node1"), cluster.submit("node3"))
        ids.extend(job["job_id"] for job in jobs)
        await asyncio.sleep(0.4)
        evidence["blocked"] = await cluster.states()
        assert len(evidence["blocked"]["printer1"]["jobs"]) == 4
        await cluster.heal()
        await cluster.completed(7)
        evidence["healed"] = await cluster.converged(ids)
        assert_safety(evidence["healed"])

        print("4. Crash-stop de node3: novo pedido bloqueia; histórico prévio permanece.", flush=True)
        await cluster.crash("node3")
        await cluster.submit("node1")
        await asyncio.sleep(0.4)
        evidence["crash_stop"] = await cluster.converged(ids)
        assert "node3" in evidence["crash_stop"]["node1"]["ra"]["resources"]["printer1"]["waiting"]
        assert_safety(evidence["crash_stop"])
        directory = Path(args.output)
        (directory / "scenarios.json").write_text(json.dumps(evidence, indent=2))
        if args.backend != "existing":
            (directory / "verification.json").write_text(json.dumps(verify(cluster.log_dir), indent=2))
        print(f"Cenários verificados; evidências em {directory.resolve()}", flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--backend", choices=["docker", "local", "existing"], default="docker")
    parser.add_argument("--output", default="data/demo")
    asyncio.run(run(parser.parse_args()))
