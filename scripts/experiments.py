"""Ensaios reproduzíveis: N=3/4/5, sementes independentes, normal e perda.

Salva metadados, JSONL bruto, estados, métricas por ensaio e resumo CSV.
Uso: python scripts/experiments.py --backend docker
"""
import argparse
import asyncio
from collections import defaultdict
import csv
from datetime import datetime, timezone
import importlib.metadata
import json
import os
from pathlib import Path
import platform
import statistics
import subprocess
import time

from cluster import Cluster, assert_safety
from verify import verify


def percentile(values, fraction):
    values = sorted(values)
    if not values:
        return None
    position = (len(values) - 1) * fraction
    left = int(position)
    right = min(left + 1, len(values) - 1)
    return values[left] + (values[right] - values[left]) * (position - left)


def metadata(args):
    def version(*command):
        try:
            return subprocess.check_output(command, text=True, stderr=subprocess.STDOUT, timeout=5).strip()
        except (OSError, subprocess.SubprocessError) as error:
            return str(error)
    return {"started_utc": datetime.now(timezone.utc).isoformat(), "arguments": vars(args),
        "os": platform.platform(), "python": platform.python_version(),
        "machine": platform.machine(), "processor": platform.processor(), "cpu_count": os.cpu_count(),
        "cpu_info": Path("/proc/cpuinfo").read_text() if Path("/proc/cpuinfo").exists() else None,
        "memory_info": Path("/proc/meminfo").read_text() if Path("/proc/meminfo").exists() else None,
        "aiohttp": importlib.metadata.version("aiohttp"), "compose": version("docker", "compose", "version"),
        "docker": version("docker", "version"), "git": version("git", "rev-parse", "HEAD"),
        "git_status": version("git", "status", "--short"),
        "page_seconds": 0.05, "retry_interval": 0.03, "rpc_timeout": 0.8,
        "gossip_interval": args.gossip_interval, "failure_loss": 0.2, "failure_delay_ms": 10,
        "gossip_fanout": int(os.environ.get("GOSSIP_FANOUT", "1")),
        "data_items": int(os.environ.get("DATA_ITEMS", "5")),
        "topology": f"malha completa; computadores={args.nodes}; "
                    f"impressoras={args.printers}"}


def gossip_latencies(log_dir, job_ids, count):
    learned = defaultdict(dict)
    origins = {}
    for path in Path(log_dir).glob("node*.jsonl"):
        for line in path.read_text().splitlines():
            event = json.loads(line)
            if event["event"] == "record_learned" and event["job_id"] in job_ids:
                learned[event["job_id"]][event["node"]] = event["mono_ns"]
                origins[event["job_id"]] = event["origin"]
    assert set(learned) == set(job_ids), "registro sem observação"
    result = []
    for job_id in job_ids:
        observations = learned[job_id]
        assert len(observations) == count, f"réplicas incompletas: {job_id}"
        origin = origins[job_id]
        assert origin in observations, f"nascimento local ausente: {job_id}"
        result.append((max(observations.values()) - observations[origin]) / 1e6)
    return result


async def trial(args, count, seed, scenario, output):
    async with Cluster(count=count, seed=seed, output=output, backend=args.backend,
                       gossip_interval=args.gossip_interval,
                       printer_count=args.printers) as cluster:
        # Aquecimento separado da amostra: uma impressão e convergência completa.
        warm_job = await cluster.submit("node1", "printer1", document="warmup.pdf")
        await cluster.completed(1)
        await cluster.converged([warm_job["job_id"]])
        await asyncio.sleep(args.warmup)
        baseline = await cluster.states()
        if scenario == "loss":
            for node in cluster.computers:
                await cluster.request(node, "/fault", {"loss": 0.2, "delay_ms": 10})
        elif scenario == "partition":
            split = args.partition_split if args.partition_split is not None else count - 1
            await cluster.partition(cluster.computers[:split], cluster.computers[split:])
        started = time.monotonic()
        submitted = await asyncio.gather(*(cluster.submit(node,
            printer=f"printer{1 + (index % args.printers)}", document=f"{node}-{index}.pdf")
            for index in range(args.jobs_per_node) for node in cluster.computers))
        if scenario == "partition":
            await asyncio.sleep(args.partition_seconds)
            blocked = await cluster.states()
            assert all(job["status"] == "waiting_permission" for name, state in blocked.items()
                       if name.startswith("node") for job in state["jobs"] if job["job_id"] != warm_job["job_id"])
            (output / "partition.json").write_text(json.dumps(blocked, indent=2))
            await cluster.heal()
        await cluster.completed(len(submitted) + 1, timeout=args.timeout)
        states = await cluster.converged([warm_job["job_id"]] + [job["job_id"] for job in submitted], timeout=args.timeout)
        duration = time.monotonic() - started
        assert_safety(states)
        (output / "states.json").write_text(json.dumps(states, indent=2))
        checks = verify(cluster.log_dir)
        assert not checks["open_cs"] and not checks["unfinished_printers"], checks
        (output / "verification.json").write_text(json.dumps(checks, indent=2))
        ids = {job["job_id"] for job in submitted}
        jobs = [job for name, state in states.items() if name.startswith("node")
                for job in state["jobs"] if job["job_id"] in ids]
        latencies = [job["acquire_ms"] for job in jobs]
        gossip_ms = gossip_latencies(cluster.log_dir, ids, count)
        metrics = defaultdict(int)
        for node in cluster.computers:
            for key, value in states[node]["metrics"].items():
                metrics[key] += value - baseline[node]["metrics"].get(key, 0)
        result = {"nodes": count, "seed": seed, "scenario": scenario,
            "jobs": len(jobs), "duration_s": duration, "warmup_s": args.warmup,
            "throughput_jobs_s": len(jobs) / duration,
            "ra_acquire_mean_ms": statistics.mean(latencies),
            "ra_acquire_p50_ms": percentile(latencies, .5),
            "ra_acquire_p95_ms": percentile(latencies, .95),
            "ra_acquire_p99_ms": percentile(latencies, .99),
            "ra_messages": metrics["ra_request_attempts"] + metrics["ra_reply_attempts"],
            "ra_bytes": sum(value for key, value in metrics.items() if key.startswith("ra_") and key.endswith("bytes")),
            "gossip_convergence_mean_ms": statistics.mean(gossip_ms),
            "gossip_convergence_p50_ms": percentile(gossip_ms, .5),
            "gossip_convergence_p95_ms": percentile(gossip_ms, .95),
            "gossip_convergence_p99_ms": percentile(gossip_ms, .99),
            "gossip_messages": metrics["gossip_attempts"],
            "gossip_bytes": metrics["gossip_bytes"] + metrics["gossip_response_bytes"]}
        (output / "samples.json").write_text(json.dumps({"jobs": jobs,
            "gossip_convergence_ms": gossip_ms, "rpc_metrics": dict(metrics)}, indent=2))
        (output / "metrics.json").write_text(json.dumps(result, indent=2))
        return result


def write_csv(path, rows):
    with path.open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


async def run(args):
    output = Path(args.output).resolve()
    output.mkdir(parents=True, exist_ok=True)
    (output / "environment.json").write_text(json.dumps(metadata(args), indent=2))
    results = []
    for count in args.nodes:
        for scenario in args.scenarios:
            for seed in args.seeds:
                name = f"n{count}-{scenario}-seed{seed}"
                directory = output / name
                if directory.exists():
                    raise ValueError(f"destino já existe; escolha outra pasta: {directory}")
                directory.mkdir()
                result = await trial(args, count, seed, scenario, directory)
                results.append(result)
                write_csv(output / "trials.csv", results)
                print(json.dumps(result), flush=True)
    summary = []
    for count in args.nodes:
        for scenario in args.scenarios:
            group = [result for result in results if result["nodes"] == count and result["scenario"] == scenario]
            for metric in results[0]:
                if metric in {"nodes", "seed", "scenario"}:
                    continue
                values = [result[metric] for result in group]
                summary.append({"nodes": count, "scenario": scenario, "metric": metric,
                    "repetitions": len(values), "mean": statistics.mean(values),
                    "stdev": statistics.stdev(values) if len(values) > 1 else 0,
                    "min": min(values), "max": max(values)})
    write_csv(output / "summary.csv", summary)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--backend", choices=["docker", "local"], default="docker")
    parser.add_argument("--nodes", type=int, nargs="+", default=[3, 4, 5])
    parser.add_argument("--printers", type=int, default=2)
    parser.add_argument("--seeds", type=int, nargs="+", default=[11, 22, 33])
    parser.add_argument("--scenarios", choices=["normal", "loss", "partition"], nargs="+", default=["normal", "loss"])
    parser.add_argument("--jobs-per-node", type=int, default=3)
    parser.add_argument("--warmup", type=float, default=0.5)
    parser.add_argument("--gossip-interval", type=float, default=0.08)
    parser.add_argument("--partition-seconds", type=float, default=0.4)
    parser.add_argument("--partition-split", type=int, default=None,
                        help="computers in the first partition group; defaults to N-1")
    parser.add_argument("--timeout", type=float, default=30)
    parser.add_argument("--output", default="data/experiments/" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S"))
    args = parser.parse_args()
    if any(n < 3 or n > 15 for n in args.nodes) or not 1 <= args.printers <= 5 or args.jobs_per_node < 1:
        parser.error("use 3..15 nós, 1..5 impressoras e pelo menos um trabalho por nó")
    if args.partition_split is not None and any(not 1 <= args.partition_split < n for n in args.nodes):
        parser.error("partition split must leave at least one computer in each group")
    if args.gossip_interval <= 0 or args.warmup < 0 or args.partition_seconds < 0 or args.timeout <= 0:
        parser.error("intervalos e timeout inválidos")
    asyncio.run(run(args))
