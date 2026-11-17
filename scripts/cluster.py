"""Ambiente de testes/experimentos. Nós só interagem através de HTTP.

Backend docker = referência Compose; local = mesmos serviços em processos
independentes no loopback; existing = ambiente já iniciado por sim.sh.
"""
import asyncio
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import time

from aiohttp import ClientError, ClientSession, ClientTimeout

ROOT = Path(__file__).resolve().parents[1]


async def wait_until(check, timeout=15, interval=0.03):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = await check()
        if result:
            return result
        await asyncio.sleep(interval)
    raise TimeoutError(f"condição não satisfeita em {timeout}s")


class Cluster:
    def __init__(self, count=3, seed=42, output=None, backend="local", gossip_interval=0.08,
                 printer_count=2):
        self.count, self.seed, self.backend = count, seed, backend
        self.printer_count = printer_count
        self.output = Path(output or "data/test-run").resolve()
        self.output.mkdir(parents=True, exist_ok=True)
        self.log_dir = self.output / "logs"
        self.log_dir.mkdir(exist_ok=True)
        self.gossip_interval = gossip_interval
        self.processes = {}
        self.files = []
        names = ([f"node{i}" for i in range(1, count + 1)] +
                 [f"printer{i}" for i in range(1, printer_count + 1)])
        self.urls = {}
        if backend == "local":
            sockets = []
            try:
                for name in names:
                    sock = socket.socket()
                    sock.bind(("127.0.0.1", 0))
                    sockets.append(sock)
                    self.urls[name] = f"http://127.0.0.1:{sock.getsockname()[1]}"
            finally:
                for sock in sockets:
                    sock.close()
        else:
            self.urls = {f"node{i}": f"http://127.0.0.1:{8000+i}" for i in range(1, count + 1)}
            self.urls.update({f"printer{i}": f"http://127.0.0.1:{9000+i}"
                              for i in range(1, printer_count + 1)})
        self.alive = set(names)
        self.env = {**os.environ, "NUM_NODES": str(count), "SEED": str(seed),
            "GOSSIP_INTERVAL": str(gossip_interval), "RETRY_INTERVAL": "0.03",
            "PAGE_SECONDS": "0.05", "LOG_DIR": str(self.log_dir), "QUIET": "1",
            "PYTHONPATH": str(ROOT / "src"), "PYTHONUNBUFFERED": "1"}

    async def __aenter__(self):
        self.session = ClientSession(timeout=ClientTimeout(total=2))
        try:
            if self.backend == "local":
                peer_urls = {name: url for name, url in self.urls.items() if name.startswith("node")}
                printers = {name: url for name, url in self.urls.items() if name.startswith("printer")}
                for name, url in self.urls.items():
                    env = {**self.env, "NODE_ID": name, "PORT": url.rsplit(":", 1)[1],
                        "PEER_URLS": json.dumps(peer_urls), "PRINTER_URLS": json.dumps(printers)}
                    stream = open(self.output / f"{name}.stdout", "w")
                    self.files.append(stream)
                    self.processes[name] = await asyncio.create_subprocess_exec(
                        sys.executable, "-m", "node.printer" if name.startswith("printer") else "node.main",
                        cwd=ROOT, env=env, stdout=stream, stderr=subprocess.STDOUT)
            elif self.backend == "docker":
                await self.compose("down", "--remove-orphans")
                await self.compose("up", "-d", "--build", "--wait", "--wait-timeout", "90", *self.urls)
            elif self.backend != "existing":
                raise ValueError("backend inválido")
            for name in self.urls:
                await wait_until(lambda name=name: self.healthy(name), timeout=30)
            return self
        except BaseException:
            await self.__aexit__(None, None, None)
            raise

    async def compose(self, *args):
        stream = open(self.output / "compose.stdout", "a")
        self.files.append(stream)
        process = await asyncio.create_subprocess_exec("docker", "compose", *args,
            cwd=ROOT, env=self.env, stdout=stream, stderr=subprocess.STDOUT)
        if await process.wait():
            raise RuntimeError(f"docker compose {' '.join(args)} falhou; veja {self.output / 'compose.stdout'}")

    async def __aexit__(self, *args):
        if self.backend == "docker":
            await self.compose("down", "--remove-orphans")
        for process in self.processes.values():
            if process.returncode is None:
                process.terminate()
        for process in self.processes.values():
            try:
                await asyncio.wait_for(process.wait(), 5)
            except asyncio.TimeoutError:
                process.kill()
                await process.wait()
        await self.session.close()
        for stream in self.files:
            stream.close()

    async def request(self, name, path, payload=None):
        method = self.session.get if payload is None else self.session.post
        kwargs = {} if payload is None else {"json": payload}
        async with method(self.urls[name] + path, **kwargs) as response:
            response.raise_for_status()
            return await response.json()

    async def healthy(self, name):
        try:
            return await self.request(name, "/health")
        except (ClientError, asyncio.TimeoutError):
            return False

    @property
    def computers(self):
        return sorted(name for name in self.alive if name.startswith("node"))

    async def states(self):
        names = sorted(self.alive)
        states = await asyncio.gather(*(self.request(name, "/state") for name in names))
        return dict(zip(names, states))

    async def submit(self, node="node1", printer="printer1", pages=1, document="teste.pdf"):
        return await self.request(node, "/jobs", {"printer": printer, "pages": pages, "document": document})

    async def completed(self, expected, timeout=20):
        async def check():
            states = await self.states()
            jobs = [job for name, state in states.items() if name.startswith("node") for job in state["jobs"]]
            errors = [job for job in jobs if job["status"] == "error"]
            if errors:
                raise AssertionError(errors)
            return states if sum(job["status"] == "completed" for job in jobs) >= expected else None
        return await wait_until(check, timeout)

    async def converged(self, expected_ids, timeout=20):
        expected_ids = set(expected_ids)

        async def check():
            states = await self.states()
            replicas = [state["gossip"]["records"] for name, state in states.items() if name.startswith("node")]
            return states if replicas and all(
                {r["job_id"] for r in replica} == expected_ids and replica == replicas[0]
                for replica in replicas) else None
        return await wait_until(check, timeout)

    async def partition(self, left, right, types=None):
        for group, other in ((left, right), (right, left)):
            for name in group:
                await self.request(name, "/fault", {"blocked_peers": list(other), "types": types or []})

    async def heal(self):
        for name in self.computers:
            await self.request(name, "/fault", {"blocked_peers": [], "types": [], "loss": 0, "delay_ms": 0})

    async def crash(self, name):
        if self.backend == "local":
            self.processes[name].kill()
            await self.processes[name].wait()
        else:
            await self.compose("kill", "-s", "SIGKILL", name)
        self.alive.remove(name)


def assert_safety(states):
    """Impressora observa colisões; estado RA é verificado nos logs por verify.py."""
    for name, state in states.items():
        if not name.startswith("printer"):
            continue
        assert state["overlap_attempts"] == 0, f"colisão em {name}"
        jobs = sorted(state["jobs"], key=lambda job: job["started_mono_ns"])
        for previous, current in zip(jobs, jobs[1:]):
            assert previous.get("completed_mono_ns", float("inf")) <= current["started_mono_ns"], name
