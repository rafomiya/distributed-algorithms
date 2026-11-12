"""Computador do IC: coordenação RA e replicação do histórico por gossip."""
import asyncio
from collections import Counter
import json
import math
import os
import random
import socket
import time
import uuid

from aiohttp import ClientError, ClientSession, ClientTimeout, web

from .algo import Gossip, RicartAgrawala
from .common import EventLog


class Node:
    def __init__(self):
        self.id = os.environ.get("NODE_ID", socket.gethostname())
        self.boot_id = f"{self.id}:{uuid.uuid4().hex}"
        self.n = int(os.environ.get("NUM_NODES", "3"))
        self.port = int(os.environ.get("PORT", "8000"))
        self.seed = int(os.environ.get("SEED", "42"))
        self.rpc_timeout = float(os.environ.get("RPC_TIMEOUT", "0.8"))
        self.retry_interval = float(os.environ.get("RETRY_INTERVAL", "0.2"))
        self.rng = random.Random(f"{self.seed}:{self.id}:gossip")
        self.fault_rng = random.Random(f"{self.seed}:{self.id}:fault")
        peer_suffix = os.environ.get("PEER_HOST_SUFFIX", "")
        self.peer_urls = json.loads(os.environ.get("PEER_URLS", json.dumps({
            f"node{i}": f"http://node{i}{peer_suffix}:8000" for i in range(1, self.n + 1)
        })))
        if self.id not in self.peer_urls:
            raise ValueError("NODE_ID deve fazer parte da participação fixa")
        self.peers = sorted(set(self.peer_urls) - {self.id})
        self.printers = json.loads(os.environ.get("PRINTER_URLS", json.dumps({
            "printer1": "http://printer1:8000", "printer2": "http://printer2:8000",
        })))
        self.log = EventLog(self.id, os.environ.get("LOG_DIR", "/data/logs"))
        self.session = None
        self.jobs = {}
        self.tasks = set()
        self.metrics = Counter()
        self.fault = {"delay_ms": 0, "loss": 0.0, "blocked_peers": [], "types": []}
        self.gossip = Gossip(self, float(os.environ.get("GOSSIP_INTERVAL", "0.5")))
        self.ra = RicartAgrawala(self, self.printers, self.retry_interval)

    async def start(self, app):
        self.session = ClientSession(timeout=ClientTimeout(total=self.rpc_timeout))
        await self.ra.start()
        await self.gossip.start()
        self.log("start", peers=self.peers, printers=self.printers, seed=self.seed,
                 model="fixed-membership/crash-stop")

    async def stop(self, app):
        await self.gossip.stop()
        # Interrompe concessões antes de cancelar quem pode estar imprimindo.
        # Cancelamento local não confirma que um efeito remoto terminou.
        await self.ra.stop()
        for task in self.tasks:
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        await self.session.close()
        self.log.close()

    async def send(self, peer, mtype, payload):
        """RPC com deadline, perdas/reordenação simuladas e métricas por algoritmo."""
        body = {"type": mtype, "from": self.id, "payload": payload}
        encoded = json.dumps(body).encode()
        self.metrics[f"{mtype}_attempts"] += 1
        self.metrics[f"{mtype}_bytes"] += len(encoded)
        self.log("rpc_send", peer=peer, message_type=mtype, bytes=len(encoded))
        affected = not self.fault["types"] or mtype in self.fault["types"]
        if affected:
            if peer in self.fault["blocked_peers"] or self.fault_rng.random() < self.fault["loss"]:
                self.metrics[f"{mtype}_dropped"] += 1
                return None
            await asyncio.sleep(self.fault["delay_ms"] / 1000)
        try:
            async with self.session.post(self.peer_urls[peer] + "/rpc", json=body) as r:
                r.raise_for_status()
                response = await r.json()
                self.metrics[f"{mtype}_response_bytes"] += len(json.dumps(response).encode())
                return response
        except (ClientError, asyncio.TimeoutError):
            self.metrics[f"{mtype}_failures"] += 1
            return None

    async def h_rpc(self, req):
        try:
            msg = await req.json()
            if msg["from"] not in self.peers:
                raise ValueError("remetente não participante")
            if msg["type"] == "gossip":
                result = await self.gossip.handle(msg)
            elif msg["type"] in {"ra_request", "ra_reply"}:
                result = await self.ra.handle(msg)
            else:
                raise ValueError("tipo desconhecido")
            return web.json_response(result)
        except (KeyError, TypeError, ValueError) as error:
            raise web.HTTPBadRequest(text=str(error)) from error

    async def h_health(self, req):
        return web.json_response({"ok": True, "role": "computer", "node": self.id})

    async def h_state(self, req):
        return web.json_response({"node": self.id, "peers": self.peers,
            "printers": list(self.printers), "ra": self.ra.state(),
            "gossip": self.gossip.state(), "jobs": list(self.jobs.values()),
            "metrics": dict(self.metrics), "fault": self.fault})

    async def h_records(self, req):
        return web.json_response(self.gossip.values())

    async def h_jobs(self, req):
        try:
            payload = await req.json()
            printer = payload["printer"]
            pages = payload.get("pages", 1)
            document = payload.get("document", "documento.txt")
            if printer not in self.printers:
                raise ValueError("impressora desconhecida")
            if type(pages) is not int or not 1 <= pages <= 100:
                raise ValueError("pages deve ser inteiro entre 1 e 100")
            if not isinstance(document, str) or not 1 <= len(document) <= 200:
                raise ValueError("document deve ter entre 1 e 200 caracteres")
        except (KeyError, TypeError, ValueError) as error:
            raise web.HTTPBadRequest(text=str(error)) from error
        job_id = f"{self.boot_id}:{len(self.jobs) + 1}"
        job = {"job_id": job_id, "computer": self.id, "printer": printer,
               "document": document, "pages": pages, "status": "queued"}
        self.jobs[job_id] = job
        self.log("job_submitted", **job)
        task = asyncio.create_task(self._print(job))
        self.tasks.add(task)
        task.add_done_callback(self.tasks.discard)
        return web.json_response(dict(job), status=202)

    async def _print(self, job):
        started = time.monotonic()
        job["status"] = "waiting_permission"
        try:
            async with self.ra.hold(job["printer"]) as request_id:
                job["status"] = "printing"
                job["request_id"] = request_id
                job["acquire_ms"] = (time.monotonic() - started) * 1000
                # Resultado incerto mantém a seção crítica. Reenvio tem mesmo job_id.
                while True:
                    try:
                        async with self.session.post(
                            self.printers[job["printer"]] + "/print", json=job,
                        ) as response:
                            response.raise_for_status()
                            result = await response.json()
                        if result["status"] == "completed":
                            break
                    except (ClientError, asyncio.TimeoutError):
                        self.log("printer_unreachable", job_id=job["job_id"],
                                 printer=job["printer"])
                    await asyncio.sleep(self.retry_interval)
                record = {key: result[key] for key in (
                    "job_id", "computer", "printer", "document", "pages",
                    "started_at", "completed_at", "started_mono_ns", "completed_mono_ns")}
                self.gossip.merge([record])
                job["status"] = "completed"
                job["total_ms"] = (time.monotonic() - started) * 1000
                self.log("job_completed", **job)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            job["status"] = "error"
            self.log("job_error", job_id=job["job_id"], error=repr(error))

    async def h_fault(self, req):
        try:
            payload = await req.json()
            fault = {**self.fault, **payload}
            if set(payload) - set(self.fault):
                raise ValueError("campo de falha desconhecido")
            if not all(isinstance(fault[k], (int, float)) and math.isfinite(fault[k])
                       for k in ("delay_ms", "loss")):
                raise ValueError("delay_ms e loss devem ser números finitos")
            if not 0 <= fault["loss"] <= 1 or not 0 <= fault["delay_ms"] <= 10000:
                raise ValueError("loss fora de [0,1] ou delay_ms fora de [0,10000]")
            if not isinstance(fault["blocked_peers"], list) or not set(fault["blocked_peers"]) <= set(self.peers):
                raise ValueError("blocked_peers inválido")
            if not isinstance(fault["types"], list) or not set(fault["types"]) <= {"gossip", "ra_request", "ra_reply"}:
                raise ValueError("types inválido")
            self.fault = fault
        except (TypeError, ValueError) as error:
            raise web.HTTPBadRequest(text=str(error)) from error
        self.log("fault_changed", **self.fault)
        return web.json_response(self.fault)

    def app(self):
        app = web.Application(client_max_size=16 * 1024 * 1024)
        app.add_routes([web.post("/rpc", self.h_rpc), web.get("/health", self.h_health),
            web.get("/state", self.h_state), web.get("/records", self.h_records),
            web.post("/jobs", self.h_jobs), web.post("/fault", self.h_fault)])
        app.on_startup.append(self.start)
        app.on_cleanup.append(self.stop)
        return app


def main():
    node = Node()
    web.run_app(node.app(), host="0.0.0.0", port=node.port, access_log=None)


if __name__ == "__main__":
    main()
