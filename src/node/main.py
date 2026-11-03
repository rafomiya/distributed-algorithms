"""Nó genérico: servidor HTTP/JSON + cliente com timeout explícito + log JSONL.
Toda comunicação entre nós passa por Node.send() (deadline sempre definido)."""
import asyncio
import json
import os
import random
import socket
import time
 
from aiohttp import ClientSession, ClientTimeout, web

from .algo import load


class Node:
    def __init__(self):
        self.id = socket.gethostname()                      # node1, node2, ...
        self.n = int(os.environ.get("NUM_NODES", "3"))
        self.port = int(os.environ.get("PORT", "8000"))
        self.seed = int(os.environ.get("SEED", "42"))
        self.rpc_timeout = float(os.environ.get("RPC_TIMEOUT", "0.8"))
        self.rng = random.Random(f"{self.seed}-{self.id}")
        self.peers = [
            f"node{i}"
            for i in range(1, self.n + 1)
            if f"node{i}" != self.id
        ]
        os.makedirs("/data/logs", exist_ok=True)
        self.logf = open(f"/data/logs/{self.id}.jsonl", "a", buffering=1)
        self.session = None
        self.algo = load(os.environ.get("ALGO", "heartbeat"))(self)

    def log(self, event, **kw):
        line = json.dumps({
            "ts": time.time(),
            "node": self.id,
            "event": event,
            **kw
        })
        print(line, flush=True)
        self.logf.write(line + "\n")

    async def send(self, peer, mtype, payload):
        """RPC com timeout. Devolve dict, ou None se falhou (timeout/recusa)."""
        body = {"type": mtype, "from": self.id, "payload": payload}
        try:
            async with self.session.post(
                f"http://{peer}:{self.port}/rpc",
                json=body,
                timeout=ClientTimeout(total=self.rpc_timeout),
            ) as r:
                return await r.json()
        except Exception:
            return None

    async def h_rpc(self, req):
        return web.json_response(await self.algo.handle(await req.json()))

    async def h_health(self, req):
        return web.json_response({"ok": True, "node": self.id})

    async def h_state(self, req):
        return web.json_response({
            "node": self.id,
            "peers": self.peers,
            **self.algo.state()
        })


async def main():
    node = Node()
    node.session = ClientSession()
    app = web.Application()
    app.add_routes([
        web.post("/rpc", node.h_rpc),
        web.get("/health", node.h_health),
        web.get("/state", node.h_state),
    ])
    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", node.port).start()
    node.log(
        "start",
        peers=node.peers,
        seed=node.seed,
        algo=os.environ.get("ALGO", "heartbeat"),
    )
    await node.algo.start()
    await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())
