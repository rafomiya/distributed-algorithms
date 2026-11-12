"""Ricart–Agrawala por recurso, com retransmissão e REPLY explícito.

Participação fixa, falhas crash-stop. Nenhum timeout concede permissão.
HTTP ACK confirma transporte; somente ra_reply conta como permissão.
"""
import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass, field


@dataclass
class Resource:
    local_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    status: str = "RELEASED"
    request_id: str = ""
    timestamp: int = 0
    waiting: set = field(default_factory=set)
    deferred: dict = field(default_factory=dict)
    ready: asyncio.Event = field(default_factory=asyncio.Event)


class RicartAgrawala:
    def __init__(self, node, resources, retry_interval=0.2):
        self.node = node
        self.clock = 0
        self.sequence = 0
        self.resources = {name: Resource() for name in resources}
        self.retry_interval = retry_interval
        self.outbox = {}
        self.task = None

    def tick(self, received=0):
        self.clock = max(self.clock, received) + 1
        return self.clock

    async def start(self):
        self.task = asyncio.create_task(self._reply_loop())

    async def stop(self):
        if self.task:
            self.task.cancel()
            await asyncio.gather(self.task, return_exceptions=True)

    def _reply(self, peer, resource, request_id):
        self.outbox[(peer, resource, request_id)] = {
            "resource": resource, "request_id": request_id,
        }

    async def _reply_loop(self):
        while True:
            pending = list(self.outbox.items())

            async def deliver(key, payload):
                peer, _, _ = key
                response = await self.node.send(
                    peer, "ra_reply", {**payload, "clock": self.tick()})
                if response is not None:
                    self.outbox.pop(key, None)

            await asyncio.gather(*(deliver(key, value) for key, value in pending))
            await asyncio.sleep(self.retry_interval)

    async def handle(self, msg):
        peer, payload = msg["from"], msg["payload"]
        resource, request_id = payload["resource"], payload["request_id"]
        state = self.resources[resource]
        self.tick(payload["clock"])
        if msg["type"] == "ra_reply":
            if state.status == "WANTED" and state.request_id == request_id:
                state.waiting.discard(peer)
                if not state.waiting:
                    state.ready.set()
        elif msg["type"] == "ra_request":
            incoming = (payload["timestamp"], peer)
            own = (state.timestamp, self.node.id)
            defer = state.status == "HELD" or (
                state.status == "WANTED" and own < incoming)
            if defer:
                state.deferred[(peer, request_id)] = True
            else:
                self._reply(peer, resource, request_id)
        else:
            raise ValueError("tipo de mensagem RA desconhecido")
        return {"ok": True}

    @asynccontextmanager
    async def hold(self, resource):
        state = self.resources[resource]
        async with state.local_lock:
            self.sequence += 1
            state.request_id = f"{self.node.boot_id}:{self.sequence}"
            state.timestamp = self.tick()
            state.waiting = set(self.node.peers)
            state.ready.clear()
            state.status = "WANTED"
            self.node.log("ra_request", resource=resource,
                          request_id=state.request_id, clock=state.timestamp)
            try:
                while state.waiting:
                    await asyncio.gather(*(self.node.send(peer, "ra_request", {
                        "resource": resource, "request_id": state.request_id,
                        "timestamp": state.timestamp, "clock": self.tick(),
                    }) for peer in sorted(state.waiting)))
                    if state.waiting:
                        try:
                            await asyncio.wait_for(state.ready.wait(), self.retry_interval)
                        except asyncio.TimeoutError:
                            pass
                state.status = "HELD"
                self.node.log("cs_enter", resource=resource, request_id=state.request_id)
                yield state.request_id
            finally:
                if state.status == "HELD":
                    self.node.log("cs_exit", resource=resource, request_id=state.request_id)
                state.status = "RELEASED"
                for peer, request_id in state.deferred:
                    self._reply(peer, resource, request_id)
                state.deferred.clear()

    def state(self):
        return {"clock": self.clock, "resources": {
            name: {"status": state.status, "request_id": state.request_id,
                   "waiting": sorted(state.waiting), "deferred": len(state.deferred)}
            for name, state in self.resources.items()
        }}
