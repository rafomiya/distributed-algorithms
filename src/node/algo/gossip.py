"""Gossip push/pull (anti-entropy) sobre um repositório replicado.

Cada nó é dono de uma entrada (versão + dados) e guarda uma réplica das
entradas de todos os nós. Só o dono escreve na sua entrada, então a maior
versão sempre vence, sem conflitos.

A cada rodada o nó escolhe `fanout` pares aleatórios e troca estado com eles:
envia as entradas; o par faz o merge e devolve o que é mais novo.

Configuração por ambiente: GOSSIP_MODE, GOSSIP_FANOUT, GOSSIP_INTERVAL, DATA_ITEMS.
"""
from __future__ import annotations

import asyncio
import os
import time
from dataclasses import asdict, dataclass


MSG_GOSSIP = "gossip"
MSG_UPDATE_LOCAL = "update_local"

ROUND_JITTER_FRACTION = 0.1


@dataclass
class Entry:
    version: int
    data: list[int]


def create_node_data(node_number: int, item_count: int) -> list[int]:
    """Múltiplos do quadrado do número do nó: os dados que o nó guarda."""
    return [i * node_number ** 2 for i in range(1, item_count + 1)]


int_env = lambda x: int(os.environ.get(x))

class Gossip:
    def __init__(self, node, fanout=1, interval_s=1.0, data_items=5):
        self.node = node
        self.fanout = int_env("GOSSIP_FANOUT") if fanout is None else fanout
        self.interval_s = int_env("GOSSIP_INTERVAL") if interval_s is None else interval_s
        self.data_items = int_env("DATA_ITEMS") if data_items is None else data_items

        self.num = int(node.id.removeprefix("node"))
        self.closed_neigh = set(node.peers) | {node.id}
        self.store: dict[str, Entry] = {
            node.id: Entry(1, create_node_data(self.num, self.data_items))
        }

        self.started_at = time.monotonic()
        self.seconds_until_all_known: float | None = None
        self.rounds = 0
        self.requests_sent = 0
        self.requests_failed = 0
        self._rounds_task: asyncio.Task | None = None

    @property
    def own_version(self) -> Entry:
        return self.store[self.node.id].version

    def version_of(self, owner: str) -> int:
        return self.store[owner].version if owner in self.store else 0

    # ---------- ciclo de vida ----------
    async def start(self):
        self._log_if_all_nodes_known()
        self._rounds_task = asyncio.create_task(self._run_rounds())

    def stop(self):
        if self._rounds_task:
            self._rounds_task.cancel()

    # ---------- lado ativo: inicia as trocas ----------
    async def _run_rounds(self):
        all_peers = self.node.peers
        while True:
            self.rounds += 1
            chosen_peers = self.node.rng.sample(all_peers, min(self.fanout, len(all_peers)))
            for peer in chosen_peers:
                asyncio.create_task(self._gossip_with(peer))
                
            jitter = self.node.rng.uniform(0, ROUND_JITTER_FRACTION)
            await asyncio.sleep(self.interval_s * (1 + jitter))

    async def _gossip_with(self, peer: str):
        request = {
            "version_control": self._version_control(),
            "entries": {
                owner: asdict(entry)
                for owner, entry in self.store.items()
            },
        }
        self.requests_sent += 1
        reply = await self.node.send(peer, MSG_GOSSIP, request)
        if reply is None:
            self.requests_failed += 1
            return
        self._merge(reply.get("entries", {}), via="pull", peer=peer)

    # ---------- lado passivo: responde às mensagens ----------
    async def handle(self, msg):
        match msg["type"]:
            case "gossip":
                return self._handle_gossip(msg["from"], msg["payload"])
            case "update_local":
                self.update_local(msg.get("payload", {}).get("data"))
                return {"ok": True, "version": self.own_version}
            case unknown:
                return {"error": f"unknown type {unknown}"}

    def _handle_gossip(self, sender: str, request: dict) -> dict:
        self._merge(request.get("entries", {}), via="push", peer=sender)
        return {"entries": self._entries_newer_than(request.get("version_control", {}))}

    # ---------- réplica ----------
    def _version_control(self) -> dict[str, int]:
        return {
            owner: entry.version for owner, entry in self.store.items()
        }

    def _entries_newer_than(self, version_control: dict[str, int]) -> dict[str, dict]:
        return {
            owner: asdict(entry)
            for owner, entry in self.store.items()
            if entry.version > version_control.get(owner, 0)
        }

    def _merge(self, received: dict[str, dict], via: str, peer: str):
        learned_any = False
        for owner, raw_entry in received.items():
            entry = Entry(**raw_entry)
            if entry.version <= self.version_of(owner):
                continue  # versão local é mais nova ou a mesma
            # se `owner` é este nó (reinício sem estado), recupera a própria entrada
            self.store[owner] = entry
            learned_any = True
            self.node.log("learn", owner=owner, version=entry.version, via=via, peer=peer)
        if learned_any:
            self._log_if_all_nodes_known()

    def _log_if_all_nodes_known(self):
        if self.seconds_until_all_known is None and self.closed_neigh <= self.store.keys():
            self.seconds_until_all_known = round(time.monotonic() - self.started_at, 3)
            self.node.log("complete", elapsed_s=self.seconds_until_all_known)

    def update_local(self, new_data: list[int] | None = None):
        """Escrita local: publica uma nova versão da própria entrada."""
        next_version = self.own_version + 1
        if new_data is None:
            new_data = self._default_data_for_version(next_version)
        self.store[self.node.id] = Entry(next_version, new_data)
        self.node.log("update", version=next_version)

    def _default_data_for_version(self, version: int) -> list[int]:
        return [
            value + version - 1
            for value in create_node_data(self.num, self.data_items)
        ]

    def state(self):
        return {
            "fanout": self.fanout,
            "interval_s": self.interval_s,
            "rounds": self.rounds,
            "requests_sent": self.requests_sent,
            "requests_failed": self.requests_failed,
            "seconds_until_all_known": self.seconds_until_all_known,
            "store": {
                owner: asdict(entry)
                for owner, entry in sorted(self.store.items())
            },
        }
