"""Gossip push/pull de entradas versionadas e histórico de impressões.

Cada dono escreve sua entrada (dados de exemplo e registros imutáveis locais).
Versões maiores substituem os dados, preservando todo o histórico já publicado.
Cada rodada contata até `fanout` pares, enviando as entradas conhecidas; a
resposta contém apenas as versões que o solicitante ainda não conhece.

Configuração: GOSSIP_FANOUT, GOSSIP_INTERVAL (segundos) e DATA_ITEMS.
O modelo continua sendo de participação fixa e falhas crash-stop.
"""
from __future__ import annotations

import asyncio
import copy
import math
import os
import time
from dataclasses import asdict, dataclass, field


MSG_GOSSIP = "gossip"
MSG_UPDATE_LOCAL = "update_local"
ROUND_JITTER_FRACTION = 0.1


@dataclass
class Entry:
    version: int
    data: list[int]
    records: list[dict] = field(default_factory=list)


def create_node_data(node_number: int, item_count: int) -> list[int]:
    """Múltiplos do quadrado do número do nó: os dados que o nó guarda."""
    return [i * node_number ** 2 for i in range(1, item_count + 1)]


class Gossip:
    def __init__(self, node, fanout=None, interval_s=None, data_items=None):
        self.node = node
        self.fanout = int(os.environ.get("GOSSIP_FANOUT", "1")) if fanout is None else fanout
        self.interval_s = float(os.environ.get("GOSSIP_INTERVAL", "0.5")) if interval_s is None else float(interval_s)
        self.data_items = int(os.environ.get("DATA_ITEMS", "5")) if data_items is None else data_items
        if type(self.fanout) is not int or self.fanout < 1:
            raise ValueError("GOSSIP_FANOUT deve ser um inteiro positivo")
        if not math.isfinite(self.interval_s) or self.interval_s <= 0:
            raise ValueError("GOSSIP_INTERVAL deve ser um número finito positivo")
        if type(self.data_items) is not int or self.data_items < 0:
            raise ValueError("DATA_ITEMS deve ser um inteiro não negativo")

        self.num = int(node.id.removeprefix("node"))
        self.closed_neigh = set(node.peers) | {node.id}
        self.store: dict[str, Entry] = {
            node.id: Entry(1, create_node_data(self.num, self.data_items))
        }
        self.records: dict[str, dict] = {}
        self.started_at = time.monotonic()
        self.seconds_until_all_known: float | None = None
        self.rounds = 0
        self.requests_sent = 0
        self.requests_failed = 0
        self._rounds_task: asyncio.Task | None = None

    @property
    def own_version(self) -> int:
        return self.store[self.node.id].version

    def version_of(self, owner: str) -> int:
        return self.store[owner].version if owner in self.store else 0

    # ---------- ciclo de vida ----------
    async def start(self):
        if self._rounds_task and not self._rounds_task.done():
            return
        self._log_if_all_nodes_known()
        self._rounds_task = asyncio.create_task(self._run_rounds())

    async def stop(self):
        if self._rounds_task:
            self._rounds_task.cancel()
            await asyncio.gather(self._rounds_task, return_exceptions=True)
            self._rounds_task = None

    # ---------- lado ativo: inicia as trocas ----------
    async def _run_rounds(self):
        while True:
            self.rounds += 1
            peers = self.node.rng.sample(self.node.peers, min(self.fanout, len(self.node.peers)))
            # Cada rodada aguarda suas trocas: não acumula tarefas em rede lenta.
            # Cancelar a rodada também cancela e aguarda todas as RPCs em curso.
            results = await asyncio.gather(*(self.exchange(peer) for peer in peers),
                                           return_exceptions=True)
            for peer, result in zip(peers, results):
                if isinstance(result, Exception):
                    self.requests_failed += 1
                    self.node.log("gossip_error", peer=peer, error=repr(result))
            jitter = self.node.rng.uniform(0, ROUND_JITTER_FRACTION)
            await asyncio.sleep(self.interval_s * (1 + jitter))

    async def exchange(self, peer: str):
        """Inicia uma troca push/pull, inclusive para testes e chamadas manuais."""
        await self._gossip_with(peer)

    async def _gossip_with(self, peer: str):
        request = {
            "version_control": self._version_control(),
            "entries": {owner: asdict(entry) for owner, entry in self.store.items()},
        }
        self.requests_sent += 1
        reply = await self.node.send(peer, MSG_GOSSIP, request)
        if reply is None:
            self.requests_failed += 1
            return
        self._merge(reply["entries"], via="pull", peer=peer)

    # ---------- lado passivo: responde às mensagens ----------
    async def handle(self, msg):
        if msg["type"] == MSG_GOSSIP:
            return self._handle_gossip(msg["from"], msg["payload"])
        if msg["type"] == MSG_UPDATE_LOCAL:
            self.update_local(msg.get("payload", {}).get("data"))
            return {"ok": True, "version": self.own_version}
        raise ValueError(f"unknown type {msg['type']}")

    def _handle_gossip(self, sender: str, request: dict) -> dict:
        versions = request.get("version_control", {})
        if not isinstance(versions, dict) or any(
            owner not in self.closed_neigh or type(version) is not int or version < 0
            for owner, version in versions.items()
        ):
            raise ValueError("controle de versões inválido")
        self._merge(request.get("entries", {}), via="push", peer=sender)
        return {"entries": self._entries_newer_than(versions)}

    # ---------- histórico imutável consumido pela aplicação ----------
    @staticmethod
    def _record_map(records, owner):
        if not isinstance(records, list):
            raise ValueError("records deve ser uma lista")
        result = {}
        for record in records:
            if not isinstance(record, dict) or not isinstance(record.get("job_id"), str) or not record["job_id"]:
                raise ValueError("registro sem job_id válido")
            if record.get("computer") != owner:
                raise ValueError("somente o dono publica seus registros")
            key = record["job_id"]
            if key in result and result[key] != record:
                raise ValueError(f"registro imutável conflitante: {key}")
            result[key] = copy.deepcopy(record)
        return result

    def _check_conflicts(self, records):
        for key, record in records.items():
            if key in self.records and self.records[key] != record:
                raise ValueError(f"registro imutável conflitante: {key}")

    def _learn_records(self, records):
        for key, record in records.items():
            if key not in self.records:
                self.records[key] = record
                self.node.log("record_learned", job_id=key, origin=record["computer"])

    def merge(self, records):
        """Publica impressões locais, sem duplicá-las ou modificar as anteriores."""
        records = self._record_map(records, self.node.id)
        self._check_conflicts(records)
        added = {key: record for key, record in records.items() if key not in self.records}
        if added:
            own = self.store[self.node.id]
            self.store[self.node.id] = Entry(own.version + 1, copy.deepcopy(own.data),
                                            copy.deepcopy(own.records) + list(added.values()))
            self._learn_records(copy.deepcopy(added))
        return len(added)

    def values(self):
        return [copy.deepcopy(self.records[key]) for key in sorted(self.records)]

    # ---------- réplica de entradas versionadas ----------
    def _version_control(self) -> dict[str, int]:
        return {owner: entry.version for owner, entry in self.store.items()}

    def _entries_newer_than(self, version_control: dict[str, int]) -> dict[str, dict]:
        return {owner: asdict(entry) for owner, entry in self.store.items()
                if entry.version > version_control.get(owner, 0)}

    def _merge(self, received: dict[str, dict], via: str, peer: str):
        if not isinstance(received, dict):
            raise ValueError("entries deve ser um objeto")
        updates = {}
        learned = {}
        # Valida todo o lote antes de modificar o estado ou registrar aprendizado.
        for owner, raw_entry in received.items():
            if owner not in self.closed_neigh or not isinstance(raw_entry, dict):
                raise ValueError("entrada de dono desconhecido ou inválida")
            entry = Entry(**copy.deepcopy(raw_entry))
            if type(entry.version) is not int or entry.version < 1:
                raise ValueError("versão deve ser um inteiro positivo")
            if not isinstance(entry.data, list) or any(type(value) is not int for value in entry.data):
                raise ValueError("data deve ser uma lista de inteiros")
            if entry.version < self.version_of(owner):
                continue
            # Apenas o dono produz versões novas; igualdade deve ser idempotente.
            records = self._record_map(entry.records, owner)
            self._check_conflicts(records)
            previous = self.store.get(owner)
            if previous:
                old_records = self._record_map(previous.records, owner)
                if not old_records.keys() <= records.keys():
                    raise ValueError("uma nova versão não pode remover impressões")
                if entry.version == previous.version:
                    if entry != previous:
                        raise ValueError("mesma versão com conteúdo diferente")
                    continue
            # O mesmo job_id não pode pertencer a dois donos no mesmo lote.
            for key, record in records.items():
                if key in learned and learned[key] != record:
                    raise ValueError(f"registro imutável conflitante: {key}")
            learned.update(records)
            updates[owner] = entry
        self.store.update(updates)
        self._learn_records(learned)
        for owner, entry in updates.items():
            self.node.log("learn", owner=owner, version=entry.version, via=via, peer=peer)
        if updates:
            self._log_if_all_nodes_known()

    def _log_if_all_nodes_known(self):
        if self.seconds_until_all_known is None and self.closed_neigh <= self.store.keys():
            self.seconds_until_all_known = round(time.monotonic() - self.started_at, 3)
            self.node.log("complete", elapsed_s=self.seconds_until_all_known)

    def update_local(self, new_data: list[int] | None = None):
        """Atualiza os dados de exemplo, preservando o histórico local publicado."""
        next_version = self.own_version + 1
        if new_data is None:
            new_data = self._default_data_for_version(next_version)
        if not isinstance(new_data, list) or any(type(value) is not int for value in new_data):
            raise ValueError("data deve ser uma lista de inteiros")
        self.store[self.node.id] = Entry(next_version, copy.deepcopy(new_data),
                                        copy.deepcopy(self.store[self.node.id].records))
        self.node.log("update", version=next_version)

    def _default_data_for_version(self, version: int) -> list[int]:
        return [value + version - 1 for value in create_node_data(self.num, self.data_items)]

    def state(self):
        return {
            # "fanout": self.fanout,
            # "interval_s": self.interval_s,
            "rounds": self.rounds,
            "requests_sent": self.requests_sent,
            "requests_failed": self.requests_failed,
            "seconds_until_all_known": self.seconds_until_all_known,
            "record_count": len(self.records),
            "records": self.values(),
            "store": {owner: asdict(entry) for owner, entry in sorted(self.store.items())},
        }
