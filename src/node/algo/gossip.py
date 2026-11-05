import asyncio
from ..main import Node


def create_node_data(num: int, n: int):
    """
    retornando uma lista dos múltiplos do quadrado
    do número do nó para representar os dados que ele
    guarda
    """
    return [i * num **2 for i in range(1, n + 1)]


class Gossip:
    def __init__(self, node):
        self.node = node
        self.node.data

    async def start(self):
        ...

    async def _push(self, peer):
        r = await self.node.send(peer, "push", {"data": self.node.data})

    async def _pull(self, r):
        self.node.data |= r["data"]

    async def _loop(self):
        while True:
            peer = self.node.rng.choice(self.node.peers)

    async def _ping(self, peer):
        ...

    def _seen(self, peer, rtt_ms=None):
        ...

    def _check(self):
        ...

    async def handle(self, msg):
        ...

    def state(self):
        ...
