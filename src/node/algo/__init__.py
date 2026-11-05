"""Registro de algoritmos."""
from .gossip import Gossip

REGISTRY = {
    "gossip": Gossip,
}


def load(name):
    return REGISTRY[name]