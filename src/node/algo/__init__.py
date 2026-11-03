"""Registro de algoritmos. Para adicionar um: crie o módulo e registre aqui."""
# from .heartbeat import Heartbeat

REGISTRY = {
    # "ricart_agrawala": RicartAgrawala,
    # "bully": Bully,
    # "raft": Raft,
}


def load(name):
    return REGISTRY[name]