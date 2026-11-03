# distributed-algorithms
A repository to store distributed algorithms implementations.

```bash
scripts/sim.sh build
scripts/sim.sh up 5            # 5 nós, algoritmo "heartbeat"
scripts/sim.sh status
scripts/sim.sh crash 3 && scripts/sim.sh recover 3
scripts/sim.sh netem 1 200 10  # +200ms, 10% de perda no nó 1
scripts/sim.sh partition "1,2" "3,4,5" && scripts/sim.sh heal
scripts/sim.sh demo            # cenário completo
scripts/sim.sh down
```

Logs (JSONL) em `data/logs/nodeX.jsonl`

Novas implementações vão em `src/node/algo/<nome>.py` (start/handle/state),
registre em `src/node/algo/__init__.py` e rode `scripts/sim.sh up N <nome> <seed>`.
