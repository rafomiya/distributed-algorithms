# IC | Impressao distribuida — Ricart-Agrawala + Gossip
# Compatible with just >= 1.21. Recipe parameters are passed through the shell
# environment or "$@", so spaces, quotes and shell metacharacters stay literal.

set shell := ["bash", "-eu", "-o", "pipefail", "-c"]
set positional-arguments := true
set dotenv-load := false

export venv := env_var_or_default("VENV", ".venv")
export python := env_var_or_default("PYTHON", venv + "/bin/python")

# Lista os comandos na ordem dos grupos abaixo (padrao ao executar just).
help:
    @just --list --unsorted

# ── Preparacao e ambiente ────────────────────────────────────────────────────

# Cria o ambiente Python e instala as dependencias: just setup [python3.12].
setup $interpreter="python3":
    "$interpreter" -m venv "$venv"
    "$venv/bin/python" -m pip install -r requirements.txt

# Constroi a imagem Docker.
build:
    scripts/sim.sh build

# Inicia uma nova execucao completa: just up [N=3] [SEED=42].
up $n="3" $seed="42":
    scripts/sim.sh up "$n" "$seed"

# Encerra o Compose e preserva os logs.
down:
    scripts/sim.sh down

# Consulta computadores e impressoras.
status:
    scripts/sim.sh status

# Acompanha todos os logs ou os servicos indicados: just logs [node1 printer1].
logs *services:
    scripts/sim.sh logs "$@"

# ── Impressao e demonstracao ─────────────────────────────────────────────────

# Envia trabalho: just print [NODE=1] [PRINTER=1] [PAGES=1] [DOCUMENT].
print $node="1" $printer="1" $pages="1" $document="documento.txt":
    "$python" scripts/client.py --node "$node" --printer "$printer" --pages "$pages" --document "$document"

# Mostra o historico replicado: just records [NODE=1].
records $node="1":
    curl --fail --silent --show-error --max-time 3 "http://localhost:800$node/records" | "$python" -m json.tool

# Recria o Compose com 3 computadores e demonstra os cenarios: just demo [OUTPUT].
demo $output="data/demo":
    scripts/sim.sh up 3 42
    "$python" scripts/scenarios.py --backend existing --output "$output"

# Demonstra os cenarios em processos locais: just demo-local [OUTPUT].
demo-local $output="data/demo-local":
    "$python" scripts/scenarios.py --backend local --output "$output"

# ── Falhas e rede ────────────────────────────────────────────────────────────

# Separa grupos de computadores: just partition [LEFT=1,2] [RIGHT=3].
partition $left="1,2" $right="3":
    scripts/sim.sh partition "$left" "$right"

# Remove falhas da aplicacao; nao reverte crash, pause ou netem.
heal:
    scripts/sim.sh heal

# Encerra um participante nesta execucao: just crash node3.
crash $service:
    scripts/sim.sh crash "$service"

# Suspende um servico: just pause printer1.
pause $service:
    scripts/sim.sh pause "$service"

# Retoma um servico suspenso: just unpause printer1.
unpause $service:
    scripts/sim.sh unpause "$service"

# Simula falha de RPC: just fault [NODE=1] [DELAY_MS=50] [LOSS=0.2] [TYPES].
fault $node="1" $delay_ms="50" $loss="0.2" $types="":
    "$python" -c 'import json, sys; print(json.dumps({"delay_ms": float(sys.argv[1]), "loss": float(sys.argv[2]), "types": sys.argv[3].split()}))' "$delay_ms" "$loss" "$types" \
        | curl --fail --silent --show-error --max-time 3 "http://localhost:800$node/fault" -H 'Content-Type: application/json' --data-binary @-
    @printf '\n'

# Degrada a rede Linux: just netem [NODE=1] [DELAY_MS=50] [LOSS_PERCENT=10] [JITTER_MS=0].
netem $node="1" $delay_ms="50" $loss_percent="10" $jitter_ms="0":
    scripts/sim.sh netem "$node" "$delay_ms" "$loss_percent" "$jitter_ms"

# Remove tc/netem de um computador: just netem-clear [NODE=1].
netem-clear $node="1":
    scripts/sim.sh netem-clear "$node"

# ── Testes e verificacao ──────────────────────────────────────────────────────

# Executa os testes unitarios e de integracao HTTP local.
test:
    PYTHONPATH=src:. "$python" -m unittest discover -s tests -v

# Verifica os JSONL de uma execucao: just verify data/logs/EXECUCAO.
verify $log_dir:
    "$python" scripts/verify.py "$log_dir"

# Verifica whitespace, sintaxe Bash e configuracao Compose, sem iniciar servicos.
check:
    git diff --check
    bash -n scripts/sim.sh scripts/demo.sh
    docker compose config --quiet

# ── Experimentos ─────────────────────────────────────────────────────────────

# Executa a matriz no Compose; aceita flags de scripts/experiments.py.
experiments *args:
    "$python" scripts/experiments.py --backend docker "$@"

# Executa a matriz em processos locais; aceita flags de scripts/experiments.py.
experiments-local *args:
    "$python" scripts/experiments.py --backend local "$@"
