#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

nodes() { seq -f "node%g" 1 "$1"; }
num()   { grep -E '^NUM_NODES=' .env 2>/dev/null | cut -d= -f2 || echo 3; }

usage() { cat <<USG
Ciclo de vida
  build                      constrói a imagem
  up [N=3] [ALGO] [SEED]     sobe N nós (máx 5) e espera ficarem saudáveis
  down                       remove contêineres, rede e limpa regras
  status                     GET /state de cada nó
  logs [nodeX]               segue os logs

Falhas (crash-stop / crash-recovery)
  crash <i>                  kill -9 do nó i (crash-stop)
  recover <i>                reinicia o nó i (crash-recovery)
  pause <i> | unpause <i>    congela/descongela (nó lento/suspenso)

Rede (tc/netem, iptables dentro dos contêineres)
  netem <i> <delay_ms> [loss_%] [jitter_ms]   atraso/perda/jitter na saída do nó i
  netem-clear <i>
  partition "1,2" "3,4,5"    bloqueia tráfego entre os dois grupos
  heal                       remove todas as partições (iptables -F)

Reprodução
  demo                       cenário normal -> falha -> recuperação (scripts/demo.sh)
USG
}

wait_healthy() {
  for n in $(nodes "$1"); do
    for _ in $(seq 1 40); do
      [ "$(docker inspect -f '{{.State.Health.Status}}' "$n" 2>/dev/null)" = healthy ] && break
      sleep 0.5
    done
  done
}

cmd=${1:-help}; shift || true
case "$cmd" in
  build) docker compose build ;;
  up)
    N=${1:-3}; ALGO=${2:-heartbeat}; SEED=${3:-42}
    [ "$N" -ge 1 ] && [ "$N" -le 5 ] || { echo "N deve estar entre 1 e 5"; exit 1; }
    printf 'NUM_NODES=%s\nALGO=%s\nSEED=%s\n' "$N" "$ALGO" "$SEED" > .env
    rm -f data/logs/*.jsonl
    docker compose up -d --build $(nodes "$N")
    wait_healthy "$N"
    echo "OK: $N nós no ar (algo=$ALGO seed=$SEED)" ;;
  down) docker compose down -v --remove-orphans ;;
  status)
    for i in $(seq 1 "$(num)"); do
      printf 'node%s: ' "$i"
      curl -fs --max-time 1 "localhost:800$i/state" || echo "DOWN"; echo
    done ;;
  logs) docker compose logs -f --no-color "$@" ;;
  crash)   docker kill "node$1" >/dev/null && echo "node$1 crashed" ;;
  recover) docker compose start "node$1" && echo "node$1 recovered" ;;
  pause)   docker pause "node$1" ;;
  unpause) docker unpause "node$1" ;;
  netem)
    i=$1; d=$2; l=${3:-0}; j=${4:-0}
    docker exec "node$i" tc qdisc replace dev eth0 root netem \
      delay "${d}ms" "${j}ms" loss "${l}%"
    echo "node$i: delay=${d}ms jitter=${j}ms loss=${l}%" ;;
  netem-clear) docker exec "node$1" tc qdisc del dev eth0 root 2>/dev/null || true ;;
  partition)
    A=${1//,/ }; B=${2//,/ }
    for a in $A; do for b in $B; do
      ipa=$(docker exec "node$a" getent hosts "node$a" | awk '{print $1}')
      ipb=$(docker exec "node$b" getent hosts "node$b" | awk '{print $1}')
      docker exec "node$a" sh -c "iptables -A INPUT -s $ipb -j DROP; iptables -A OUTPUT -d $ipb -j DROP"
      docker exec "node$b" sh -c "iptables -A INPUT -s $ipa -j DROP; iptables -A OUTPUT -d $ipa -j DROP"
    done; done
    echo "partição: {$1} | {$2}" ;;
  heal)
    for n in $(docker ps --format '{{.Names}}' | grep -E '^node[0-9]+$'); do
      docker exec "$n" iptables -F
    done; echo "rede restaurada" ;;
  demo) exec scripts/demo.sh ;;
  *) usage ;;
esac
