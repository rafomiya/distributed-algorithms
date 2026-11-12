#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

nodes() { seq -f 'node%g' 1 "$1"; }
num() { sed -n 's/^NUM_NODES=//p' .env 2>/dev/null || true; }
usage() {
  cat <<'HELP'
Uso: scripts/sim.sh <comando>
  build                           constrói a imagem
  up [N=3] [SEED=42]               inicia N computadores (3..5) e 2 impressoras
  down                            remove o ambiente; preserva logs
  status                          consulta computadores e impressoras
  print <computador> <impressora> [páginas] [documento]
  logs [serviço]                   acompanha logs
  crash <serviço>                  crash-stop (ex.: node3 ou printer1)
  pause <serviço> | unpause <serviço>
  netem <i> <atraso_ms> [perda_%] [jitter_ms]
  netem-clear <i>
  partition "1,2" "3"             partição entre computadores, em nível de aplicação
  heal                            remove falhas de aplicação
  test                            testes unitários + processos HTTP locais
  demo                            demonstração normal, partição e crash-stop
Reinício individual NÃO é suportado. Use up para uma nova execução completa.
HELP
}

cmd=${1:-help}; shift || true
case "$cmd" in
  build) docker compose build ;;
  up)
    N=${1:-3}; SEED=${2:-42}
    [[ "$N" =~ ^[3-5]$ ]] || { echo 'N deve estar entre 3 e 5'; exit 1; }
    printf 'NUM_NODES=%s\nSEED=%s\n' "$N" "$SEED" > .env
    docker compose down --remove-orphans
    export LOG_DIR=${LOG_DIR:-./data/logs/$(date -u +%Y%m%dT%H%M%S)-$$}
    mkdir -p "$LOG_DIR"
    docker compose up -d --build --wait --wait-timeout 60 printer1 printer2 $(nodes "$N")
    echo "Ambiente pronto: $N computadores, 2 impressoras. Logs: $LOG_DIR" ;;
  down) docker compose down --remove-orphans ;;
  status)
    N=$(num); N=${N:-3}
    for i in $(seq 1 "$N"); do
      echo "node$i"
      curl -fs --max-time 2 "http://localhost:$((8000+i))/state" || true
      echo
    done
    for i in 1 2; do
      echo "printer$i"
      curl -fs --max-time 2 "http://localhost:$((9000+i))/state" || true
      echo
    done ;;
  print)
    python3 scripts/client.py --node "$1" --printer "$2" --pages "${3:-1}" --document "${4:-documento.txt}" ;;
  logs) docker compose logs -f --no-color "$@" ;;
  crash) docker compose kill -s SIGKILL "$1" ;;
  pause) docker compose pause "$1" ;;
  unpause) docker compose unpause "$1" ;;
  netem)
    docker compose exec -T "node$1" tc qdisc replace dev eth0 root netem \
      delay "${2}ms" "${4:-0}ms" loss "${3:-0}%" ;;
  netem-clear) docker compose exec -T "node$1" tc qdisc del dev eth0 root ;;
  partition)
    N=$(num); N=${N:-3}
    python3 scripts/client.py --partition "$1" "$2" --count "$N" ;;
  heal)
    N=$(num); N=${N:-3}
    python3 scripts/client.py --heal --count "$N" ;;
  test) PYTHONPATH=src:. python3 -m unittest discover -s tests -v ;;
  demo) exec scripts/demo.sh ;;
  *) usage; [ "$cmd" = help ] ;;
esac
