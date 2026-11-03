#!/usr/bin/env bash
# Cenário de referência: normal -> crash -> recuperação -> atraso/perda -> partição -> heal
set -euo pipefail
cd "$(dirname "$0")/.."
S=scripts/sim.sh
step() { echo; echo "=== $* ==="; }

$S up 5 heartbeat 42
step "1. Cenário normal (5s)";                                                    sleep 5; $S status
step "2. Falha no node3";                             $S crash 3;                 sleep 5; $S status
step "3. Recuperação do node3";                       $S recover 3;               sleep 4; $S status
step "4. Rede degradada no node1 +200ms e 10% perda"; $S netem 1 200 10 20;       sleep 5; $S status
                                                      $S netem-clear 1
step "5. Partição {1,2} | {3,4,5}";                   $S partition "1,2" "3,4,5"; sleep 5; $S status
step "6. Heal";                                       $S heal;                    sleep 5; $S status
step "Logs em data/logs/"
echo; echo "Para encerrar: scripts/sim.sh down"
