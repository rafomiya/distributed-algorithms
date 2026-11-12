#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
scripts/sim.sh up 3 42
python3 scripts/scenarios.py --backend existing --output data/demo
