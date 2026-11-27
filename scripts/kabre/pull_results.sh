#!/usr/bin/env bash
# pull_results.sh: downloads bench/results/*.csv and the Slurm job logs from Kabré
# (same as: kabre.sh pull).
#   ./scripts/kabre/pull_results.sh [ssh_destination] [remote_dir]
set -euo pipefail
[ $# -ge 1 ] && export KABRE_HOST="$1"
[ $# -ge 2 ] && export KABRE_DIR="$2"
exec "$(dirname "${BASH_SOURCE[0]}")/kabre.sh" pull
