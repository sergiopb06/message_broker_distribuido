#!/usr/bin/env bash
# sync_to_kabre.sh: copies this repository to Kabré (same as: kabre.sh sync).
#   ./scripts/kabre/sync_to_kabre.sh [ssh_destination] [remote_dir]
# Defaults: the "kabre" entry of ~/.ssh/config and ~/message_broker_distribuido.
set -euo pipefail
[ $# -ge 1 ] && export KABRE_HOST="$1"
[ $# -ge 2 ] && export KABRE_DIR="$2"
exec "$(dirname "${BASH_SOURCE[0]}")/kabre.sh" sync
