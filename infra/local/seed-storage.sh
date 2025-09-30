#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
if [ -f "$ROOT/.env" ]; then
  # shellcheck disable=SC1090
  source "$ROOT/.env"
fi
# seed containers (reads *_CONTAINER entries from local.settings.json)
python3 "$ROOT/infra/local/seed-containers.py"
# seed Queues (reads *Queues entries from local.settings.json)
python3 "$ROOT/infra/local/seed-storage.py"
