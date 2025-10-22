#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
# optional .env at repo root (for local overrides; not required)
if [ -f "$ROOT/.env" ]; then
  # shellcheck disable=SC1090
  source "$ROOT/.env"
fi

python3 "$ROOT/infra/local/seed-containers.py"
python3 "$ROOT/infra/local/seed-queues.py"
