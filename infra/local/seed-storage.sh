#!/usr/bin/env bash
# Back-compat shim — prefer infra/local/seed-queues.sh
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
exec "$ROOT/infra/local/seed-queues.sh"
