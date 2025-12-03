#!/usr/bin/env bash
set -euo pipefail

# Reset local Azurite state (containers + queues). This targets the devstore
# defaults used in compose and local runs.

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

echo "[reset] stopping azurite container if running"
docker rm -f azurite >/dev/null 2>&1 || true

echo "[reset] removing local azurite data directories"
rm -rf "${ROOT}/_azurite" "${ROOT}/_azurite.old"

echo "[reset] removing azurite volumes (if present)"
docker volume rm streamfoundry_azurite-data >/dev/null 2>&1 || true
docker volume rm azurite-data >/dev/null 2>&1 || true

if [[ "${1:-}" == "--seed" ]]; then
  echo "[reset] seeding containers/queues via infra/local/seeder.sh"
  (cd "${ROOT}" && bash infra/local/seeder.sh)
else
  echo "[reset] skipped seeding; run 'bash infra/local/seeder.sh' if needed"
fi

echo "[reset] done"
