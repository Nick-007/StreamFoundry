#!/usr/bin/env bash
# Back-compat shim — invokes the composite seeder
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
exec "$ROOT/infra/local/seeder.sh"
