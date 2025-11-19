#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ENV_FILE="$ROOT/.func.env"

python3 <<PY > "$ENV_FILE"
import json, shlex
from pathlib import Path

root = Path("$ROOT")
local = root / "local.settings.json"
if not local.exists():
    raise SystemExit("local.settings.json not found")
values = json.loads(local.read_text()).get("Values", {})
for key, value in values.items():
    if value is None:
        continue
    print(f"{key}={shlex.quote(str(value))}")
PY

set -a
. "$ENV_FILE"
set +a

trap 'rm -f "$ENV_FILE"' EXIT

python3 "$ROOT/scripts/dev/wait-for-queues.py"

func start --verbose
