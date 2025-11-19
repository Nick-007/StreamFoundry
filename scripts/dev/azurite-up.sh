#!/usr/bin/env bash
set -euo pipefail

# Starts Azurite with a persisted /data mount, waits for the queue endpoint,
# and then runs the standard infra/local/seeder.sh to recreate containers/queues.

if ! command -v docker >/dev/null 2>&1; then
  echo "[azurite-up] ERROR: docker is not available on PATH" >&2
  exit 1
fi

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
DATA_DIR="${AZURITE_DATA_DIR:-$ROOT/_azurite}"
CONTAINER_NAME="${AZURITE_CONTAINER_NAME:-streamfoundry-azurite}"
IMAGE="${AZURITE_IMAGE:-mcr.microsoft.com/azure-storage/azurite}"
BLOB_PORT="${AZURITE_BLOB_PORT:-10000}"
QUEUE_PORT="${AZURITE_QUEUE_PORT:-10001}"
TABLE_PORT="${AZURITE_TABLE_PORT:-10002}"
SEEDER="$ROOT/infra/local/seeder.sh"

if [[ ! -f "$SEEDER" ]]; then
  echo "[azurite-up] ERROR: seeder script not found at $SEEDER" >&2
  exit 2
fi

mkdir -p "$DATA_DIR"

timestamp() { date -u +"%Y-%m-%dT%H:%M:%SZ"; }
log() { echo "[azurite-up $(timestamp)] $*"; }

log "Ensuring container '$CONTAINER_NAME' is not already running..."
docker rm -f "$CONTAINER_NAME" >/dev/null 2>&1 || true

log "Starting Azurite container '$CONTAINER_NAME'..."
docker run -d \
  --name "$CONTAINER_NAME" \
  -p "${BLOB_PORT}:10000" \
  -p "${QUEUE_PORT}:10001" \
  -p "${TABLE_PORT}:10002" \
  -v "$DATA_DIR:/data" \
  "$IMAGE" \
  /bin/sh -c "azurite --blobHost 0.0.0.0 --queueHost 0.0.0.0 --tableHost 0.0.0.0 --location /data --debug /data/debug.log" \
  >/dev/null

# Determine the queue endpoint host:port that the Functions app will hit.
QUEUE_ENDPOINT="$(
  python3 - <<'PY'
import json, os, sys
from pathlib import Path
conn = os.environ.get("AzureWebJobsStorage")
if not conn:
    try:
        data = json.loads(Path("local.settings.json").read_text())
        conn = data.get("Values", {}).get("AzureWebJobsStorage")
    except Exception:
        conn = None
if not conn:
    print("http://127.0.0.1:10001/devstoreaccount1", end="")
    sys.exit(0)
queue_endpoint = None
for part in conn.split(";"):
    if part.lower().startswith("queueendpoint="):
        queue_endpoint = part.split("=", 1)[1]
        break
print((queue_endpoint or "http://127.0.0.1:10001/devstoreaccount1").rstrip("/"), end="")
PY
)"

QUEUE_HOST="$(QUEUE_ENDPOINT="$QUEUE_ENDPOINT" python3 - <<'PY'
from urllib.parse import urlparse
import os
url = urlparse(os.environ["QUEUE_ENDPOINT"])
host = url.hostname or "127.0.0.1"
if host == "0.0.0.0":
    host = "127.0.0.1"
print(host, end="")
PY
)"

QUEUE_PORT_REAL="$(QUEUE_ENDPOINT="$QUEUE_ENDPOINT" python3 - <<'PY'
from urllib.parse import urlparse
import os
url = urlparse(os.environ["QUEUE_ENDPOINT"])
if url.port:
    print(url.port, end="")
else:
    print(443 if url.scheme == "https" else 80, end="")
PY
)"

log "Waiting for queue endpoint ${QUEUE_HOST}:${QUEUE_PORT_REAL}..."
for attempt in {1..60}; do
  if QUEUE_HOST="$QUEUE_HOST" QUEUE_PORT_REAL="$QUEUE_PORT_REAL" python3 - <<'PY'
import os, socket, sys
host = os.environ["QUEUE_HOST"]
port = int(os.environ["QUEUE_PORT_REAL"])
s = socket.socket()
s.settimeout(1.0)
try:
    s.connect((host, port))
except OSError:
    sys.exit(1)
finally:
    try:
        s.close()
    except Exception:
        pass
PY
  then
    log "Queue endpoint is reachable."
    break
  fi
  sleep 1
  if [[ $attempt -eq 60 ]]; then
    log "ERROR: queue endpoint did not respond within 60s" >&2
    exit 3
  fi
done

log "Running seeder..."
bash "$SEEDER"

log "Seeder finished. Azurite logs: $DATA_DIR/debug.log"
