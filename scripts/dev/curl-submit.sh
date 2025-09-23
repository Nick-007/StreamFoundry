#!/usr/bin/env bash
set -euo pipefail
HOST="${HOST:-localhost}"
PORT="${PORT:-7071}"
curl -v -m 20 --connect-timeout 5 -H "Content-Type: application/json" -d '{"test":"ping"}' "http://$HOST:$PORT/api/submit"
