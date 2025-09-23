#!/usr/bin/env bash
set -euo pipefail
HOST="${HOST:-localhost}"
PORT="${PORT:-7071}"
curl -v -m 10 --connect-timeout 3 "http://$HOST:$PORT/api/submit" -I || true
