#!/usr/bin/env bash
set -euo pipefail

ENV_FILE="${1:-}"
if [[ -z "$ENV_FILE" || ! -f "$ENV_FILE" ]]; then
  echo "Usage: $0 /path/to/payload.env" >&2
  exit 1
fi

IMAGE="${TRANSCODE_WORKER_IMAGE:-streamfoundry-transcoder:local}"
# Allow the network to be specified (needed so worker can reach azurite from compose)
NET="${TRANSCODE_DOCKER_NETWORK:-docker_default}"

echo "[worker-launch] image=$IMAGE network=${NET:-<none>} env_file=$ENV_FILE"
echo "[worker-launch] --- env payload ---"
cat "$ENV_FILE"
echo "[worker-launch] --------------------"

if [[ -n "$NET" ]]; then
  docker run --rm --network "$NET" --env-file "$ENV_FILE" "$IMAGE"
else
  docker run --rm --env-file "$ENV_FILE" "$IMAGE"
fi
