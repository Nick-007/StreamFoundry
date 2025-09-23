#!/usr/bin/env bash
# List any blobs with a 'locks/' prefix across all containers (to spot stuck leases).
# Requires Azure CLI. Works with Azurite via connection string.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
if [ -f "$ROOT/.env" ]; then
  # shellcheck disable=SC1090
  source "$ROOT/.env"
fi

CONN="${AzureWebJobsStorage:-${AZURE_STORAGE_CONNECTION_STRING:-UseDevelopmentStorage=true}}"

echo "Scanning for lock blobs (prefix 'locks/') in all containers..."
containers=$(az storage container list --connection-string "$CONN" --query "[].name" -o tsv)
found=0
for c in $containers; do
  names=$(az storage blob list --connection-string "$CONN" --container-name "$c" --prefix "locks/" --query "[].name" -o tsv || true)
  if [ -n "${names:-}" ]; then
    echo "Container: $c"
    while IFS= read -r n; do
      if [ -n "$n" ]; then
        props=$(az storage blob show --connection-string "$CONN" --container-name "$c" --name "$n" --query "{name:name, lastModified:properties.lastModified, leaseState:properties.leaseState, leaseStatus:properties.leaseStatus}" -o tsv || true)
        echo "  $props"
        found=$((found+1))
      fi
    done <<< "$names"
  fi
done

if [ "$found" -eq 0 ]; then
  echo "No lock-prefixed blobs found."
else
  echo "Total lock blobs: $found"
fi
