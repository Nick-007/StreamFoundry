#!/usr/bin/env pwsh
# Back-compat shim — prefer infra/local/seed-queues.ps1
$ROOT = (Resolve-Path "$PSScriptRoot/../..").Path
& "$ROOT/infra/local/seed-queues.ps1"
