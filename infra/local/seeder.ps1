#!/usr/bin/env pwsh
param()

$ROOT = (Resolve-Path "$PSScriptRoot/../..").Path

# Optional .env (no-op if absent)
$envPath = Join-Path $ROOT ".env"
if (Test-Path $envPath) {
  foreach ($line in Get-Content $envPath) {
    if ($line -match '^[#\s]') { continue }
    $kv = $line -split '=', 2
    if ($kv.Length -eq 2) { $env:$($kv[0]) = $kv[1] }
  }
}

python "$ROOT/infra/local/seed-containers.py"
python "$ROOT/infra/local/seed-queues.py"
