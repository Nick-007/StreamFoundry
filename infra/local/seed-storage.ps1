Param()
$ROOT = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$envPath = Join-Path $ROOT ".env"
if (Test-Path $envPath) {
    foreach ($line in Get-Content $envPath) {
        if ($line -match "^[A-Za-z_][A-Za-z0-9_]*=.*$") {
            $name,$val = $line -split "=",2
            if (-not [string]::IsNullOrWhiteSpace($name) -and -not $val.StartsWith("#")) {
                $env:$name = $val
            }
        }
    }
}
python "$ROOT/infra/local/seed-storage.py"
