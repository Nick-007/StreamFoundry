#!/usr/bin/env python3
"""
Push Function App settings to Azure using the values from a local settings file.

Usage:
    python scripts/dev/push_appsettings.py \
        --function-app <app-name> \
        --resource-group <resource-group> \
        [--slot <slot-name>] \
        [--settings-file local.settings.json]

The script reads the "Values" block from the specified settings file (default
local.settings.json) and forwards the key/value pairs to Azure via:

    az functionapp config appsettings set --settings KEY=VALUE ...

Secrets stay on your machine; nothing is committed to source control.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path


def load_settings(path: Path) -> dict[str, str]:
    if not path.exists():
        raise FileNotFoundError(f"Settings file not found: {path}")
    try:
        raw = json.loads(path.read_text())
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON in {path}: {exc}") from exc

    values = raw.get("Values")
    if not isinstance(values, dict) or not values:
        raise ValueError(f"No 'Values' section found in {path}")
    # Azure app settings expect strings
    return {str(k): "" if v is None else str(v) for k, v in values.items()}


def push_settings(app: str, rg: str, values: dict[str, str], slot: str | None = None) -> None:
    cmd = [
        "az",
        "functionapp",
        "config",
        "appsettings",
        "set",
        "--name",
        app,
        "--resource-group",
        rg,
    ]
    if slot:
        cmd += ["--slot", slot]
    cmd += ["--settings"]
    cmd += [f"{k}={v}" for k, v in values.items()]

    try:
        subprocess.run(cmd, check=True)
    except FileNotFoundError as exc:
        raise RuntimeError("Azure CLI ('az') not found in PATH") from exc
    except subprocess.CalledProcessError as exc:
        raise RuntimeError(f"Failed to push app settings (exit code {exc.returncode})") from exc


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sync Function App settings to Azure")
    parser.add_argument("--function-app", required=True, help="Azure Function App name")
    parser.add_argument("--resource-group", required=True, help="Azure resource group")
    parser.add_argument("--slot", help="Optional deployment slot")
    parser.add_argument(
        "--settings-file",
        default="local.settings.json",
        help="Path to the local settings JSON (default: local.settings.json)",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    settings_path = Path(args.settings_file)
    values = load_settings(settings_path)
    push_settings(args.function_app, args.resource_group, values, slot=args.slot)
    print(f"Pushed {len(values)} settings from {settings_path} to {args.function_app}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
