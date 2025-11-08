#!/usr/bin/env python3
"""
Push Function App settings to Azure using the authoritative template +
environment overrides + Key Vault references.

Usage:
    python scripts/dev/push_appsettings.py \
        --function-app <app-name> \
        --resource-group <resource-group> \
        --key-vault <vault-name> \
        [--slot <slot-name>] \
        [--template-file config/appsettings.template.json] \
        [--overrides-file config/appsettings.dev.json]

The template defines every required key along with whether the value should come
from Key Vault or a literal default. The overrides file (optional) provides
environment-specific values for non-secret keys. Secrets should already live in
Key Vault; the script emits `@Microsoft.KeyVault(...)` references so the
Function App resolves them at runtime.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict


def _ensure_str_map(data: dict[str, Any]) -> dict[str, str]:
    return {str(k): "" if v is None else str(v) for k, v in data.items()}


def load_template(path: Path) -> dict[str, dict[str, Any]]:
    if not path.exists():
        raise FileNotFoundError(f"Template file not found: {path}")
    try:
        raw = json.loads(path.read_text())
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON in {path}: {exc}") from exc

    template = raw.get("appSettings") if isinstance(raw, dict) else None
    if not isinstance(template, dict) or not template:
        raise ValueError(f"No 'appSettings' section found in {path}")

    normalized: dict[str, dict[str, Any]] = {}
    for key, meta in template.items():
        if meta is None:
            normalized[key] = {}
        elif isinstance(meta, str):
            normalized[key] = {"default": meta}
        elif isinstance(meta, dict):
            normalized[key] = meta
        else:
            raise ValueError(f"Unsupported template entry type for key '{key}'")
    return normalized


def load_overrides(path: Path | None) -> dict[str, str]:
    if path is None:
        return {}
    if not path.exists():
        raise FileNotFoundError(f"Overrides file not found: {path}")
    try:
        raw = json.loads(path.read_text())
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON in {path}: {exc}") from exc

    values: Dict[str, Any]
    if isinstance(raw, dict) and "Values" in raw and isinstance(raw["Values"], dict):
        values = raw["Values"]
    else:
        values = raw
    if not isinstance(values, dict):
        raise ValueError(f"Overrides file must be a JSON object: {path}")
    return _ensure_str_map(values)


def build_app_settings(
    template: dict[str, dict[str, Any]],
    overrides: dict[str, str],
    key_vault_name: str | None,
) -> dict[str, str]:
    final: dict[str, str] = {}
    missing_keys: list[str] = []

    for key, meta in template.items():
        entry_type = str(meta.get("type", "static")).lower()

        if entry_type == "keyvault":
            secret_name = meta.get("secretName") or key.lower().replace("_", "-")
            if key_vault_name:
                final[key] = f"@Microsoft.KeyVault(VaultName={key_vault_name}, SecretName={secret_name})"
            elif key in overrides:
                final[key] = overrides[key]
            else:
                missing_keys.append(key)
        else:
            if key in overrides:
                final[key] = overrides[key]
            elif "default" in meta:
                final[key] = str(meta["default"])
            else:
                missing_keys.append(key)

    if missing_keys:
        raise ValueError(
            "Missing values for required settings: "
            + ", ".join(missing_keys)
        )

    extra_overrides = {k: v for k, v in overrides.items() if k not in final}
    if extra_overrides:
        print(
            f"[push_appsettings] WARNING: overrides provided for keys not defined in template: "
            f"{', '.join(extra_overrides.keys())}",
            file=sys.stderr,
        )
        final.update(extra_overrides)

    return final


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
    parser.add_argument("--template-file", default="config/appsettings.template.json", help="Template JSON")
    parser.add_argument("--overrides-file", help="Optional overrides JSON (e.g., config/appsettings.dev.json)")
    parser.add_argument("--key-vault", help="Key Vault name for secret-backed settings")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    template = load_template(Path(args.template_file))
    overrides = load_overrides(Path(args.overrides_file)) if args.overrides_file else {}
    values = build_app_settings(template, overrides, key_vault_name=args.key_vault)
    push_settings(args.function_app, args.resource_group, values, slot=args.slot)
    print(f"Pushed {len(values)} settings from template {args.template_file} to {args.function_app}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
