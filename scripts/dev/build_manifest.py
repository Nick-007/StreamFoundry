#!/usr/bin/env python3
"""
End-to-end helper for preparing player-ready manifests:
  1. Generate container SAS (dash/hls) for the requested lifetime
  2. Embed the SAS into DASH MPD + HLS master (using local files if available,
     otherwise downloading them from blob storage)
  3. Optionally write the full URLs back to the raw blob metadata

Usage:
  python scripts/dev/build_manifest.py --stem STEM/ICT/... --fingerprint v_1234...
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
import shutil
import re
from typing import Dict, Optional, Tuple
from urllib.parse import urlparse

from azure.storage.blob import BlobServiceClient, ContentSettings
from azure.core.exceptions import ResourceNotFoundError

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from scripts.dev.storage import (  # type: ignore
    load_connection_string,
    parse_connection_string,
    set_blob_metadata,
)
from scripts.dev.embed_sas import embed_to_m3u8, embed_to_mpd  # type: ignore
from azure.storage.blob import generate_container_sas, ContainerSasPermissions


def load_settings() -> dict:
    path = ROOT / "local.settings.json"
    if path.exists():
        try:
            data = json.loads(path.read_text())
            return data.get("Values", {}) or {}
        except Exception:
            return {}
    return {}

def generate_container_sas_token(
    svc: BlobServiceClient,
    account_name: str,
    account_key: str,
    container: str,
    minutes: int,
    *,
    allow_http: bool = True,
) -> str:
    expiry = datetime.utcnow() + timedelta(minutes=minutes)
    perms = ContainerSasPermissions(read=True, list=True)
    protocol = "https,http" if allow_http else None
    return generate_container_sas(
        account_name=account_name,
        container_name=container,
        account_key=account_key,
        permission=perms,
        expiry=expiry,
        protocol=protocol,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate signed manifests for DASH/HLS")
    parser.add_argument("--fingerprint", required=True, help="Fingerprint version (v_<hash>)")
    parser.add_argument("--stem", required=True, help="Stem path (without trailing slash)")
    parser.add_argument("--minutes", type=int, default=120, help="Lifetime for SAS tokens")
    parser.add_argument("--dash-manifest", help="Local path to DASH MPD (defaults to dist/dash/stream.mpd)")
    parser.add_argument("--hls-manifest", help="Local path to HLS master playlist (defaults to dist/hls/master.m3u8)")
    parser.add_argument("--raw-container", help="Raw container name to update metadata")
    parser.add_argument("--raw-key", help="Raw blob key to update metadata")
    parser.add_argument("--output-dir", help="Directory where .sas manifests should be written (defaults to dist/signed/...)")
    parser.add_argument("--print-manifest", choices=("dash", "hls", "both"),
                        help="Write the signed manifest(s) to stdout instead of just saving to disk")
    parser.add_argument("--upload-signed", action="store_true",
                        help="Upload the signed manifests to the scratch container and print their HTTP URLs")
    args = parser.parse_args()

    settings = load_settings()
    base_url = os.getenv("BASE_URL") or settings.get("BASE_URL")
    dash_container = os.getenv("DASH_CONTAINER") or settings.get("DASH_CONTAINER", "dash")
    hls_container = os.getenv("HLS_CONTAINER") or settings.get("HLS_CONTAINER", "hls")
    dash_base_url = os.getenv("DASH_BASE_URL") or settings.get("DASH_BASE_URL")
    hls_base_url = os.getenv("HLS_BASE_URL") or settings.get("HLS_BASE_URL")
    signed_container = os.getenv("SIGNED_CONTAINER") or settings.get("SIGNED_CONTAINER", "signed-manifests")
    signed_base_url = os.getenv("SIGNED_BASE_URL") or settings.get("SIGNED_BASE_URL")

    conn = load_connection_string()
    svc = BlobServiceClient.from_connection_string(conn)
    account, key, _ = parse_connection_string(conn)

    allows_http = any(
        url and url.lower().startswith("http://")
        for url in (dash_base_url, hls_base_url, base_url)
    )

    dash_token = generate_container_sas_token(
        svc, account, key, dash_container, args.minutes, allow_http=allows_http
    )
    hls_token = generate_container_sas_token(
        svc, account, key, hls_container, args.minutes, allow_http=allows_http
    )

    stem_path = args.stem.strip("/")
    if dash_base_url:
        dash_prefix = f"{dash_base_url.rstrip('/')}/{args.fingerprint}/{stem_path}"
    elif base_url:
        dash_prefix = f"{base_url.rstrip('/')}/{dash_container}/{args.fingerprint}/{stem_path}"
    else:
        raise SystemExit("BASE_URL or DASH_BASE_URL must be configured")

    if hls_base_url:
        hls_prefix = f"{hls_base_url.rstrip('/')}/{args.fingerprint}/{stem_path}"
    elif base_url:
        hls_prefix = f"{base_url.rstrip('/')}/{hls_container}/{args.fingerprint}/{stem_path}"
    else:
        raise SystemExit("BASE_URL or HLS_BASE_URL must be configured")

    if args.upload_signed:
        if signed_base_url:
            hls_master_prefix = f"{signed_base_url.rstrip('/')}/{args.fingerprint}/{stem_path}"
        elif base_url:
            hls_master_prefix = f"{base_url.rstrip('/')}/{signed_container}/{args.fingerprint}/{stem_path}"
        else:
            raise SystemExit("BASE_URL or SIGNED_BASE_URL must be configured for upload")
    else:
        hls_master_prefix = hls_prefix

    def resolve_manifest(
        requested_path: Optional[str],
        default_rel: str,
        *,
        container: str,
        filename: str,
        workspace_root: Optional[Path] = None,
    ) -> Path:
        candidates = []
        if requested_path:
            candidates.append(Path(requested_path))
        default_path = Path(default_rel)
        candidates.append(default_path)
        if workspace_root:
            candidates.append(workspace_root / default_path)
        for candidate in candidates:
            if candidate.exists():
                return candidate

        blob_path = "/".join(
            part for part in (args.fingerprint.strip("/"), stem_path, filename) if part
        )
        download_target = tmp_dir / filename
        download_target.parent.mkdir(parents=True, exist_ok=True)
        try:
            blob_client = svc.get_blob_client(container=container, blob=blob_path)
            data = blob_client.download_blob(timeout=300).readall()
        except ResourceNotFoundError as exc:
            raise SystemExit(f"Remote manifest missing: {container}/{blob_path}") from exc
        download_target.write_bytes(data)
        print(f"[build] downloaded {container}/{blob_path} -> {download_target}")
        return download_target

    dash_copy: Optional[Path] = None
    hls_copy: Optional[Path] = None

    def extract_playlist_entries(manifest_text: str) -> Tuple[Dict[str, str], Dict[str, str]]:
        direct: Dict[str, str] = {}
        attribute: Dict[str, str] = {}
        for line in manifest_text.splitlines():
            stripped = line.strip()
            if not stripped:
                continue
            if stripped.startswith("#"):
                match = re.search(r'URI="([^"]+\.m3u8)"', stripped)
                if match:
                    uri = match.group(1)
                    parsed = urlparse(uri)
                    name = Path(parsed.path).name
                    attribute[uri] = name
                continue
            if ".m3u8" in stripped:
                parsed = urlparse(stripped)
                name = Path(parsed.path).name
                direct[stripped] = name
        return direct, attribute

    variant_outputs: Dict[str, Path] = {}
    variant_copies: Dict[str, Path] = {}
    scratch_master_bytes: Optional[bytes] = None

    with tempfile.TemporaryDirectory(prefix="sf-manifest-") as tmp_dir_str:
        tmp_dir = Path(tmp_dir_str)
        workspace_root = None
        tmp_root = settings.get("TMP_DIR")
        if tmp_root:
            workspace_root = Path(tmp_root) / stem_path

        dash_manifest = resolve_manifest(
            args.dash_manifest,
            "dist/dash/stream.mpd",
            container=dash_container,
            filename="stream.mpd",
            workspace_root=workspace_root,
        )
        hls_manifest = resolve_manifest(
            args.hls_manifest,
            "dist/hls/master.m3u8",
            container=hls_container,
            filename="master.m3u8",
            workspace_root=workspace_root,
        )

        dash_out = embed_to_mpd(dash_manifest, dash_prefix, dash_token)
        master_text = hls_manifest.read_text(encoding="utf-8")
        direct_entries, attribute_entries = extract_playlist_entries(master_text)
        hls_out = embed_to_m3u8(hls_manifest, hls_master_prefix, hls_token)

        if args.upload_signed:
            if not signed_base_url and not base_url:
                raise SystemExit("SIGNED_BASE_URL or BASE_URL must be configured to publish signed variants")
            signed_variant_prefix = (
                f"{signed_base_url.rstrip('/')}/{args.fingerprint}/{stem_path}"
                if signed_base_url
                else f"{base_url.rstrip('/')}/{signed_container}/{args.fingerprint}/{stem_path}"
            )
            master_signed_text = hls_out.read_text(encoding="utf-8")
            uri_pattern = re.compile(r'URI="([^"]+)"')
            scratch_lines = []
            for line in master_signed_text.splitlines():
                stripped = line.strip()
                if stripped in direct_entries:
                    scratch_lines.append(f"{signed_variant_prefix}/{direct_entries[stripped]}")
                    continue

                def _rewrite(match: re.Match[str]) -> str:
                    original = match.group(1)
                    name = attribute_entries.get(original)
                    if not name:
                        return match.group(0)
                    return f'URI="{signed_variant_prefix}/{name}"'

                updated = uri_pattern.sub(_rewrite, line)
                scratch_lines.append(updated)
            scratch_master_bytes = ("\n".join(scratch_lines) + "\n").encode("utf-8")

        # Process variant playlists (video/audio) to embed SAS for segments.
        all_playlist_names = {name for name in direct_entries.values()} | {name for name in attribute_entries.values()}
        for name in sorted(all_playlist_names):
            variant_blob = "/".join(
                part for part in (args.fingerprint.strip("/"), stem_path, name) if part
            )
            variant_path = resolve_manifest(
                None,
                f"dist/hls/{name}",
                container=hls_container,
                filename=name,
                workspace_root=workspace_root,
            )
            variant_out = embed_to_m3u8(variant_path, hls_prefix, hls_token)
            variant_outputs[name] = variant_out

        if args.output_dir:
            output_dir = Path(args.output_dir).expanduser()
        else:
            output_dir = Path("dist") / "signed" / args.fingerprint.strip("/")
            if stem_path:
                output_dir = output_dir / Path(stem_path)
        output_dir.mkdir(parents=True, exist_ok=True)

        dash_copy = output_dir / dash_out.name
        hls_copy = output_dir / hls_out.name
        shutil.copy2(dash_out, dash_copy)
        shutil.copy2(hls_out, hls_copy)

        for name, path in variant_outputs.items():
            copy_path = output_dir / Path(name).with_suffix(Path(name).suffix + ".sas").name
            shutil.copy2(path, copy_path)
            variant_copies[name] = copy_path

    dash_url = f"{dash_prefix}/stream.mpd?{dash_token}"
    hls_url = f"{hls_prefix}/master.m3u8?{hls_token}"

    print("[build] DASH manifest:", dash_out)
    print("[build] HLS manifest:", hls_out)
    print("[build] Saved DASH manifest copy:", dash_copy)
    print("[build] Saved HLS manifest copy:", hls_copy)
    print("[build] DASH URL:", dash_url)
    print("[build] HLS URL:", hls_url)

    scratch_urls = {}
    if args.upload_signed:
        if not signed_container:
            raise SystemExit("SIGNED_CONTAINER must be configured to upload signed manifests")
        container_client = svc.get_container_client(signed_container)

        def _upload(copy: Optional[Path], *, content_type: str, label: str, target_name: str, data: Optional[bytes] = None) -> None:
            if copy is None and data is None:
                return
            if copy is not None and not copy.exists():
                if data is None:
                    return
            blob_path = "/".join(
                part for part in (args.fingerprint.strip("/"), stem_path, target_name) if part
            )
            blob_client = container_client.get_blob_client(blob_path)
            try:
                blob_client.upload_blob(
                    data if data is not None else copy.read_bytes(),
                    overwrite=True,
                    content_settings=ContentSettings(content_type=content_type),
                    timeout=300,
                )
            except Exception as exc:
                print(f"[build] WARN: failed to upload {label} manifest ({exc})")
                return
            if signed_base_url:
                url = f"{signed_base_url.rstrip('/')}/{blob_path}"
            elif base_url:
                url = f"{base_url.rstrip('/')}/{signed_container}/{blob_path}"
            else:
                url = None
            scratch_urls[label] = url or blob_client.url
            print(f"[build] uploaded {label} manifest to {blob_client.url}")

        _upload(
            dash_copy,
            content_type="application/dash+xml",
            label="dash",
            target_name="stream.mpd",
        )
        _upload(
            hls_copy if scratch_master_bytes is None else None,
            content_type="application/vnd.apple.mpegurl",
            label="hls",
            target_name="master.m3u8",
            data=scratch_master_bytes,
        )
        for name, copy_path in variant_copies.items():
            _upload(
                copy_path,
                content_type="application/vnd.apple.mpegurl",
                label=name,
                target_name=name,
            )

    if scratch_urls:
        if "dash" in scratch_urls:
            print("[build] Scratch DASH URL:", scratch_urls["dash"])
        if "hls" in scratch_urls:
            print("[build] Scratch HLS URL:", scratch_urls["hls"])
        for key, value in scratch_urls.items():
            if key not in {"dash", "hls"}:
                print(f"[build] Scratch {key} URL:", value)

    if args.print_manifest in ("dash", "both") and dash_copy:
        print("\n[build] DASH manifest contents:\n")
        sys.stdout.write(dash_copy.read_text(encoding="utf-8"))
        if not sys.stdout.isatty():
            sys.stdout.write("\n")
    if args.print_manifest in ("hls", "both") and hls_copy:
        print("\n[build] HLS manifest contents:\n")
        sys.stdout.write(hls_copy.read_text(encoding="utf-8"))
        if not sys.stdout.isatty():
            sys.stdout.write("\n")

    if args.raw_container and args.raw_key:
        metadata = {
            "sf_dash_url": dash_url,
            "sf_hls_url": hls_url,
        }
        try:
            set_blob_metadata(args.raw_container, args.raw_key, metadata, merge=True)
            print(f"[build] Updated metadata for {args.raw_container}/{args.raw_key}")
        except Exception as exc:
            print(f"[build] WARN: failed to update raw metadata ({exc})")


if __name__ == "__main__":
    main()
