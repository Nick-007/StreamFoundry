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
from typing import Dict, Optional, Tuple, Set
from urllib.parse import urlparse

from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceNotFoundError

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from scripts.dev.storage import (  # type: ignore
    load_connection_string,
    parse_connection_string,
    set_blob_metadata,
)
from scripts.dev.embed_sas import embed_to_m3u8, embed_to_mpd, embed_thumbnails_vtt  # type: ignore
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
    args = parser.parse_args()

    settings = load_settings()
    base_url = os.getenv("BASE_URL") or settings.get("BASE_URL")
    dash_container = os.getenv("DASH_CONTAINER") or settings.get("DASH_CONTAINER", "dash")
    hls_container = os.getenv("HLS_CONTAINER") or settings.get("HLS_CONTAINER", "hls")
    dash_base_url = os.getenv("DASH_BASE_URL") or settings.get("DASH_BASE_URL")
    hls_base_url = os.getenv("HLS_BASE_URL") or settings.get("HLS_BASE_URL")

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
    thumbnails_out: Optional[Path] = None
    thumbnails_copy: Optional[Path] = None

    def extract_playlist_entries(manifest_text: str) -> Tuple[Set[str], Set[str], Set[str]]:
        direct: Set[str] = set()
        attribute: Set[str] = set()
        captions: Set[str] = set()
        attr_pattern = re.compile(r'URI="([^"]+)"')
        for line in manifest_text.splitlines():
            stripped = line.strip()
            if not stripped:
                continue
            if stripped.startswith("#"):
                for uri in attr_pattern.findall(stripped):
                    parsed = urlparse(uri)
                    name = Path(parsed.path).name
                    ext = Path(name).suffix.lower()
                    if ext == ".m3u8":
                        attribute.add(name)
                    elif ext == ".vtt":
                        captions.add(name)
                continue
            if ".m3u8" in stripped:
                parsed = urlparse(stripped)
                name = Path(parsed.path).name
                direct.add(name)
        return direct, attribute, captions

    variant_outputs: Dict[str, Path] = {}
    variant_copies: Dict[str, Path] = {}
    caption_outputs: Dict[str, Path] = {}
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
        direct_entries, attribute_entries, caption_entries = extract_playlist_entries(master_text)
        hls_out = embed_to_m3u8(hls_manifest, hls_master_prefix, hls_token)

        # Process variant playlists (video/audio) to embed SAS for segments.
        all_playlist_names = sorted(direct_entries | attribute_entries)
        for name in all_playlist_names:
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

        for name in sorted(caption_entries):
            try:
                caption_path = resolve_manifest(
                    None,
                    f"dist/hls/captions/{name}",
                    container=hls_container,
                    filename=f"captions/{name}",
                    workspace_root=workspace_root,
                )
                caption_outputs[name] = caption_path
            except SystemExit:
                continue
            except Exception:
                continue

        thumbnails_manifest: Optional[Path] = None
        try:
            thumbnails_manifest = resolve_manifest(
                None,
                "dist/hls/thumbnails/thumbnails.vtt",
                container=hls_container,
                filename="thumbnails/thumbnails.vtt",
                workspace_root=workspace_root,
            )
        except SystemExit:
            thumbnails_manifest = None
        except Exception:
            thumbnails_manifest = None

        if thumbnails_manifest and thumbnails_manifest.exists():
            thumbnail_prefix = f"{hls_prefix}/thumbnails"
            thumbnails_out = embed_thumbnails_vtt(thumbnails_manifest, thumbnail_prefix, hls_token)

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

        if caption_outputs:
            captions_dir = output_dir / "captions"
            captions_dir.mkdir(parents=True, exist_ok=True)
            for name, path in caption_outputs.items():
                copy_path = captions_dir / name
                shutil.copy2(path, copy_path)

        if thumbnails_out:
            thumb_dir = output_dir / "thumbnails"
            thumb_dir.mkdir(parents=True, exist_ok=True)
            thumbnails_copy = thumb_dir / thumbnails_out.name
            shutil.copy2(thumbnails_out, thumbnails_copy)

    dash_url = f"{dash_prefix}/stream.mpd?{dash_token}"
    hls_url = f"{hls_prefix}/master.m3u8?{hls_token}"
    trickplay_url = None
    if thumbnails_out:
        trickplay_url = f"{hls_prefix}/thumbnails/thumbnails.vtt?{hls_token}"

    print("[build] DASH manifest:", dash_out)
    print("[build] HLS manifest:", hls_out)
    print("[build] Saved DASH manifest copy:", dash_copy)
    print("[build] Saved HLS manifest copy:", hls_copy)
    print("[build] DASH URL:", dash_url)
    print("[build] HLS URL:", hls_url)
    if thumbnails_out:
        print("[build] Trick-play VTT:", thumbnails_out)
        print("[build] Trick-play URL:", trickplay_url)

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
        if trickplay_url:
            metadata["sf_trickplay_vtt"] = trickplay_url
        try:
            set_blob_metadata(args.raw_container, args.raw_key, metadata, merge=True)
            print(f"[build] Updated metadata for {args.raw_container}/{args.raw_key}")
        except Exception as exc:
            print(f"[build] WARN: failed to update raw metadata ({exc})")


if __name__ == "__main__":
    main()
