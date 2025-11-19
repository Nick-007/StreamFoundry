#!/usr/bin/env python3
"""
Tiny CORS proxy for local Azurite testing.

Forwards requests to the configured target (default: Azurite blob endpoint)
and injects Access-Control-Allow-* headers so browsers on other origins can
fetch manifests/segments without hitting Azurite's CORS quirks.
"""

from __future__ import annotations

import argparse
import http.server
import urllib.parse
import urllib.request
import sys


class Proxy(http.server.BaseHTTPRequestHandler):
    target: str = ""

    def _set_cors(self):
        # Reflect the incoming Origin if present; otherwise allow all for local dev
        origin = self.headers.get("Origin") or "*"
        self.send_header("Access-Control-Allow-Origin", origin)
        self.send_header("Vary", "Origin")
        self.send_header("Access-Control-Allow-Methods", "GET,HEAD,OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "*")
        self.send_header("Access-Control-Max-Age", "3600")

    def do_OPTIONS(self):  # noqa: N802
        self.send_response(200)
        self._set_cors()
        self.end_headers()

    def do_HEAD(self):  # noqa: N802
        try:
            upstream = urllib.parse.urljoin(self.target, self.path.lstrip("/"))
            req = urllib.request.Request(upstream, headers={"User-Agent": "cors-proxy"})
            req.get_method = lambda: "HEAD"
            with urllib.request.urlopen(req) as resp:  # nosec: B310
                self.send_response(resp.status)
                for hk, hv in resp.getheaders():
                    if hk.lower() in ("connection", "keep-alive", "proxy-authenticate", "proxy-authorization", "te", "trailers", "transfer-encoding", "upgrade"):
                        continue
                    self.send_header(hk, hv)
                self._set_cors()
                self.end_headers()
        except Exception as exc:  # noqa: BLE001
            self.send_response(502)
            self._set_cors()
            self.end_headers()
            msg = f"Error proxying HEAD {self.path}: {exc}".encode()
            self.wfile.write(msg)

    def do_GET(self):  # noqa: N802
        try:
            upstream = urllib.parse.urljoin(self.target, self.path.lstrip("/"))
            req = urllib.request.Request(upstream, headers={"User-Agent": "cors-proxy"})
            with urllib.request.urlopen(req) as resp:  # nosec: B310
                data = resp.read()
                self.send_response(resp.status)
                for hk, hv in resp.getheaders():
                    # skip hop-by-hop headers
                    if hk.lower() in ("connection", "keep-alive", "proxy-authenticate", "proxy-authorization", "te", "trailers", "transfer-encoding", "upgrade"):
                        continue
                    self.send_header(hk, hv)
                self._set_cors()
                self.end_headers()
                self.wfile.write(data)
        except Exception as exc:  # noqa: BLE001
            self.send_response(502)
            self._set_cors()
            self.end_headers()
            msg = f"Error proxying to {self.path}: {exc}".encode()
            self.wfile.write(msg)


def run(port: int, target: str):
    Proxy.target = target.rstrip("/") + "/"
    server = http.server.ThreadingHTTPServer(("", port), Proxy)
    print(f"[cors-proxy] listening on http://127.0.0.1:{port} -> {Proxy.target}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


def main():
    parser = argparse.ArgumentParser(description="Simple CORS proxy for Azurite")
    parser.add_argument("--port", type=int, default=8080, help="Local port to listen on (default: 8080)")
    parser.add_argument(
        "--target",
        default="http://127.0.0.1:10000/devstoreaccount1/",
        help="Upstream Azurite blob endpoint to proxy to",
    )
    args = parser.parse_args()
    run(args.port, args.target)


if __name__ == "__main__":
    main()
