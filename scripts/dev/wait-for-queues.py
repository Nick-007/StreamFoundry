#!/usr/bin/env python3
import json
import os
import socket
import sys
import time
from urllib.parse import urlparse

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOCAL = os.path.join(ROOT, 'local.settings.json')

conn = None
if os.path.exists(LOCAL):
    try:
        data = json.loads(open(LOCAL).read())
        conn = data.get('Values', {}).get('AzureWebJobsStorage')
    except Exception:
        conn = None
if not conn:
    conn = os.environ.get('AzureWebJobsStorage')
if not conn:
    print('AzureWebJobsStorage not provided', file=sys.stderr)
    sys.exit(1)
queue_endpoint = None
for part in conn.split(';'):
    if part.lower().startswith('queueendpoint='):
        queue_endpoint = part.split('=', 1)[1]
        break
queue_endpoint = queue_endpoint or 'http://127.0.0.1:10001/devstoreaccount1'
parsed = urlparse(queue_endpoint)
host = parsed.hostname or '127.0.0.1'
port = parsed.port or (443 if parsed.scheme == 'https' else 80)
if host == '0.0.0.0':
    host = '127.0.0.1'
slots = 60
for attempt in range(slots):
    try:
        with socket.create_connection((host, port), timeout=1):
            print(f'queue endpoint {host}:{port} reachable')
            sys.exit(0)
    except OSError:
        time.sleep(1)
print('queue endpoint not reachable after waiting', file=sys.stderr)
sys.exit(1)
