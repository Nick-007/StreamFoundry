#!/usr/bin/env python3
import os
from azure.storage.queue import QueueServiceClient

conn = os.environ.get("AzureWebJobsStorage") or os.environ.get("AZURE_STORAGE_CONNECTION_STRING","UseDevelopmentStorage=true")
base = os.environ.get("TRANSCODE_QUEUE","transcode-jobs")
poison = base + "-poison"

qs = QueueServiceClient.from_connection_string(conn)
q = qs.get_queue_client(poison)
try:
    count = 0
    while True:
        msgs = q.receive_messages(messages_per_page=32, visibility_timeout=1)
        got_any = False
        for m in msgs.by_page():
            for msg in m:
                q.delete_message(msg)
                count += 1
                got_any = True
        if not got_any:
            break
    print(f"Purged {count} messages from {poison}")
except Exception as e:
    print(f"Error purging {poison}: {e}")
