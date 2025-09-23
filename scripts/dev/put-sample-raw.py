#!/usr/bin/env python3
import os
from azure.storage.blob import BlobServiceClient
conn = os.environ.get("AzureWebJobsStorage") or os.environ.get("AZURE_STORAGE_CONNECTION_STRING","UseDevelopmentStorage=true")
container = os.environ.get("IN_CONTAINER","inbound")
blob = "samples/hello.txt"
data = b"hello from seed"
svc = BlobServiceClient.from_connection_string(conn)
bc = svc.get_blob_client(container, blob)
bc.upload_blob(data, overwrite=True)
print(f"uploaded {blob} to {container}")
