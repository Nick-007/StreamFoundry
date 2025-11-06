#!/bin/bash

# Define variables
storage_account="devstoreaccount1"
container="raw"
local_folder="$HOME/Downloads/blob_up"
azurite_url="http://127.0.0.1:10000/$storage_account/$container"
account_key="Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="  

for file in "$local_folder"/*; do
  filename=$(basename "$file")
  curl -X PUT \
    -T "$file" \
    -H "x-ms-blob-type: BlockBlob" \
    -H "Authorization: SharedKey $storage_account:$account_key" \
    -H "x-ms-version: 2020-10-02" \
    -H "x-ms-date: $(date -u '+%a, %d %b %Y %H:%M:%S GMT')" \
    "$azurite_url/$filename"
done
