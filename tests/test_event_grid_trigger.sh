#!/bin/bash

# Test script for simulating Azure Event Grid blob creation events
# This allows testing the BlobEnqueuer function locally since Azurite doesn't emit Event Grid events

# Default values
FUNCTIONS_HOST="http://localhost:7071"
CONTAINER_NAME="raw"
BLOB_NAME="sample.mp4"
CONTENT_TYPE="video/mp4"
CONTENT_LENGTH=1024

# Help message
show_help() {
    echo "Usage: $0 [options]"
    echo "Options:"
    echo "  -h, --host         Functions host URL (default: $FUNCTIONS_HOST)"
    echo "  -c, --container    Container name (default: $CONTAINER_NAME)"
    echo "  -b, --blob        Blob name (default: $BLOB_NAME)"
    echo "  -t, --type        Content type (default: $CONTENT_TYPE)"
    echo "  -l, --length      Content length (default: $CONTENT_LENGTH)"
    echo "  --help            Show this help message"
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    key="$1"
    case $key in
        -h|--host)
            FUNCTIONS_HOST="$2"
            shift
            shift
            ;;
        -c|--container)
            CONTAINER_NAME="$2"
            shift
            shift
            ;;
        -b|--blob)
            BLOB_NAME="$2"
            shift
            shift
            ;;
        -t|--type)
            CONTENT_TYPE="$2"
            shift
            shift
            ;;
        -l|--length)
            CONTENT_LENGTH="$2"
            shift
            shift
            ;;
        --help)
            show_help
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            show_help
            exit 1
            ;;
    esac
done

# Generate UUIDs for required fields
EVENT_ID=$(uuidgen)
CLIENT_REQUEST_ID=$(uuidgen)
REQUEST_ID=$(uuidgen)

# Get current UTC time
EVENT_TIME=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

# Construct the blob URL using Azurite's local endpoint
BLOB_URL="http://127.0.0.1:10000/devstoreaccount1/$CONTAINER_NAME/$BLOB_NAME"

# Send the Event Grid notification
curl -X POST "$FUNCTIONS_HOST/runtime/webhooks/eventgrid?functionName=blob_enqueuer" \
  -H "Content-Type: application/json" \
  -H "aeg-event-type: Notification" \
  -d '[
  {
    "topic": "/subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/myResourceGroup/providers/Microsoft.Storage/storageAccounts/mystorageaccount",
    "subject": "/blobServices/default/containers/'"$CONTAINER_NAME"'/blobs/'"$BLOB_NAME"'",
    "eventType": "Microsoft.Storage.BlobCreated",
    "id": "'"$EVENT_ID"'",
    "data": {
      "api": "PutBlob",
      "clientRequestId": "'"$CLIENT_REQUEST_ID"'",
      "requestId": "'"$REQUEST_ID"'",
      "eTag": "0x8D687D676A182A1",
      "contentType": "'"$CONTENT_TYPE"'",
      "contentLength": '"$CONTENT_LENGTH"',
      "blobType": "BlockBlob",
      "url": "'"$BLOB_URL"'",
      "sequencer": "00000000000000000000000000000000000000000000000"
    },
    "dataVersion": "",
    "metadataVersion": "1",
    "eventTime": "'"$EVENT_TIME"'"
  }
]'

echo -e "\nEvent Grid notification sent for blob: $BLOB_NAME"