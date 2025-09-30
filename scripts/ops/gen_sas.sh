# 0) Get the same connection string your Functions use
CONN="DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;QueueEndpoint=http://127.0.0.1:10001/devstoreaccount1;TableEndpoint=http://127.0.0.1:10002/devstoreaccount1"

# 1) Make sure the queue exists (safe to re-run)
az storage queue create --name transcode-jobs --connection-string "$CONN" >/dev/null

# 2) Generate a SAS for THAT queue (read+list, valid 1h)
EXP="$(date -u -d '+1 hour' '+%Y-%m-%dT%H:%MZ')"   # on mac: EXP="$(date -u -v+1H '+%Y-%m-%dT%H:%MZ')"
QS="$(az storage queue generate-sas \
  --name transcode-jobs \
  --permissions rp \
  --expiry "$EXP" \
  --connection-string "$CONN" \
  -o tsv)"

# 3) print the SAS for curl ingest
echo "${QS}"
