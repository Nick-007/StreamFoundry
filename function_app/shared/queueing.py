from azure.storage.queue import QueueClient
from .config import get
def queue_client(queue_name: str) -> QueueClient:
    conn = get("AzureWebJobsStorage")
    if not conn: raise RuntimeError("AzureWebJobsStorage not configured")
    qc = QueueClient.from_connection_string(conn_str=conn, queue_name=queue_name)
    try: qc.create_queue()
    except Exception: pass
    return qc
def enqueue(queue_name: str, message_text: str):
    qc = queue_client(queue_name); qc.send_message(message_text)
