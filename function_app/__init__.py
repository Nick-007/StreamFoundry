import azure.functions as func

app = func.FunctionApp()

from . import BlobIngestor, QueueIngestor, SubmitJob