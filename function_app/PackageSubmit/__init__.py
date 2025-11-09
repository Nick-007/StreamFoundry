# function_app/PackageSubmit/__init__.py
from __future__ import annotations
import json
import logging
import azure.functions as func
from pydantic import ValidationError

from .. import app
from ..shared.ingest import validate_and_normalize, dump_normalized, pydantic_errors_json
from ..packager import _handle_packaging
from ..shared.config import AppSettings

settings = AppSettings()
LOGGER = logging.getLogger("package.submit")
PACKAGING_QUEUE = settings.PACKAGING_QUEUE


def handle_package_submit(req: func.HttpRequest, outmsg: func.Out[str]) -> func.HttpResponse:
    # toggle via query ?mode=sync or header x-run-sync: 1
    mode = (req.params.get("mode") or "").lower()
    sync = mode == "sync" or req.headers.get("x-run-sync") in ("1", "true", "yes")

    try:
        body = req.get_json()
    except ValueError:
        return func.HttpResponse(json.dumps({"error": "Invalid JSON body"}), status_code=400, mimetype="application/json")

    try:
        payload = validate_and_normalize(body)
    except ValidationError as ve:
        return func.HttpResponse(pydantic_errors_json(ve), status_code=422, mimetype="application/json")

    if sync:
        job_id = payload.id or "<unknown>"
        LOGGER.info("Packaging (sync via submit) start: id=%s", job_id)
        try:
            _handle_packaging(payload=payload, log=LOGGER.info)
            return func.HttpResponse(
                json.dumps({"status": "ok", "job": job_id}),
                status_code=200,
                mimetype="application/json",
            )
        except Exception as e:
            LOGGER.exception("Packaging (sync via submit) failed: id=%s", job_id)
            return func.HttpResponse(
                json.dumps({"status": "error", "job": job_id, "detail": str(e)}),
                status_code=500,
                mimetype="application/json",
            )

    # default: enqueue
    normalized = dump_normalized(payload)
    outmsg.set(json.dumps(normalized))
    LOGGER.info("Packaging job queued: id=%s", normalized.get("id"))
    return func.HttpResponse(
        json.dumps({"status": "queued", "job": normalized.get("id")}),
        status_code=202,
        mimetype="application/json",
    )


package_submit = app.route(route="package/submit", methods=["POST"])(
    app.queue_output(
        arg_name="outmsg",
        queue_name=PACKAGING_QUEUE,
        connection="AzureWebJobsStorage",
    )(handle_package_submit)
)
