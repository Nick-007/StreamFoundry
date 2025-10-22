import json
import azure.functions as func

# Queue name and storage connection (standard Functions binding)
from ..shared.config import get
from ..shared.normalize import normalize_only_rung
PACKAGING_QUEUE = get("PACKAGING_QUEUE", "packaging-jobs")

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

@app.function_name("KickPackage")
@app.route(route="kick-package", methods=["GET", "POST"])
@app.queue_output(
    arg_name="pkg_out",
    queue_name=PACKAGING_QUEUE,
    connection="AzureWebJobsStorage",
)
def kick_package(req: func.HttpRequest, pkg_out: func.Out[str]) -> func.HttpResponse:
    """
    Enqueue a packaging job.
    Accepts JSON body or query string:
      id (required): job/stem identifier (e.g., "bbb")
      only_rung (optional): e.g., 1080, "1080p", 4, "4"
    """
    try:
        # Parse input (prefer JSON body; fall back to query string)
        body = {}
        if req.method == "POST":
            ct = (req.headers.get("content-type") or "").lower()
            if "application/json" in ct:
                body = req.get_json(silent=True) or {}
            else:
                try:
                    body = json.loads(req.get_body() or b"{}")
                except Exception:
                    body = {}

        q = req.params
        job_id = (body.get("id") or q.get("id") or "").strip()
        raw_only_rung = body.get("only_rung") if "only_rung" in body else q.get("only_rung")

        if not job_id:
            return func.HttpResponse(
                json.dumps({"error": "missing required field 'id'"}),
                status_code=400,
                mimetype="application/json",
            )

        # Normalize only_rung: emit list[str] labels like ["720p","1080p"]
        only_rung = normalize_only_rung(raw_only_rung)

        payload = {"id": job_id}
        if only_rung is not None:
            payload["only_rung"] = only_rung
        pkg_out.set(json.dumps(payload))

        return func.HttpResponse(
            json.dumps({"ok": True, "queued": PACKAGING_QUEUE, "payload": payload}),
            status_code=202,
            mimetype="application/json",
        )

    except Exception as e:
        return func.HttpResponse(
            json.dumps({"ok": False, "error": str(e)}),
            status_code=500,
            mimetype="application/json",
        )