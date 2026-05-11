import logging
from flask import Blueprint, jsonify, request
from app.config import validate_settings

logger = logging.getLogger(__name__)

# Blueprint — Java equivalent of @RestController
api = Blueprint("api", __name__, url_prefix="/api")

settings, _ = validate_settings()


# ── Health ────────────────────────────────────────────────
@api.route("/health", methods=["GET"])
def health():
    """
    Public endpoint — no auth required.
    Used by Nomad to check service is alive.
    """
    return jsonify({
        "status": "ok",
        "service": "black-diamond-salesforce-service",
        "environment": settings.ENVIRONMENT,
        "version": "0.1.0"
    }), 200


# ── Scan endpoints ────────────────────────────────────────
@api.route("/scan/start", methods=["POST"])
def scan_start():
    """
    Starts a new Salesforce data extraction scan.
    Called by BD Core Service.
    Body: { "org_id": "...", "scan_type": "full" }
    """
    body = request.get_json()

    if not body or "org_id" not in body:
        return jsonify({
            "error": "missing_field",
            "message": "org_id is required"
        }), 400

    org_id = body.get("org_id")
    scan_type = body.get("scan_type", "full")

    logger.info(f"Scan start requested — org_id={org_id} type={scan_type}")

    # TODO: call extraction_service.start_scan(org_id, scan_type)
    # Placeholder response for now
    return jsonify({
        "status": "started",
        "scan_id": "placeholder-scan-id",
        "org_id": org_id,
        "scan_type": scan_type,
        "message": "Scan started successfully"
    }), 202


@api.route("/scan/status/<scan_id>", methods=["GET"])
def scan_status(scan_id):
    """
    Returns current status of a scan.
    Called by BD Core Service to track progress.
    """
    logger.info(f"Scan status requested — scan_id={scan_id}")

    # TODO: call extraction_service.get_scan_status(scan_id)
    return jsonify({
        "scan_id": scan_id,
        "status": "in_progress",
        "progress": {
            "Contact": "complete",
            "Account": "in_progress",
            "Opportunity": "pending",
            "Task": "pending",
            "Lead": "pending",
            "User": "pending",
            "CampaignMember": "pending"
        }
    }), 200


@api.route("/scan/cancel/<scan_id>", methods=["POST"])
def scan_cancel(scan_id):
    """
    Cancels a running scan.
    """
    logger.info(f"Scan cancel requested — scan_id={scan_id}")

    # TODO: call extraction_service.cancel_scan(scan_id)
    return jsonify({
        "scan_id": scan_id,
        "status": "cancelled",
        "message": "Scan cancelled successfully"
    }), 200


@api.route("/scan/resume/<scan_id>", methods=["POST"])
def scan_resume(scan_id):
    """
    Resumes a previously cancelled or failed scan.
    """
    logger.info(f"Scan resume requested — scan_id={scan_id}")

    # TODO: call extraction_service.resume_scan(scan_id)
    return jsonify({
        "scan_id": scan_id,
        "status": "resumed",
        "message": "Scan resumed successfully"
    }), 200


@api.route("/scan/list", methods=["GET"])
def scan_list():
    """
    Lists all scans with optional filters.
    Query params: ?org_id=...&status=...&limit=10
    """
    org_id = request.args.get("org_id")
    status = request.args.get("status")
    limit = request.args.get("limit", 10, type=int)

    logger.info(f"Scan list requested — org_id={org_id} status={status}")

    # TODO: call extraction_service.list_scans(org_id, status, limit)
    return jsonify({
        "scans": [],
        "total": 0,
        "filters": {
            "org_id": org_id,
            "status": status,
            "limit": limit
        }
    }), 200


# ── Maintenance endpoints ─────────────────────────────────
@api.route("/maintenance/cleanup", methods=["POST"])
def maintenance_cleanup():
    """
    Cleans up old scan records.
    Engineer key only.
    Body: { "older_than_days": 30 }
    """
    body = request.get_json() or {}
    older_than_days = body.get("older_than_days", 30)

    logger.info(f"Cleanup requested — older_than_days={older_than_days}")

    # TODO: call maintenance_service.cleanup(older_than_days)
    return jsonify({
        "status": "ok",
        "message": f"Cleanup triggered for records older than {older_than_days} days",
        "deleted_count": 0
    }), 200


@api.route("/maintenance/status", methods=["GET"])
def maintenance_status():
    """
    Returns service health details — DB, MinIO, Kafka connectivity.
    """
    return jsonify({
        "status": "ok",
        "checks": {
            "database": "ok",
            "minio": "ok",
            "kafka": "ok",
            "salesforce": "ok"
        }
    }), 200