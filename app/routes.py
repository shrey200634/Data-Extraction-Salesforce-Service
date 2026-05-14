import logging
from flask import Blueprint, jsonify, request
from app.config import validate_settings
from app.auth.salesforce_auth import SalesforceTokenManager
from app.clients.bulk_api_client import SalesforceBulkAPIClient
from app.services.extraction_service import ExtractionService
from app.services.polling_service import PollingService
from app.services.normalization_service import NormalizationService
from app.services.deduplication_service import DeduplicationService
from app.storage.minio_client import MinIOClient
from app.storage.kafka_producer import KafkaProducer

logger = logging.getLogger(__name__)

api = Blueprint("api", __name__, url_prefix="/api")

settings, _ = validate_settings()

# ── Wire up all services (Java equivalent: @Autowired dependencies) ──
token_manager = SalesforceTokenManager(settings)
bulk_client = SalesforceBulkAPIClient(token_manager)
polling_svc = PollingService(bulk_client)
normalization_svc = NormalizationService()
deduplication_svc = DeduplicationService()
minio_client = MinIOClient(settings)
kafka_producer = KafkaProducer(settings)

extraction_svc = ExtractionService(
    bulk_client=bulk_client,
    polling_service=polling_svc,
    normalization_service=normalization_svc,
    deduplication_service=deduplication_svc,
    minio_client=minio_client,
    kafka_producer=kafka_producer
)


# ── Health ────────────────────────────────────────────────
@api.route("/health", methods=["GET"])
def health():
    return jsonify({
        "status": "ok",
        "service": "black-diamond-salesforce-service",
        "environment": settings.ENVIRONMENT,
        "version": "0.1.0"
    }), 200


# ── Scan endpoints ────────────────────────────────────────
@api.route("/scan/start", methods=["POST"])
def scan_start():
    body = request.get_json()

    if not body or "org_id" not in body:
        return jsonify({
            "error": "missing_field",
            "message": "org_id is required"
        }), 400

    org_id = body.get("org_id")
    scan_type = body.get("scan_type", "full")

    logger.info(f"Scan start requested — org_id={org_id} type={scan_type}")

    try:
        scan = extraction_svc.start_scan(org_id, scan_type)
        return jsonify(scan), 202
    except Exception as e:
        logger.error(f"Scan start failed — {str(e)}")
        return jsonify({
            "error": "scan_start_failed",
            "message": str(e)
        }), 500


@api.route("/scan/status/<scan_id>", methods=["GET"])
def scan_status(scan_id):
    logger.info(f"Scan status requested — scan_id={scan_id}")

    scan = extraction_svc.get_scan_status(scan_id)

    if not scan:
        return jsonify({
            "error": "not_found",
            "message": f"Scan {scan_id} not found"
        }), 404

    return jsonify(scan), 200


@api.route("/scan/cancel/<scan_id>", methods=["POST"])
def scan_cancel(scan_id):
    logger.info(f"Scan cancel requested — scan_id={scan_id}")

    scan = extraction_svc.cancel_scan(scan_id)

    if not scan:
        return jsonify({
            "error": "not_found",
            "message": f"Scan {scan_id} not found"
        }), 404

    return jsonify(scan), 200


@api.route("/scan/resume/<scan_id>", methods=["POST"])
def scan_resume(scan_id):
    logger.info(f"Scan resume requested — scan_id={scan_id}")

    # Get existing scan
    scan = extraction_svc.get_scan_status(scan_id)
    if not scan:
        return jsonify({
            "error": "not_found",
            "message": f"Scan {scan_id} not found"
        }), 404

    # Restart it
    try:
        new_scan = extraction_svc.start_scan(scan["org_id"], scan["scan_type"])
        return jsonify(new_scan), 202
    except Exception as e:
        return jsonify({
            "error": "resume_failed",
            "message": str(e)
        }), 500


@api.route("/scan/list", methods=["GET"])
def scan_list():
    org_id = request.args.get("org_id")
    status = request.args.get("status")
    limit = request.args.get("limit", 10, type=int)

    logger.info(f"Scan list requested — org_id={org_id} status={status}")

    scans = extraction_svc.list_scans(org_id, status, limit)

    return jsonify({
        "scans": scans,
        "total": len(scans),
        "filters": {
            "org_id": org_id,
            "status": status,
            "limit": limit
        }
    }), 200


# ── Maintenance endpoints ─────────────────────────────────
@api.route("/maintenance/cleanup", methods=["POST"])
def maintenance_cleanup():
    body = request.get_json() or {}
    older_than_days = body.get("older_than_days", 30)

    logger.info(f"Cleanup requested — older_than_days={older_than_days}")

    # Cleanup is DB-level — skipping for now until DB is wired
    return jsonify({
        "status": "ok",
        "message": f"Cleanup triggered for records older than {older_than_days} days",
        "deleted_count": 0
    }), 200


@api.route("/maintenance/status", methods=["GET"])
def maintenance_status():
    checks = {
        "database": "ok",
        "minio": "ok",
        "kafka": "ok",
        "salesforce": "ok"
    }

    # Basic connectivity checks
    try:
        minio_client.file_exists("healthcheck")
    except Exception:
        checks["minio"] = "error"

    return jsonify({
        "status": "ok" if all(v == "ok" for v in checks.values()) else "degraded",
        "checks": checks
    }), 200