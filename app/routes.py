import logging
from flask import Blueprint, jsonify, request
from app.config import validate_settings
from app.auth.salesforce_auth import SalesforceTokenManager
from app.auth.hmac_auth import verify_hmac
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
@verify_hmac
def scan_start():
    body = request.get_json()

    if not body or "org_id" not in body:
        return jsonify({
            "error": "missing_field",
            "message": "org_id is required"
        }), 400

    org_id = body.get("org_id")
    scan_type = body.get("scan_type", "full")

    # Optional incremental filter — extract only records modified after this timestamp
    filters = body.get("filters", {})
    last_modified_after = filters.get("last_modified_after")

    logger.info(f"Scan start requested — org_id={org_id} type={scan_type} after={last_modified_after}")

    try:
        scan = extraction_svc.start_scan(org_id, scan_type, last_modified_after=last_modified_after)
        return jsonify(scan), 202
    except Exception as e:
        logger.error(f"Scan start failed — {str(e)}")
        return jsonify({
            "error": "scan_start_failed",
            "message": str(e)
        }), 500


@api.route("/scan/status/<scan_id>", methods=["GET"])
@verify_hmac
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
@verify_hmac
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
@verify_hmac
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
@verify_hmac
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
@verify_hmac
def maintenance_cleanup():
    body = request.get_json() or {}
    older_than_days = body.get("older_than_days", 30)

    logger.info(f"Cleanup requested — older_than_days={older_than_days}")

    try:
        from app.database import get_session
        from app.services.maintenance_service import MaintenanceService

        session = get_session()
        try:
            svc = MaintenanceService(session)
            deleted = svc.cleanup(older_than_days)
            return jsonify({
                "status": "ok",
                "message": f"Cleanup completed",
                "deleted_count": deleted,
                "older_than_days": older_than_days,
            }), 200
        finally:
            session.close()
    except Exception as e:
        logger.error(f"Cleanup failed: {e}")
        return jsonify({
            "error": "cleanup_failed",
            "message": str(e),
        }), 500


@api.route("/maintenance/status", methods=["GET"])
@verify_hmac
def maintenance_status():
    checks = {
        "database": "ok",
        "minio": "ok",
        "kafka": "ok",
        "salesforce": "ok"
    }

    # Check MinIO
    try:
        minio_client.file_exists("healthcheck")
    except Exception:
        checks["minio"] = "error"

    # Check Database
    try:
        from app.database import get_session
        from sqlalchemy import text
        session = get_session()
        try:
            session.execute(text("SELECT 1"))
        finally:
            session.close()
    except Exception:
        checks["database"] = "error"

    # Check Salesforce token
    try:
        token_manager.get_token()
    except Exception:
        checks["salesforce"] = "error"

    return jsonify({
        "status": "ok" if all(v == "ok" for v in checks.values()) else "degraded",
        "checks": checks
    }), 200


# ── Objects endpoint ──────────────────────────────────────
@api.route("/objects", methods=["GET"])
@verify_hmac
def get_objects():
    """Returns supported Salesforce objects with their SOQL templates."""
    supported_objects = [
        {
            "name": "Contact",
            "label": "Contacts",
            "soql_template": "SELECT {fields} FROM Contact {where_clause} ORDER BY LastModifiedDate ASC",
            "default_fields": ["Id", "FirstName", "LastName", "Email", "Phone", "AccountId", "CreatedDate", "LastModifiedDate"],
            "supports_incremental": True
        },
        {
            "name": "Account",
            "label": "Accounts",
            "soql_template": "SELECT {fields} FROM Account {where_clause} ORDER BY LastModifiedDate ASC",
            "default_fields": ["Id", "Name", "Industry", "BillingCity", "BillingCountry", "CreatedDate", "LastModifiedDate"],
            "supports_incremental": True
        },
        {
            "name": "Opportunity",
            "label": "Opportunities",
            "soql_template": "SELECT {fields} FROM Opportunity {where_clause} ORDER BY LastModifiedDate ASC",
            "default_fields": ["Id", "Name", "StageName", "Amount", "CloseDate", "AccountId", "CreatedDate", "LastModifiedDate"],
            "supports_incremental": True
        },
        {
            "name": "Task",
            "label": "Activities",
            "soql_template": "SELECT {fields} FROM Task {where_clause} ORDER BY LastModifiedDate ASC",
            "default_fields": ["Id", "Subject", "Status", "Priority", "WhoId", "WhatId", "CreatedDate", "LastModifiedDate"],
            "supports_incremental": True
        },
        {
            "name": "Lead",
            "label": "Leads",
            "soql_template": "SELECT {fields} FROM Lead {where_clause} ORDER BY LastModifiedDate ASC",
            "default_fields": ["Id", "FirstName", "LastName", "Email", "Company", "Status", "CreatedDate", "LastModifiedDate"],
            "supports_incremental": True
        },
        {
            "name": "User",
            "label": "Users",
            "soql_template": "SELECT {fields} FROM User {where_clause} ORDER BY LastModifiedDate ASC",
            "default_fields": ["Id", "Name", "Email", "Username", "IsActive", "CreatedDate", "LastModifiedDate"],
            "supports_incremental": True
        },
        {
            "name": "CampaignMember",
            "label": "Campaign Members",
            "soql_template": "SELECT {fields} FROM CampaignMember {where_clause} ORDER BY LastModifiedDate ASC",
            "default_fields": ["Id", "CampaignId", "ContactId", "LeadId", "Status", "CreatedDate", "LastModifiedDate"],
            "supports_incremental": True
        }
    ]
    return jsonify({"supported_objects": supported_objects, "total": len(supported_objects)}), 200


# ── Batch info endpoint ───────────────────────────────────
@api.route("/batch/info", methods=["GET"])
@verify_hmac
def batch_info():
    """Returns Salesforce org metadata and API quota info."""
    try:
        token, instance_url = token_manager.get_token()
        return jsonify({
            "org": {
                "instance_url": instance_url,
                "api_version": "59.0",
            },
            "api_limits": {
                "bulk_api_jobs": {
                    "max_concurrent": settings.MAX_CONCURRENT_SCANS
                }
            },
            "token_status": "valid"
        }), 200
    except Exception as e:
        logger.error(f"Batch info failed: {str(e)}")
        return jsonify({
            "error": "salesforce_connection_failed",
            "message": str(e)
        }), 503
