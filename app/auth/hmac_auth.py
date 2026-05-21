import hashlib
import hmac
import time
import logging
from functools import wraps
from flask import request, jsonify
from app.config import validate_settings

logger = logging.getLogger(__name__)

settings, _ = validate_settings()


def verify_hmac(f):
    """
    Decorator that verifies HMAC signature on incoming requests.
    Checks both core-service key and engineer key.
    
    
    Usage:
        @api.route("/scan/start", methods=["POST"])
        @verify_hmac
        def scan_start():
            ...
    """
    @wraps(f)
    def decorated(*args, **kwargs):
        # Skip HMAC check if disabled (local dev)
        if not settings.HMAC_ENABLED:
            return f(*args, **kwargs)

        # Get signature from request headers
        signature = request.headers.get("X-HMAC-Signature")
        timestamp = request.headers.get("X-HMAC-Timestamp")

        if not signature or not timestamp:
            return jsonify({
                "error": "unauthorized",
                "message": "Missing HMAC signature or timestamp headers"
            }), 401

        # Check timestamp is not too old (prevent replay attacks)
        try:
            request_time = int(timestamp)
            current_time = int(time.time())
            max_age = int(settings.HMAC_SIGNATURE_MAX_AGE) if hasattr(settings, 'HMAC_SIGNATURE_MAX_AGE') else 300

            if abs(current_time - request_time) > max_age:
                return jsonify({
                    "error": "unauthorized",
                    "message": "HMAC signature expired"
                }), 401
        except (ValueError, TypeError):
            return jsonify({
                "error": "unauthorized",
                "message": "Invalid timestamp"
            }), 401

        # Build the message to verify
        # message = METHOD + PATH + TIMESTAMP + BODY
        body = request.get_data(as_text=True) or ""
        message = f"{request.method}{request.path}{timestamp}{body}"

        # Try core key first, then engineer key
        valid = False
        for key in [settings.HMAC_SECRET_KEY_CORE, settings.HMAC_SECRET_KEY_ENGINEER]:
            expected = hmac.new(
                key.encode("utf-8"),
                message.encode("utf-8"),
                hashlib.sha256
            ).hexdigest()

            if hmac.compare_digest(signature, expected):
                valid = True
                break

        if not valid:
            logger.warning(f"HMAC verification failed for {request.method} {request.path}")
            return jsonify({
                "error": "unauthorized",
                "message": "Invalid HMAC signature"
            }), 401

        return f(*args, **kwargs)

    return decorated
