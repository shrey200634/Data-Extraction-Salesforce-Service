import sys
import logging
from flask import Flask, jsonify
from app.config import validate_settings

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


def create_app():
    # Validate config at startup — fail fast if anything missing
    settings, errors = validate_settings()

    if errors:
        logger.error("=" * 60)
        logger.error("STARTUP FAILED — fix your .env and restart")
        logger.error("=" * 60)
        for e in errors:
            logger.error(f"  [{e['field']}] {e['error']}")
        logger.error("=" * 60)
        sys.exit(1)

    # Create Flask app
    app = Flask(__name__)
    app.config["SECRET_KEY"] = settings.SECRET_KEY

    # Health endpoint — no auth, required by Nomad
    @app.route("/api/health")
    def health():
        return jsonify({
            "status": "ok",
            "service": "black-diamond-salesforce-service",
            "environment": settings.ENVIRONMENT,
            "version": "0.1.0"
        }), 200

    logger.info(f"Service started — env={settings.ENVIRONMENT} port=5711")
    return app