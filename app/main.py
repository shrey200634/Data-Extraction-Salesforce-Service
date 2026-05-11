import sys
import logging
from flask import Flask
from app.config import validate_settings
from app.routes import api

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


def create_app():
    # Validate config at startup — fail fast
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

    # Register all routes from routes.py
    app.register_blueprint(api)

    logger.info(f"Service started — env={settings.ENVIRONMENT} port=5711")
    return app