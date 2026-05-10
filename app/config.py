import sys
import logging
from pydantic_settings import BaseSettings
from pydantic import field_validator

logger = logging.getLogger(__name__)


class Settings(BaseSettings):
    # Salesforce
    SF_CONSUMER_KEY: str
    SF_PRIVATE_KEY_PEM: str
    SF_USERNAME: str
    SF_LOGIN_URL: str = "https://test.salesforce.com"

    # Flask
    FLASK_DEBUG: bool = False
    SECRET_KEY: str
    ENVIRONMENT: str = "dev"
    LOG_LEVEL: str = "DEBUG"
    ALLOWED_ORIGINS: str = "http://localhost:3000"

    # HMAC
    HMAC_ENABLED: bool = True
    HMAC_SECRET_KEY_CORE: str
    HMAC_SECRET_KEY_ENGINEER: str

    # MinIO
    MINIO_ENABLED: bool = True
    MINIO_ENDPOINT: str
    MINIO_ACCESS_KEY: str
    MINIO_SECRET_KEY: str
    MINIO_BUCKET: str = "salesforce-dev"
    MINIO_SECURE: bool = False

    # Kafka
    KAFKA_ENABLED: bool = True
    KAFKA_BOOTSTRAP_SERVERS: str

    # Database
    DATABASE_URL: str

    # ClickHouse
    CLICKHOUSE_ENABLED: bool = False
    CLICKHOUSE_HOST: str = "localhost"
    CLICKHOUSE_PORT: int = 9440

    # Service
    MAX_CONCURRENT_SCANS: int = 5
    SF_MAX_JOB_TIMEOUT_HOURS: int = 2
    PII_MASKING_ENABLED: bool = False
    PII_HMAC_KEY: str = ""

    @field_validator("SECRET_KEY")
    @classmethod
    def secret_key_min_length(cls, v):
        if len(v) < 32:
            raise ValueError("SECRET_KEY must be at least 32 characters")
        return v

    @field_validator("ENVIRONMENT")
    @classmethod
    def environment_valid(cls, v):
        if v not in ("dev", "stage", "prod"):
            raise ValueError("ENVIRONMENT must be dev, stage, or prod")
        return v

    model_config = {"env_file": ".env", "case_sensitive": True}


def validate_settings():
    errors = []
    try:
        settings = Settings()
        return settings, []
    except Exception as e:
        if hasattr(e, "errors"):
            for error in e.errors():
                errors.append({
                    "field": error["loc"][0],
                    "error": error["msg"]
                })
        else:
            errors.append({"field": "unknown", "error": str(e)})
        return None, errors