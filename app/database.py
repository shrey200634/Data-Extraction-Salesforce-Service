"""
Database setup — SQLAlchemy engine and session factory.
Java equivalent: a Spring DataSource + SessionFactory bean.
"""
import logging
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from app.config import validate_settings
from app.models.scan import Base

logger = logging.getLogger(__name__)

settings, _ = validate_settings()

# Create engine
engine = create_engine(
    settings.DATABASE_URL,
    pool_pre_ping=True,  # Verifies connection before each use
    pool_size=5,
    max_overflow=10,
    echo=False,
)

# Session factory
SessionLocal = scoped_session(sessionmaker(bind=engine, autoflush=False, autocommit=False))


def init_db():
    """
    Creates all tables defined in the models.
    Called once at startup. Idempotent — safe to run multiple times.
    """
    try:
        # Import all models so they're registered with Base.metadata
        from app.models.scan import Scan
        from app.models.job import Job

        Base.metadata.create_all(bind=engine)
        logger.info("Database tables created/verified")
    except Exception as e:
        logger.error(f"Database initialization failed: {e}")
        raise


def get_session():
    """
    Returns a new database session.
    Caller is responsible for closing it.
    """
    return SessionLocal()
