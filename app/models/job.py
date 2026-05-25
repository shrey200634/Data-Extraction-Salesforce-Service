import uuid
from datetime import datetime
from sqlalchemy import Column, String, DateTime, Integer, ForeignKey
from app.models.scan import Base


class JobStatus:
    """String constants for job status."""
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETE = "complete"
    FAILED = "failed"


class Job(Base):
    """
    Represents one Salesforce Bulk API job for a single object.
    One Scan has many Jobs (one per SF object).
    """
    __tablename__ = "jobs"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    scan_id = Column(String, ForeignKey("scans.id"), nullable=False, index=True)
    sf_job_id = Column(String, nullable=True)
    sf_object = Column(String, nullable=False)
    status = Column(String, default=JobStatus.PENDING)
    started_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    completed_at = Column(DateTime, nullable=True)
    records_processed = Column(Integer, default=0)
    pages_downloaded = Column(Integer, default=0)
    minio_path = Column(String, nullable=True)
    error_message = Column(String, nullable=True)

    def to_dict(self):
        return {
            "job_id": self.id,
            "scan_id": self.scan_id,
            "sf_job_id": self.sf_job_id,
            "sf_object": self.sf_object,
            "status": self.status,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "records_processed": self.records_processed,
            "pages_downloaded": self.pages_downloaded,
            "minio_path": self.minio_path,
            "error_message": self.error_message
        }
