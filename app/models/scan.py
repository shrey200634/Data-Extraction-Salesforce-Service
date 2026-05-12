import uuid
from datetime import datetime
from sqlalchemy import Column, String, DateTime, Integer, Enum
from sqlalchemy.orm import declarative_base
import enum

Base = declarative_base()


class ScanStatus(str, enum.Enum):
    STARTED = "started"
    IN_PROGRESS = "in_progress"
    COMPLETE = "complete"
    FAILED = "failed"
    CANCELLED = "cancelled"


class ScanType(str, enum.Enum):
    FULL = "full"
    INCREMENTAL = "incremental"


class Scan(Base):
  
    __tablename__ = "scans"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    org_id = Column(String, nullable=False, index=True)
    scan_type = Column(String, default=ScanType.FULL)
    status = Column(String, default=ScanStatus.STARTED)
    started_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    completed_at = Column(DateTime, nullable=True)
    error_message = Column(String, nullable=True)
    total_records = Column(Integer, default=0)

    def to_dict(self):
        return {
            "scan_id": self.id,
            "org_id": self.org_id,
            "scan_type": self.scan_type,
            "status": self.status,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "error_message": self.error_message,
            "total_records": self.total_records
        }