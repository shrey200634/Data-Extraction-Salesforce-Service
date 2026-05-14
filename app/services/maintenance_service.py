import logging
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from app.models.scan import Scan

logger = logging.getLogger(__name__)


class MaintenanceService:
    """
    Cleans up old scan records from the database.
    """

    def __init__(self, db_session: Session):
        self.db = db_session

    def cleanup(self, older_than_days: int = 30) -> int:
        """
        Deletes scan records older than N days.
        Returns count of deleted records.
        """
        cutoff_date = datetime.utcnow() - timedelta(days=older_than_days)

        try:
            old_scans = self.db.query(Scan).filter(
                Scan.started_at < cutoff_date
            ).all()

            count = len(old_scans)
            for scan in old_scans:
                self.db.delete(scan)

            self.db.commit()
            logger.info(f"Maintenance cleanup — deleted {count} scans older than {older_than_days} days")
            return count

        except Exception as e:
            self.db.rollback()
            logger.error(f"Maintenance cleanup failed — {str(e)}")
            raise