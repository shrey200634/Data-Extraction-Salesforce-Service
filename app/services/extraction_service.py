import uuid
import logging
from datetime import datetime
from app.clients.bulk_api_client import SalesforceBulkAPIClient, SF_OBJECTS
from app.services.polling_service import PollingService
from app.services.normalization_service import NormalizationService
from app.services.deduplication_service import DeduplicationService
from app.storage.minio_client import MinIOClient
from app.storage.kafka_producer import KafkaProducer
from app.database import get_session
from app.models.scan import Scan, ScanStatus
from app.models.job import Job, JobStatus

logger = logging.getLogger(__name__)


class ExtractionService:
    """
    Orchestrates the full scan lifecycle.
    Receives scan request -> creates Bulk API jobs ->
    polls -> downloads -> normalizes -> uploads to MinIO -> publishes to Kafka.

    Uses PostgreSQL for persistent scan/job state.
    """

    def __init__(
        self,
        bulk_client: SalesforceBulkAPIClient,
        polling_service: PollingService,
        normalization_service: NormalizationService,
        deduplication_service: DeduplicationService,
        minio_client: MinIOClient,
        kafka_producer: KafkaProducer
    ):
        self.bulk_client = bulk_client
        self.polling = polling_service
        self.normalizer = normalization_service
        self.deduplicator = deduplication_service
        self.minio = minio_client
        self.kafka = kafka_producer

    # ---------------------------------------------------------
    # Scan lifecycle
    # ---------------------------------------------------------

    def start_scan(self, org_id: str, scan_type: str = "full", last_modified_after: str = None) -> dict:
        """
        Starts a new extraction scan.
        Persists scan + job records to PostgreSQL.
        Creates one Bulk API job per SF object.
        Returns scan info immediately - processing happens async.
        
        Args:
            org_id: Organization identifier
            scan_type: "full" or "incremental"
            last_modified_after: ISO8601 timestamp for incremental extraction (optional)
        """
        scan_id = str(uuid.uuid4())
        session = get_session()

        try:
            # Create scan record
            scan = Scan(
                id=scan_id,
                org_id=org_id,
                scan_type=scan_type,
                status=ScanStatus.IN_PROGRESS.value,
            )
            session.add(scan)

            # Create one Job record per SF object (initially pending)
            for sf_object in SF_OBJECTS:
                job = Job(
                    scan_id=scan_id,
                    sf_object=sf_object,
                    status=JobStatus.PENDING,
                )
                session.add(job)

            session.commit()
            logger.info(f"Scan persisted - scan_id={scan_id} org_id={org_id}")
        except Exception as e:
            session.rollback()
            logger.error(f"Failed to persist scan - {e}")
            raise
        finally:
            session.close()

        # Publish scan started event
        self.kafka.publish_scan_event(scan_id, org_id, "scan_started")

        # Create Bulk API jobs and start polling
        for sf_object in SF_OBJECTS:
            try:
                query = self.bulk_client.build_query(sf_object, last_modified_after=last_modified_after)
                sf_job = self.bulk_client.create_job(sf_object, query)
                sf_job_id = sf_job["id"]

                # Update job with SF job id and IN_PROGRESS status
                self._update_job(scan_id, sf_object, {
                    "sf_job_id": sf_job_id,
                    "status": JobStatus.IN_PROGRESS,
                })

                # Poll async - triggers download when complete
                self.polling.poll_job_async(
                    job_id=sf_job_id,
                    sf_object=sf_object,
                    on_complete=lambda jid, obj, j: self._on_job_complete(scan_id, org_id, jid, obj),
                    on_error=lambda jid, obj, err: self._on_job_error(scan_id, org_id, jid, obj, err)
                )

            except Exception as e:
                logger.error(f"Failed to create job for {sf_object} - {e}")
                self._update_job(scan_id, sf_object, {
                    "status": JobStatus.FAILED,
                    "error_message": str(e),
                })

        return self.get_scan_status(scan_id)

    def _on_job_complete(self, scan_id: str, org_id: str, sf_job_id: str, sf_object: str):
        """Called when a Bulk API job finishes - download, normalize, upload, publish."""
        logger.info(f"Processing results - scan_id={scan_id} object={sf_object}")

        try:
            all_records = []
            for csv_chunk in self.bulk_client.get_results(sf_job_id):
                records = self.normalizer.csv_to_records(csv_chunk)
                all_records.extend(records)

            all_records = self.deduplicator.deduplicate(all_records)
            minio_path = None

            if all_records:
                # Re-fetch CSV for parquet conversion (could be optimized later)
                first_csv = next(self.bulk_client.get_results(sf_job_id))
                parquet_bytes = self.normalizer.csv_to_parquet(first_csv, sf_object)
                minio_path = self.minio.upload_parquet(parquet_bytes, scan_id, org_id, sf_object)
                logger.info(f"Uploaded to MinIO - path={minio_path}")

                self.kafka.publish_records(sf_object, all_records, scan_id, org_id)

            # Update job: complete
            self._update_job(scan_id, sf_object, {
                "status": JobStatus.COMPLETE,
                "records_processed": len(all_records),
                "minio_path": minio_path,
                "completed_at": datetime.utcnow(),
            })

            self._check_scan_complete(scan_id, org_id)

            # Cleanup SF job
            try:
                self.bulk_client.delete_job(sf_job_id)
            except Exception as e:
                logger.warning(f"Failed to delete SF job {sf_job_id}: {e}")

        except Exception as e:
            logger.error(f"Result processing failed - object={sf_object} error={e}")
            self._on_job_error(scan_id, org_id, sf_job_id, sf_object, str(e))

    def _on_job_error(self, scan_id: str, org_id: str, sf_job_id: str, sf_object: str, error: str):
        """Called when a job fails."""
        self._update_job(scan_id, sf_object, {
            "status": JobStatus.FAILED,
            "error_message": error,
            "completed_at": datetime.utcnow(),
        })
        self._check_scan_complete(scan_id, org_id)

    def _check_scan_complete(self, scan_id: str, org_id: str):
        """Marks scan complete when all jobs are done."""
        session = get_session()
        try:
            jobs = session.query(Job).filter(Job.scan_id == scan_id).all()
            terminal_states = {JobStatus.COMPLETE, JobStatus.FAILED}
            all_done = all(j.status in terminal_states for j in jobs)

            if all_done:
                has_failures = any(j.status == JobStatus.FAILED for j in jobs)
                total_records = sum(j.records_processed or 0 for j in jobs)

                scan = session.query(Scan).filter(Scan.id == scan_id).first()
                if scan:
                    scan.status = ScanStatus.FAILED.value if has_failures else ScanStatus.COMPLETE.value
                    scan.completed_at = datetime.utcnow()
                    scan.total_records = total_records
                    session.commit()

                self.kafka.publish_scan_event(
                    scan_id, org_id,
                    "scan_failed" if has_failures else "scan_complete"
                )
                logger.info(f"Scan finished - scan_id={scan_id} status={scan.status if scan else 'unknown'}")
        except Exception as e:
            session.rollback()
            logger.error(f"check_scan_complete failed - {e}")
        finally:
            session.close()

    # ---------------------------------------------------------
    # Helpers
    # ---------------------------------------------------------

    def _update_job(self, scan_id: str, sf_object: str, updates: dict):
        """Updates a Job row with the given fields."""
        session = get_session()
        try:
            job = session.query(Job).filter(
                Job.scan_id == scan_id,
                Job.sf_object == sf_object,
            ).first()
            if not job:
                logger.warning(f"Job not found - scan={scan_id} object={sf_object}")
                return

            for key, value in updates.items():
                setattr(job, key, value)
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"_update_job failed - {e}")
        finally:
            session.close()

    def _scan_to_dict(self, scan: Scan, jobs: list) -> dict:
        """Converts a Scan ORM object plus its jobs into the API response shape."""
        job_status_map = {j.sf_object: j.status for j in jobs}
        errors = {j.sf_object: j.error_message for j in jobs if j.error_message}

        return {
            "scan_id": scan.id,
            "org_id": scan.org_id,
            "scan_type": scan.scan_type,
            "status": scan.status,
            "started_at": scan.started_at.isoformat() if scan.started_at else None,
            "updated_at": scan.updated_at.isoformat() if scan.updated_at else None,
            "completed_at": scan.completed_at.isoformat() if scan.completed_at else None,
            "total_records": scan.total_records,
            "jobs": job_status_map,
            "errors": errors,
        }

    # ---------------------------------------------------------
    # Public read APIs
    # ---------------------------------------------------------

    def get_scan_status(self, scan_id: str) -> dict:
        """Returns current scan state from the database."""
        session = get_session()
        try:
            scan = session.query(Scan).filter(Scan.id == scan_id).first()
            if not scan:
                return None
            jobs = session.query(Job).filter(Job.scan_id == scan_id).all()
            return self._scan_to_dict(scan, jobs)
        finally:
            session.close()

    def list_scans(self, org_id: str = None, status: str = None, limit: int = 10) -> list:
        """Returns list of scans with optional filters."""
        session = get_session()
        try:
            query = session.query(Scan)
            if org_id:
                query = query.filter(Scan.org_id == org_id)
            if status:
                query = query.filter(Scan.status == status)

            scans = query.order_by(Scan.started_at.desc()).limit(limit).all()
            scan_ids = [s.id for s in scans]

            jobs_by_scan = {}
            if scan_ids:
                jobs = session.query(Job).filter(Job.scan_id.in_(scan_ids)).all()
                for j in jobs:
                    jobs_by_scan.setdefault(j.scan_id, []).append(j)

            return [self._scan_to_dict(s, jobs_by_scan.get(s.id, [])) for s in scans]
        finally:
            session.close()

    def cancel_scan(self, scan_id: str) -> dict:
        """Cancels a running scan."""
        session = get_session()
        try:
            scan = session.query(Scan).filter(Scan.id == scan_id).first()
            if not scan:
                return None

            scan.status = ScanStatus.CANCELLED.value
            scan.completed_at = datetime.utcnow()
            session.commit()
            session.refresh(scan)

            jobs = session.query(Job).filter(Job.scan_id == scan_id).all()
            return self._scan_to_dict(scan, jobs)
        except Exception as e:
            session.rollback()
            logger.error(f"cancel_scan failed - {e}")
            raise
        finally:
            session.close()
