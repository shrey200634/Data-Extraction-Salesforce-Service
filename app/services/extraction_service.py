import uuid
import logging
from datetime import datetime
from app.clients.bulk_api_client import SalesforceBulkAPIClient, SF_OBJECTS
from app.services.polling_service import PollingService
from app.services.normalization_service import NormalizationService
from app.services.deduplication_service import DeduplicationService
from app.storage.minio_client import MinIOClient
from app.storage.kafka_producer import KafkaProducer

logger = logging.getLogger(__name__)

# In-memory scan store for now (replace with DB later)
_scans = {}


class ExtractionService:
    """
    Orchestrates the full scan lifecycle.
    Receives scan request → creates Bulk API jobs →
    polls → downloads → normalizes → uploads to MinIO → publishes to Kafka.
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

    def start_scan(self, org_id: str, scan_type: str = "full") -> dict:
        """
        Starts a new extraction scan.
        Creates one Bulk API job per SF object.
        Returns scan info immediately — processing happens async.
        """
        scan_id = str(uuid.uuid4())
        started_at = datetime.utcnow().isoformat()

        # Initialize scan state
        _scans[scan_id] = {
            "scan_id": scan_id,
            "org_id": org_id,
            "scan_type": scan_type,
            "status": "in_progress",
            "started_at": started_at,
            "updated_at": started_at,
            "jobs": {obj: "pending" for obj in SF_OBJECTS},
            "errors": {}
        }

        logger.info(f"Scan started — scan_id={scan_id} org_id={org_id}")

        # Publish scan started event to Kafka
        self.kafka.publish_scan_event(scan_id, org_id, "scan_started")

        # Create one Bulk API job per SF object
        for sf_object in SF_OBJECTS:
            try:
                query = self.bulk_client.build_query(sf_object)
                job = self.bulk_client.create_job(sf_object, query)
                job_id = job["id"]

                _scans[scan_id]["jobs"][sf_object] = "in_progress"

                # Poll async — triggers download when complete
                self.polling.poll_job_async(
                    job_id=job_id,
                    sf_object=sf_object,
                    on_complete=lambda jid, obj, j: self._on_job_complete(scan_id, org_id, jid, obj),
                    on_error=lambda jid, obj, err: self._on_job_error(scan_id, jid, obj, err)
                )

            except Exception as e:
                logger.error(f"Failed to create job for {sf_object} — {str(e)}")
                _scans[scan_id]["jobs"][sf_object] = "failed"
                _scans[scan_id]["errors"][sf_object] = str(e)

        return _scans[scan_id]

    def _on_job_complete(self, scan_id: str, org_id: str, job_id: str, sf_object: str):
        """
        Called when a Bulk API job finishes.
        Downloads CSV → normalizes → deduplicates → uploads → publishes.
        """
        logger.info(f"Processing results — scan_id={scan_id} object={sf_object}")

        try:
            all_records = []

            # Download all pages
            for csv_chunk in self.bulk_client.get_results(job_id):
                records = self.normalizer.csv_to_records(csv_chunk)
                all_records.extend(records)

            # Deduplicate
            all_records = self.deduplicator.deduplicate(all_records)

            # Convert to Parquet and upload to MinIO
            if all_records:
                first_csv = next(self.bulk_client.get_results(job_id))
                parquet_bytes = self.normalizer.csv_to_parquet(first_csv, sf_object)
                minio_path = self.minio.upload_parquet(parquet_bytes, scan_id, org_id, sf_object)
                logger.info(f"Uploaded to MinIO — path={minio_path}")

                # Publish records to Kafka
                self.kafka.publish_records(sf_object, all_records, scan_id, org_id)

            # Update scan state
            _scans[scan_id]["jobs"][sf_object] = "complete"
            _scans[scan_id]["updated_at"] = datetime.utcnow().isoformat()

            # Check if all jobs done
            self._check_scan_complete(scan_id, org_id)

            # Cleanup job from Salesforce
            self.bulk_client.delete_job(job_id)

        except Exception as e:
            logger.error(f"Result processing failed — object={sf_object} error={str(e)}")
            self._on_job_error(scan_id, job_id, sf_object, str(e))

    def _on_job_error(self, scan_id: str, job_id: str, sf_object: str, error: str):
        """Called when a job fails."""
        _scans[scan_id]["jobs"][sf_object] = "failed"
        _scans[scan_id]["errors"][sf_object] = error
        _scans[scan_id]["updated_at"] = datetime.utcnow().isoformat()
        self._check_scan_complete(scan_id, _scans[scan_id]["org_id"])

    def _check_scan_complete(self, scan_id: str, org_id: str):
        """Marks scan complete when all jobs are done."""
        jobs = _scans[scan_id]["jobs"]
        all_done = all(s in ("complete", "failed") for s in jobs.values())

        if all_done:
            has_failures = any(s == "failed" for s in jobs.values())
            _scans[scan_id]["status"] = "failed" if has_failures else "complete"
            _scans[scan_id]["updated_at"] = datetime.utcnow().isoformat()

            self.kafka.publish_scan_event(
                scan_id, org_id,
                "scan_failed" if has_failures else "scan_complete"
            )
            logger.info(f"Scan finished — scan_id={scan_id} status={_scans[scan_id]['status']}")

    def get_scan_status(self, scan_id: str) -> dict:
        """Returns current scan state."""
        if scan_id not in _scans:
            return None
        return _scans[scan_id]

    def list_scans(self, org_id: str = None, status: str = None, limit: int = 10) -> list:
        """Returns list of scans with optional filters."""
        scans = list(_scans.values())
        if org_id:
            scans = [s for s in scans if s["org_id"] == org_id]
        if status:
            scans = [s for s in scans if s["status"] == status]
        return scans[:limit]

    def cancel_scan(self, scan_id: str) -> dict:
        """Cancels a running scan."""
        if scan_id not in _scans:
            return None
        _scans[scan_id]["status"] = "cancelled"
        _scans[scan_id]["updated_at"] = datetime.utcnow().isoformat()
        return _scans[scan_id]