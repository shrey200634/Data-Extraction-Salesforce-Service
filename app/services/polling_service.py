import logging
import threading
from datetime import datetime
from app.clients.bulk_api_client import SalesforceBulkAPIClient

logger = logging.getLogger(__name__)


class PollingService:
    """
    Runs in a background thread.
    Polls Salesforce job status until complete then triggers download.
    """

    def __init__(self, bulk_client: SalesforceBulkAPIClient):
        self.bulk_client = bulk_client
        self._active_polls = {}

    def poll_job_async(self, job_id: str, sf_object: str, on_complete, on_error):
        """
        Starts polling a job in a background thread.
        Calls on_complete(job_id, sf_object) when done.
        Calls on_error(job_id, error) on failure.
        """
        thread = threading.Thread(
            target=self._poll_job,
            args=(job_id, sf_object, on_complete, on_error),
            daemon=True,
            name=f"poll-{job_id[:8]}"
        )
        self._active_polls[job_id] = thread
        thread.start()
        logger.info(f"Polling started — job_id={job_id} object={sf_object}")

    def _poll_job(self, job_id: str, sf_object: str, on_complete, on_error):
        """Internal — runs in background thread."""
        try:
            job = self.bulk_client.poll_job(job_id)
            logger.info(f"Job complete — job_id={job_id} object={sf_object}")
            on_complete(job_id, sf_object, job)
        except Exception as e:
            logger.error(f"Job failed — job_id={job_id} error={str(e)}")
            on_error(job_id, sf_object, str(e))
        finally:
            self._active_polls.pop(job_id, None)

    def active_poll_count(self) -> int:
        return len(self._active_polls)