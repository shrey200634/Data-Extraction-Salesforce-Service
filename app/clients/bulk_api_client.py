import time
import logging
import requests

logger = logging.getLogger(__name__)

# Salesforce objects we extract
SF_OBJECTS = [
    "Contact",
    "Account",
    "Opportunity",
    "Task",
    "Lead",
    "User",
    "CampaignMember"
]

# Polling intervals in seconds (adaptive)
POLL_INTERVALS = [5, 5, 15, 15, 30, 60, 120]


class SalesforceBulkAPIClient:
    """
    Wraps Salesforce Bulk API 2.0.
    Handles job lifecycle: create → poll → paginate → cleanup.

    
    """

    def __init__(self, token_manager):
        self.token_manager = token_manager

    def _headers(self):
        token, _ = self.token_manager.get_token()
        return {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }

    def _base_url(self):
        _, instance_url = self.token_manager.get_token()
        return f"{instance_url}/services/data/v59.0/jobs/query"

    def create_job(self, sf_object: str, query: str) -> dict:
        """
        Creates a Bulk API 2.0 query job.
        Returns job info dict with job id.
        """
        payload = {
            "operation": "query",
            "query": query,
            "contentType": "CSV",
            "columnDelimiter": "COMMA",
            "lineEnding": "LF"
        }

        logger.info(f"Creating bulk job for {sf_object}")
        response = requests.post(
            self._base_url(),
            json=payload,
            headers=self._headers()
        )
        response.raise_for_status()
        job = response.json()
        logger.info(f"Job created — id={job['id']} object={sf_object}")
        return job

    def poll_job(self, job_id: str) -> dict:
        """
        Polls job status until complete or failed.
        Uses adaptive polling intervals.
        Returns final job state dict.
        """
        url = f"{self._base_url()}/{job_id}"
        interval_index = 0

        while True:
            response = requests.get(url, headers=self._headers())
            response.raise_for_status()
            job = response.json()
            state = job.get("state")

            logger.info(f"Job {job_id} state={state}")

            if state == "JobComplete":
                return job
            elif state in ("Failed", "Aborted"):
                raise Exception(f"Job {job_id} failed with state={state}")

            # Adaptive wait
            wait = POLL_INTERVALS[min(interval_index, len(POLL_INTERVALS) - 1)]
            logger.debug(f"Job {job_id} not ready — waiting {wait}s")
            time.sleep(wait)
            interval_index += 1

    def get_results(self, job_id: str):
        """
        Downloads all result pages for a completed job.
        Yields CSV text chunks page by page.
        """
        locator = None

        while True:
            url = f"{self._base_url()}/{job_id}/results"
            params = {"maxRecords": 50000}
            if locator:
                params["locator"] = locator

            response = requests.get(
                url,
                headers=self._headers(),
                params=params
            )
            response.raise_for_status()

            yield response.text

            # Check if more pages exist
            locator = response.headers.get("Sforce-Locator")
            if not locator or locator == "null":
                logger.info(f"Job {job_id} — all pages downloaded")
                break

            logger.debug(f"Job {job_id} — fetching next page locator={locator}")

    def delete_job(self, job_id: str):
        """Cleans up job from Salesforce after download."""
        url = f"{self._base_url()}/{job_id}"
        response = requests.delete(url, headers=self._headers())
        response.raise_for_status()
        logger.info(f"Job {job_id} deleted from Salesforce")

    def build_query(self, sf_object: str) -> str:
        """
        Builds SOQL query for each Salesforce object.
        Java equivalent: building a JPA/SQL query string.
        """
        fields = {
            "Contact": "Id, FirstName, LastName, Email, Phone, AccountId, CreatedDate, LastModifiedDate",
            "Account": "Id, Name, Industry, BillingCity, BillingCountry, CreatedDate, LastModifiedDate",
            "Opportunity": "Id, Name, StageName, Amount, CloseDate, AccountId, CreatedDate, LastModifiedDate",
            "Task": "Id, Subject, Status, Priority, WhoId, WhatId, CreatedDate, LastModifiedDate",
            "Lead": "Id, FirstName, LastName, Email, Company, Status, CreatedDate, LastModifiedDate",
            "User": "Id, Name, Email, Username, IsActive, CreatedDate, LastModifiedDate",
            "CampaignMember": "Id, CampaignId, ContactId, LeadId, Status, CreatedDate, LastModifiedDate"
        }
        selected = fields.get(sf_object, "Id, CreatedDate, LastModifiedDate")
        return f"SELECT {selected} FROM {sf_object}"