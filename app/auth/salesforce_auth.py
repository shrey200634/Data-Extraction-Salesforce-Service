import time
import logging
import requests
import jwt
from cryptography.hazmat.primitives import serialization

logger = logging.getLogger(__name__)


class SalesforceTokenManager:
    """
    Handles OAuth 2.0 JWT Bearer flow with Salesforce.
    Caches the token and auto-refreshes 5 min before expiry.

    Java equivalent: A @Component that wraps OAuth2RestTemplate
    with token refresh logic.
    """

    def __init__(self, settings):
        self.consumer_key = settings.SF_CONSUMER_KEY
        self.private_key_pem = settings.SF_PRIVATE_KEY_PEM
        self.username = settings.SF_USERNAME
        self.login_url = settings.SF_LOGIN_URL

        self._access_token = None
        self._instance_url = None
        self._token_expiry = 0

    def get_token(self):
        """
        Returns a valid access token.
        Refreshes automatically if expired or about to expire.
        """
        if self._is_token_valid():
            logger.debug("Using cached Salesforce token")
            return self._access_token, self._instance_url

        logger.info("Fetching new Salesforce access token")
        return self._fetch_token()

    def _is_token_valid(self):
        """Token valid if it exists and doesn't expire in next 5 min."""
        if not self._access_token:
            return False
        return time.time() < (self._token_expiry - 300)

    def _fetch_token(self):
        """
        Makes the JWT Bearer OAuth call to Salesforce.
        Returns (access_token, instance_url).
        """
        try:
            # Build JWT assertion
            jwt_payload = {
                "iss": self.consumer_key,
                "sub": self.username,
                "aud": self.login_url,
                "exp": int(time.time()) + 300  # 5 min expiry
            }

            # Load private key — pre-validated at config time by fix_private_key
            # so this should never fail for a properly configured service
            private_key = serialization.load_pem_private_key(
                self.private_key_pem.encode("utf-8"),
                password=None,
            )

            # Sign the JWT
            assertion = jwt.encode(
                jwt_payload,
                private_key,
                algorithm="RS256"
            )

            # Exchange JWT for access token
            token_url = f"{self.login_url}/services/oauth2/token"
            response = requests.post(token_url, data={
                "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
                "assertion": assertion
            })
            response.raise_for_status()

            data = response.json()
            self._access_token = data["access_token"]
            self._instance_url = data["instance_url"]
            self._token_expiry = time.time() + 3600  # assume 1hr expiry

            logger.info(f"Salesforce token obtained — instance={self._instance_url}")
            return self._access_token, self._instance_url

        except requests.HTTPError as e:
            logger.error(f"Salesforce auth failed: {e.response.text}")
            raise
        except Exception as e:
            logger.error(f"Salesforce token error: {str(e)}")
            raise

    def invalidate(self):
        """Force token refresh on next call."""
        self._access_token = None
        self._token_expiry = 0
        logger.info("Salesforce token invalidated")