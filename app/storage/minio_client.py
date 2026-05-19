import io
import logging
import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


class MinIOClient:
    """
    Wraps boto3 (S3-compatible) to upload Parquet files to MinIO.
    """

    def __init__(self, settings):
        self.bucket = settings.MINIO_BUCKET
        self.enabled = settings.MINIO_ENABLED

        self.client = boto3.client(
            "s3",
            endpoint_url=f"{'https' if settings.MINIO_SECURE else 'http'}://{settings.MINIO_ENDPOINT}",
            aws_access_key_id=settings.MINIO_ACCESS_KEY,
            aws_secret_access_key=settings.MINIO_SECRET_KEY,
            config=Config(signature_version="s3v4"),
            region_name="us-east-1"3
        )

        self._ensure_bucket_exists()

    def _ensure_bucket_exists(self):
        """Creates bucket if it doesn't exist. Gracefully handles connection failures at startup."""
        if not self.enabled:
            return
        try:
            self.client.head_bucket(Bucket=self.bucket)
            logger.info(f"MinIO bucket exists — {self.bucket}")
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "")
            if error_code == "404":
                self.client.create_bucket(Bucket=self.bucket)
                logger.info(f"MinIO bucket created — {self.bucket}")
            else:
                logger.warning(f"MinIO bucket check failed — {e}")
        except Exception as e:
            logger.warning(f"MinIO not reachable at startup — will retry on first upload. Error: {e}")

    def upload_parquet(self, data: bytes, scan_id: str, org_id: str, sf_object: str) -> str:
        """
        Uploads a Parquet file to MinIO.
        Path convention: {org_id}/{scan_id}/{sf_object}.parquet
        Returns the full MinIO path.
        """
        if not self.enabled:
            logger.info("MinIO disabled — skipping upload")
            return "minio-disabled"

        path = f"{org_id}/{scan_id}/{sf_object}.parquet"

        try:
            self.client.put_object(
                Bucket=self.bucket,
                Key=path,
                Body=io.BytesIO(data),
                ContentType="application/octet-stream"
            )
            logger.info(f"Uploaded to MinIO — bucket={self.bucket} path={path}")
            return path

        except ClientError as e:
            logger.error(f"MinIO upload failed — path={path} error={str(e)}")
            raise

    def file_exists(self, path: str) -> bool:
        """Checks if a file already exists in MinIO."""
        try:
            self.client.head_object(Bucket=self.bucket, Key=path)
            return True
        except ClientError:
            return False

    def get_file_url(self, path: str) -> str:
        """Returns a presigned URL valid for 1 hour."""
        return self.client.generate_presigned_url(
            "get_object",
            Params={"Bucket": self.bucket, "Key": path},
            ExpiresIn=3600
        )