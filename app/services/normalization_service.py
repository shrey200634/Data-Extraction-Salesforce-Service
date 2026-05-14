import io
import logging
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)


class NormalizationService:
    """
    Converts raw CSV from Salesforce into Parquet format.
    """

    def csv_to_parquet(self, csv_text: str, sf_object: str) -> bytes:
        """
        Takes raw CSV string from Salesforce Bulk API.
        Returns Parquet bytes ready to upload to MinIO.
        """
        try:
            # Read CSV into pandas DataFrame
            df = pd.read_csv(io.StringIO(csv_text))

            if df.empty:
                logger.warning(f"Empty CSV for {sf_object} — returning empty parquet")

            # Clean column names — remove spaces
            df.columns = [col.strip() for col in df.columns]

            # Parse date columns
            date_cols = ["CreatedDate", "LastModifiedDate", "CloseDate"]
            for col in date_cols:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)

            # Add metadata columns
            df["_sf_object"] = sf_object
            df["_extracted_at"] = pd.Timestamp.utcnow()

            # Convert to Parquet bytes
            table = pa.Table.from_pandas(df, preserve_index=False)
            buffer = io.BytesIO()
            pq.write_table(table, buffer, compression="snappy")

            parquet_bytes = buffer.getvalue()
            logger.info(f"CSV→Parquet done — object={sf_object} rows={len(df)} bytes={len(parquet_bytes)}")
            return parquet_bytes

        except Exception as e:
            logger.error(f"Normalization failed — object={sf_object} error={str(e)}")
            raise

    def csv_to_records(self, csv_text: str) -> list:
        """
        Converts CSV text to list of dicts for Kafka publishing.
        """
        try:
            df = pd.read_csv(io.StringIO(csv_text))
            df.columns = [col.strip() for col in df.columns]
            # Convert NaN to None for JSON serialization
            df = df.where(pd.notnull(df), None)
            return df.to_dict(orient="records")
        except Exception as e:
            logger.error(f"CSV to records failed — {str(e)}")
            raise