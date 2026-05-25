"""Unit tests for NormalizationService."""
import pytest
import io
import pandas as pd
from app.services.normalization_service import NormalizationService


class TestNormalizationService:

    def setup_method(self):
        self.norm = NormalizationService()

    def test_csv_to_records_basic(self):
        csv_text = "Id,Name,Email\n003abc,Alice,alice@example.com\n003xyz,Bob,bob@example.com\n"
        records = self.norm.csv_to_records(csv_text)
        assert len(records) == 2
        assert records[0]["Id"] == "003abc"
        assert records[0]["Name"] == "Alice"

    def test_csv_to_records_handles_nan(self):
        csv_text = "Id,Name,Email\n001,Alice,\n002,Bob,bob@example.com\n"
        records = self.norm.csv_to_records(csv_text)
        # NaN should be converted to None
        assert records[0]["Email"] is None

    def test_csv_to_parquet_returns_bytes(self):
        csv_text = "Id,Name,CreatedDate\n001,Alice,2026-01-01T00:00:00.000+0000\n"
        parquet_bytes = self.norm.csv_to_parquet(csv_text, "Contact")
        assert isinstance(parquet_bytes, bytes)
        assert len(parquet_bytes) > 0

    def test_parquet_round_trip(self):
        csv_text = "Id,Name\n001,Alice\n002,Bob\n"
        parquet_bytes = self.norm.csv_to_parquet(csv_text, "Contact")

        # Read parquet back and verify content
        df = pd.read_parquet(io.BytesIO(parquet_bytes))
        assert len(df) == 2
        assert "_sf_object" in df.columns
        assert df["_sf_object"].iloc[0] == "Contact"
