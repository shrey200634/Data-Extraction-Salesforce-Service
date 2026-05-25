"""Unit tests for DeduplicationService."""
import pytest
from app.services.deduplication_service import DeduplicationService


class TestDeduplicationService:

    def setup_method(self):
        self.dedup = DeduplicationService()

    def test_empty_list_returns_empty(self):
        assert self.dedup.deduplicate([]) == []

    def test_no_duplicates_returns_same(self):
        records = [
            {"Id": "001", "Name": "Alice"},
            {"Id": "002", "Name": "Bob"},
            {"Id": "003", "Name": "Carol"},
        ]
        result = self.dedup.deduplicate(records)
        assert len(result) == 3

    def test_removes_duplicates_by_id(self):
        records = [
            {"Id": "001", "Name": "Alice"},
            {"Id": "002", "Name": "Bob"},
            {"Id": "001", "Name": "Alice Updated"},  # duplicate Id
        ]
        result = self.dedup.deduplicate(records)
        assert len(result) == 2
        # Last occurrence wins
        alice = next(r for r in result if r["Id"] == "001")
        assert alice["Name"] == "Alice Updated"

    def test_records_without_id_are_kept(self):
        records = [
            {"Name": "Alice"},
            {"Name": "Bob"},
        ]
        result = self.dedup.deduplicate(records)
        assert len(result) == 2

    def test_custom_id_field(self):
        records = [
            {"sf_id": "A", "Name": "Alice"},
            {"sf_id": "A", "Name": "Alice 2"},
            {"sf_id": "B", "Name": "Bob"},
        ]
        result = self.dedup.deduplicate(records, id_field="sf_id")
        assert len(result) == 2
