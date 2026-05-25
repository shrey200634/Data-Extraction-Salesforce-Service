"""Unit tests for SOQL query building, including incremental extraction."""
import pytest
from unittest.mock import MagicMock
from app.clients.bulk_api_client import SalesforceBulkAPIClient


class TestQueryBuilder:

    def setup_method(self):
        token_mgr = MagicMock()
        self.client = SalesforceBulkAPIClient(token_mgr)

    def test_full_extraction_query_contact(self):
        query = self.client.build_query("Contact")
        assert "SELECT" in query
        assert "FROM Contact" in query
        assert "ORDER BY LastModifiedDate ASC" in query
        assert "WHERE" not in query

    def test_full_extraction_query_account(self):
        query = self.client.build_query("Account")
        assert "FROM Account" in query
        assert "Industry" in query

    def test_incremental_query_adds_where_clause(self):
        query = self.client.build_query(
            "Contact",
            last_modified_after="2026-01-01T00:00:00Z"
        )
        assert "WHERE LastModifiedDate >= 2026-01-01T00:00:00Z" in query
        assert "ORDER BY LastModifiedDate ASC" in query

    def test_incremental_query_for_all_objects(self):
        cutoff = "2026-05-01T00:00:00Z"
        for obj in ["Contact", "Account", "Opportunity", "Task", "Lead", "User", "CampaignMember"]:
            query = self.client.build_query(obj, last_modified_after=cutoff)
            assert f"FROM {obj}" in query
            assert f"WHERE LastModifiedDate >= {cutoff}" in query
