"""Integration tests with real ZenMoney API.

These tests require ZENMONEY_TOKEN environment variable to be set.
Run with: ZENMONEY_TOKEN=xxx pytest tests/test_integration.py -v
"""

import os

import pytest

from zenmoney_mcp.database import Database
from zenmoney_mcp.sync_engine import SyncEngine


# Skip all tests in this module if ZENMONEY_TOKEN is not set
pytestmark = pytest.mark.skipif(
    os.environ.get("ZENMONEY_TOKEN") is None,
    reason="ZENMONEY_TOKEN environment variable not set",
)


@pytest.fixture
def integration_db() -> Database:
    """Create in-memory database for integration tests."""
    db = Database(":memory:")
    db.init_schema()
    return db


@pytest.fixture
def integration_sync_engine(integration_db: Database) -> SyncEngine:
    """Create sync engine with real token."""
    token = os.environ.get("ZENMONEY_TOKEN")
    return SyncEngine(integration_db, token)


class TestIntegrationSync:
    """Integration tests for sync with real API."""

    @pytest.mark.asyncio
    async def test_full_sync(self, integration_sync_engine: SyncEngine):
        """Test full sync fetches data from API."""
        result = await integration_sync_engine.sync(force_full=True)

        assert result["status"] == "synced"
        assert result["new_server_timestamp"] > 0
        assert result["sync_duration_ms"] >= 0

        # Verify we got some data
        db = integration_sync_engine.db
        assert db.count_table("instruments") > 0
        assert db.count_table("users") > 0
        assert db.count_table("accounts") >= 0  # User might have no accounts
        assert db.count_table("tags") >= 0  # User might have no tags

    @pytest.mark.asyncio
    async def test_incremental_sync(self, integration_sync_engine: SyncEngine):
        """Test incremental sync after full sync."""
        # First full sync
        await integration_sync_engine.sync(force_full=True)
        first_timestamp = integration_sync_engine.db.get_server_timestamp()

        # Second incremental sync
        result = await integration_sync_engine.sync(force_full=False)

        assert result["status"] == "synced"
        # Timestamp should be same or newer
        assert result["new_server_timestamp"] >= first_timestamp

    @pytest.mark.asyncio
    async def test_sync_data_structure(self, integration_sync_engine: SyncEngine):
        """Test that synced data has correct structure."""
        await integration_sync_engine.sync(force_full=True)

        db = integration_sync_engine.db
        conn = db.connect()

        # Check instruments have required fields
        if db.count_table("instruments") > 0:
            row = conn.execute("SELECT * FROM instruments LIMIT 1").fetchone()
            assert row["id"] is not None
            assert row["short_title"] is not None
            assert row["rate"] is not None

        # Check accounts have required fields
        if db.count_table("accounts") > 0:
            row = conn.execute("SELECT * FROM accounts LIMIT 1").fetchone()
            assert row["id"] is not None
            assert row["type"] is not None
            assert row["instrument"] is not None

        # Check user currency is set
        user_currency = db.get_user_currency()
        if user_currency:
            rate = db.get_instrument_rate(user_currency)
            assert rate > 0
