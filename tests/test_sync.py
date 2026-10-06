"""Tests for sync engine."""

import pytest

from zenmoney_mcp.database import Database
from zenmoney_mcp.sync_engine import SyncEngine


class TestSyncEngine:
    """Test sync engine operations."""

    def test_apply_diff_data_inserts_entities(
        self, sync_engine: SyncEngine, sample_diff_response: dict
    ):
        """Test that apply_diff_data inserts all entity types."""
        result = sync_engine.apply_diff_data(sample_diff_response)

        assert result["status"] == "synced"
        assert result["updated"]["instruments"] == 2
        assert result["updated"]["users"] == 1
        assert result["updated"]["accounts"] == 1
        assert result["updated"]["tags"] == 1
        assert result["updated"]["merchants"] == 1
        assert result["updated"]["transactions"] == 1

        # Verify data in DB
        db = sync_engine.db
        assert db.count_table("instruments") == 2
        assert db.count_table("accounts") == 1
        assert db.count_table("transactions") == 1

    def test_apply_diff_data_saves_server_timestamp(
        self, sync_engine: SyncEngine, sample_diff_response: dict
    ):
        """Test that server timestamp is saved after sync."""
        sync_engine.apply_diff_data(sample_diff_response)

        timestamp = sync_engine.db.get_server_timestamp()
        assert timestamp == 1739261400

    def test_apply_diff_data_handles_deletions(
        self, db: Database, diff_with_deletions: dict
    ):
        """Test that deletion[] entries cause hard deletes."""
        # First insert items that will be deleted
        db.upsert_transactions([
            {
                "id": "tx-to-delete",
                "date": "2026-02-01",
                "user": 1,
                "outcome": 100,
                "outcomeInstrument": 1,
                "outcomeAccount": "acc-1",
                "changed": 1000000,
            }
        ])
        db.upsert_accounts([
            {
                "id": "acc-to-delete",
                "title": "To Delete",
                "type": "ccard",
                "instrument": 1,
                "balance": 0,
                "user": 1,
                "changed": 1000000,
            }
        ])

        assert db.count_table("transactions") == 1
        assert db.count_table("accounts") == 1

        # Apply diff with deletions
        sync_engine = SyncEngine(db, "test_token")
        result = sync_engine.apply_diff_data(diff_with_deletions)

        # Items from diff_with_deletions are inserted
        # tx-to-delete and acc-to-delete are deleted
        assert result["deleted"]["transactions"] == 1
        assert result["deleted"]["accounts"] == 1

        # Verify the new items from diff are present
        conn = db.connect()
        tx = conn.execute("SELECT id FROM transactions").fetchall()
        assert len(tx) == 1
        assert tx[0]["id"] == "tx-1"  # From sample_diff_response

    def test_apply_diff_data_empty_response(self, sync_engine: SyncEngine):
        """Test handling of empty diff response."""
        empty_response = {
            "serverTimestamp": 1739261500,
        }

        result = sync_engine.apply_diff_data(empty_response)

        assert result["status"] == "synced"
        assert result["updated"] == {}
        assert result["deleted"] == {}

    def test_apply_diff_data_updates_existing(self, sync_engine: SyncEngine):
        """Test that upsert updates existing records."""
        # Insert initial data
        initial = {
            "serverTimestamp": 1000000,
            "instrument": [
                {"id": 1, "title": "Old Title", "shortTitle": "RUB", "rate": 1.0, "changed": 1000000}
            ],
        }
        sync_engine.apply_diff_data(initial)

        conn = sync_engine.db.connect()
        row = conn.execute("SELECT title FROM instruments WHERE id = 1").fetchone()
        assert row["title"] == "Old Title"

        # Update with new data
        update = {
            "serverTimestamp": 1000001,
            "instrument": [
                {"id": 1, "title": "New Title", "shortTitle": "RUB", "rate": 1.0, "changed": 1000001}
            ],
        }
        sync_engine.apply_diff_data(update)

        row = conn.execute("SELECT title FROM instruments WHERE id = 1").fetchone()
        assert row["title"] == "New Title"
        assert sync_engine.db.count_table("instruments") == 1  # Still one record

    def test_apply_diff_handles_transaction_tags(self, sync_engine: SyncEngine):
        """Test that transaction tags are stored as JSON array."""
        diff = {
            "serverTimestamp": 1000000,
            "transaction": [
                {
                    "id": "tx-multi-tag",
                    "date": "2026-02-10",
                    "user": 1,
                    "outcome": 100,
                    "outcomeInstrument": 1,
                    "outcomeAccount": "acc-1",
                    "tag": ["tag-1", "tag-2", "tag-3"],
                    "changed": 1000000,
                }
            ],
        }
        sync_engine.apply_diff_data(diff)

        conn = sync_engine.db.connect()
        row = conn.execute("SELECT tag FROM transactions WHERE id = ?", ("tx-multi-tag",)).fetchone()

        import json
        tags = json.loads(row["tag"])
        assert tags == ["tag-1", "tag-2", "tag-3"]

    def test_apply_diff_handles_null_tag(self, sync_engine: SyncEngine):
        """Test that null tag is stored as NULL."""
        diff = {
            "serverTimestamp": 1000000,
            "transaction": [
                {
                    "id": "tx-no-tag",
                    "date": "2026-02-10",
                    "user": 1,
                    "outcome": 100,
                    "outcomeInstrument": 1,
                    "outcomeAccount": "acc-1",
                    "tag": None,
                    "changed": 1000000,
                }
            ],
        }
        sync_engine.apply_diff_data(diff)

        conn = sync_engine.db.connect()
        row = conn.execute("SELECT tag FROM transactions WHERE id = ?", ("tx-no-tag",)).fetchone()
        assert row["tag"] is None


class TestSyncEngineWithPopulatedDB:
    """Tests using populated database fixture."""

    def test_populated_db_has_test_data(self, populated_db: Database):
        """Verify populated_db fixture has expected test data."""
        assert populated_db.count_table("instruments") == 3
        assert populated_db.count_table("users") == 1
        assert populated_db.count_table("accounts") == 5
        assert populated_db.count_table("tags") == 5
        assert populated_db.count_table("merchants") == 2
        assert populated_db.count_table("transactions") == 10
        assert populated_db.count_table("budgets") == 3
        assert populated_db.count_table("reminder_markers") == 3

    def test_populated_db_user_currency(self, populated_db: Database):
        """Verify user currency is set correctly."""
        currency_id = populated_db.get_user_currency()
        assert currency_id == 1  # RUB

    def test_populated_db_instrument_rates(self, populated_db: Database):
        """Verify instrument rates are set correctly."""
        assert populated_db.get_instrument_rate(1) == 1.0  # RUB
        assert populated_db.get_instrument_rate(2) == 90.0  # USD
        assert populated_db.get_instrument_rate(3) == 100.0  # EUR


class TestDeletedReminderMarkers:
    """Deleting a reminder leaves its planned markers behind: the server does not
    send marker updates for them, so the cache must retire them itself."""

    @staticmethod
    def _marker(marker_id: str, state: str, reminder: str = "rem-1") -> dict:
        return {
            "id": marker_id, "user": 1, "reminder": reminder, "date": "2026-10-15",
            "state": state, "income": 0, "outcome": 100, "outcomeAccount": "acc-1",
            "changed": 1000000,
        }

    def _sync_reminder_with_markers(self, sync_engine: SyncEngine) -> None:
        sync_engine.apply_diff_data({
            "serverTimestamp": 1000000,
            "reminder": [{
                "id": "rem-1", "user": 1, "interval": "month", "step": 1,
                "startDate": "2026-01-15", "income": 0, "outcome": 100,
                "outcomeAccount": "acc-1", "changed": 1000000,
            }],
            "reminderMarker": [
                self._marker("rm-planned", "planned"),
                self._marker("rm-processed", "processed"),
            ],
        })

    def test_planned_markers_of_a_deleted_reminder_are_retired(self, sync_engine: SyncEngine):
        self._sync_reminder_with_markers(sync_engine)

        result = sync_engine.apply_diff_data({
            "serverTimestamp": 1000001,
            "deletion": [{"object": "reminder", "id": "rem-1", "stamp": 1000001}],
        })

        states = dict(sync_engine.db.connect().execute(
            "SELECT id, state FROM reminder_markers"
        ).fetchall())
        assert states == {"rm-planned": "deleted", "rm-processed": "processed"}
        assert result["deleted"] == {"reminders": 1, "reminder_markers": 1}

    def test_planned_markers_of_a_live_reminder_are_kept(self, sync_engine: SyncEngine):
        self._sync_reminder_with_markers(sync_engine)

        result = sync_engine.apply_diff_data({"serverTimestamp": 1000001})

        state = sync_engine.db.connect().execute(
            "SELECT state FROM reminder_markers WHERE id = 'rm-planned'"
        ).fetchone()["state"]
        assert state == "planned"
        assert result["deleted"] == {}
