"""Tests for database schema and CRUD operations."""

import json

import pytest

from zenmoney_mcp.database import Database


class TestDatabaseSchema:
    """Test database schema creation."""

    def test_init_schema_creates_tables(self, db: Database):
        """Test that init_schema creates all required tables."""
        conn = db.connect()

        # List all tables
        tables = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        ).fetchall()
        table_names = {row["name"] for row in tables}

        expected_tables = {
            "accounts",
            "budgets",
            "companies",
            "instruments",
            "merchants",
            "reminder_markers",
            "reminders",
            "sync_meta",
            "tags",
            "transactions",
            "users",
        }

        assert expected_tables <= table_names

    def test_init_schema_creates_indexes(self, db: Database):
        """Test that init_schema creates all required indexes."""
        conn = db.connect()

        indexes = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index' AND name LIKE 'idx_%'"
        ).fetchall()
        index_names = {row["name"] for row in indexes}

        expected_indexes = {
            "idx_accounts_type",
            "idx_accounts_archive",
            "idx_tags_parent",
            "idx_tx_date",
            "idx_tx_deleted",
            "idx_tx_income_account",
            "idx_tx_outcome_account",
            "idx_budgets_date",
            "idx_rm_state",
            "idx_rm_date",
        }

        assert expected_indexes <= index_names


class TestSyncMeta:
    """Test sync metadata operations."""

    def test_get_set_meta(self, db: Database):
        """Test getting and setting metadata."""
        assert db.get_meta("test_key") is None

        db.set_meta("test_key", "test_value")
        assert db.get_meta("test_key") == "test_value"

        db.set_meta("test_key", "new_value")
        assert db.get_meta("test_key") == "new_value"

    def test_server_timestamp(self, db: Database):
        """Test server timestamp operations."""
        assert db.get_server_timestamp() == 0

        db.set_server_timestamp(1739261400)
        assert db.get_server_timestamp() == 1739261400


class TestUpsertOperations:
    """Test upsert operations for all entity types."""

    def test_upsert_instruments(self, db: Database):
        """Test upserting instruments."""
        items = [
            {"id": 1, "title": "Рубль", "shortTitle": "RUB", "symbol": "₽", "rate": 1.0, "changed": 1000000},
            {"id": 2, "title": "Доллар", "shortTitle": "USD", "symbol": "$", "rate": 90.0, "changed": 1000000},
        ]

        count = db.upsert_instruments(items)
        assert count == 2
        assert db.count_table("instruments") == 2

        # Update one
        items_update = [{"id": 1, "title": "Российский рубль", "shortTitle": "RUB", "symbol": "₽", "rate": 1.0, "changed": 1000001}]
        count = db.upsert_instruments(items_update)
        assert count == 1
        assert db.count_table("instruments") == 2  # Still 2

    def test_upsert_accounts(self, db: Database):
        """Test upserting accounts."""
        items = [
            {
                "id": "acc-1",
                "title": "Test Account",
                "type": "ccard",
                "instrument": 1,
                "balance": 10000,
                "creditLimit": 50000,
                "inBalance": True,
                "savings": False,
                "archive": False,
                "user": 1,
                "changed": 1000000,
            }
        ]

        count = db.upsert_accounts(items)
        assert count == 1

        conn = db.connect()
        row = conn.execute("SELECT * FROM accounts WHERE id = ?", ("acc-1",)).fetchone()
        assert row["title"] == "Test Account"
        assert row["in_balance"] == 1
        assert row["savings"] == 0
        assert row["archive"] == 0

    def test_upsert_tags(self, db: Database):
        """Test upserting tags with parent relationship."""
        items = [
            {"id": "tag-parent", "title": "Food", "parent": None, "showOutcome": True, "changed": 1000000},
            {"id": "tag-child", "title": "Groceries", "parent": "tag-parent", "showOutcome": True, "changed": 1000000},
        ]

        count = db.upsert_tags(items)
        assert count == 2

        conn = db.connect()
        child = conn.execute("SELECT * FROM tags WHERE id = ?", ("tag-child",)).fetchone()
        assert child["parent"] == "tag-parent"

    def test_upsert_transactions(self, db: Database):
        """Test upserting transactions with tag array."""
        items = [
            {
                "id": "tx-1",
                "date": "2026-02-10",
                "user": 1,
                "deleted": False,
                "hold": False,
                "income": 0,
                "incomeInstrument": 1,
                "incomeAccount": "acc-1",
                "outcome": 500,
                "outcomeInstrument": 1,
                "outcomeAccount": "acc-1",
                "tag": ["tag-1", "tag-2"],
                "payee": "Test Store",
                "changed": 1000000,
            }
        ]

        count = db.upsert_transactions(items)
        assert count == 1

        conn = db.connect()
        row = conn.execute("SELECT * FROM transactions WHERE id = ?", ("tx-1",)).fetchone()
        assert row["outcome"] == 500
        assert row["deleted"] == 0
        assert json.loads(row["tag"]) == ["tag-1", "tag-2"]

    def test_upsert_budgets(self, db: Database):
        """Test upserting budgets."""
        items = [
            {
                "user": 1,
                "tag": "tag-1",
                "date": "2026-02-01",
                "income": 0,
                "incomeLock": False,
                "outcome": 10000,
                "outcomeLock": True,
                "changed": 1000000,
            }
        ]

        count = db.upsert_budgets(items)
        assert count == 1

        conn = db.connect()
        row = conn.execute("SELECT * FROM budgets WHERE tag = ?", ("tag-1",)).fetchone()
        assert row["outcome"] == 10000
        assert row["outcome_lock"] == 1


class TestDeleteOperations:
    """Test hard delete operations for deletion[]."""

    def test_delete_by_ids(self, db: Database):
        """Test deleting records by IDs."""
        # First insert some data
        db.upsert_instruments([
            {"id": 1, "title": "A", "changed": 1},
            {"id": 2, "title": "B", "changed": 1},
            {"id": 3, "title": "C", "changed": 1},
        ])
        assert db.count_table("instruments") == 3

        # Delete some
        count = db.delete_by_ids("instruments", [1, 3])
        assert count == 2
        assert db.count_table("instruments") == 1

    def test_delete_empty_list(self, db: Database):
        """Test deleting with empty list does nothing."""
        db.upsert_instruments([{"id": 1, "title": "A", "changed": 1}])
        count = db.delete_by_ids("instruments", [])
        assert count == 0
        assert db.count_table("instruments") == 1


class TestQueryHelpers:
    """Test query helper methods."""

    def test_get_user_currency(self, populated_db: Database):
        """Test getting user's primary currency."""
        currency_id = populated_db.get_user_currency()
        assert currency_id == 1  # RUB

    def test_get_instrument_rate(self, populated_db: Database):
        """Test getting instrument exchange rate."""
        rub_rate = populated_db.get_instrument_rate(1)
        usd_rate = populated_db.get_instrument_rate(2)

        assert rub_rate == 1.0
        assert usd_rate == 90.0

    def test_count_table(self, populated_db: Database):
        """Test counting table rows."""
        assert populated_db.count_table("accounts") == 5
        assert populated_db.count_table("tags") == 5
        assert populated_db.count_table("transactions") == 10
