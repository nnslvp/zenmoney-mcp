"""SQLite database schema and CRUD operations for ZenMoney cache."""

import json
import sqlite3
from pathlib import Path
from typing import Any


SCHEMA = """
-- Системные справочники
CREATE TABLE IF NOT EXISTS instruments (
    id          INTEGER PRIMARY KEY,
    title       TEXT,
    short_title TEXT,    -- 'USD', 'RUB', 'EUR'
    symbol      TEXT,    -- '$', '₽', '€'
    rate        REAL,    -- стоимость 1 единицы в рублях
    changed     INTEGER
);

CREATE TABLE IF NOT EXISTS companies (
    id          INTEGER PRIMARY KEY,
    title       TEXT,
    country     TEXT,
    changed     INTEGER
);

CREATE TABLE IF NOT EXISTS users (
    id       INTEGER PRIMARY KEY,
    login    TEXT,
    currency INTEGER,  -- основная валюта пользователя (instruments.id)
    parent   INTEGER,
    changed  INTEGER
);

-- Пользовательские сущности
CREATE TABLE IF NOT EXISTS accounts (
    id              TEXT PRIMARY KEY,  -- UUID
    title           TEXT,
    type            TEXT,     -- 'cash','ccard','checking','loan','deposit','emoney','debt'
    instrument      INTEGER,  -- instruments.id
    company         INTEGER,
    balance         REAL,
    credit_limit    REAL,
    in_balance      INTEGER,  -- bool: учитывать в общем балансе
    savings         INTEGER,  -- bool: накопительный
    archive         INTEGER,  -- bool: архивный
    user            INTEGER,
    role            INTEGER,
    changed         INTEGER
);

CREATE TABLE IF NOT EXISTS tags (
    id              TEXT PRIMARY KEY,  -- UUID
    title           TEXT,
    parent          TEXT,  -- tags.id, один уровень вложенности
    show_income     INTEGER,
    show_outcome    INTEGER,
    budget_income   INTEGER,
    budget_outcome  INTEGER,
    required        INTEGER,
    user            INTEGER,
    changed         INTEGER
);

CREATE TABLE IF NOT EXISTS merchants (
    id      TEXT PRIMARY KEY,  -- UUID
    title   TEXT,
    user    INTEGER,
    changed INTEGER
);

CREATE TABLE IF NOT EXISTS transactions (
    id                   TEXT PRIMARY KEY,  -- UUID
    date                 TEXT,     -- 'YYYY-MM-DD'
    user                 INTEGER,
    deleted              INTEGER DEFAULT 0,
    hold                 INTEGER,
    income               REAL DEFAULT 0,
    income_instrument    INTEGER,
    income_account       TEXT,
    outcome              REAL DEFAULT 0,
    outcome_instrument   INTEGER,
    outcome_account      TEXT,
    tag                  TEXT,     -- JSON массив UUID: '["uuid1","uuid2"]'
    merchant             TEXT,  -- merchants.id
    payee                TEXT,
    original_payee       TEXT,
    comment              TEXT,
    mcc                  INTEGER,
    op_income            REAL,
    op_income_instrument INTEGER,
    op_outcome           REAL,
    op_outcome_instrument INTEGER,
    latitude             REAL,
    longitude            REAL,
    reminder_marker      TEXT,
    created              INTEGER,
    changed              INTEGER
);

CREATE TABLE IF NOT EXISTS budgets (
    user         INTEGER,
    tag          TEXT,     -- Tag.id, NULL (без категории), или '0000...0000' (итого)
    date         TEXT,     -- 'YYYY-MM-DD' первый день месяца
    income       REAL,
    income_lock  INTEGER,
    outcome      REAL,
    outcome_lock INTEGER,
    changed      INTEGER,
    PRIMARY KEY (tag, date, user)
);

CREATE TABLE IF NOT EXISTS reminders (
    id        TEXT PRIMARY KEY,
    user      INTEGER,
    interval  TEXT,    -- 'day','week','month','year' или NULL
    step      INTEGER,
    start_date TEXT,
    end_date   TEXT,
    income     REAL,
    outcome    REAL,
    income_account  TEXT,
    outcome_account TEXT,
    tag        TEXT,
    merchant   TEXT,
    payee      TEXT,
    comment    TEXT,
    notify     INTEGER,
    changed    INTEGER
);

CREATE TABLE IF NOT EXISTS reminder_markers (
    id        TEXT PRIMARY KEY,
    user      INTEGER,
    reminder  TEXT,  -- reminders.id
    date      TEXT,
    state     TEXT,   -- 'planned','processed','deleted'
    income    REAL,
    outcome   REAL,
    income_account  TEXT,
    outcome_account TEXT,
    tag       TEXT,
    merchant  TEXT,
    payee     TEXT,
    comment   TEXT,
    changed   INTEGER
);

-- Мета-информация синхронизации
CREATE TABLE IF NOT EXISTS sync_meta (
    key   TEXT PRIMARY KEY,
    value TEXT
);
"""

INDEXES = """
CREATE INDEX IF NOT EXISTS idx_accounts_type ON accounts(type);
CREATE INDEX IF NOT EXISTS idx_accounts_archive ON accounts(archive);
CREATE INDEX IF NOT EXISTS idx_tags_parent ON tags(parent);
CREATE INDEX IF NOT EXISTS idx_tx_date ON transactions(date);
CREATE INDEX IF NOT EXISTS idx_tx_deleted ON transactions(deleted);
CREATE INDEX IF NOT EXISTS idx_tx_income_account ON transactions(income_account);
CREATE INDEX IF NOT EXISTS idx_tx_outcome_account ON transactions(outcome_account);
CREATE INDEX IF NOT EXISTS idx_budgets_date ON budgets(date);
CREATE INDEX IF NOT EXISTS idx_rm_state ON reminder_markers(state);
CREATE INDEX IF NOT EXISTS idx_rm_date ON reminder_markers(date);
"""


class Database:
    """SQLite database wrapper for ZenMoney cache."""

    def __init__(self, db_path: str | Path | None = None):
        """Initialize database connection.

        Args:
            db_path: Path to SQLite file, or None/":memory:" for in-memory DB.
        """
        if db_path is None:
            db_path = ":memory:"
        self.db_path = str(db_path)
        self._conn: sqlite3.Connection | None = None

    def connect(self) -> sqlite3.Connection:
        """Get or create database connection."""
        if self._conn is None:
            self._conn = sqlite3.connect(self.db_path, check_same_thread=False)
            self._conn.row_factory = sqlite3.Row
            # Enable WAL mode for better concurrency (only for file-based DBs)
            if self.db_path != ":memory:":
                self._conn.execute("PRAGMA journal_mode=WAL")
        return self._conn

    def close(self) -> None:
        """Close database connection."""
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def init_schema(self) -> None:
        """Create all tables and indexes."""
        conn = self.connect()
        conn.executescript(SCHEMA)
        conn.executescript(INDEXES)
        conn.commit()

    # -------------------------------------------------------------------------
    # Sync metadata
    # -------------------------------------------------------------------------

    def get_meta(self, key: str) -> str | None:
        """Get metadata value by key."""
        conn = self.connect()
        row = conn.execute(
            "SELECT value FROM sync_meta WHERE key = ?", (key,)
        ).fetchone()
        return row["value"] if row else None

    def set_meta(self, key: str, value: str) -> None:
        """Set metadata value."""
        conn = self.connect()
        conn.execute(
            "INSERT OR REPLACE INTO sync_meta (key, value) VALUES (?, ?)",
            (key, value),
        )
        conn.commit()

    def get_server_timestamp(self) -> int:
        """Get last sync server timestamp, or 0 if never synced."""
        val = self.get_meta("server_timestamp")
        return int(val) if val else 0

    def set_server_timestamp(self, ts: int) -> None:
        """Save server timestamp after sync."""
        self.set_meta("server_timestamp", str(ts))

    # -------------------------------------------------------------------------
    # Generic upsert for all entity types
    # -------------------------------------------------------------------------

    def upsert_instruments(self, items: list[dict[str, Any]]) -> int:
        """Upsert instruments from diff response."""
        conn = self.connect()
        count = 0
        for item in items:
            conn.execute(
                """
                INSERT OR REPLACE INTO instruments
                (id, title, short_title, symbol, rate, changed)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    item["id"],
                    item.get("title"),
                    item.get("shortTitle"),
                    item.get("symbol"),
                    item.get("rate"),
                    item.get("changed"),
                ),
            )
            count += 1
        conn.commit()
        return count

    def upsert_companies(self, items: list[dict[str, Any]]) -> int:
        """Upsert companies from diff response."""
        conn = self.connect()
        count = 0
        for item in items:
            conn.execute(
                """
                INSERT OR REPLACE INTO companies
                (id, title, country, changed)
                VALUES (?, ?, ?, ?)
                """,
                (
                    item["id"],
                    item.get("title"),
                    item.get("country"),
                    item.get("changed"),
                ),
            )
            count += 1
        conn.commit()
        return count

    def upsert_users(self, items: list[dict[str, Any]]) -> int:
        """Upsert users from diff response."""
        conn = self.connect()
        count = 0
        for item in items:
            conn.execute(
                """
                INSERT OR REPLACE INTO users
                (id, login, currency, parent, changed)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    item["id"],
                    item.get("login"),
                    item.get("currency"),
                    item.get("parent"),
                    item.get("changed"),
                ),
            )
            count += 1
        conn.commit()
        return count

    def upsert_accounts(self, items: list[dict[str, Any]]) -> int:
        """Upsert accounts from diff response."""
        conn = self.connect()
        count = 0
        for item in items:
            conn.execute(
                """
                INSERT OR REPLACE INTO accounts
                (id, title, type, instrument, company, balance, credit_limit,
                 in_balance, savings, archive, user, role, changed)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    item["id"],
                    item.get("title"),
                    item.get("type"),
                    item.get("instrument"),
                    item.get("company"),
                    item.get("balance"),
                    item.get("creditLimit"),
                    1 if item.get("inBalance", True) else 0,
                    1 if item.get("savings", False) else 0,
                    1 if item.get("archive", False) else 0,
                    item.get("user"),
                    item.get("role"),
                    item.get("changed"),
                ),
            )
            count += 1
        conn.commit()
        return count

    def upsert_tags(self, items: list[dict[str, Any]]) -> int:
        """Upsert tags from diff response."""
        conn = self.connect()
        count = 0
        for item in items:
            conn.execute(
                """
                INSERT OR REPLACE INTO tags
                (id, title, parent, show_income, show_outcome,
                 budget_income, budget_outcome, required, user, changed)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    item["id"],
                    item.get("title"),
                    item.get("parent"),
                    1 if item.get("showIncome", False) else 0,
                    1 if item.get("showOutcome", False) else 0,
                    1 if item.get("budgetIncome", False) else 0,
                    1 if item.get("budgetOutcome", False) else 0,
                    1 if item.get("required") else 0,
                    item.get("user"),
                    item.get("changed"),
                ),
            )
            count += 1
        conn.commit()
        return count

    def upsert_merchants(self, items: list[dict[str, Any]]) -> int:
        """Upsert merchants from diff response."""
        conn = self.connect()
        count = 0
        for item in items:
            conn.execute(
                """
                INSERT OR REPLACE INTO merchants (id, title, user, changed)
                VALUES (?, ?, ?, ?)
                """,
                (
                    item["id"],
                    item.get("title"),
                    item.get("user"),
                    item.get("changed"),
                ),
            )
            count += 1
        conn.commit()
        return count

    def upsert_transactions(self, items: list[dict[str, Any]]) -> int:
        """Upsert transactions from diff response."""
        conn = self.connect()
        count = 0
        for item in items:
            # tag is a list of UUIDs, store as JSON
            tag_value = item.get("tag")
            if isinstance(tag_value, list):
                tag_json = json.dumps(tag_value)
            elif tag_value is None:
                tag_json = None
            else:
                tag_json = json.dumps([tag_value])

            conn.execute(
                """
                INSERT OR REPLACE INTO transactions
                (id, date, user, deleted, hold, income, income_instrument, income_account,
                 outcome, outcome_instrument, outcome_account, tag, merchant, payee,
                 original_payee, comment, mcc, op_income, op_income_instrument,
                 op_outcome, op_outcome_instrument, latitude, longitude,
                 reminder_marker, created, changed)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    item["id"],
                    item.get("date"),
                    item.get("user"),
                    1 if item.get("deleted", False) else 0,
                    1 if item.get("hold", False) else 0,
                    item.get("income", 0),
                    item.get("incomeInstrument"),
                    item.get("incomeAccount"),
                    item.get("outcome", 0),
                    item.get("outcomeInstrument"),
                    item.get("outcomeAccount"),
                    tag_json,
                    item.get("merchant"),
                    item.get("payee"),
                    item.get("originalPayee"),
                    item.get("comment"),
                    item.get("mcc"),
                    item.get("opIncome"),
                    item.get("opIncomeInstrument"),
                    item.get("opOutcome"),
                    item.get("opOutcomeInstrument"),
                    item.get("latitude"),
                    item.get("longitude"),
                    item.get("reminderMarker"),
                    item.get("created"),
                    item.get("changed"),
                ),
            )
            count += 1
        conn.commit()
        return count

    def upsert_budgets(self, items: list[dict[str, Any]]) -> int:
        """Upsert budgets from diff response."""
        conn = self.connect()
        count = 0
        for item in items:
            conn.execute(
                """
                INSERT OR REPLACE INTO budgets
                (user, tag, date, income, income_lock, outcome, outcome_lock, changed)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    item.get("user"),
                    item.get("tag"),
                    item.get("date"),
                    item.get("income"),
                    1 if item.get("incomeLock", False) else 0,
                    item.get("outcome"),
                    1 if item.get("outcomeLock", False) else 0,
                    item.get("changed"),
                ),
            )
            count += 1
        conn.commit()
        return count

    def upsert_reminders(self, items: list[dict[str, Any]]) -> int:
        """Upsert reminders from diff response."""
        conn = self.connect()
        count = 0
        for item in items:
            # tag can be a list
            tag_value = item.get("tag")
            if isinstance(tag_value, list):
                tag_json = json.dumps(tag_value)
            elif tag_value is None:
                tag_json = None
            else:
                tag_json = json.dumps([tag_value])

            conn.execute(
                """
                INSERT OR REPLACE INTO reminders
                (id, user, interval, step, start_date, end_date, income, outcome,
                 income_account, outcome_account, tag, merchant, payee, comment, notify, changed)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    item["id"],
                    item.get("user"),
                    item.get("interval"),
                    item.get("step"),
                    item.get("startDate"),
                    item.get("endDate"),
                    item.get("income"),
                    item.get("outcome"),
                    item.get("incomeAccount"),
                    item.get("outcomeAccount"),
                    tag_json,
                    item.get("merchant"),
                    item.get("payee"),
                    item.get("comment"),
                    1 if item.get("notify", False) else 0,
                    item.get("changed"),
                ),
            )
            count += 1
        conn.commit()
        return count

    def upsert_reminder_markers(self, items: list[dict[str, Any]]) -> int:
        """Upsert reminder markers from diff response."""
        conn = self.connect()
        count = 0
        for item in items:
            # tag can be a list
            tag_value = item.get("tag")
            if isinstance(tag_value, list):
                tag_json = json.dumps(tag_value)
            elif tag_value is None:
                tag_json = None
            else:
                tag_json = json.dumps([tag_value])

            conn.execute(
                """
                INSERT OR REPLACE INTO reminder_markers
                (id, user, reminder, date, state, income, outcome,
                 income_account, outcome_account, tag, merchant, payee, comment, changed)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    item["id"],
                    item.get("user"),
                    item.get("reminder"),
                    item.get("date"),
                    item.get("state"),
                    item.get("income"),
                    item.get("outcome"),
                    item.get("incomeAccount"),
                    item.get("outcomeAccount"),
                    tag_json,
                    item.get("merchant"),
                    item.get("payee"),
                    item.get("comment"),
                    item.get("changed"),
                ),
            )
            count += 1
        conn.commit()
        return count

    # -------------------------------------------------------------------------
    # Hard delete for deletion[] from diff
    # -------------------------------------------------------------------------

    def delete_by_ids(self, table: str, ids: list[str | int]) -> int:
        """Delete records by IDs (hard delete from deletion[])."""
        if not ids:
            return 0
        conn = self.connect()
        placeholders = ",".join("?" * len(ids))
        cursor = conn.execute(
            f"DELETE FROM {table} WHERE id IN ({placeholders})", ids  # noqa: S608
        )
        conn.commit()
        return cursor.rowcount

    # -------------------------------------------------------------------------
    # Query helpers
    # -------------------------------------------------------------------------

    def count_table(self, table: str) -> int:
        """Count rows in a table."""
        conn = self.connect()
        row = conn.execute(f"SELECT COUNT(*) as cnt FROM {table}").fetchone()  # noqa: S608
        return row["cnt"]

    def get_user_currency(self) -> int | None:
        """Get primary user's currency instrument ID."""
        conn = self.connect()
        row = conn.execute(
            "SELECT currency FROM users WHERE parent IS NULL LIMIT 1"
        ).fetchone()
        return row["currency"] if row else None

    def get_instrument_rate(self, instrument_id: int) -> float:
        """Get instrument rate (cost of 1 unit in RUB)."""
        conn = self.connect()
        row = conn.execute(
            "SELECT rate FROM instruments WHERE id = ?", (instrument_id,)
        ).fetchone()
        return row["rate"] if row and row["rate"] else 1.0
