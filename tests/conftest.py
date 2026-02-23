"""Test fixtures for ZenMoney MCP server tests."""

import json
from datetime import date, timedelta

import pytest

from zenmoney_mcp.database import Database
from zenmoney_mcp.sync_engine import SyncEngine


@pytest.fixture
def db() -> Database:
    """Create in-memory database with schema."""
    database = Database(":memory:")
    database.init_schema()
    return database


@pytest.fixture
def populated_db(db: Database) -> Database:
    """Create in-memory database populated with test fixtures.

    Based on test data from spec section 7.8.
    """
    conn = db.connect()

    # Instruments
    instruments = [
        (1, "Российский рубль", "RUB", "₽", 1.0, 1000000),
        (2, "Доллар США", "USD", "$", 90.0, 1000000),
        (3, "Евро", "EUR", "€", 100.0, 1000000),
    ]
    conn.executemany(
        "INSERT INTO instruments (id, title, short_title, symbol, rate, changed) VALUES (?, ?, ?, ?, ?, ?)",
        instruments,
    )

    # Users
    users = [
        (1, "test@example.com", 1, None, 1000000),  # Primary user with RUB currency
    ]
    conn.executemany(
        "INSERT INTO users (id, login, currency, parent, changed) VALUES (?, ?, ?, ?, ?)",
        users,
    )

    # Accounts
    accounts = [
        ("acc-rub", "Тинькофф Black", "ccard", 1, None, 50000.0, 150000.0, 1, 0, 0, 1, None, 1000000),
        ("acc-usd", "Наличные USD", "cash", 2, None, 1000.0, None, 1, 0, 0, 1, None, 1000000),
        ("acc-save", "Накопительный Сбер", "deposit", 1, None, 500000.0, None, 1, 1, 0, 1, None, 1000000),
        ("acc-debt", "Долги", "debt", 1, None, 5000.0, None, 0, 0, 0, 1, None, 1000000),
        ("acc-arch", "Старая карта", "ccard", 1, None, 10000.0, None, 1, 0, 1, 1, None, 1000000),
    ]
    conn.executemany(
        """INSERT INTO accounts
        (id, title, type, instrument, company, balance, credit_limit, in_balance, savings, archive, user, role, changed)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        accounts,
    )

    # Tags
    tags = [
        ("tag-food", "Еда", None, 0, 1, 0, 1, 0, 1, 1000000),
        ("tag-grocery", "Продукты", "tag-food", 0, 1, 0, 1, 0, 1, 1000000),
        ("tag-restaurant", "Рестораны", "tag-food", 0, 1, 0, 1, 0, 1, 1000000),
        ("tag-transport", "Транспорт", None, 0, 1, 0, 1, 0, 1, 1000000),
        ("tag-salary", "Зарплата", None, 1, 0, 1, 0, 0, 1, 1000000),
    ]
    conn.executemany(
        """INSERT INTO tags
        (id, title, parent, show_income, show_outcome, budget_income, budget_outcome, required, user, changed)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        tags,
    )

    # Merchants
    merchants = [
        ("m-pyat", "Пятёрочка", 1, 1000000),
        ("m-yandex", "Яндекс.Такси", 1, 1000000),
    ]
    conn.executemany(
        "INSERT INTO merchants (id, title, user, changed) VALUES (?, ?, ?, ?)",
        merchants,
    )

    # Transactions (current month)
    today = date.today()
    current_month_start = today.replace(day=1)

    def make_date(day_offset: int) -> str:
        return (current_month_start + timedelta(days=day_offset)).isoformat()

    transactions = [
        # tx1: expense - groceries
        ("tx1", make_date(0), 1, 0, 0, 0.0, 1, "acc-rub", 1500.0, 1, "acc-rub",
         json.dumps(["tag-grocery"]), "m-pyat", "Пятёрочка", None, None, 5411, None, None, None, None, None, None, None, 1000000, 1000001),
        # tx2: expense - restaurant
        ("tx2", make_date(1), 1, 0, 0, 0.0, 1, "acc-rub", 3000.0, 1, "acc-rub",
         json.dumps(["tag-restaurant"]), None, "KFC", None, None, 5812, None, None, None, None, None, None, None, 1000000, 1000002),
        # tx3: expense - transport
        ("tx3", make_date(2), 1, 0, 0, 0.0, 1, "acc-rub", 500.0, 1, "acc-rub",
         json.dumps(["tag-transport"]), "m-yandex", "Яндекс.Такси", None, None, 4121, None, None, None, None, None, None, None, 1000000, 1000003),
        # tx4: expense - uncategorized
        ("tx4", make_date(3), 1, 0, 0, 0.0, 1, "acc-rub", 200.0, 1, "acc-rub",
         None, None, None, None, "Кофе", None, None, None, None, None, None, None, None, 1000000, 1000004),
        # tx5: income - salary
        ("tx5", make_date(4), 1, 0, 0, 150000.0, 1, "acc-rub", 0.0, 1, "acc-rub",
         json.dumps(["tag-salary"]), None, "ООО Работа", None, None, None, None, None, None, None, None, None, None, 1000000, 1000005),
        # tx6: transfer between own accounts
        ("tx6", make_date(5), 1, 0, 0, 50000.0, 1, "acc-save", 50000.0, 1, "acc-rub",
         None, None, None, None, "Перевод на накопления", None, None, None, None, None, None, None, None, 1000000, 1000006),
        # tx7: currency exchange
        ("tx7", make_date(6), 1, 0, 0, 100.0, 2, "acc-usd", 9000.0, 1, "acc-rub",
         None, None, None, None, "Покупка долларов", None, None, None, None, None, None, None, None, 1000000, 1000007),
        # tx8: debt - lent money
        ("tx8", make_date(7), 1, 0, 0, 5000.0, 1, "acc-debt", 5000.0, 1, "acc-rub",
         None, None, "Паша", None, "До зарплаты", None, None, None, None, None, None, None, None, 1000000, 1000008),
        # tx9: deleted expense
        ("tx9", make_date(8), 1, 1, 0, 0.0, 1, "acc-rub", 800.0, 1, "acc-rub",
         json.dumps(["tag-grocery"]), "m-pyat", "Пятёрочка", None, None, 5411, None, None, None, None, None, None, None, 1000000, 1000009),
        # tx10: hold expense
        ("tx10", make_date(9), 1, 0, 1, 0.0, 1, "acc-rub", 350.0, 1, "acc-rub",
         json.dumps(["tag-grocery"]), "m-pyat", "Пятёрочка", None, None, 5411, None, None, None, None, None, None, None, 1000000, 1000010),
    ]
    conn.executemany(
        """INSERT INTO transactions
        (id, date, user, deleted, hold, income, income_instrument, income_account,
         outcome, outcome_instrument, outcome_account, tag, merchant, payee, original_payee,
         comment, mcc, op_income, op_income_instrument, op_outcome, op_outcome_instrument,
         latitude, longitude, reminder_marker, created, changed)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        transactions,
    )

    # Budgets (current month)
    month_start = current_month_start.isoformat()
    budgets = [
        (1, "tag-food", month_start, 0.0, 0, 10000.0, 1, 1000000),
        (1, "tag-transport", month_start, 0.0, 0, 5000.0, 1, 1000000),
        (1, "00000000-0000-0000-0000-000000000000", month_start, 0.0, 0, 80000.0, 1, 1000000),  # Total budget
    ]
    conn.executemany(
        """INSERT INTO budgets
        (user, tag, date, income, income_lock, outcome, outcome_lock, changed)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        budgets,
    )

    # Reminder markers
    rm_date1 = (today + timedelta(days=5)).isoformat()
    rm_date2 = (today + timedelta(days=15)).isoformat()
    rm_date3 = (today - timedelta(days=5)).isoformat()

    reminder_markers = [
        ("rm1", 1, None, rm_date1, "planned", 0.0, 45000.0, None, "acc-rub", None, None, "Аренда", None, 1000000),
        ("rm2", 1, None, rm_date2, "planned", 0.0, 2000.0, None, "acc-rub", None, None, "Паша", "Возврат долга", 1000000),
        ("rm3", 1, None, rm_date3, "processed", 0.0, 1000.0, None, "acc-rub", None, None, "Тест", None, 1000000),
    ]
    conn.executemany(
        """INSERT INTO reminder_markers
        (id, user, reminder, date, state, income, outcome, income_account, outcome_account,
         tag, merchant, payee, comment, changed)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        reminder_markers,
    )

    conn.commit()
    return db


@pytest.fixture
def sync_engine(db: Database) -> SyncEngine:
    """Create sync engine with test database."""
    return SyncEngine(db, "test_token")


@pytest.fixture
def sample_diff_response() -> dict:
    """Sample /v8/diff/ API response for testing sync."""
    return {
        "serverTimestamp": 1739261400,
        "instrument": [
            {"id": 1, "title": "Российский рубль", "shortTitle": "RUB", "symbol": "₽", "rate": 1.0, "changed": 1000000},
            {"id": 2, "title": "Доллар США", "shortTitle": "USD", "symbol": "$", "rate": 90.0, "changed": 1000000},
        ],
        "user": [
            {"id": 1, "login": "test@example.com", "currency": 1, "parent": None, "changed": 1000000},
        ],
        "account": [
            {
                "id": "acc-1",
                "title": "Test Account",
                "type": "ccard",
                "instrument": 1,
                "balance": 10000.0,
                "inBalance": True,
                "savings": False,
                "archive": False,
                "user": 1,
                "changed": 1000000,
            },
        ],
        "tag": [
            {"id": "tag-1", "title": "Test Category", "parent": None, "showOutcome": True, "changed": 1000000},
        ],
        "merchant": [
            {"id": "m-1", "title": "Test Merchant", "user": 1, "changed": 1000000},
        ],
        "transaction": [
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
                "tag": ["tag-1"],
                "merchant": "m-1",
                "payee": "Test Merchant",
                "changed": 1000000,
            },
        ],
        "deletion": [],
    }


@pytest.fixture
def diff_with_deletions(sample_diff_response: dict) -> dict:
    """Diff response with deletion entries."""
    response = sample_diff_response.copy()
    response["deletion"] = [
        {"object": "transaction", "id": "tx-to-delete", "stamp": 1000001},
        {"object": "account", "id": "acc-to-delete", "stamp": 1000001},
    ]
    return response
