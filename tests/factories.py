"""Row factories for tests: insert one row with sensible defaults, override via kwargs.

Defaults line up with the ``populated_db`` fixture (user 1, instrument 1, account
``acc-rub``). A row with an existing primary key replaces the old one, so a test
can also use these to adjust fixture rows. Lists are stored as JSON, the way
``tag`` columns are.
"""

import json
import uuid
from datetime import date
from typing import Any

from zenmoney_mcp.database import Database


def _insert(db: Database, table: str, defaults: dict[str, Any], overrides: dict[str, Any]) -> dict[str, Any]:
    unknown = overrides.keys() - defaults.keys()
    if unknown:
        raise TypeError(f"Unknown {table} columns: {sorted(unknown)}")

    row = {**defaults, **overrides}
    values = [json.dumps(v) if isinstance(v, list) else v for v in row.values()]
    conn = db.connect()
    conn.execute(
        f"INSERT OR REPLACE INTO {table} ({', '.join(row)}) VALUES ({', '.join('?' * len(row))})",
        values,
    )
    conn.commit()
    return row


def insert_instrument(db: Database, **overrides: Any) -> dict[str, Any]:
    """``rate`` is the price of one unit in the reference currency (instrument 1 = 1.0)."""
    defaults = dict(id=99, title="Test currency", short_title="TST", symbol="T", rate=1.0, changed=1000000)
    return _insert(db, "instruments", defaults, overrides)


def insert_account(db: Database, **overrides: Any) -> dict[str, Any]:
    defaults = dict(
        id=str(uuid.uuid4()), title="Test account", type="ccard", instrument=1, company=None,
        balance=0.0, credit_limit=None, in_balance=1, savings=0, archive=0, user=1,
        role=None, changed=1000000,
    )
    return _insert(db, "accounts", defaults, overrides)


def insert_tag(db: Database, **overrides: Any) -> dict[str, Any]:
    """An expense category by default."""
    defaults = dict(
        id=str(uuid.uuid4()), title="Test category", parent=None, show_income=0, show_outcome=1,
        budget_income=0, budget_outcome=1, required=0, user=1, changed=1000000,
    )
    return _insert(db, "tags", defaults, overrides)


def insert_merchant(db: Database, **overrides: Any) -> dict[str, Any]:
    defaults = dict(id=str(uuid.uuid4()), title="Test merchant", user=1, changed=1000000)
    return _insert(db, "merchants", defaults, overrides)


def insert_transaction(db: Database, **overrides: Any) -> dict[str, Any]:
    """Dated today, both sides on ``acc-rub``; set ``outcome`` or ``income`` (or both for a transfer)."""
    defaults = dict(
        id=str(uuid.uuid4()), date=date.today().isoformat(), user=1, deleted=0, hold=0,
        income=0.0, income_instrument=1, income_account="acc-rub",
        outcome=0.0, outcome_instrument=1, outcome_account="acc-rub",
        tag=None, merchant=None, payee=None, original_payee=None, comment=None, mcc=None,
        op_income=None, op_income_instrument=None, op_outcome=None, op_outcome_instrument=None,
        latitude=None, longitude=None, reminder_marker=None, created=1000000, changed=1000000,
    )
    return _insert(db, "transactions", defaults, overrides)


def insert_budget(db: Database, **overrides: Any) -> dict[str, Any]:
    """``date`` is the first day of the budget month; ``tag`` None means uncategorized."""
    defaults = dict(
        user=1, tag=None, date=date.today().replace(day=1).isoformat(),
        income=0.0, income_lock=0, outcome=0.0, outcome_lock=0, changed=1000000,
    )
    return _insert(db, "budgets", defaults, overrides)


def insert_reminder(db: Database, **overrides: Any) -> dict[str, Any]:
    defaults = dict(
        id=str(uuid.uuid4()), user=1, interval="month", step=1,
        start_date=date.today().isoformat(), end_date=None, income=0.0, outcome=0.0,
        income_account="acc-rub", outcome_account="acc-rub", tag=None, merchant=None,
        payee=None, comment=None, notify=0, changed=1000000,
    )
    return _insert(db, "reminders", defaults, overrides)


def insert_reminder_marker(db: Database, **overrides: Any) -> dict[str, Any]:
    defaults = dict(
        id=str(uuid.uuid4()), user=1, reminder=None, date=date.today().isoformat(),
        state="planned", income=0.0, outcome=0.0, income_account="acc-rub",
        outcome_account="acc-rub", tag=None, merchant=None, payee=None, comment=None,
        changed=1000000,
    )
    return _insert(db, "reminder_markers", defaults, overrides)
