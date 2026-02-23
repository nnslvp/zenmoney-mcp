"""Tests for utility functions."""

import pytest

from zenmoney_mcp.database import Database
from zenmoney_mcp.utils import (
    classify_transaction,
    convert_to_user_currency,
    is_pure_expense,
    is_pure_income,
    is_transfer,
)


class TestCurrencyConversion:
    """Test currency conversion functions."""

    def test_convert_same_currency(self, populated_db: Database):
        """Test conversion when source and target currencies are the same."""
        # RUB to RUB (user currency is RUB)
        result = convert_to_user_currency(1000, 1, populated_db)
        assert result == 1000.0

    def test_convert_usd_to_rub(self, populated_db: Database):
        """Test conversion from USD to RUB."""
        # 100 USD * 90 (rate) / 1 (RUB rate) = 9000 RUB
        result = convert_to_user_currency(100, 2, populated_db)
        assert result == 9000.0

    def test_convert_eur_to_rub(self, populated_db: Database):
        """Test conversion from EUR to RUB."""
        # 100 EUR * 100 (rate) / 1 (RUB rate) = 10000 RUB
        result = convert_to_user_currency(100, 3, populated_db)
        assert result == 10000.0

    def test_convert_with_explicit_user_currency(self, populated_db: Database):
        """Test conversion with explicit user currency ID."""
        # 100 USD to RUB, explicitly specifying user currency
        result = convert_to_user_currency(100, 2, populated_db, user_currency_id=1)
        assert result == 9000.0


class TestTransactionClassification:
    """Test transaction classification functions."""

    def test_classify_pure_expense(self):
        """Test classification of pure expense."""
        tx = {"income": 0, "outcome": 1000}
        assert classify_transaction(tx) == "outcome"

    def test_classify_pure_income(self):
        """Test classification of pure income."""
        tx = {"income": 50000, "outcome": 0}
        assert classify_transaction(tx) == "income"

    def test_classify_transfer(self):
        """Test classification of transfer between own accounts."""
        tx = {
            "income": 1000,
            "outcome": 1000,
            "incomeAccount": "acc-1",
            "outcomeAccount": "acc-2",
            "incomeInstrument": 1,
            "outcomeInstrument": 1,
        }
        accounts = {
            "acc-1": {"type": "ccard"},
            "acc-2": {"type": "ccard"},
        }
        assert classify_transaction(tx, accounts) == "transfer"

    def test_classify_exchange(self):
        """Test classification of currency exchange."""
        tx = {
            "income": 100,
            "outcome": 9000,
            "incomeAccount": "acc-usd",
            "outcomeAccount": "acc-rub",
            "incomeInstrument": 2,  # USD
            "outcomeInstrument": 1,  # RUB
        }
        accounts = {
            "acc-usd": {"type": "cash"},
            "acc-rub": {"type": "ccard"},
        }
        assert classify_transaction(tx, accounts) == "exchange"

    def test_classify_debt_out(self):
        """Test classification of lending money (debt out)."""
        tx = {
            "income": 5000,
            "outcome": 5000,
            "incomeAccount": "acc-debt",
            "outcomeAccount": "acc-rub",
        }
        accounts = {
            "acc-debt": {"type": "debt"},
            "acc-rub": {"type": "ccard"},
        }
        assert classify_transaction(tx, accounts) == "debt_out"

    def test_classify_debt_in(self):
        """Test classification of borrowing money (debt in)."""
        tx = {
            "income": 5000,
            "outcome": 5000,
            "incomeAccount": "acc-rub",
            "outcomeAccount": "acc-debt",
        }
        accounts = {
            "acc-debt": {"type": "debt"},
            "acc-rub": {"type": "ccard"},
        }
        assert classify_transaction(tx, accounts) == "debt_in"

    def test_classify_transfer_without_accounts(self):
        """Test transfer classification without accounts dict defaults to transfer."""
        tx = {"income": 1000, "outcome": 1000}
        assert classify_transaction(tx) == "transfer"


class TestTransactionPredicates:
    """Test transaction predicate functions."""

    def test_is_transfer_true(self):
        """Test is_transfer returns True for transfers."""
        tx = {"income": 1000, "outcome": 1000}
        assert is_transfer(tx) is True

    def test_is_transfer_false_expense(self):
        """Test is_transfer returns False for expenses."""
        tx = {"income": 0, "outcome": 1000}
        assert is_transfer(tx) is False

    def test_is_transfer_false_income(self):
        """Test is_transfer returns False for income."""
        tx = {"income": 1000, "outcome": 0}
        assert is_transfer(tx) is False

    def test_is_pure_expense_true(self):
        """Test is_pure_expense returns True for expenses."""
        tx = {"income": 0, "outcome": 1000}
        assert is_pure_expense(tx) is True

    def test_is_pure_expense_false(self):
        """Test is_pure_expense returns False for transfers."""
        tx = {"income": 500, "outcome": 1000}
        assert is_pure_expense(tx) is False

    def test_is_pure_income_true(self):
        """Test is_pure_income returns True for income."""
        tx = {"income": 1000, "outcome": 0}
        assert is_pure_income(tx) is True

    def test_is_pure_income_false(self):
        """Test is_pure_income returns False for expenses."""
        tx = {"income": 0, "outcome": 1000}
        assert is_pure_income(tx) is False

    def test_handles_none_values(self):
        """Test predicates handle None values gracefully."""
        tx = {"income": None, "outcome": 1000}
        assert is_pure_expense(tx) is True
        assert is_transfer(tx) is False

        tx2 = {"income": 1000, "outcome": None}
        assert is_pure_income(tx2) is True
        assert is_transfer(tx2) is False
