"""analyze_transfers: the rate of a currency exchange must be readable without guessing."""

import pytest

from zenmoney_mcp.analytics import analyze_transfers
from zenmoney_mcp.database import Database

from .factories import insert_transaction


def _transfer(result: dict, comment: str) -> dict:
    return next(t for t in result["transfers"] if t["comment"] == comment)


class TestExchangeRate:
    @pytest.fixture
    def exchange_db(self, populated_db: Database) -> Database:
        # Sold 100 USD for 9150 RUB, bought 20 USD for 1840 RUB
        insert_transaction(populated_db, date="2025-03-10", comment="sold dollars",
                           outcome=100.0, outcome_instrument=2, outcome_account="acc-usd",
                           income=9150.0, income_instrument=1, income_account="acc-rub")
        insert_transaction(populated_db, date="2025-03-12", comment="bought dollars",
                           outcome=1840.0, outcome_instrument=1, outcome_account="acc-rub",
                           income=20.0, income_instrument=2, income_account="acc-usd")
        insert_transaction(populated_db, date="2025-03-14", comment="to savings",
                           outcome=500.0, outcome_account="acc-rub",
                           income=500.0, income_account="acc-save")
        return populated_db

    def test_rate_is_the_price_of_one_unit_of_the_dearer_currency(self, exchange_db: Database):
        sold = _transfer(analyze_transfers(exchange_db, period="2025-03"), "sold dollars")

        assert sold["effective_rate"] == 91.5
        assert sold["rate_description"] == "1 USD = 91.5 RUB"

    def test_rate_reads_the_same_way_whichever_way_the_money_went(self, exchange_db: Database):
        bought = _transfer(analyze_transfers(exchange_db, period="2025-03"), "bought dollars")

        # 1840 RUB for 20 USD: still quoted per dollar, not as 0.0109 USD per ruble
        assert bought["effective_rate"] == 92.0
        assert bought["rate_description"] == "1 USD = 92.0 RUB"

    def test_same_currency_transfer_has_no_rate(self, exchange_db: Database):
        moved = _transfer(analyze_transfers(exchange_db, period="2025-03"), "to savings")

        assert "effective_rate" not in moved
        assert "rate_description" not in moved
