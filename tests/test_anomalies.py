"""detect_anomalies: unusually large payments and possible duplicates."""

from datetime import date, timedelta

from zenmoney_mcp.analytics import detect_anomalies
from zenmoney_mcp.database import Database

from .factories import insert_tag, insert_transaction


def _days_ago(days: int) -> str:
    return (date.today() - timedelta(days=days)).isoformat()


def _detect(db: Database, **arguments) -> dict:
    """Analyze the last ten days: the tests date their transactions there (today by default)."""
    return detect_anomalies(db, start_date=_days_ago(10), **arguments)


def _spend(db: Database, tag: dict, amounts: list[float]) -> None:
    for amount in amounts:
        insert_transaction(db, outcome=amount, tag=[tag["id"]])


class TestOutliers:
    def test_unusually_large_payment_is_flagged(self, populated_db: Database):
        books = insert_tag(populated_db, title="Books")
        _spend(populated_db, books, [100.0] * 10 + [900.0])

        result = _detect(populated_db)

        [outlier] = result["outliers"]
        assert (outlier["amount"], outlier["category"]) == (900.0, "Books")
        assert (outlier["z_score"], outlier["severity"]) == (3.16, "high")

    def test_unusually_small_payment_is_not_flagged(self, populated_db: Database):
        books = insert_tag(populated_db, title="Books")
        _spend(populated_db, books, [100.0] * 10 + [1.0])

        result = _detect(populated_db)

        assert result["outliers"] == []
        assert result["summary"]["outliers_count"] == 0

    def test_strongest_outliers_come_first_when_the_list_is_cut(self, populated_db: Database):
        # One large payment among n equal ones has z = sqrt(n), so the categories
        # created last hold the strongest outliers
        for equal_payments in range(5, 22):
            tag = insert_tag(populated_db, title=f"Category {equal_payments}")
            _spend(populated_db, tag, [100.0] * equal_payments + [1000.0])

        result = _detect(populated_db)

        z_scores = [outlier["z_score"] for outlier in result["outliers"]]
        assert len(z_scores) == 15
        assert z_scores == sorted(z_scores, reverse=True)
        assert z_scores[0] == 4.58  # sqrt(21)
        summary = result["summary"]
        assert (summary["outliers_count"], summary["outliers_returned"]) == (17, 15)


class TestDuplicates:
    def test_three_identical_payments_are_one_group(self, populated_db: Database):
        ids = [insert_transaction(populated_db, payee="Bakery", outcome=45.0)["id"] for _ in range(3)]

        result = _detect(populated_db)

        [group] = result["possible_duplicates"]
        assert (group["payee"], group["amount"], group["count"]) == ("Bakery", 45.0, 3)
        assert sorted(group["transactions"]) == sorted(ids)
        assert result["summary"]["duplicates_count"] == 1

    def test_same_payment_a_day_later_is_a_duplicate_but_not_days_later(self, populated_db: Database):
        insert_transaction(populated_db, payee="Bakery", outcome=45.0, date=_days_ago(5))
        insert_transaction(populated_db, payee="Bakery", outcome=45.0, date=_days_ago(4))
        insert_transaction(populated_db, payee="Bakery", outcome=45.0, date=_days_ago(1))

        result = _detect(populated_db)

        [group] = result["possible_duplicates"]
        assert (group["count"], group["date"]) == (2, _days_ago(5))

    def test_other_payee_other_amount_or_no_payee_is_not_a_duplicate(self, populated_db: Database):
        insert_transaction(populated_db, payee="Bakery", outcome=45.0)
        insert_transaction(populated_db, payee="Butcher", outcome=45.0)
        insert_transaction(populated_db, payee="Bakery", outcome=46.0)
        insert_transaction(populated_db, outcome=77.0)
        insert_transaction(populated_db, outcome=77.0)

        result = _detect(populated_db)

        assert result["possible_duplicates"] == []

    def test_largest_duplicates_come_first_when_the_list_is_cut(self, populated_db: Database):
        for amount in range(1, 17):
            for _ in range(2):
                insert_transaction(populated_db, payee=f"Shop {amount}", outcome=float(amount))

        result = _detect(populated_db)

        amounts = [group["amount"] for group in result["possible_duplicates"]]
        assert amounts == [float(amount) for amount in range(16, 1, -1)]
        summary = result["summary"]
        assert (summary["duplicates_count"], summary["duplicates_returned"]) == (16, 15)

    def test_number_of_queries_does_not_grow_with_the_number_of_transactions(self, populated_db: Database):
        """Rates used to be looked up for every pair of transactions: years of history took minutes."""
        conn = populated_db.connect()

        def count_queries() -> int:
            statements: list[str] = []
            conn.set_trace_callback(statements.append)
            try:
                _detect(populated_db)
            finally:
                conn.set_trace_callback(None)
            return len(statements)

        def spend_dollars(shops: range) -> None:
            for shop in shops:
                insert_transaction(
                    populated_db, payee=f"Shop {shop}", outcome=10.0 + shop,
                    outcome_instrument=2, outcome_account="acc-usd", income_instrument=2, income_account="acc-usd",
                )

        spend_dollars(range(20))
        queries_for_few = count_queries()
        spend_dollars(range(20, 300))
        queries_for_many = count_queries()

        assert queries_for_many == queries_for_few
