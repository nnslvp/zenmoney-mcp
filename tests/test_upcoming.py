"""get_upcoming_payments: planned reminder markers ahead, overdue ones and transfers."""

from datetime import date, timedelta

import pytest

from zenmoney_mcp.analytics import get_upcoming_payments
from zenmoney_mcp.database import Database

from .factories import insert_account, insert_reminder_marker, insert_tag


def _in_days(days: int) -> str:
    return (date.today() + timedelta(days=days)).isoformat()


@pytest.fixture
def plan_db(populated_db: Database) -> Database:
    """The populated fixture without its reminder markers: every test plans its own."""
    conn = populated_db.connect()
    conn.execute("DELETE FROM reminder_markers")
    conn.commit()
    return populated_db


class TestTransfers:
    def test_transfer_between_accounts_is_listed_apart_from_payments(self, plan_db: Database):
        insert_reminder_marker(plan_db, date=_in_days(2), payee="Landlord", outcome=4000.0)
        insert_reminder_marker(
            plan_db, date=_in_days(3), outcome=30000.0, income=30000.0,
            outcome_account="acc-rub", income_account="acc-save", comment="To savings",
        )

        result = get_upcoming_payments(plan_db)

        assert [payment["payee"] for payment in result["upcoming"]] == ["Landlord"]
        assert result["total_upcoming_outcome"] == 4000.0
        assert result["total_upcoming_income"] == 0
        [transfer] = result["transfers"]
        assert transfer["type"] == "transfer"
        assert (transfer["from_account"], transfer["to_account"]) == ("Тинькофф Black", "Накопительный Сбер")
        assert (transfer["amount"], transfer["currency"], transfer["comment"]) == (30000.0, "RUB", "To savings")
        assert result["total_transfers"] == 30000.0
        assert (result["upcoming_count"], result["transfers_count"]) == (1, 1)

    def test_transfer_does_not_load_the_week(self, plan_db: Database):
        insert_reminder_marker(plan_db, date=_in_days(2), payee="Landlord", outcome=4000.0)
        insert_reminder_marker(
            plan_db, date=_in_days(2), outcome=30000.0, income=30000.0,
            outcome_account="acc-rub", income_account="acc-save",
        )

        result = get_upcoming_payments(plan_db)

        assert [week["amount"] for week in result["weekly_load"]] == [4000.0]


class TestCurrency:
    def test_foreign_currency_payment_is_converted_and_keeps_the_original(self, plan_db: Database):
        euro_card = insert_account(plan_db, title="Euro card", instrument=3)  # 1 EUR = 100 RUB
        insert_reminder_marker(
            plan_db, date=_in_days(2), payee="Streaming", outcome=10.0,
            outcome_account=euro_card["id"], income_account=euro_card["id"],
        )
        insert_reminder_marker(plan_db, date=_in_days(3), payee="Gym", outcome=500.0)

        result = get_upcoming_payments(plan_db)

        streaming, gym = result["upcoming"]
        assert (streaming["amount"], streaming["currency"]) == (1000.0, "RUB")
        assert (streaming["original_amount"], streaming["original_currency"]) == (10.0, "EUR")
        assert (gym["amount"], gym["original_amount"], gym["original_currency"]) == (500.0, 500.0, "RUB")
        assert result["total_upcoming_outcome"] == 1500.0
        assert result["currency"] == "RUB"

    def test_expected_income_is_converted_from_the_receiving_account(self, plan_db: Database):
        insert_reminder_marker(
            plan_db, date=_in_days(2), payee="Client", income=100.0,
            income_account="acc-usd", outcome_account="acc-usd",
        )

        result = get_upcoming_payments(plan_db)

        [income] = result["upcoming"]
        assert (income["type"], income["amount"], income["account"]) == ("income", 9000.0, "Наличные USD")
        assert (income["original_amount"], income["original_currency"]) == (100.0, "USD")
        assert result["total_upcoming_income"] == 9000.0
        assert result["total_upcoming_outcome"] == 0


    def test_currency_exchange_shows_both_sides(self, plan_db: Database):
        insert_reminder_marker(
            plan_db, date=_in_days(3), outcome=100.0, income=8900.0,
            outcome_account="acc-usd", income_account="acc-rub",
        )

        result = get_upcoming_payments(plan_db)

        [transfer] = result["transfers"]
        assert (transfer["amount"], transfer["currency"]) == (9000.0, "RUB")  # 1 USD = 90 RUB
        assert (transfer["original_amount"], transfer["original_currency"]) == (100.0, "USD")
        assert (transfer["received_amount"], transfer["received_currency"]) == (8900.0, "RUB")

class TestPayeeName:
    def test_payee_falls_back_to_merchant_comment_then_category(self, plan_db: Database):
        office = insert_tag(plan_db, title="Office")
        insert_reminder_marker(plan_db, date=_in_days(1), merchant="m-yandex", comment="Rides", outcome=1.0)
        insert_reminder_marker(plan_db, date=_in_days(2), payee="Landlord", comment="Lease", outcome=2.0)
        insert_reminder_marker(plan_db, date=_in_days(3), comment="Cleaning and tea", tag=[office["id"]], outcome=3.0)
        insert_reminder_marker(plan_db, date=_in_days(4), tag=[office["id"]], outcome=4.0)
        insert_reminder_marker(plan_db, date=_in_days(5), outcome=5.0)

        result = get_upcoming_payments(plan_db)

        assert [payment["payee"] for payment in result["upcoming"]] == [
            "Яндекс.Такси", "Landlord", "Cleaning and tea", "Office", "Unknown",
        ]


class TestArguments:
    @pytest.mark.parametrize("days_ahead", [-1, 3651, 7.5, "30", None])
    def test_days_ahead_must_be_a_whole_number_of_days_in_range(self, plan_db: Database, days_ahead):
        with pytest.raises(ValueError, match="days_ahead"):
            get_upcoming_payments(plan_db, days_ahead=days_ahead)

    def test_horizon_limits_upcoming_items(self, plan_db: Database):
        insert_reminder_marker(plan_db, date=_in_days(7), payee="Gym", outcome=500.0)
        insert_reminder_marker(plan_db, date=_in_days(8), payee="Landlord", outcome=4000.0)

        result = get_upcoming_payments(plan_db, days_ahead=7)

        assert [payment["payee"] for payment in result["upcoming"]] == ["Gym"]
        period = result["period"]
        assert (period["start"], period["end"], period["days"]) == (_in_days(0), _in_days(7), 7)


class TestOverdue:
    def test_planned_payment_past_its_date_is_reported_as_overdue(self, plan_db: Database):
        insert_reminder_marker(plan_db, date=_in_days(-6), payee="Landlord", outcome=4000.0)
        insert_reminder_marker(plan_db, date=_in_days(2), payee="Gym", outcome=500.0)

        result = get_upcoming_payments(plan_db)

        assert [payment["payee"] for payment in result["upcoming"]] == ["Gym"]
        assert result["total_upcoming_outcome"] == 500.0
        [overdue] = result["overdue"]
        assert (overdue["payee"], overdue["amount"], overdue["days_overdue"]) == ("Landlord", 4000.0, 6)
        assert result["total_overdue_outcome"] == 4000.0
        assert result["overdue_count"] == 1

    def test_payment_due_today_is_upcoming_not_overdue(self, plan_db: Database):
        insert_reminder_marker(plan_db, date=_in_days(0), payee="Gym", outcome=500.0)

        result = get_upcoming_payments(plan_db)

        assert [payment["payee"] for payment in result["upcoming"]] == ["Gym"]
        assert result["overdue"] == []

    def test_overdue_looks_back_thirty_days(self, plan_db: Database):
        insert_reminder_marker(plan_db, date=_in_days(-30), payee="Recent", outcome=100.0)
        insert_reminder_marker(plan_db, date=_in_days(-31), payee="Forgotten", outcome=100.0)

        result = get_upcoming_payments(plan_db)

        assert [payment["payee"] for payment in result["overdue"]] == ["Recent"]
        assert result["period"]["overdue_from"] == _in_days(-30)

    def test_processed_and_deleted_markers_are_not_overdue(self, plan_db: Database):
        insert_reminder_marker(plan_db, date=_in_days(-4), payee="Paid", outcome=100.0, state="processed")
        insert_reminder_marker(plan_db, date=_in_days(-4), payee="Skipped", outcome=100.0, state="deleted")

        result = get_upcoming_payments(plan_db)

        assert result["overdue"] == []
        assert result["total_overdue_outcome"] == 0

    def test_overdue_income_and_transfers_stay_out_of_the_overdue_outcome(self, plan_db: Database):
        insert_reminder_marker(plan_db, date=_in_days(-3), payee="Landlord", outcome=4000.0)
        insert_reminder_marker(plan_db, date=_in_days(-2), payee="Client", income=7000.0)
        insert_reminder_marker(
            plan_db, date=_in_days(-1), outcome=30000.0, income=30000.0,
            outcome_account="acc-rub", income_account="acc-save",
        )

        result = get_upcoming_payments(plan_db)

        assert [item["type"] for item in result["overdue"]] == ["outcome", "income", "transfer"]
        assert result["total_overdue_outcome"] == 4000.0
        assert result["total_overdue_income"] == 7000.0
        assert result["transfers"] == [] and result["total_transfers"] == 0

    def test_overdue_payment_does_not_load_the_week(self, plan_db: Database):
        insert_reminder_marker(plan_db, date=_in_days(-1), payee="Landlord", outcome=4000.0)
        insert_reminder_marker(plan_db, date=_in_days(1), payee="Gym", outcome=500.0)

        result = get_upcoming_payments(plan_db)

        assert sum(week["amount"] for week in result["weekly_load"]) == 500.0
