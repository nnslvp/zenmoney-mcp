"""detect_recurring: regular payments found in history and declared as reminders."""

import json
from datetime import date, timedelta

import pytest

from zenmoney_mcp.analytics import detect_recurring
from zenmoney_mcp.database import Database

from .factories import insert_account, insert_reminder, insert_reminder_marker, insert_tag, insert_transaction


def _days_ago(days: int) -> str:
    return (date.today() - timedelta(days=days)).isoformat()


def _pay(db: Database, days_ago: tuple[int, ...] = (65, 35, 5), **columns) -> None:
    """Insert one expense per offset (days before today); a month apart by default."""
    for days in days_ago:
        insert_transaction(db, date=_days_ago(days), **columns)


def _named(result: dict, name: str) -> list[dict]:
    return [item for item in result["recurring"] if item["name"] == name]


class TestReminderAmounts:
    def test_foreign_currency_reminder_is_converted_and_keeps_the_original(self, populated_db: Database):
        euro_card = insert_account(populated_db, title="Euro card", instrument=3)  # 1 EUR = 100 RUB
        insert_reminder(
            populated_db, payee="Streaming", outcome=10.0,
            outcome_account=euro_card["id"], income_account=euro_card["id"],
        )

        result = detect_recurring(populated_db)

        [item] = _named(result, "Streaming")
        assert item["avg_amount"] == 1000.0
        assert (item["original_amount"], item["original_currency"]) == (10.0, "EUR")
        assert result["currency"] == "RUB"
        assert result["total_monthly_estimate"] == 1000.0

    def test_monthly_reminder_costs_twelve_payments_a_year(self, populated_db: Database):
        insert_reminder(populated_db, payee="Gym", outcome=100.0, interval="month", step=1)

        result = detect_recurring(populated_db)

        [item] = _named(result, "Gym")
        assert item["yearly_cost"] == 1200.0
        assert result["total_monthly_estimate"] == 100.0

    def test_reminder_step_stretches_the_interval(self, populated_db: Database):
        insert_reminder(populated_db, payee="Water delivery", outcome=300.0, interval="month", step=2)

        result = detect_recurring(populated_db)

        [item] = _named(result, "Water delivery")
        assert item["frequency"] == "every 2 months"
        assert item["yearly_cost"] == 1800.0
        assert result["total_monthly_estimate"] == 150.0


class TestDetectedAmounts:
    def test_monthly_payment_costs_twelve_payments_a_year(self, populated_db: Database):
        _pay(populated_db, payee="Music", outcome=100.0)

        result = detect_recurring(populated_db)

        [item] = _named(result, "Music")
        assert item["frequency"] == "monthly"
        assert item["yearly_cost"] == 1200.0

    def test_foreign_currency_payment_is_converted_and_keeps_the_original(self, populated_db: Database):
        _pay(
            populated_db, payee="Cloud storage", outcome=3.0,  # 1 USD = 90 RUB
            outcome_instrument=2, outcome_account="acc-usd", income_instrument=2, income_account="acc-usd",
        )

        result = detect_recurring(populated_db)

        [item] = _named(result, "Cloud storage")
        assert item["avg_amount"] == 270.0
        assert (item["original_amount"], item["original_currency"]) == (3.0, "USD")


class TestNames:
    def test_detected_payment_without_payee_is_named_by_its_comment(self, populated_db: Database):
        storage = insert_tag(populated_db, title="Storage")
        _pay(populated_db, outcome=50.0, tag=[storage["id"]], comment="Locker")

        result = detect_recurring(populated_db)

        assert [item["name"] for item in result["recurring"]] == ["Locker"]

    def test_detected_payment_without_payee_and_comment_is_named_by_its_category(self, populated_db: Database):
        storage = insert_tag(populated_db, title="Storage")
        _pay(populated_db, outcome=50.0, tag=[storage["id"]])

        result = detect_recurring(populated_db)

        assert [item["name"] for item in result["recurring"]] == ["Storage"]

    def test_reminder_without_payee_is_named_by_merchant_then_comment_then_category(self, populated_db: Database):
        office = insert_tag(populated_db, title="Office")
        insert_reminder(populated_db, outcome=300.0, merchant="m-yandex", comment="Rides", tag=[office["id"]])
        insert_reminder(populated_db, outcome=200.0, comment="Studio lease", tag=[office["id"]])
        insert_reminder(populated_db, outcome=100.0, tag=[office["id"]])

        result = detect_recurring(populated_db)

        assert [item["name"] for item in result["recurring"]] == ["Яндекс.Такси", "Studio lease", "Office"]


class TestDeduplication:
    def test_reminder_without_payee_survives_an_unrelated_payee_less_pattern(self, populated_db: Database):
        storage = insert_tag(populated_db, title="Storage")
        office = insert_tag(populated_db, title="Office")
        _pay(populated_db, outcome=50.0, tag=[storage["id"]], comment="Locker")
        insert_reminder(populated_db, outcome=4000.0, tag=[office["id"]], comment="Studio lease")

        result = detect_recurring(populated_db)

        assert sorted(item["avg_amount"] for item in result["recurring"]) == [50.0, 4000.0]
        assert result["total_monthly_estimate"] == 4050.0

    def test_reminder_and_the_payments_made_from_it_are_one_item(self, populated_db: Database):
        reminder = insert_reminder(populated_db, outcome=4000.0, comment="Studio lease")
        for days in (65, 35, 5):
            marker = insert_reminder_marker(
                populated_db, reminder=reminder["id"], date=_days_ago(days), state="processed", outcome=4000.0,
            )
            # The bank statement names the landlord and the amount drifted from the plan
            insert_transaction(
                populated_db, date=_days_ago(days), payee="Landlord", outcome=4600.0, reminder_marker=marker["id"],
            )

        result = detect_recurring(populated_db)

        [item] = result["recurring"]
        assert (item["name"], item["source"], item["avg_amount"]) == ("Studio lease", "reminder", 4000.0)
        assert (item["last_payment"], item["occurrences"]) == (_days_ago(5), 3)

    def test_reminder_matching_a_detected_payee_and_amount_is_one_item(self, populated_db: Database):
        _pay(populated_db, payee="Music", outcome=100.0)
        insert_reminder(populated_db, payee="music ", outcome=105.0)

        result = detect_recurring(populated_db)

        assert [(item["name"], item["source"]) for item in result["recurring"]] == [("music", "reminder")]

    def test_reminder_matching_a_detected_merchant_and_amount_is_one_item(self, populated_db: Database):
        _pay(populated_db, merchant="m-yandex", payee="YANDEX*TAXI 4411", outcome=100.0)
        insert_reminder(populated_db, merchant="m-yandex", outcome=100.0)

        result = detect_recurring(populated_db)

        assert [(item["name"], item["source"]) for item in result["recurring"]] == [("Яндекс.Такси", "reminder")]

    def test_two_subscriptions_of_one_payee_are_both_listed(self, populated_db: Database):
        _pay(populated_db, payee="Acme", outcome=15.0)
        insert_reminder(populated_db, payee="Acme", outcome=240.0, interval="year")

        result = detect_recurring(populated_db)

        assert sorted(item["avg_amount"] for item in _named(result, "Acme")) == [15.0, 240.0]

    def test_payee_less_reminder_matching_category_and_amount_of_a_pattern_is_one_item(self, populated_db: Database):
        office = insert_tag(populated_db, title="Office")
        _pay(populated_db, payee="Landlord", outcome=4000.0, tag=[office["id"]])
        insert_reminder(populated_db, outcome=4100.0, tag=[office["id"]], comment="Studio lease")

        result = detect_recurring(populated_db)

        assert [(item["name"], item["avg_amount"]) for item in result["recurring"]] == [("Studio lease", 4100.0)]

    def test_reminder_with_another_payee_in_the_same_category_stays_separate(self, populated_db: Database):
        media = insert_tag(populated_db, title="Media")
        _pay(populated_db, payee="Music", outcome=100.0, tag=[media["id"]])
        insert_reminder(populated_db, payee="Movies", outcome=100.0, tag=[media["id"]])

        result = detect_recurring(populated_db)

        assert sorted(item["name"] for item in result["recurring"]) == ["Movies", "Music"]


class TestRegularityAndRecency:
    def test_subscription_that_started_recently_is_found_with_a_long_lookback(self, populated_db: Database):
        _pay(populated_db, days_ago=(65, 35, 5), payee="Music", outcome=100.0)

        result = detect_recurring(populated_db, lookback_months=12)

        [item] = _named(result, "Music")
        assert (item["occurrences"], item["confidence"]) == (3, 1.0)

    def test_longer_lookback_does_not_lose_what_a_shorter_one_found(self, populated_db: Database):
        _pay(populated_db, days_ago=(65, 35, 5), payee="Music", outcome=100.0)
        _pay(populated_db, days_ago=(40, 10), payee="Gym", outcome=200.0)

        short = detect_recurring(populated_db, lookback_months=3)
        long = detect_recurring(populated_db, lookback_months=12)

        assert sorted(item["name"] for item in short["recurring"]) == ["Gym", "Music"]
        assert sorted(item["name"] for item in long["recurring"]) == ["Gym", "Music"]

    def test_three_years_of_calendar_months_are_fully_regular(self, populated_db: Database):
        month_start = date.today().replace(day=1)
        for _ in range(36):
            insert_transaction(populated_db, date=month_start.isoformat(), payee="Music", outcome=100.0)
            month_start = (month_start - timedelta(days=1)).replace(day=1)

        result = detect_recurring(populated_db, lookback_months=40)

        [item] = _named(result, "Music")
        assert (item["occurrences"], item["confidence"]) == (36, 1.0)

    def test_two_occurrences_give_less_confidence_than_three(self, populated_db: Database):
        _pay(populated_db, days_ago=(40, 10), payee="Gym", outcome=200.0)

        result = detect_recurring(populated_db, lookback_months=12)

        [item] = _named(result, "Gym")
        assert item["confidence"] == 0.67

    def test_subscription_with_a_skipped_month_is_found_with_lower_confidence(self, populated_db: Database):
        _pay(populated_db, days_ago=(125, 95, 35, 5), payee="Music", outcome=100.0)

        result = detect_recurring(populated_db, lookback_months=6)

        [item] = _named(result, "Music")
        assert item["frequency"] == "monthly"
        assert item["confidence"] == 0.8  # 4 payments where 5 were due

    def test_subscription_that_stopped_is_listed_as_ended_and_not_counted(self, populated_db: Database):
        _pay(populated_db, days_ago=(170, 140, 110, 80), payee="Old club", outcome=100.0)
        _pay(populated_db, days_ago=(65, 35, 5), payee="Music", outcome=30.0)

        result = detect_recurring(populated_db, lookback_months=6)

        assert [item["name"] for item in result["recurring"]] == ["Music"]
        assert result["total_monthly_estimate"] == 30.0
        assert result["total_found"] == 1
        [ended] = result["ended"]
        assert (ended["name"], ended["last_payment"], ended["avg_amount"]) == ("Old club", _days_ago(80), 100.0)
        assert result["ended_count"] == 1

    def test_monthly_payment_is_still_active_a_few_days_after_its_due_date(self, populated_db: Database):
        _pay(populated_db, days_ago=(100, 70, 40), payee="Music", outcome=100.0)

        result = detect_recurring(populated_db, lookback_months=6)

        assert [item["name"] for item in result["recurring"]] == ["Music"]
        assert result["ended"] == []

    def test_two_same_priced_purchases_two_weeks_apart_are_not_a_pattern(self, populated_db: Database):
        _pay(populated_db, days_ago=(15, 1), payee="Workshop", outcome=750.0)

        for lookback_months in (1, 3):
            result = detect_recurring(populated_db, lookback_months=lookback_months)
            assert result["recurring"] == [] and result["ended"] == []

    def test_three_payments_a_week_apart_are_a_weekly_pattern(self, populated_db: Database):
        _pay(populated_db, days_ago=(15, 8, 1), payee="Class", outcome=50.0)

        result = detect_recurring(populated_db)

        [item] = _named(result, "Class")
        assert (item["frequency"], item["yearly_cost"]) == ("weekly", 2600.0)

    def test_slowly_drifting_price_stays_one_subscription_over_a_long_lookback(self, populated_db: Database):
        # A fee charged in another currency moves a little every month: 20% over a year
        for month in range(13):
            insert_transaction(
                populated_db, date=_days_ago(5 + 30 * (12 - month)), payee="Bank fee", outcome=20.0 + month / 3,
            )

        short = detect_recurring(populated_db, lookback_months=3)
        long = detect_recurring(populated_db, lookback_months=14)

        assert [item["occurrences"] for item in _named(short, "Bank fee")] == [3]
        assert [item["occurrences"] for item in _named(long, "Bank fee")] == [13]

    def test_price_jump_beyond_tolerance_is_not_one_stable_payment(self, populated_db: Database):
        _pay(populated_db, days_ago=(95, 65), payee="Music", outcome=10.0)
        _pay(populated_db, days_ago=(35, 5), payee="Music", outcome=40.0)

        result = detect_recurring(populated_db, lookback_months=6)

        assert _named(result, "Music") == []

    def test_ended_list_is_cut_to_the_ten_most_recent(self, populated_db: Database):
        for club in range(12):
            _pay(populated_db, days_ago=(160 + club, 130 + club, 100 + club), payee=f"Club {club}", outcome=100.0)

        result = detect_recurring(populated_db, lookback_months=7)

        assert [item["name"] for item in result["ended"]] == [f"Club {club}" for club in range(10)]
        assert result["ended_count"] == 12

    def test_reminder_does_not_absorb_a_pattern_that_ended(self, populated_db: Database):
        _pay(populated_db, days_ago=(170, 140, 110), payee="Music", outcome=100.0)
        insert_reminder(populated_db, payee="Music", outcome=100.0)

        result = detect_recurring(populated_db, lookback_months=6)

        [item] = result["recurring"]
        assert item["source"] == "reminder" and "last_payment" not in item
        assert [ended["name"] for ended in result["ended"]] == ["Music"]


class TestPaymentType:
    @pytest.mark.parametrize(
        ("category", "expected"),
        [
            ("Онлайн-подписки", "subscription"),
            ("Subscriptions", "subscription"),
            ("Коммунальные услуги", "utility"),
            ("Utilities", "utility"),
            ("Кредиты", "loan"),
            ("Страхование", "insurance"),
            ("Insurance", "insurance"),
            ("Groceries", "other"),
        ],
    )
    def test_type_follows_the_category_title_in_any_word_form(
        self, populated_db: Database, category: str, expected: str,
    ):
        tag = insert_tag(populated_db, title=category)
        _pay(populated_db, payee="Paid from history", outcome=100.0, tag=[tag["id"]])
        insert_reminder(populated_db, payee="Planned", outcome=500.0, tag=[tag["id"]])

        result = detect_recurring(populated_db)

        assert {item["name"]: item["type"] for item in result["recurring"]} == {
            "Paid from history": expected,
            "Planned": expected,
        }

    def test_uncategorized_payment_has_type_other(self, populated_db: Database):
        _pay(populated_db, payee="Music", outcome=100.0)

        result = detect_recurring(populated_db)

        assert [item["type"] for item in result["recurring"]] == ["other"]


class TestAccounts:
    def test_subscription_paid_from_an_off_balance_account_is_found(self, populated_db: Database):
        side_card = insert_account(populated_db, title="Side card", in_balance=0)
        _pay(
            populated_db, payee="Music", outcome=100.0,
            outcome_account=side_card["id"], income_account=side_card["id"],
        )

        result = detect_recurring(populated_db)

        [item] = _named(result, "Music")
        assert item["account"] == "Side card"

    def test_reminder_shows_its_account(self, populated_db: Database):
        insert_reminder(populated_db, payee="Gym", outcome=100.0, outcome_account="acc-save")

        result = detect_recurring(populated_db)

        [item] = _named(result, "Gym")
        assert item["account"] == "Накопительный Сбер"


class TestWhichRemindersCount:
    def test_planned_transfer_between_accounts_is_not_a_recurring_payment(self, populated_db: Database):
        insert_reminder(
            populated_db, comment="Monthly savings", outcome=5000.0, income=5000.0,
            outcome_account="acc-rub", income_account="acc-save",
        )

        result = detect_recurring(populated_db)

        assert result["recurring"] == []
        assert result["total_monthly_estimate"] == 0

    def test_reminder_past_its_end_date_is_not_active(self, populated_db: Database):
        insert_reminder(populated_db, payee="Course", outcome=300.0, start_date=_days_ago(200), end_date=_days_ago(10))
        insert_reminder(populated_db, payee="Gym", outcome=100.0, start_date=_days_ago(200), end_date=_days_ago(-10))

        result = detect_recurring(populated_db)

        assert [item["name"] for item in result["recurring"]] == ["Gym"]

    def test_one_off_reminder_is_not_recurring(self, populated_db: Database):
        insert_reminder(populated_db, payee="Sofa", outcome=3000.0, interval=None, step=0)

        result = detect_recurring(populated_db)

        assert result["recurring"] == []


class TestArguments:
    @pytest.mark.parametrize("lookback_months", [0, -3, 2.5, "3", None])
    def test_lookback_must_be_a_positive_whole_number_of_months(self, populated_db: Database, lookback_months):
        with pytest.raises(ValueError, match="lookback_months"):
            detect_recurring(populated_db, lookback_months=lookback_months)

    @pytest.mark.parametrize("tolerance_pct", [-1, "10", None])
    def test_tolerance_must_be_a_non_negative_number(self, populated_db: Database, tolerance_pct):
        with pytest.raises(ValueError, match="tolerance_pct"):
            detect_recurring(populated_db, tolerance_pct=tolerance_pct)


class TestOutput:
    def test_answer_is_plain_json(self, populated_db: Database):
        _pay(populated_db, payee="Music", outcome=100.0)
        insert_reminder(populated_db, payee="Music", outcome=100.0)
        _pay(populated_db, days_ago=(170, 140, 110), payee="Old club", outcome=50.0)

        result = detect_recurring(populated_db, lookback_months=6)

        assert json.loads(json.dumps(result)) == result
        assert (result["total_found"], result["ended_count"]) == (1, 1)
