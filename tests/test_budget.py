"""Budget plan vs actual: check_budget_health (T5) and the budgets/current resource (R3).

Most tests build their own budget month (2024-03) on top of ``populated_db``: it is
far in the past, so the fixture's own rows (all dated around today) stay out of it.
The resource always answers for the current month, so its tests share that month
with the fixture's budgets.
"""

import json
from datetime import date

import pytest
from mcp.shared.memory import create_connected_server_and_client_session

from zenmoney_mcp import server as zm_server
from zenmoney_mcp.analytics import check_budget_health, get_current_budgets_resource
from zenmoney_mcp.database import Database

from .factories import insert_account, insert_budget, insert_reminder_marker, insert_tag, insert_transaction

MONTH = "2024-03"
BUDGET_DATE = "2024-03-01"
TOTAL_TAG = "00000000-0000-0000-0000-000000000000"


def _category(result: dict, name: str) -> dict:
    return next(c for c in result["categories"] if c["name"] == name)


class TestMonthArgument:
    """The month must be a real 'YYYY-MM'; anything else is an error, not 'this month'."""

    @pytest.mark.parametrize("month", ["abc", "2024-03-15", "2024-13", "2024-00", "2024-3", "", "0000-05"])
    def test_invalid_month_is_rejected(self, populated_db: Database, month: str):
        with pytest.raises(ValueError, match="Invalid month"):
            check_budget_health(populated_db, month=month)


class TestScheduledOperations:
    """An unlocked budget is its stored amount plus the month's scheduled operations."""

    def test_scheduled_expenses_of_the_month_are_added_to_an_unlocked_budget(self, populated_db: Database):
        insert_tag(populated_db, id="tag-hobby", title="Hobby")
        insert_budget(populated_db, tag="tag-hobby", date=BUDGET_DATE, outcome=100.0)
        insert_reminder_marker(populated_db, tag=["tag-hobby"], date="2024-03-10", state="planned", outcome=40.0)
        # Already paid: still part of the month's plan
        insert_reminder_marker(populated_db, tag=["tag-hobby"], date="2024-03-20", state="processed", outcome=60.0)
        insert_reminder_marker(populated_db, tag=["tag-hobby"], date="2024-03-15", state="deleted", outcome=500.0)
        insert_reminder_marker(populated_db, tag=["tag-hobby"], date="2024-04-02", state="planned", outcome=900.0)

        result = check_budget_health(populated_db, month=MONTH)

        assert _category(result, "Hobby")["planned"] == 200.0

    def test_scheduled_transfers_and_incomes_do_not_raise_the_plan(self, populated_db: Database):
        insert_tag(populated_db, id="tag-hobby", title="Hobby")
        insert_budget(populated_db, tag="tag-hobby", date=BUDGET_DATE, outcome=100.0)
        # Both sides filled: a transfer between accounts, not an expense
        insert_reminder_marker(
            populated_db, tag=["tag-hobby"], date="2024-03-10", outcome=700.0, income=700.0,
            income_account="acc-save",
        )
        insert_reminder_marker(populated_db, tag=["tag-hobby"], date="2024-03-11", income=300.0)

        result = check_budget_health(populated_db, month=MONTH)

        assert _category(result, "Hobby")["planned"] == 100.0

    def test_scheduled_expense_is_converted_from_its_account_currency(self, populated_db: Database):
        """User currency EUR (rate 100), marker on the USD account (rate 90): 10 USD = 9 EUR."""
        conn = populated_db.connect()
        conn.execute("UPDATE users SET currency = 3 WHERE id = 1")
        conn.commit()
        insert_tag(populated_db, id="tag-hobby", title="Hobby")
        insert_budget(populated_db, tag="tag-hobby", date=BUDGET_DATE, outcome=100.0)
        insert_reminder_marker(
            populated_db, tag=["tag-hobby"], date="2024-03-10", outcome=10.0, outcome_account="acc-usd",
        )

        result = check_budget_health(populated_db, month=MONTH)

        assert result["currency"] == "EUR"
        assert _category(result, "Hobby")["planned"] == 109.0

    def test_scheduled_expense_from_an_off_balance_account_is_not_planned(self, populated_db: Database):
        """Spending from such an account never reaches the actual, so it must not be in the plan."""
        insert_account(populated_db, id="acc-off", in_balance=0)
        insert_tag(populated_db, id="tag-hobby", title="Hobby")
        insert_budget(populated_db, tag="tag-hobby", date=BUDGET_DATE, outcome=100.0)
        insert_reminder_marker(
            populated_db, tag=["tag-hobby"], date="2024-03-10", outcome=40.0, outcome_account="acc-off",
        )
        insert_transaction(
            populated_db, date="2024-03-10", tag=["tag-hobby"], outcome=40.0, outcome_account="acc-off",
        )

        result = check_budget_health(populated_db, month=MONTH)

        hobby = _category(result, "Hobby")
        assert hobby["planned"] == 100.0
        assert hobby["actual"] == 0.0


class TestUncategorizedBudget:
    """A budget row without a tag is the budget for expenses that have no category."""

    def test_uncategorized_budget_counts_expenses_without_a_category(self, populated_db: Database):
        insert_tag(populated_db, id="tag-hobby", title="Hobby")
        insert_budget(populated_db, tag=None, date=BUDGET_DATE, outcome=500.0)
        insert_transaction(populated_db, date="2024-03-05", tag=None, outcome=120.0)
        insert_transaction(populated_db, date="2024-03-06", tag=None, outcome=80.0)
        insert_transaction(populated_db, date="2024-03-07", tag=["tag-hobby"], outcome=999.0)

        result = check_budget_health(populated_db, month=MONTH)

        uncategorized = _category(result, "Uncategorized")
        assert uncategorized["planned"] == 500.0
        assert uncategorized["actual"] == 200.0
        assert uncategorized["pct_used"] == 40.0


class TestCategoryWithoutPlan:
    """A plan of zero or less is no budget at all, not a budget that is on track."""

    def test_spending_against_a_zero_plan_is_reported_as_no_budget(self, populated_db: Database):
        insert_tag(populated_db, id="tag-hobby", title="Hobby")
        insert_budget(populated_db, tag="tag-hobby", date=BUDGET_DATE, outcome=0.0)
        insert_transaction(populated_db, date="2024-03-05", tag=["tag-hobby"], outcome=75.0)

        result = check_budget_health(populated_db, month=MONTH)

        hobby = _category(result, "Hobby")
        assert hobby["planned"] == 0
        assert hobby["actual"] == 75.0
        assert hobby["status"] == "no_budget"
        assert hobby["pct_used"] is None
        assert hobby["pace"] is None

    def test_negative_plan_is_clamped_to_zero_with_a_warning(self, populated_db: Database):
        insert_tag(populated_db, id="tag-hobby", title="Hobby")
        insert_budget(populated_db, tag="tag-hobby", date=BUDGET_DATE, outcome=-50.0)
        insert_transaction(populated_db, date="2024-03-05", tag=["tag-hobby"], outcome=20.0)

        result = check_budget_health(populated_db, month=MONTH)

        hobby = _category(result, "Hobby")
        assert hobby["planned"] == 0
        assert hobby["status"] == "no_budget"
        assert hobby["warning"] == "Computed planned was -50.0, clamped to 0"
        assert result["overall"]["planned"] == 0

    def test_month_with_spending_but_no_plan_at_all_is_no_budget_overall(self, populated_db: Database):
        insert_tag(populated_db, id="tag-hobby", title="Hobby")
        insert_budget(populated_db, tag="tag-hobby", date=BUDGET_DATE, outcome=0.0)
        insert_transaction(populated_db, date="2024-03-05", tag=["tag-hobby"], outcome=75.0)

        result = check_budget_health(populated_db, month=MONTH)

        assert result["overall"]["actual"] == 75.0
        assert result["overall"]["status"] == "no_budget"
        assert result["overall"]["pct_used"] is None


class TestSubcategories:
    """A child category's budget and spending belong to its parent's entry."""

    def test_child_budget_is_nested_under_its_parent_and_counted_once(self, populated_db: Database):
        insert_tag(populated_db, id="tag-garden", title="Garden")
        insert_tag(populated_db, id="tag-seeds", title="Seeds", parent="tag-garden")
        insert_budget(populated_db, tag="tag-garden", date=BUDGET_DATE, outcome=400.0)
        insert_budget(populated_db, tag="tag-seeds", date=BUDGET_DATE, outcome=600.0)
        insert_transaction(populated_db, date="2024-03-05", tag=["tag-seeds"], outcome=250.0)
        insert_transaction(populated_db, date="2024-03-06", tag=["tag-garden"], outcome=100.0)

        result = check_budget_health(populated_db, month=MONTH)

        assert [c["name"] for c in result["categories"]] == ["Garden"]
        garden = result["categories"][0]
        assert garden["actual"] == 350.0
        assert [(s["name"], s["planned"], s["actual"], s["status"]) for s in garden["subcategories"]] == [
            ("Seeds", 600.0, 250.0, "on_track"),
        ]
        assert result["overall"]["actual"] == 350.0

    def test_child_budget_without_a_parent_budget_rolls_up_into_the_parent(self, populated_db: Database):
        insert_tag(populated_db, id="tag-music", title="Music")
        insert_tag(populated_db, id="tag-concerts", title="Concerts", parent="tag-music")
        insert_budget(populated_db, tag="tag-concerts", date=BUDGET_DATE, outcome=300.0)
        insert_transaction(populated_db, date="2024-03-05", tag=["tag-concerts"], outcome=120.0)
        insert_transaction(populated_db, date="2024-03-06", tag=["tag-music"], outcome=30.0)

        result = check_budget_health(populated_db, month=MONTH)

        assert [c["name"] for c in result["categories"]] == ["Music"]
        music = result["categories"][0]
        assert music["actual"] == 150.0
        assert [(s["name"], s["planned"], s["actual"]) for s in music["subcategories"]] == [
            ("Concerts", 300.0, 120.0),
        ]
        assert result["overall"]["actual"] == 150.0


class TestParentPlan:
    """An unlocked parent stores what is left over its children."""

    def test_parent_plan_is_its_own_amount_plus_its_children(self, populated_db: Database):
        insert_tag(populated_db, id="tag-garden", title="Garden")
        insert_tag(populated_db, id="tag-seeds", title="Seeds", parent="tag-garden")
        insert_budget(populated_db, tag="tag-garden", date=BUDGET_DATE, outcome=400.0)
        insert_budget(populated_db, tag="tag-seeds", date=BUDGET_DATE, outcome=600.0)
        insert_transaction(populated_db, date="2024-03-05", tag=["tag-seeds"], outcome=590.0)
        insert_transaction(populated_db, date="2024-03-06", tag=["tag-garden"], outcome=310.0)

        result = check_budget_health(populated_db, month=MONTH)

        garden = _category(result, "Garden")
        assert garden["planned"] == 1000.0
        assert garden["actual"] == 900.0
        assert garden["pct_used"] == 90.0
        assert garden["status"] == "warning"
        assert result["overall"]["planned"] == 1000.0

    def test_negative_parent_remainder_is_offset_by_its_children(self, populated_db: Database):
        """Children 200 + 350 with the parent stored as -200 means 350 for the whole category."""
        insert_tag(populated_db, id="tag-garden", title="Garden")
        insert_tag(populated_db, id="tag-seeds", title="Seeds", parent="tag-garden")
        insert_tag(populated_db, id="tag-tools", title="Tools", parent="tag-garden")
        insert_budget(populated_db, tag="tag-garden", date=BUDGET_DATE, outcome=-200.0)
        insert_budget(populated_db, tag="tag-seeds", date=BUDGET_DATE, outcome=200.0)
        insert_budget(populated_db, tag="tag-tools", date=BUDGET_DATE, outcome=350.0)
        insert_transaction(populated_db, date="2024-03-05", tag=["tag-tools"], outcome=70.0)

        result = check_budget_health(populated_db, month=MONTH)

        garden = _category(result, "Garden")
        assert garden["planned"] == 350.0
        assert garden["status"] == "on_track"
        assert "warning" not in garden

    def test_child_plan_becomes_the_plan_of_a_parent_without_its_own_budget(self, populated_db: Database):
        insert_tag(populated_db, id="tag-music", title="Music")
        insert_tag(populated_db, id="tag-concerts", title="Concerts", parent="tag-music")
        insert_budget(populated_db, tag="tag-concerts", date=BUDGET_DATE, outcome=300.0)
        insert_transaction(populated_db, date="2024-03-05", tag=["tag-concerts"], outcome=120.0)

        result = check_budget_health(populated_db, month=MONTH)

        music = _category(result, "Music")
        assert music["planned"] == 300.0
        assert music["pct_used"] == 40.0

    def test_locked_parent_budget_is_exact(self, populated_db: Database):
        """A locked amount is the exact budget of the category: nothing is added on top."""
        insert_tag(populated_db, id="tag-garden", title="Garden")
        insert_tag(populated_db, id="tag-seeds", title="Seeds", parent="tag-garden")
        insert_budget(populated_db, tag="tag-garden", date=BUDGET_DATE, outcome=1000.0, outcome_lock=1)
        insert_budget(populated_db, tag="tag-seeds", date=BUDGET_DATE, outcome=600.0)
        insert_reminder_marker(populated_db, tag=["tag-garden"], date="2024-03-10", outcome=40.0)
        insert_transaction(populated_db, date="2024-03-05", tag=["tag-seeds"], outcome=250.0)

        result = check_budget_health(populated_db, month=MONTH)

        garden = _category(result, "Garden")
        assert garden["planned"] == 1000.0
        assert garden["actual"] == 250.0
        assert [(s["name"], s["planned"]) for s in garden["subcategories"]] == [("Seeds", 600.0)]


class TestMonthTotal:
    """The month-total row is the budget of the whole month, on top of the categories."""

    def test_month_with_only_an_unlocked_total_reports_it_as_the_overall_plan(self, populated_db: Database):
        insert_tag(populated_db, id="tag-hobby", title="Hobby")
        insert_budget(populated_db, tag=TOTAL_TAG, date=BUDGET_DATE, outcome=900.0)
        insert_transaction(populated_db, date="2024-03-05", tag=["tag-hobby"], outcome=200.0)
        insert_transaction(populated_db, date="2024-03-06", tag=None, outcome=100.0)

        result = check_budget_health(populated_db, month=MONTH)

        assert result["categories"] == []
        overall = result["overall"]
        assert overall["planned"] == 900.0
        assert overall["planned_source"] == "month_total"
        # A budget for the whole month is measured against everything spent in it
        assert overall["actual"] == 300.0
        assert overall["actual_scope"] == "all_spending"

    def test_unlocked_total_is_what_is_left_over_the_category_budgets(self, populated_db: Database):
        insert_tag(populated_db, id="tag-hobby", title="Hobby")
        insert_budget(populated_db, tag=TOTAL_TAG, date=BUDGET_DATE, outcome=900.0)
        insert_budget(populated_db, tag="tag-hobby", date=BUDGET_DATE, outcome=100.0)

        result = check_budget_health(populated_db, month=MONTH)

        overall = result["overall"]
        assert overall["planned"] == 1000.0
        assert overall["planned_source"] == "month_total_plus_category_budgets"
        assert overall["planned_breakdown"] == {"month_total": 900.0, "categories": 100.0}

    def test_locked_total_is_the_exact_overall_plan(self, populated_db: Database):
        insert_tag(populated_db, id="tag-hobby", title="Hobby")
        insert_budget(populated_db, tag=TOTAL_TAG, date=BUDGET_DATE, outcome=5000.0, outcome_lock=1)
        insert_budget(populated_db, tag="tag-hobby", date=BUDGET_DATE, outcome=100.0)

        result = check_budget_health(populated_db, month=MONTH)

        overall = result["overall"]
        assert overall["planned"] == 5000.0
        assert overall["planned_source"] == "month_total"
        assert "planned_breakdown" not in overall

    def test_without_a_month_total_overall_covers_the_budgeted_categories(self, populated_db: Database):
        """An unlocked total of 0 is how the API spells 'no budget': same as no total row."""
        insert_tag(populated_db, id="tag-hobby", title="Hobby")
        insert_tag(populated_db, id="tag-books", title="Books")
        insert_budget(populated_db, tag=TOTAL_TAG, date=BUDGET_DATE, outcome=0.0)
        insert_budget(populated_db, tag="tag-hobby", date=BUDGET_DATE, outcome=100.0)
        insert_transaction(populated_db, date="2024-03-05", tag=["tag-hobby"], outcome=60.0)
        insert_transaction(populated_db, date="2024-03-06", tag=["tag-books"], outcome=500.0)

        result = check_budget_health(populated_db, month=MONTH)

        overall = result["overall"]
        assert overall["planned"] == 100.0
        assert overall["planned_source"] == "category_budgets"
        assert overall["actual"] == 60.0
        assert overall["actual_scope"] == "budgeted_categories"


class TestPlannedBreakdown:
    """When the plan is more than the stored amount, its parts are shown."""

    def test_breakdown_shows_budget_scheduled_and_subcategories(self, populated_db: Database):
        insert_tag(populated_db, id="tag-garden", title="Garden")
        insert_tag(populated_db, id="tag-seeds", title="Seeds", parent="tag-garden")
        insert_budget(populated_db, tag="tag-garden", date=BUDGET_DATE, outcome=400.0)
        insert_budget(populated_db, tag="tag-seeds", date=BUDGET_DATE, outcome=600.0)
        insert_reminder_marker(populated_db, tag=["tag-garden"], date="2024-03-10", outcome=50.0)
        insert_reminder_marker(populated_db, tag=["tag-seeds"], date="2024-03-11", outcome=25.0)

        result = check_budget_health(populated_db, month=MONTH)

        garden = _category(result, "Garden")
        assert garden["planned"] == 1075.0
        assert garden["planned_breakdown"] == {"budget": 400.0, "scheduled": 50.0, "subcategories": 625.0}
        seeds = garden["subcategories"][0]
        assert seeds["planned"] == 625.0
        assert seeds["planned_breakdown"] == {"budget": 600.0, "scheduled": 25.0}

    def test_plain_budget_has_no_breakdown(self, populated_db: Database):
        insert_tag(populated_db, id="tag-hobby", title="Hobby")
        insert_budget(populated_db, tag="tag-hobby", date=BUDGET_DATE, outcome=100.0)

        result = check_budget_health(populated_db, month=MONTH)

        assert "planned_breakdown" not in _category(result, "Hobby")


class TestScheduledWithoutBudgetRow:
    """No budget row is the same as an unlocked 0: scheduled expenses alone make a plan."""

    def test_scheduled_expense_of_a_category_without_a_row_is_its_plan(self, populated_db: Database):
        insert_tag(populated_db, id="tag-hobby", title="Hobby")
        insert_tag(populated_db, id="tag-insurance", title="Insurance")
        insert_budget(populated_db, tag="tag-hobby", date=BUDGET_DATE, outcome=100.0)
        insert_reminder_marker(populated_db, tag=["tag-insurance"], date="2024-03-10", state="processed", outcome=45.0)
        insert_transaction(populated_db, date="2024-03-10", tag=["tag-insurance"], outcome=45.0)

        result = check_budget_health(populated_db, month=MONTH)

        insurance = _category(result, "Insurance")
        assert insurance["planned"] == 45.0
        assert insurance["planned_breakdown"] == {"budget": 0.0, "scheduled": 45.0, "subcategories": 0.0}
        assert insurance["actual"] == 45.0
        assert result["overall"]["planned"] == 145.0

    def test_scheduled_expense_of_a_child_without_a_row_counts_for_its_parent(self, populated_db: Database):
        insert_tag(populated_db, id="tag-garden", title="Garden")
        insert_tag(populated_db, id="tag-seeds", title="Seeds", parent="tag-garden")
        insert_budget(populated_db, tag="tag-garden", date=BUDGET_DATE, outcome=400.0)
        insert_reminder_marker(populated_db, tag=["tag-seeds"], date="2024-03-10", outcome=30.0)

        result = check_budget_health(populated_db, month=MONTH)

        garden = _category(result, "Garden")
        assert garden["planned"] == 430.0
        assert [(s["name"], s["planned"]) for s in garden["subcategories"]] == [("Seeds", 30.0)]


class TestStatusThresholds:
    def test_spending_exactly_the_plan_is_not_overspent(self, populated_db: Database):
        """A scheduled bill paid in full uses the plan up; nothing was overspent."""
        insert_tag(populated_db, id="tag-insurance", title="Insurance")
        insert_reminder_marker(populated_db, tag=["tag-insurance"], date="2024-03-10", state="processed", outcome=45.0)
        insert_transaction(populated_db, date="2024-03-10", tag=["tag-insurance"], outcome=45.0)

        result = check_budget_health(populated_db, month=MONTH)

        insurance = _category(result, "Insurance")
        assert insurance["pct_used"] == 100.0
        assert insurance["remaining"] == 0.0
        assert insurance["status"] == "warning"
        assert "insight" not in insurance

    def test_fully_used_budget_of_the_current_month_gets_no_exhaustion_forecast(self, populated_db: Database):
        today = date.today().isoformat()
        insert_tag(populated_db, id="tag-insurance", title="Insurance")
        insert_reminder_marker(populated_db, tag=["tag-insurance"], date=today, state="processed", outcome=45.0)
        insert_transaction(populated_db, date=today, tag=["tag-insurance"], outcome=45.0)

        result = check_budget_health(populated_db)

        insurance = _category(result, "Insurance")
        assert insurance["status"] == "warning"
        assert "insight" not in insurance

    def test_spending_above_the_plan_is_overspent(self, populated_db: Database):
        insert_tag(populated_db, id="tag-hobby", title="Hobby")
        insert_budget(populated_db, tag="tag-hobby", date=BUDGET_DATE, outcome=100.0)
        insert_transaction(populated_db, date="2024-03-10", tag=["tag-hobby"], outcome=100.5)

        result = check_budget_health(populated_db, month=MONTH)

        hobby = _category(result, "Hobby")
        assert hobby["status"] == "overspent"
        assert hobby["insight"] == "Overspent by 0.5 RUB"


class TestBudgetsResource:
    """R3 shows the plan of the current budget month exactly as the tool computes it.

    The fixture already has budgets in the current month: two locked categories
    and a locked month total.
    """

    def test_resource_reports_the_same_plan_as_the_tool(self, populated_db: Database):
        insert_tag(populated_db, id="tag-garden", title="Garden")
        insert_tag(populated_db, id="tag-seeds", title="Seeds", parent="tag-garden")
        insert_tag(populated_db, id="tag-tools", title="Tools", parent="tag-garden")
        insert_budget(populated_db, tag="tag-garden", outcome=-200.0)
        insert_budget(populated_db, tag="tag-seeds", outcome=200.0)
        insert_budget(populated_db, tag="tag-tools", outcome=350.0)
        insert_reminder_marker(populated_db, tag=["tag-garden"], outcome=50.0)
        # Replace the fixture's locked total with an unlocked one
        insert_budget(populated_db, tag=TOTAL_TAG, outcome=900.0)

        resource = get_current_budgets_resource(populated_db)
        tool = check_budget_health(populated_db)

        garden = next(b for b in resource["budgets"] if b["tag_title"] == "Garden")
        assert garden["planned_outcome"] == 400.0
        assert garden["planned_breakdown"] == {"budget": -200.0, "scheduled": 50.0, "subcategories": 550.0}
        assert [(s["tag_title"], s["planned_outcome"]) for s in garden["subcategories"]] == [
            ("Tools", 350.0),
            ("Seeds", 200.0),
        ]
        assert {b["tag_title"]: b["planned_outcome"] for b in resource["budgets"]} == {
            c["name"]: c["planned"] for c in tool["categories"]
        }
        assert resource["total"]["planned_outcome"] == tool["overall"]["planned"]
        assert resource["total"]["planned_breakdown"] == tool["overall"]["planned_breakdown"]
        assert resource["month"] == tool["month"]
        assert resource["period_start"] == tool["period_start"]
        assert resource["period_end"] == tool["period_end"]
        assert resource["currency"] == tool["currency"]

    def test_month_total_is_reported_apart_from_the_categories(self, populated_db: Database):
        """Listed next to the categories it would be summed with them."""
        resource = get_current_budgets_resource(populated_db)

        assert TOTAL_TAG not in [b["tag_id"] for b in resource["budgets"]]
        assert resource["total"]["tag_title"] == "Monthly total"
        assert resource["total"]["planned_outcome"] == 80000.0
        assert resource["total"]["planned_source"] == "month_total"


class TestUnbudgetedSpending:
    """Spending in categories that have no budget is summarised, not hidden."""

    def test_spending_outside_any_budget_is_summarised_by_top_level_category(self, populated_db: Database):
        insert_tag(populated_db, id="tag-hobby", title="Hobby")
        insert_tag(populated_db, id="tag-books", title="Books")
        insert_tag(populated_db, id="tag-comics", title="Comics", parent="tag-books")
        insert_budget(populated_db, tag="tag-hobby", date=BUDGET_DATE, outcome=100.0)
        insert_transaction(populated_db, date="2024-03-05", tag=["tag-hobby"], outcome=60.0)
        insert_transaction(populated_db, date="2024-03-06", tag=["tag-books"], outcome=500.0)
        insert_transaction(populated_db, date="2024-03-07", tag=["tag-comics"], outcome=40.0)
        insert_transaction(populated_db, date="2024-03-08", tag=None, outcome=30.0)

        result = check_budget_health(populated_db, month=MONTH)

        assert result["unbudgeted"] == {
            "total": 570.0,
            "total_categories": 2,
            "returned_categories": 2,
            "top_categories": [
                {"name": "Books", "actual": 540.0},
                {"name": "Uncategorized", "actual": 30.0},
            ],
        }
        assert result["overall"]["actual"] == 60.0

    def test_only_the_largest_unbudgeted_categories_are_listed(self, populated_db: Database):
        insert_tag(populated_db, id="tag-hobby", title="Hobby")
        insert_budget(populated_db, tag="tag-hobby", date=BUDGET_DATE, outcome=100.0)
        for n in range(1, 8):
            insert_tag(populated_db, id=f"tag-extra-{n}", title=f"Extra {n}")
            insert_transaction(populated_db, date="2024-03-05", tag=[f"tag-extra-{n}"], outcome=10.0 * n)

        unbudgeted = check_budget_health(populated_db, month=MONTH)["unbudgeted"]

        assert unbudgeted["total"] == 280.0
        assert unbudgeted["total_categories"] == 7
        assert unbudgeted["returned_categories"] == 5
        assert [c["name"] for c in unbudgeted["top_categories"]] == [
            "Extra 7", "Extra 6", "Extra 5", "Extra 4", "Extra 3",
        ]

    def test_month_fully_inside_its_budgets_has_an_empty_summary(self, populated_db: Database):
        insert_tag(populated_db, id="tag-hobby", title="Hobby")
        insert_budget(populated_db, tag="tag-hobby", date=BUDGET_DATE, outcome=100.0)
        insert_transaction(populated_db, date="2024-03-05", tag=["tag-hobby"], outcome=60.0)

        result = check_budget_health(populated_db, month=MONTH)

        assert result["unbudgeted"] == {
            "total": 0.0,
            "total_categories": 0,
            "returned_categories": 0,
            "top_categories": [],
        }


class TestActualsInSeveralCurrencies:
    def test_expenses_of_one_category_in_two_currencies_are_each_converted(self, populated_db: Database):
        """100 RUB + 10 USD (rate 90) in the same category = 1000 RUB."""
        insert_tag(populated_db, id="tag-hobby", title="Hobby")
        insert_budget(populated_db, tag="tag-hobby", date=BUDGET_DATE, outcome=5000.0)
        insert_transaction(populated_db, date="2024-03-05", tag=["tag-hobby"], outcome=100.0)
        insert_transaction(
            populated_db, date="2024-03-06", tag=["tag-hobby"], outcome=10.0,
            outcome_instrument=2, outcome_account="acc-usd",
        )

        result = check_budget_health(populated_db, month=MONTH)

        assert _category(result, "Hobby")["actual"] == 1000.0
        assert result["overall"]["actual"] == 1000.0


class TestFutureMonth:
    def test_future_month_with_post_dated_spending_has_no_exhaustion_forecast(self, populated_db: Database):
        """No day of the period has passed yet, so there is no pace to extrapolate."""
        insert_tag(populated_db, id="tag-hobby", title="Hobby")
        insert_budget(populated_db, tag="tag-hobby", date="2099-01-01", outcome=100.0)
        insert_transaction(populated_db, date="2099-01-05", tag=["tag-hobby"], outcome=90.0)

        result = check_budget_health(populated_db, month="2099-01")

        hobby = _category(result, "Hobby")
        assert result["status"] == "future"
        assert hobby["status"] == "warning"
        assert "insight" not in hobby


class TestToolOverProtocol:
    """check_budget_health as an MCP client sees it."""

    @pytest.fixture
    def served_db(self, populated_db: Database, monkeypatch) -> Database:
        """Wire the server to the test DB; auto-sync is off, so nothing touches the network."""
        monkeypatch.setenv("ZENMONEY_AUTO_SYNC_SECONDS", "0")
        zm_server.init_for_testing(populated_db)
        yield populated_db
        zm_server._db = None
        zm_server._sync_engine = None

    async def test_answer_is_json_with_overall_and_unbudgeted(self, served_db: Database):
        insert_tag(served_db, id="tag-hobby", title="Hobby")
        insert_budget(served_db, tag="tag-hobby", date=BUDGET_DATE, outcome=0.0)
        insert_transaction(served_db, date="2024-03-05", tag=["tag-hobby"], outcome=75.0)

        async with create_connected_server_and_client_session(zm_server.server) as client:
            result = await client.call_tool("check_budget_health", {"month": MONTH})

        assert not result.isError, result.content[0].text
        payload = json.loads(result.content[0].text)
        assert payload["month"] == MONTH
        assert payload["categories"][0]["status"] == "no_budget"
        assert payload["categories"][0]["pct_used"] is None
        assert payload["overall"]["actual_scope"] == "budgeted_categories"
        assert payload["unbudgeted"]["total"] == 0.0

    async def test_invalid_month_is_reported_as_a_tool_error(self, served_db: Database):
        async with create_connected_server_and_client_session(zm_server.server) as client:
            result = await client.call_tool("check_budget_health", {"month": "2024-13"})

        assert result.isError
        assert "Invalid month '2024-13'" in result.content[0].text
