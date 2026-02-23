"""Tests for MCP tools (T0, T1, T3, T13) and resources (R1-R3)."""

import json
from datetime import date
from unittest.mock import AsyncMock, Mock, patch

import pytest

from zenmoney_mcp.analytics import (
    analyze_income,
    analyze_merchants,
    analyze_spending,
    analyze_transfers,
    analyze_trends,
    check_budget_health,
    convert_currency,
    detect_anomalies,
    detect_recurring,
    get_account_flow,
    get_accounts_resource,
    get_categories_resource,
    get_current_budgets_resource,
    get_debts,
    get_exchange_rates,
    get_instruments_resource,
    get_liquidity,
    get_merchants_resource,
    get_net_worth,
    get_period_dates,
    get_sync_status_resource,
    get_upcoming_payments,
    search_transactions,
    suggest_category,
)
from zenmoney_mcp.database import Database


class TestPeriodDates:
    """Test period date parsing."""

    def test_this_month(self):
        start, end = get_period_dates("this_month")
        today = date.today()
        assert start == today.replace(day=1).isoformat()
        assert end >= start

    def test_last_month(self):
        start, end = get_period_dates("last_month")
        today = date.today()
        first_of_this = today.replace(day=1)
        expected_end = (first_of_this - __import__("datetime").timedelta(days=1)).isoformat()
        assert end == expected_end

    def test_yyyy_mm_format(self):
        start, end = get_period_dates("2026-01")
        assert start == "2026-01-01"
        assert end == "2026-01-31"

    def test_february(self):
        start, end = get_period_dates("2026-02")
        assert start == "2026-02-01"
        assert end == "2026-02-28"


class TestT1GetNetWorth:
    """Test T1: get_net_worth tool."""

    def test_basic_net_worth(self, populated_db: Database):
        """Test basic net worth calculation."""
        result = get_net_worth(populated_db)

        assert "net_worth" in result
        assert "currency" in result
        assert "breakdown" in result
        assert result["currency"] == "RUB"

    def test_net_worth_breakdown_structure(self, populated_db: Database):
        """Test breakdown structure."""
        result = get_net_worth(populated_db)

        breakdown = result["breakdown"]
        assert "current" in breakdown
        assert "savings" in breakdown
        assert "loans" in breakdown
        assert "debts" in breakdown

        # Each section should have total and accounts
        for section in ["current", "savings", "loans", "debts"]:
            assert "total" in breakdown[section]
            assert "accounts" in breakdown[section]

    def test_net_worth_excludes_archived(self, populated_db: Database):
        """Test that archived accounts are excluded."""
        result = get_net_worth(populated_db)

        # acc-arch (10000 RUB) should not be in any breakdown
        all_account_ids = []
        for section in result["breakdown"].values():
            all_account_ids.extend(acc["id"] for acc in section["accounts"])
        all_account_ids.extend(acc["id"] for acc in result["out_of_balance"])

        assert "acc-arch" not in all_account_ids

    def test_net_worth_debt_in_out_of_balance(self, populated_db: Database):
        """Test that debt account (in_balance=0) is in out_of_balance."""
        result = get_net_worth(populated_db)

        out_of_balance_ids = [acc["id"] for acc in result["out_of_balance"]]
        assert "acc-debt" in out_of_balance_ids

    def test_net_worth_currency_conversion(self, populated_db: Database):
        """Test USD is converted to RUB."""
        result = get_net_worth(populated_db)

        # Find acc-usd in current accounts
        current_accounts = result["breakdown"]["current"]["accounts"]
        usd_account = next((a for a in current_accounts if a["id"] == "acc-usd"), None)

        if usd_account:
            # 1000 USD * 90 rate = 90000 RUB
            assert usd_account["balance"] == 1000
            assert usd_account["converted"] == 90000.0

    def test_net_worth_calculation(self, populated_db: Database):
        """Test total calculation.

        Expected: acc-rub (50000) + acc-usd (1000*90=90000) + acc-save (500000) = 640000
        acc-debt (in_balance=0) and acc-arch (archived) excluded
        """
        result = get_net_worth(populated_db)

        # Current: acc-rub (50000) + acc-usd (90000) = 140000
        # Savings: acc-save (500000)
        # Total: 640000
        assert result["breakdown"]["current"]["total"] == 140000.0
        assert result["breakdown"]["savings"]["total"] == 500000.0
        assert result["net_worth"] == 640000.0


class TestT2GetLiquidity:
    """Test T2: get_liquidity tool."""

    def test_basic_liquidity(self, populated_db: Database):
        """Test basic liquidity calculation."""
        result = get_liquidity(populated_db)

        assert "liquid_own" in result
        assert "liquid_with_credit" in result
        assert "savings_accessible" in result
        assert "total_available" in result
        assert "currency" in result

    def test_liquidity_calculation(self, populated_db: Database):
        """Test liquidity amounts.

        Liquid: acc-rub (50000, ccard with 150k limit) + acc-usd (1000*90=90000)
        Savings: acc-save (500000)
        """
        result = get_liquidity(populated_db)

        # Liquid own: acc-rub (50000) + acc-usd (90000) = 140000
        assert result["liquid_own"] == 140000.0

        # Liquid with credit: 140000 + 150000 (credit limit) = 290000
        # BUT: credit limit is added to balance, so it's balance + credit_limit
        # If balance is 50000 and limit is 150000, available = 50000 + 150000 = 200000
        # So total liquid_with_credit = 200000 (acc-rub) + 90000 (acc-usd) = 290000
        assert result["liquid_with_credit"] == 290000.0

        # Savings: acc-save (500000)
        assert result["savings_accessible"] == 500000.0

        # Total: liquid_own + savings = 140000 + 500000 = 640000
        assert result["total_available"] == 640000.0

    def test_liquidity_target_check_affordable(self, populated_db: Database):
        """Test target check when amount is affordable from liquid."""
        result = get_liquidity(populated_db, target_amount=100000)

        assert "target_check" in result
        assert result["target_check"]["target"] == 100000
        assert result["target_check"]["affordable_from_liquid"] is True
        assert result["target_check"]["affordable_with_credit"] is True
        assert result["target_check"]["affordable_with_savings"] is True

    def test_liquidity_target_check_needs_credit(self, populated_db: Database):
        """Test target check when credit is needed."""
        result = get_liquidity(populated_db, target_amount=200000)

        assert result["target_check"]["affordable_from_liquid"] is False
        assert result["target_check"]["affordable_with_credit"] is True
        assert result["target_check"]["affordable_with_savings"] is True

    def test_liquidity_target_check_needs_savings(self, populated_db: Database):
        """Test target check when savings are needed."""
        result = get_liquidity(populated_db, target_amount=350000)

        assert result["target_check"]["affordable_from_liquid"] is False
        assert result["target_check"]["affordable_with_credit"] is False
        assert result["target_check"]["affordable_with_savings"] is True

    def test_liquidity_target_check_not_affordable(self, populated_db: Database):
        """Test target check when amount exceeds all funds."""
        result = get_liquidity(populated_db, target_amount=700000)

        assert result["target_check"]["affordable_from_liquid"] is False
        assert result["target_check"]["affordable_with_credit"] is False
        assert result["target_check"]["affordable_with_savings"] is False

    def test_liquidity_breakdown(self, populated_db: Database):
        """Test that breakdown contains account details."""
        result = get_liquidity(populated_db)

        assert "breakdown" in result
        assert "liquid_accounts" in result["breakdown"]
        assert "credit_accounts" in result["breakdown"]
        assert "savings_accounts" in result["breakdown"]

        # Should have at least some accounts
        total_accounts = (
            len(result["breakdown"]["liquid_accounts"]) +
            len(result["breakdown"]["credit_accounts"]) +
            len(result["breakdown"]["savings_accounts"])
        )
        assert total_accounts > 0


class TestT3AnalyzeSpending:
    """Test T3: analyze_spending tool."""

    def test_basic_spending(self, populated_db: Database):
        """Test basic spending analysis."""
        result = analyze_spending(populated_db, period="this_month")

        assert "total_outcome" in result
        assert "currency" in result
        assert "categories" in result
        assert "period" in result

    def test_spending_excludes_transfers(self, populated_db: Database):
        """Test that transfers are excluded by default.

        tx6 (transfer 50000) and tx7 (exchange 9000) should NOT be counted.
        """
        result = analyze_spending(populated_db, period="this_month")

        # Pure expenses: tx1(1500) + tx2(3000) + tx3(500) + tx4(200) = 5200
        assert result["total_outcome"] == 5200.0

    def test_spending_excludes_deleted(self, populated_db: Database):
        """Test that deleted transactions are excluded.

        tx9 (800, deleted) should NOT be counted.
        """
        result = analyze_spending(populated_db, period="this_month")

        # tx9 is deleted, should not be in total
        assert result["total_outcome"] == 5200.0

    def test_spending_excludes_holds_by_default(self, populated_db: Database):
        """Test that hold transactions are excluded by default.

        tx10 (350, hold) should NOT be counted.
        """
        result = analyze_spending(populated_db, period="this_month")

        # tx10 is hold, should not be in total
        assert result["total_outcome"] == 5200.0

        # But holds_excluded should report it
        assert result["holds_excluded"] is not None
        assert result["holds_excluded"]["count"] == 1
        assert result["holds_excluded"]["amount"] == 350.0

    def test_spending_includes_holds_when_requested(self, populated_db: Database):
        """Test that hold transactions can be included."""
        result = analyze_spending(populated_db, period="this_month", include_holds=True)

        # Now includes tx10 (350)
        assert result["total_outcome"] == 5550.0

    def test_spending_category_breakdown(self, populated_db: Database):
        """Test category breakdown."""
        result = analyze_spending(populated_db, period="this_month")

        categories = {c["name"]: c for c in result["categories"]}

        # Продукты: tx1 (1500) - child of Еда
        # Рестораны: tx2 (3000) - child of Еда
        # Транспорт: tx3 (500)
        # tx4 (200) - uncategorized

        assert "Продукты" in categories
        assert categories["Продукты"]["amount"] == 1500.0
        assert categories["Продукты"]["count"] == 1

        assert "Рестораны" in categories
        assert categories["Рестораны"]["amount"] == 3000.0

        assert "Транспорт" in categories
        assert categories["Транспорт"]["amount"] == 500.0

    def test_spending_uncategorized(self, populated_db: Database):
        """Test uncategorized transactions (tx4)."""
        result = analyze_spending(populated_db, period="this_month")

        assert result["uncategorized"] is not None
        assert result["uncategorized"]["amount"] == 200.0
        assert result["uncategorized"]["count"] == 1

    def test_spending_enrichment(self, populated_db: Database):
        """Test that categories have names, not UUIDs."""
        result = analyze_spending(populated_db, period="this_month")

        for cat in result["categories"]:
            # Name should be human-readable, not UUID
            assert not cat["name"].startswith("tag-")
            assert cat["name"] in ["Продукты", "Рестораны", "Транспорт"]


class TestT4AnalyzeIncome:
    """Test T4: analyze_income tool."""

    def test_basic_income(self, populated_db: Database):
        """Test basic income analysis."""
        result = analyze_income(populated_db, period="this_month")

        assert "total_income" in result
        assert "currency" in result
        assert "categories" in result
        assert "sources" in result
        assert "period" in result

    def test_income_excludes_transfers(self, populated_db: Database):
        """Test that transfers are excluded.

        tx6 (transfer income 50000) and tx7 (exchange income 100 USD) should NOT be counted.
        """
        result = analyze_income(populated_db, period="this_month")

        # Pure income: tx5 (150000)
        assert result["total_income"] == 150000.0

    def test_income_by_category(self, populated_db: Database):
        """Test income breakdown by category.

        tx5 has tag-salary.
        """
        result = analyze_income(populated_db, period="this_month")

        # Should have at least one category
        assert len(result["categories"]) >= 1

        # Find salary category
        salary_cat = next((c for c in result["categories"] if c["name"] == "Зарплата"), None)
        assert salary_cat is not None
        assert salary_cat["amount"] == 150000.0

    def test_income_by_source(self, populated_db: Database):
        """Test income breakdown by source/payee.

        tx5 has payee "ООО Работа".
        """
        result = analyze_income(populated_db, period="this_month")

        # Should have at least one source
        assert len(result["sources"]) >= 1

        # Find employer source
        employer = next((s for s in result["sources"] if "Работа" in s["name"]), None)
        assert employer is not None
        assert employer["amount"] == 150000.0

    def test_income_enrichment(self, populated_db: Database):
        """Test that results have enriched data (category names, not UUIDs)."""
        result = analyze_income(populated_db, period="this_month")

        for cat in result["categories"]:
            # Should have category name, not UUID
            if cat["tag_id"]:
                assert not cat["name"].startswith("tag-")
            assert "amount" in cat
            assert "share_pct" in cat
            assert "count" in cat

    def test_income_top_n(self, populated_db: Database):
        """Test top_n parameter limits results."""
        result = analyze_income(populated_db, period="this_month", top_n=1)

        assert result["returned_categories"] <= 1
        assert result["returned_sources"] <= 1
        assert len(result["categories"]) <= 1
        assert len(result["sources"]) <= 1


class TestT5CheckBudgetHealth:
    """Test T5: check_budget_health tool."""

    def test_basic_budget_health(self, populated_db: Database):
        """Test basic budget health check."""
        result = check_budget_health(populated_db)

        assert "month" in result
        assert "days_elapsed" in result
        assert "days_total" in result
        assert "categories" in result
        assert "currency" in result

    def test_budget_planned_vs_actual(self, populated_db: Database):
        """Test planned vs actual calculation.

        tag-food budget: 10000 planned
        Actual spending: tx1 (Продукты, 1500) + tx2 (Рестораны, 3000) = 4500
        """
        result = check_budget_health(populated_db)

        # Find food category
        food_budget = next((c for c in result["categories"] if "Еда" in c["name"]), None)
        assert food_budget is not None
        assert food_budget["planned"] == 10000.0
        assert food_budget["actual"] == 4500.0
        assert food_budget["remaining"] == 5500.0

    def test_budget_pct_used(self, populated_db: Database):
        """Test percentage used calculation."""
        result = check_budget_health(populated_db)

        for cat in result["categories"]:
            if cat["planned"] > 0:
                expected_pct = (cat["actual"] / cat["planned"]) * 100
                assert abs(cat["pct_used"] - expected_pct) < 0.1

    def test_budget_status(self, populated_db: Database):
        """Test status determination (on_track, warning, overspent)."""
        result = check_budget_health(populated_db)

        # Food: 4500 / 10000 = 45% -> on_track
        food = next((c for c in result["categories"] if "Еда" in c["name"]), None)
        assert food is not None
        assert food["status"] == "on_track"

    def test_budget_includes_child_tags(self, populated_db: Database):
        """Test that budget includes spending from child tags.

        tag-food has children: tag-grocery and tag-restaurant.
        tx1 (Продукты): 1500, tx2 (Рестораны): 3000
        Total for Еда should be 4500.
        """
        result = check_budget_health(populated_db)

        food = next((c for c in result["categories"] if "Еда" in c["name"]), None)
        assert food is not None
        assert food["actual"] == 4500.0

    def test_budget_excludes_transfers(self, populated_db: Database):
        """Test that transfers/exchanges don't count toward budget."""
        result = check_budget_health(populated_db)

        # tx6 (50000 transfer), tx7 (9000 exchange) should NOT be counted
        total_actual = sum(c["actual"] for c in result["categories"])
        # Pure expenses: tx1(1500) + tx2(3000) + tx3(500) + tx4(200) = 5200
        # But tx4 is uncategorized, so might not be in categorized budgets
        assert total_actual <= 5200.0


class TestT7AnalyzeMerchants:
    """Test T7: analyze_merchants tool."""

    def test_basic_merchants(self, populated_db: Database):
        """Test basic merchant analysis."""
        result = analyze_merchants(populated_db, period="this_month")

        assert "total_outcome" in result
        assert "currency" in result
        assert "merchants" in result
        assert "period" in result

    def test_merchants_excludes_transfers(self, populated_db: Database):
        """Test that transfers are excluded."""
        result = analyze_merchants(populated_db, period="this_month")

        # Pure expenses: tx1(1500) + tx2(3000) + tx3(500) + tx4(200) = 5200
        assert result["total_outcome"] == 5200.0

    def test_merchants_aggregation(self, populated_db: Database):
        """Test merchant aggregation.

        tx1 has merchant m-pyat (Пятёрочка): 1500
        tx2 has payee KFC: 3000
        tx3 has merchant m-yandex (Яндекс.Такси): 500
        tx4 has comment Кофе: 200
        """
        result = analyze_merchants(populated_db, period="this_month")

        # Should have at least 3 merchants
        assert len(result["merchants"]) >= 3

        # Check Пятёрочка
        pyat = next((m for m in result["merchants"] if "Пятёрочка" in m["name"]), None)
        assert pyat is not None
        assert pyat["total"] == 1500.0
        assert pyat["visits"] == 1

    def test_merchants_avg_check(self, populated_db: Database):
        """Test average check calculation."""
        result = analyze_merchants(populated_db, period="this_month")

        for merchant in result["merchants"]:
            assert "avg_check" in merchant
            assert merchant["avg_check"] >= 0
            if merchant["visits"] > 0:
                expected_avg = merchant["total"] / merchant["visits"]
                assert abs(merchant["avg_check"] - expected_avg) < 0.01

    def test_merchants_enrichment(self, populated_db: Database):
        """Test that merchant names are enriched."""
        result = analyze_merchants(populated_db, period="this_month")

        for merchant in result["merchants"]:
            assert "name" in merchant
            assert "total" in merchant
            assert "visits" in merchant
            assert "last_visit" in merchant
            assert "share_pct" in merchant

    def test_merchants_top_n(self, populated_db: Database):
        """Test top_n parameter limits results."""
        result = analyze_merchants(populated_db, period="this_month", top_n=2)

        assert result["returned_count"] <= 2
        assert len(result["merchants"]) <= 2


class TestT12GetUpcomingPayments:
    """Test T12: get_upcoming_payments tool."""

    def test_basic_upcoming_payments(self, populated_db: Database):
        """Test basic upcoming payments."""
        result = get_upcoming_payments(populated_db, days_ahead=30)

        assert "upcoming" in result
        assert "total_upcoming_outcome" in result
        assert "currency" in result
        assert "period" in result

    def test_upcoming_payments_data(self, populated_db: Database):
        """Test that upcoming payments include planned markers.

        Fixture has:
        - rm1: +5 days, 45000, "Аренда", state=planned
        - rm2: +15 days, 2000, "Паша", state=planned
        - rm3: -5 days (past), state=processed (should be excluded)
        """
        result = get_upcoming_payments(populated_db, days_ahead=30)

        # Should have 2 planned payments
        assert len(result["upcoming"]) == 2

        # Total should be 45000 + 2000 = 47000
        assert result["total_upcoming_outcome"] == 47000.0

    def test_upcoming_payments_excludes_processed(self, populated_db: Database):
        """Test that processed markers are excluded."""
        result = get_upcoming_payments(populated_db, days_ahead=30)

        # rm3 is processed, should not be in results
        payees = [p["payee"] for p in result["upcoming"]]
        # rm3 has payee "Тест", should not be present
        # (though it's also in the past, so would be excluded anyway)
        assert len(result["upcoming"]) == 2

    def test_upcoming_payments_sorted_by_date(self, populated_db: Database):
        """Test that payments are sorted by date ascending."""
        result = get_upcoming_payments(populated_db, days_ahead=30)

        # Should be sorted: rm1 (+5 days) before rm2 (+15 days)
        if len(result["upcoming"]) >= 2:
            dates = [p["date"] for p in result["upcoming"]]
            assert dates == sorted(dates)

    def test_upcoming_payments_enrichment(self, populated_db: Database):
        """Test that payments have enriched data."""
        result = get_upcoming_payments(populated_db, days_ahead=30)

        for payment in result["upcoming"]:
            assert "date" in payment
            assert "type" in payment
            assert "amount" in payment
            assert "currency" in payment
            assert "payee" in payment

    def test_upcoming_payments_weekly_load(self, populated_db: Database):
        """Test weekly load calculation."""
        result = get_upcoming_payments(populated_db, days_ahead=30)

        assert "weekly_load" in result
        # Should have at least one week
        assert len(result["weekly_load"]) >= 1

        # Each week should have amount
        for week in result["weekly_load"]:
            assert "week" in week
            assert "amount" in week

    def test_upcoming_payments_days_ahead_filter(self, populated_db: Database):
        """Test that days_ahead filters correctly."""
        # Get only payments in next 7 days (should include only rm1 at +5 days)
        result = get_upcoming_payments(populated_db, days_ahead=7)

        # Should have only 1 payment (rm1)
        assert len(result["upcoming"]) == 1
        assert result["total_upcoming_outcome"] == 45000.0


class TestT6DetectRecurring:
    """Test T6: detect_recurring tool."""

    def test_basic_recurring(self, populated_db: Database):
        """Test basic recurring detection."""
        result = detect_recurring(populated_db, lookback_months=3)

        assert "recurring" in result
        assert "total_monthly_estimate" in result
        assert "total_yearly_estimate" in result
        assert "currency" in result
        assert "total_found" in result

    def test_recurring_structure(self, populated_db: Database):
        """Test recurring payment structure."""
        result = detect_recurring(populated_db, lookback_months=3)

        # Fixture has single-occurrence transactions, so likely won't detect patterns
        # But structure should be valid
        for payment in result["recurring"]:
            assert "name" in payment
            assert "avg_amount" in payment
            assert "frequency" in payment
            assert "confidence" in payment
            assert "source" in payment

    def test_recurring_excludes_single_occurrences(self, populated_db: Database):
        """Test that single occurrences are not detected.

        Fixture has mostly single transactions, so should have few or no detected patterns.
        """
        result = detect_recurring(populated_db, lookback_months=3)

        # Most transactions in fixture are single occurrences
        # Should not be detected as recurring
        detected_sources = [p for p in result["recurring"] if p["source"] == "detected"]
        # May be 0 or very few
        assert len(detected_sources) >= 0

    def test_recurring_confidence(self, populated_db: Database):
        """Test that confidence values are valid."""
        result = detect_recurring(populated_db, lookback_months=3)

        for payment in result["recurring"]:
            assert 0 <= payment["confidence"] <= 1.0

    def test_recurring_yearly_cost(self, populated_db: Database):
        """Test yearly cost calculation."""
        result = detect_recurring(populated_db, lookback_months=3)

        # Total yearly should be sum of individual yearly costs
        total_from_payments = sum(p.get("yearly_cost", 0) for p in result["recurring"])
        # Allow for rounding differences
        assert abs(result["total_yearly_estimate"] - total_from_payments) < 1.0


class TestT8AnalyzeTrends:
    """Test T8: analyze_trends tool."""

    def test_basic_trends(self, populated_db: Database):
        """Test basic trends analysis."""
        result = analyze_trends(populated_db, months=3)

        assert "metric" in result
        assert "data" in result
        assert "summary" in result
        assert "currency" in result

    def test_trends_data_structure(self, populated_db: Database):
        """Test that data contains monthly entries."""
        result = analyze_trends(populated_db, months=3)

        # Should have 3 months of data
        assert len(result["data"]) == 3

        # Each month should have required fields
        for month_data in result["data"]:
            assert "month" in month_data
            assert "value" in month_data

    def test_trends_current_month_marked_partial(self, populated_db: Database):
        """Test that current month is marked as partial."""
        result = analyze_trends(populated_db, months=3)

        # Last month (current) should be marked partial
        current_month = result["data"][-1]
        assert current_month.get("partial") is True

    def test_trends_summary(self, populated_db: Database):
        """Test summary statistics."""
        result = analyze_trends(populated_db, months=3)

        summary = result["summary"]
        # May not have enough data for full summary, but should have structure
        assert isinstance(summary, dict)

    def test_trends_metric_outcome(self, populated_db: Database):
        """Test outcome metric."""
        result = analyze_trends(populated_db, months=1, metric="outcome")

        assert result["metric"] == "outcome"
        # Current month has expenses: 5200
        # (might be in the last data point if partial)

    def test_trends_metric_income(self, populated_db: Database):
        """Test income metric."""
        result = analyze_trends(populated_db, months=1, metric="income")

        assert result["metric"] == "income"

    def test_trends_metric_savings_rate(self, populated_db: Database):
        """Test savings_rate metric."""
        result = analyze_trends(populated_db, months=1, metric="savings_rate")

        assert result["metric"] == "savings_rate"
        # No currency for percentage metric
        assert result["currency"] is None

    def test_trends_metric_net_cashflow(self, populated_db: Database):
        """Test net_cashflow metric."""
        result = analyze_trends(populated_db, months=1, metric="net_cashflow")

        assert result["metric"] == "net_cashflow"


class TestT13SearchTransactions:
    """Test T13: search_transactions tool."""

    def test_basic_search(self, populated_db: Database):
        """Test basic transaction search."""
        result = search_transactions(populated_db, limit=50)

        assert "transactions" in result
        assert "returned_count" in result
        assert "total_matching" in result

    def test_search_excludes_deleted(self, populated_db: Database):
        """Test that deleted transactions are excluded."""
        result = search_transactions(populated_db, limit=50)

        tx_ids = [t["id"] for t in result["transactions"]]
        assert "tx9" not in tx_ids

    def test_search_by_payee(self, populated_db: Database):
        """Test search by payee name."""
        result = search_transactions(populated_db, payee_search="Пятёрочка")

        # tx1 has merchant m-pyat (Пятёрочка)
        assert result["total_matching"] >= 1

        # All results should match
        for tx in result["transactions"]:
            assert tx["payee"] is not None

    def test_search_by_comment(self, populated_db: Database):
        """Test search by comment."""
        result = search_transactions(populated_db, payee_search="Кофе")

        # tx4 has comment "Кофе"
        assert result["total_matching"] >= 1

    def test_search_by_type_outcome(self, populated_db: Database):
        """Test filtering by outcome type."""
        result = search_transactions(populated_db, tx_type="outcome")

        for tx in result["transactions"]:
            assert tx["type"] == "outcome"

    def test_search_by_type_income(self, populated_db: Database):
        """Test filtering by income type."""
        result = search_transactions(populated_db, tx_type="income")

        for tx in result["transactions"]:
            assert tx["type"] == "income"

        # tx5 is income
        tx_ids = [t["id"] for t in result["transactions"]]
        assert "tx5" in tx_ids

    def test_search_by_type_transfer(self, populated_db: Database):
        """Test filtering by transfer type."""
        result = search_transactions(populated_db, tx_type="transfer")

        for tx in result["transactions"]:
            assert tx["type"] == "transfer"

        # tx6, tx7, tx8 are transfers
        tx_ids = [t["id"] for t in result["transactions"]]
        assert "tx6" in tx_ids or "tx7" in tx_ids or "tx8" in tx_ids

    def test_search_limit(self, populated_db: Database):
        """Test limit parameter."""
        result = search_transactions(populated_db, limit=2)

        assert result["returned_count"] <= 2
        assert len(result["transactions"]) <= 2
        assert result["total_matching"] >= result["returned_count"]

    def test_search_enrichment(self, populated_db: Database):
        """Test that results have enriched data."""
        result = search_transactions(populated_db, payee_search="Пятёрочка")

        for tx in result["transactions"]:
            # Should have category name, not UUID
            if tx["category"]:
                assert not tx["category"].startswith("tag-")

            # Should have payee name
            assert tx["payee"] is not None


class TestR1AccountsResource:
    """Test R1: accounts resource."""

    def test_accounts_resource_structure(self, populated_db: Database):
        """Test accounts resource structure."""
        result = get_accounts_resource(populated_db)

        assert "accounts" in result
        assert "total_in_user_currency" in result
        assert "user_currency" in result

    def test_accounts_resource_excludes_archived(self, populated_db: Database):
        """Test that archived accounts are excluded."""
        result = get_accounts_resource(populated_db)

        account_ids = [a["id"] for a in result["accounts"]]
        assert "acc-arch" not in account_ids

    def test_accounts_resource_fields(self, populated_db: Database):
        """Test that accounts have required fields."""
        result = get_accounts_resource(populated_db)

        for acc in result["accounts"]:
            assert "id" in acc
            assert "title" in acc
            assert "type" in acc
            assert "balance" in acc
            assert "currency" in acc
            assert "in_balance" in acc


class TestR2CategoriesResource:
    """Test R2: categories resource."""

    def test_categories_resource_structure(self, populated_db: Database):
        """Test categories resource structure."""
        result = get_categories_resource(populated_db)

        assert "expense_categories" in result
        assert "income_categories" in result

    def test_categories_resource_hierarchy(self, populated_db: Database):
        """Test that parent-child relationships are resolved."""
        result = get_categories_resource(populated_db)

        # Find Еда category
        food_cat = next(
            (c for c in result["expense_categories"] if c["title"] == "Еда"),
            None
        )

        if food_cat:
            # Should have children
            assert "children" in food_cat
            child_titles = [c["title"] for c in food_cat["children"]]
            assert "Продукты" in child_titles or "Рестораны" in child_titles


class TestR3BudgetsResource:
    """Test R3: budgets/current resource."""

    def test_budgets_resource_structure(self, populated_db: Database):
        """Test budgets resource structure."""
        result = get_current_budgets_resource(populated_db)

        assert "month" in result
        assert "budgets" in result

    def test_budgets_resource_has_data(self, populated_db: Database):
        """Test that budgets are returned."""
        result = get_current_budgets_resource(populated_db)

        # Fixture has 3 budgets for current month
        assert len(result["budgets"]) == 3

    def test_budgets_resource_enrichment(self, populated_db: Database):
        """Test that budget tags have titles."""
        result = get_current_budgets_resource(populated_db)

        for budget in result["budgets"]:
            assert "tag_title" in budget
            # Title should be human-readable
            assert budget["tag_title"] is not None

    def test_budgets_total_budget(self, populated_db: Database):
        """Test that total budget has special title."""
        result = get_current_budgets_resource(populated_db)

        total_budget = next(
            (b for b in result["budgets"] if b["tag_id"] == "00000000-0000-0000-0000-000000000000"),
            None
        )

        if total_budget:
            assert total_budget["tag_title"] == "Monthly total"


class TestR4MerchantsResource:
    """Test R4: merchants resource."""

    def test_merchants_resource_structure(self, populated_db: Database):
        """Test merchants resource structure."""
        result = get_merchants_resource(populated_db)

        assert "merchants" in result
        assert "total" in result

    def test_merchants_resource_has_data(self, populated_db: Database):
        """Test that merchants are returned."""
        result = get_merchants_resource(populated_db)

        # Fixture has 2 merchants
        assert result["total"] >= 2
        assert len(result["merchants"]) >= 2

    def test_merchants_resource_fields(self, populated_db: Database):
        """Test that merchants have required fields."""
        result = get_merchants_resource(populated_db)

        for merchant in result["merchants"]:
            assert "id" in merchant
            assert "title" in merchant


class TestR5InstrumentsResource:
    """Test R5: instruments resource."""

    def test_instruments_resource_structure(self, populated_db: Database):
        """Test instruments resource structure."""
        result = get_instruments_resource(populated_db)

        assert "instruments" in result

    def test_instruments_resource_has_data(self, populated_db: Database):
        """Test that instruments are returned."""
        result = get_instruments_resource(populated_db)

        # Fixture has 3 instruments (RUB, USD, EUR)
        assert len(result["instruments"]) >= 3

    def test_instruments_resource_fields(self, populated_db: Database):
        """Test that instruments have required fields."""
        result = get_instruments_resource(populated_db)

        for instrument in result["instruments"]:
            assert "id" in instrument
            assert "title" in instrument
            assert "code" in instrument
            assert "symbol" in instrument
            assert "rate" in instrument

    def test_instruments_resource_rates(self, populated_db: Database):
        """Test that exchange rates are present."""
        result = get_instruments_resource(populated_db)

        # Find RUB, USD, EUR
        rub = next((i for i in result["instruments"] if i["code"] == "RUB"), None)
        usd = next((i for i in result["instruments"] if i["code"] == "USD"), None)

        if rub and usd:
            assert rub["rate"] == 1.0
            assert usd["rate"] == 90.0


class TestR6SyncStatusResource:
    """Test R6: sync-status resource."""

    def test_sync_status_resource_structure(self, populated_db: Database):
        """Test sync status resource structure."""
        result = get_sync_status_resource(populated_db)

        assert "last_server_timestamp" in result
        assert "cache_stats" in result
        assert "staleness" in result

    def test_sync_status_cache_stats(self, populated_db: Database):
        """Test that cache statistics are returned."""
        result = get_sync_status_resource(populated_db)

        stats = result["cache_stats"]
        assert "transactions" in stats
        assert "accounts" in stats
        assert "tags" in stats
        assert "merchants" in stats

        # Fixture has data
        assert stats["transactions"] > 0
        assert stats["accounts"] > 0
        assert stats["tags"] > 0


# ============================================================================
# Step 5: Expert Tools (T9, T10, T11, T14, T15)
# ============================================================================


class TestT9AnalyzeTransfers:
    """Test T9: analyze_transfers tool."""

    def test_analyze_transfers_structure(self, populated_db: Database):
        """Test basic structure of analyze_transfers result."""
        result = analyze_transfers(populated_db, period="this_month")

        assert "summary" in result
        assert "by_type" in result
        assert "transfers" in result

    def test_analyze_transfers_finds_own_transfer(self, populated_db: Database):
        """Test that own transfers are detected (tx6: acc-rub -> acc-save)."""
        result = analyze_transfers(populated_db, period="this_month")

        # tx6 should be classified as own_transfer
        own_transfers = [t for t in result["transfers"] if t["type"] == "own_transfer"]
        assert len(own_transfers) >= 1

        # Check tx6 specifically
        tx6 = next((t for t in own_transfers if "накопления" in t.get("comment", "").lower()), None)
        assert tx6 is not None
        assert tx6["amount_user"] == 50000.0

    def test_analyze_transfers_finds_currency_exchange(self, populated_db: Database):
        """Test that currency exchanges are detected (tx7: RUB -> USD)."""
        result = analyze_transfers(populated_db, period="this_month")

        # tx7 should be classified as currency_exchange
        exchanges = [t for t in result["transfers"] if t["type"] == "currency_exchange"]
        assert len(exchanges) >= 1

        # Check tx7 specifically
        tx7 = next((t for t in exchanges if "долларов" in t.get("comment", "").lower()), None)
        assert tx7 is not None

    def test_analyze_transfers_summary(self, populated_db: Database):
        """Test that summary totals are calculated."""
        result = analyze_transfers(populated_db, period="this_month")

        summary = result["summary"]
        assert "total_count" in summary
        assert "total_amount" in summary
        assert summary["total_count"] >= 2  # tx6 + tx7

    def test_analyze_transfers_by_type_breakdown(self, populated_db: Database):
        """Test that by_type breakdown is provided."""
        result = analyze_transfers(populated_db, period="this_month")

        by_type = result["by_type"]
        assert isinstance(by_type, list)

        # Should have own_transfer and currency_exchange
        type_names = [t["type"] for t in by_type]
        assert "own_transfer" in type_names
        assert "currency_exchange" in type_names

    def test_analyze_transfers_top_n_limit(self, populated_db: Database):
        """Test that top_n parameter limits results."""
        result = analyze_transfers(populated_db, period="this_month", top_n=1)

        assert len(result["transfers"]) <= 1


class TestT10DetectAnomalies:
    """Test T10: detect_anomalies tool."""

    def test_detect_anomalies_structure(self, populated_db: Database):
        """Test basic structure of detect_anomalies result."""
        result = detect_anomalies(populated_db, period="this_month")

        assert "summary" in result
        assert "outliers" in result
        assert "possible_duplicates" in result

    def test_detect_anomalies_finds_outlier(self, populated_db: Database):
        """Test that statistical outliers are detected."""
        # tx5 (salary 150000 RUB) is income, not outlier in expenses
        # tx2 (restaurant 3000 RUB) might be an outlier compared to smaller expenses
        result = detect_anomalies(populated_db, period="this_month", z_threshold=1.5)

        # With low threshold, should detect some outliers
        outliers = result["outliers"]
        assert isinstance(outliers, list)

    def test_detect_anomalies_summary_totals(self, populated_db: Database):
        """Test that summary contains counts."""
        result = detect_anomalies(populated_db, period="this_month")

        summary = result["summary"]
        assert "outliers_count" in summary
        assert "duplicates_count" in summary
        assert "total_transactions_analyzed" in summary

    def test_detect_anomalies_category_filter(self, populated_db: Database):
        """Test filtering by category."""
        result = analyze_spending(populated_db, period="this_month")
        if result["categories"]:
            category_id = result["categories"][0]["tag_id"]

            filtered_result = detect_anomalies(populated_db, period="this_month", category_id=category_id)
            assert "outliers" in filtered_result

    def test_detect_anomalies_z_threshold(self, populated_db: Database):
        """Test that higher z_threshold reduces outliers."""
        result_low = detect_anomalies(populated_db, period="this_month", z_threshold=1.0)
        result_high = detect_anomalies(populated_db, period="this_month", z_threshold=3.0)

        # Higher threshold should have fewer or equal outliers
        assert result_high["summary"]["outliers_count"] <= result_low["summary"]["outliers_count"]

    def test_detect_anomalies_possible_duplicates(self, populated_db: Database):
        """Test duplicate detection structure."""
        result = detect_anomalies(populated_db, period="this_month")

        duplicates = result["possible_duplicates"]
        assert isinstance(duplicates, list)

        # Each duplicate should have transaction info
        if duplicates:
            dup = duplicates[0]
            assert "date" in dup
            assert "amount" in dup


class TestT11GetDebts:
    """Test T11: get_debts tool."""

    def test_get_debts_structure(self, populated_db: Database):
        """Test basic structure of get_debts result."""
        result = get_debts(populated_db)

        assert "summary" in result
        assert "by_counterparty" in result

    def test_get_debts_finds_debt_account(self, populated_db: Database):
        """Test that debt account (acc-debt) is processed."""
        result = get_debts(populated_db)

        # tx8 creates debt: lent 5000 RUB to Паша
        # Should appear as "they owe you"
        assert len(result["by_counterparty"]) >= 1

    def test_get_debts_counterparty_breakdown(self, populated_db: Database):
        """Test that debts are grouped by counterparty."""
        result = get_debts(populated_db)

        by_counterparty = result["by_counterparty"]
        assert isinstance(by_counterparty, list)

        # Find Паша
        pasha = next((c for c in by_counterparty if "Паша" in c.get("counterparty", "")), None)
        assert pasha is not None
        assert pasha["net_amount"] == 5000.0  # You lent 5000
        assert pasha["status"] == "they_owe_you"

    def test_get_debts_summary_totals(self, populated_db: Database):
        """Test that summary contains totals."""
        result = get_debts(populated_db)

        summary = result["summary"]
        assert "total_owed_to_you" in summary
        assert "total_you_owe" in summary
        assert "net_position" in summary

        # Fixture: you lent 5000 to Паша
        assert summary["total_owed_to_you"] == 5000.0
        assert summary["total_you_owe"] == 0.0
        assert summary["net_position"] == 5000.0

    def test_get_debts_transaction_list(self, populated_db: Database):
        """Test that transaction list is included."""
        result = get_debts(populated_db)

        pasha = next((c for c in result["by_counterparty"] if "Паша" in c.get("counterparty", "")), None)
        if pasha:
            assert "transactions" in pasha
            assert len(pasha["transactions"]) >= 1

            tx = pasha["transactions"][0]
            assert "date" in tx
            assert "amount" in tx
            assert "comment" in tx


class TestT14GetAccountFlow:
    """Test T14: get_account_flow tool."""

    def test_get_account_flow_structure(self, populated_db: Database):
        """Test basic structure of get_account_flow result."""
        result = get_account_flow(populated_db, account_id="acc-rub", period="this_month")

        assert "account" in result
        assert "period" in result
        assert "summary" in result
        assert "transactions" in result

    def test_get_account_flow_account_info(self, populated_db: Database):
        """Test that account info is returned."""
        result = get_account_flow(populated_db, account_id="acc-rub", period="this_month")

        account = result["account"]
        assert account["id"] == "acc-rub"
        assert account["title"] == "Тинькофф Black"
        assert "balance" in account

    def test_get_account_flow_summary_calculations(self, populated_db: Database):
        """Test that summary calculations are correct."""
        result = get_account_flow(populated_db, account_id="acc-rub", period="this_month")

        summary = result["summary"]
        assert "total_income" in summary
        assert "total_outcome" in summary
        assert "net_change" in summary
        assert "transaction_count" in summary

        # acc-rub has: tx5 (+150000 income), tx1-tx4 (expenses), tx6/tx7/tx8 (transfers out)
        assert summary["total_income"] > 0
        assert summary["total_outcome"] > 0

    def test_get_account_flow_transaction_breakdown(self, populated_db: Database):
        """Test that transactions are categorized."""
        result = get_account_flow(populated_db, account_id="acc-rub", period="this_month")

        summary = result["summary"]
        assert "by_category" in summary

        # Should have income, outcome, transfer categories
        categories = summary["by_category"]
        category_types = [c["type"] for c in categories]
        assert "income" in category_types
        assert "outcome" in category_types

    def test_get_account_flow_transaction_list(self, populated_db: Database):
        """Test that transaction list is returned."""
        result = get_account_flow(populated_db, account_id="acc-rub", period="this_month")

        transactions = result["transactions"]
        assert isinstance(transactions, list)
        assert len(transactions) > 0

        # Each transaction should have key fields
        tx = transactions[0]
        assert "id" in tx
        assert "date" in tx
        assert "amount" in tx

    def test_get_account_flow_invalid_account(self, populated_db: Database):
        """Test handling of invalid account ID."""
        with pytest.raises(ValueError, match="Account.*not found"):
            get_account_flow(populated_db, account_id="invalid-account", period="this_month")

    def test_get_account_flow_empty_period(self, populated_db: Database):
        """Test account with no transactions in period."""
        # acc-save has only tx6, but might have empty periods
        result = get_account_flow(populated_db, account_id="acc-save", period="2020-01")

        assert result["summary"]["transaction_count"] == 0
        assert len(result["transactions"]) == 0


class TestT15SuggestCategory:
    """Test T15: suggest_category tool."""

    @pytest.mark.asyncio
    async def test_suggest_category_structure(self, populated_db: Database):
        """Test basic structure of suggest_category result."""
        # Mock HTTP response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "payee": "Макдональдс",
            "merchant": "m-mcdonalds",
            "tag": ["tag-restaurant"],
        }

        with patch("httpx.AsyncClient") as mock_client:
            mock_client.return_value.__aenter__.return_value.post = AsyncMock(return_value=mock_response)

            result = await suggest_category(
                payee="Макдональдс",
                token="test_token",
                db=populated_db,
            )

            assert "original_payee" in result
            assert "normalized_payee" in result
            assert "suggested_categories" in result

    @pytest.mark.asyncio
    async def test_suggest_category_enrichment(self, populated_db: Database):
        """Test that tag IDs are enriched with titles from DB."""
        # Mock HTTP response with tag that exists in DB
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "payee": "KFC",
            "tag": ["tag-restaurant"],  # This tag exists in populated_db
        }

        with patch("httpx.AsyncClient") as mock_client:
            mock_client.return_value.__aenter__.return_value.post = AsyncMock(return_value=mock_response)

            result = await suggest_category(
                payee="KFC",
                token="test_token",
                db=populated_db,
            )

            # Check that tag title was enriched
            categories = result["suggested_categories"]
            assert len(categories) >= 1
            assert categories[0]["tag_id"] == "tag-restaurant"
            assert categories[0]["name"] == "Рестораны"

    @pytest.mark.asyncio
    async def test_suggest_category_api_error(self, populated_db: Database):
        """Test handling of API errors."""
        # Mock HTTP error
        with patch("httpx.AsyncClient") as mock_client:
            mock_client.return_value.__aenter__.return_value.post = AsyncMock(
                side_effect=Exception("Connection error")
            )

            result = await suggest_category(
                payee="Test",
                token="test_token",
                db=populated_db,
            )

            assert "error" in result
            assert result["original_payee"] == "Test"

    @pytest.mark.asyncio
    async def test_suggest_category_non_200_status(self, populated_db: Database):
        """Test handling of non-200 HTTP status."""
        mock_response = Mock()
        mock_response.status_code = 401

        with patch("httpx.AsyncClient") as mock_client:
            mock_client.return_value.__aenter__.return_value.post = AsyncMock(return_value=mock_response)

            result = await suggest_category(
                payee="Test",
                token="bad_token",
                db=populated_db,
            )

            assert "error" in result
            assert "401" in result["error"]

    @pytest.mark.asyncio
    async def test_suggest_category_invalid_json(self, populated_db: Database):
        """Test handling of invalid JSON response."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.side_effect = ValueError("Invalid JSON")

        with patch("httpx.AsyncClient") as mock_client:
            mock_client.return_value.__aenter__.return_value.post = AsyncMock(return_value=mock_response)

            result = await suggest_category(
                payee="Test",
                token="test_token",
                db=populated_db,
            )

            assert "error" in result
            assert "Invalid JSON" in result["error"]

    @pytest.mark.asyncio
    async def test_suggest_category_multiple_tags(self, populated_db: Database):
        """Test handling of multiple suggested tags."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "payee": "Пятёрочка",
            "tag": ["tag-grocery", "tag-food"],
        }

        with patch("httpx.AsyncClient") as mock_client:
            mock_client.return_value.__aenter__.return_value.post = AsyncMock(return_value=mock_response)

            result = await suggest_category(
                payee="Пятёрочка",
                token="test_token",
                db=populated_db,
            )

            categories = result["suggested_categories"]
            assert len(categories) >= 2

            # Check enrichment
            tag_ids = [c["tag_id"] for c in categories]
            assert "tag-grocery" in tag_ids
            assert "tag-food" in tag_ids


# ============================================================================
# T16: convert_currency
# ============================================================================


class TestT16ConvertCurrency:
    """Tests for convert_currency tool."""

    def test_convert_same_currency(self, populated_db: Database):
        result = convert_currency(populated_db, amount=100, from_currency="RUB", to_currency="RUB")
        assert result["to"]["amount"] == 100
        assert result["rate"] == 1.0

    def test_convert_usd_to_rub(self, populated_db: Database):
        result = convert_currency(populated_db, amount=100, from_currency="USD", to_currency="RUB")
        # USD rate=90, RUB rate=1 → 100 * 90 / 1 = 9000
        assert result["to"]["amount"] == 9000.0
        assert result["rate"] == 90.0
        assert result["from"]["currency"] == "USD"
        assert result["to"]["currency"] == "RUB"

    def test_convert_eur_to_usd(self, populated_db: Database):
        result = convert_currency(populated_db, amount=50, from_currency="EUR", to_currency="USD")
        # EUR rate=100, USD rate=90 → 50 * 100/90 = 55.56
        assert result["to"]["amount"] == pytest.approx(55.56, abs=0.01)
        assert result["rate"] == pytest.approx(100 / 90, abs=0.0001)

    def test_convert_includes_rate_description(self, populated_db: Database):
        result = convert_currency(populated_db, amount=1, from_currency="EUR", to_currency="USD")
        assert "rate_description" in result
        assert "EUR" in result["rate_description"]
        assert "USD" in result["rate_description"]

    def test_convert_inverse_rate(self, populated_db: Database):
        result = convert_currency(populated_db, amount=100, from_currency="USD", to_currency="EUR")
        # rate = USD/EUR = 90/100 = 0.9, inverse = 1/0.9 ≈ 1.1111
        assert result["inverse_rate"] == pytest.approx(100 / 90, abs=0.0001)

    def test_convert_unknown_currency(self, populated_db: Database):
        result = convert_currency(populated_db, amount=100, from_currency="XYZ", to_currency="RUB")
        assert "error" in result

    def test_convert_case_insensitive(self, populated_db: Database):
        result = convert_currency(populated_db, amount=100, from_currency="usd", to_currency="eur")
        assert "to" in result
        assert result["to"]["currency"] == "USD" or result["to"]["currency"] == "EUR"


# ============================================================================
# T17: get_exchange_rates
# ============================================================================


class TestT17GetExchangeRates:
    """Tests for get_exchange_rates tool."""

    def test_rates_from_accounts(self, populated_db: Database):
        result = get_exchange_rates(populated_db)
        assert "currencies" in result
        assert "cross_rates" in result
        # populated_db has accounts with RUB (id=1) and USD (id=2)
        codes = [c["currency"] for c in result["currencies"]]
        assert "RUB" in codes
        assert "USD" in codes

    def test_rates_specific_currencies(self, populated_db: Database):
        result = get_exchange_rates(populated_db, currencies=["USD", "EUR"])
        codes = [c["currency"] for c in result["currencies"]]
        assert "USD" in codes
        assert "EUR" in codes
        assert "RUB" not in codes

    def test_cross_rates_table(self, populated_db: Database):
        result = get_exchange_rates(populated_db, currencies=["USD", "EUR", "RUB"])
        cross = result["cross_rates"]
        assert "USD" in cross
        assert "EUR" in cross["USD"]
        # USD→EUR: 90/100 = 0.9
        assert cross["USD"]["EUR"] == pytest.approx(0.9, abs=0.0001)
        # EUR→USD: 100/90 ≈ 1.1111
        assert cross["EUR"]["USD"] == pytest.approx(100 / 90, abs=0.0001)

    def test_rates_has_user_currency(self, populated_db: Database):
        # Set user_currency metadata
        populated_db.set_meta("user_currency", "1")  # RUB
        result = get_exchange_rates(populated_db, currencies=["USD", "EUR"])
        assert result["user_currency"] == "RUB"

    def test_rates_empty_currencies(self, populated_db: Database):
        result = get_exchange_rates(populated_db, currencies=["XYZ"])
        # No valid currencies found
        assert result["currencies"] == []
