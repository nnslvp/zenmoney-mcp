"""Behaviour of the spending / income / merchants / trends tools on invented data."""

from datetime import date

import pytest
from mcp.shared.memory import create_connected_server_and_client_session

from zenmoney_mcp import server as zm_server
from zenmoney_mcp.analytics import (
    analyze_income,
    analyze_merchants,
    analyze_spending,
    analyze_trends,
)
from zenmoney_mcp.database import Database

from .factories import insert_account, insert_tag, insert_transaction


def _month(months_ago: int, day: int = 10) -> str:
    """ISO date on ``day`` of the month that lies ``months_ago`` months before the current one."""
    today = date.today()
    index = today.year * 12 + today.month - 1 - months_ago
    return date(index // 12, index % 12 + 1, day).isoformat()


def _month_key(months_ago: int) -> str:
    return _month(months_ago)[:7]


@pytest.fixture
def served_db(populated_db: Database, monkeypatch) -> Database:
    """The MCP server wired to the test DB, answering from the cache without syncing."""
    monkeypatch.setenv("ZENMONEY_AUTO_SYNC_SECONDS", "0")
    zm_server.init_for_testing(populated_db)
    yield populated_db
    zm_server._db = None
    zm_server._sync_engine = None


async def _tool_schema(name: str) -> dict:
    """Input properties of a tool as the server advertises them."""
    tools = {tool.name: tool for tool in await zm_server.list_tools()}
    return tools[name].inputSchema["properties"]


class TestTrendsSummary:
    def test_min_and_max_name_their_months(self, populated_db: Database):
        # Amounts with more than two decimals: the rounded value differs from the raw sum.
        insert_transaction(populated_db, date=_month(3), outcome=200.456)
        insert_transaction(populated_db, date=_month(2), outcome=100.127)
        insert_transaction(populated_db, date=_month(1), outcome=300.333)

        summary = analyze_trends(populated_db, months=4)["summary"]

        assert summary["min"] == {"month": _month_key(2), "value": 100.13}
        assert summary["max"] == {"month": _month_key(1), "value": 300.33}

    def test_series_that_gets_more_negative_is_falling(self, populated_db: Database):
        # Net cashflow -150, -180, -210: each month is worse than the one before.
        for months_ago, spent in ((3, 250.0), (2, 280.0), (1, 310.0)):
            insert_transaction(populated_db, date=_month(months_ago), income=100.0)
            insert_transaction(populated_db, date=_month(months_ago), outcome=spent)

        summary = analyze_trends(populated_db, months=4, metric="net_cashflow")["summary"]

        assert summary["average"] == -180.0
        assert summary["trend_direction"] == "falling"
        assert summary["trend_pct_change_per_month"] == -16.7

    def test_anomaly_below_a_negative_average_deviates_downwards(self, populated_db: Database):
        for months_ago in range(2, 7):
            insert_transaction(populated_db, date=_month(months_ago), outcome=10.0)
        insert_transaction(populated_db, date=_month(1), outcome=400.0)

        summary = analyze_trends(populated_db, months=7, metric="net_cashflow")["summary"]

        assert summary["average"] == -75.0
        assert summary["anomalies"] == [
            {"month": _month_key(1), "value": -400.0, "deviation": "-433.3%"}
        ]

    def test_direction_follows_the_slope_when_the_average_is_zero(self, populated_db: Database):
        # Net cashflow -100, 0, +100: clearly rising, but a percentage of zero is undefined.
        insert_transaction(populated_db, date=_month(3), outcome=100.0)
        insert_transaction(populated_db, date=_month(2), income=50.0)
        insert_transaction(populated_db, date=_month(2), outcome=50.0)
        insert_transaction(populated_db, date=_month(1), income=100.0)

        summary = analyze_trends(populated_db, months=4, metric="net_cashflow")["summary"]

        assert summary["average"] == 0.0
        assert summary["trend_direction"] == "rising"
        assert summary["trend_pct_change_per_month"] is None

    def test_savings_rate_is_undefined_in_a_month_without_income(self, populated_db: Database):
        insert_transaction(populated_db, date=_month(3), income=1000.0)
        insert_transaction(populated_db, date=_month(3), outcome=500.0)
        insert_transaction(populated_db, date=_month(2), outcome=300.0)  # spent, earned nothing
        insert_transaction(populated_db, date=_month(1), income=1000.0)
        insert_transaction(populated_db, date=_month(1), outcome=700.0)

        result = analyze_trends(populated_db, months=4, metric="savings_rate")

        by_month = {m["month"]: m["value"] for m in result["data"]}
        assert by_month[_month_key(3)] == 50.0
        assert by_month[_month_key(2)] is None
        assert by_month[_month_key(1)] == 30.0
        # The undefined month is left out of the statistics instead of counting as 0 %.
        summary = result["summary"]
        assert summary["average"] == 40.0
        assert summary["min"] == {"month": _month_key(1), "value": 30.0}
        assert summary["max"] == {"month": _month_key(3), "value": 50.0}
        # 50 % -> 30 % over two months: -10 points a month, a quarter of the average.
        assert summary["trend_direction"] == "falling"
        assert summary["trend_pct_change_per_month"] == -25.0


class TestTrendsHistory:
    """``populated_db`` only has transactions in the current month."""

    def test_months_before_the_first_transaction_are_not_zeros(self, populated_db: Database):
        insert_transaction(populated_db, date=_month(2), outcome=300.0)
        insert_transaction(populated_db, date=_month(1), outcome=500.0)

        result = analyze_trends(populated_db, months=6)

        assert [m["month"] for m in result["data"]] == [_month_key(2), _month_key(1), _month_key(0)]
        assert result["summary"]["average"] == 400.0
        assert result["summary"]["min"] == {"month": _month_key(2), "value": 300.0}
        assert _month_key(2) in result["note"]

    def test_no_note_when_the_history_covers_the_request(self, populated_db: Database):
        insert_transaction(populated_db, date=_month(2), outcome=300.0)

        result = analyze_trends(populated_db, months=3)

        assert len(result["data"]) == 3
        assert "note" not in result

    def test_month_without_spending_inside_the_history_is_a_real_zero(self, populated_db: Database):
        insert_transaction(populated_db, date=_month(3), outcome=300.0)
        insert_transaction(populated_db, date=_month(1), outcome=600.0)

        result = analyze_trends(populated_db, months=4)

        by_month = {m["month"]: m["value"] for m in result["data"]}
        assert by_month[_month_key(2)] == 0.0
        assert result["summary"]["average"] == 300.0
        assert result["summary"]["min"] == {"month": _month_key(2), "value": 0.0}

    def test_single_month_returns_its_value_and_explains_the_missing_trend(self, populated_db: Database):
        result = analyze_trends(populated_db, months=1)

        assert result["data"] == [{"month": _month_key(0), "value": 5200.0, "partial": True}]
        summary = result["summary"]
        assert summary["month"] == _month_key(0)
        assert summary["value"] == 5200.0
        assert summary["partial"] is True
        assert "at least 2 complete months" in summary["note"]
        assert "trend_direction" not in summary

    def test_one_complete_month_is_not_called_a_stable_trend(self, populated_db: Database):
        insert_transaction(populated_db, date=_month(1), outcome=700.0)

        summary = analyze_trends(populated_db, months=2)["summary"]

        assert summary["month"] == _month_key(1)
        assert summary["value"] == 700.0
        assert "partial" not in summary
        assert "at least 2 complete months" in summary["note"]
        assert "trend_direction" not in summary

    def test_empty_cache_has_no_months(self, db: Database):
        result = analyze_trends(db, months=3)

        assert result["data"] == []
        assert result["summary"] == {"message": "Insufficient data for analysis"}


class TestCategoryFilter:
    @pytest.mark.parametrize("tool", [analyze_spending, analyze_merchants, analyze_trends])
    def test_unknown_category_is_rejected(self, populated_db: Database, tool):
        with pytest.raises(ValueError, match="Unknown category 'no-such-category'"):
            tool(populated_db, category_id="no-such-category")

    def test_parent_category_still_covers_its_children(self, populated_db: Database):
        # tag-food has no direct spending: 1500 and 3000 sit on its two children.
        assert analyze_spending(populated_db, category_id="tag-food")["total_outcome"] == 4500.0
        assert analyze_merchants(populated_db, category_id="tag-food")["total_outcome"] == 4500.0
        trend = analyze_trends(populated_db, months=1, category_id="tag-food")
        assert trend["data"][-1]["value"] == 4500.0
        assert trend["category"] == "Еда"


class TestTopN:
    @pytest.mark.parametrize("tool", [analyze_spending, analyze_income, analyze_merchants])
    @pytest.mark.parametrize("top_n", [0, -1, 2.5, "3", None, True])
    def test_anything_but_a_positive_integer_is_rejected(self, populated_db: Database, tool, top_n):
        with pytest.raises(ValueError, match="top_n must be an integer >= 1"):
            tool(populated_db, top_n=top_n)

    def test_whole_number_sent_as_a_float_is_accepted(self, populated_db: Database):
        # JSON has one number type: a client may send 2 as 2.0, and the schema allows it.
        result = analyze_merchants(populated_db, top_n=2.0)

        assert result["returned_count"] == 2
        assert len(result["merchants"]) == 2

    def test_spending_by_merchant_validates_too(self, populated_db: Database):
        with pytest.raises(ValueError, match="top_n"):
            analyze_spending(populated_db, group_by="merchant", top_n=-1)

    def test_more_than_100_is_capped_at_100(self, populated_db: Database):
        for i in range(105):
            tag = insert_tag(populated_db, title=f"Category {i}")
            insert_transaction(populated_db, date="2024-03-10", outcome=10.0 + i,
                               payee=f"Shop {i}", tag=[tag["id"]])
            insert_transaction(populated_db, date="2024-03-10", income=10.0 + i,
                               payee=f"Client {i}", tag=[tag["id"]])

        spending = analyze_spending(populated_db, period="2024-03", top_n=500)
        by_merchant = analyze_spending(populated_db, period="2024-03", top_n=500, group_by="merchant")
        income = analyze_income(populated_db, period="2024-03", top_n=500)
        merchants = analyze_merchants(populated_db, period="2024-03", top_n=500)

        # (rows returned, reported as returned, available in total)
        capped = (100, 100, 105)
        assert (len(spending["categories"]), spending["returned_count"], spending["total_categories"]) == capped
        assert (
            len(by_merchant["merchants"]), by_merchant["returned_count"], by_merchant["total_merchants"]
        ) == capped
        assert (len(income["sources"]), income["returned_sources"], income["total_sources"]) == capped
        assert (len(income["categories"]), income["returned_categories"], income["total_categories"]) == capped
        assert (len(merchants["merchants"]), merchants["returned_count"], merchants["total_merchants"]) == capped

    @pytest.mark.parametrize("tool_name", ["analyze_spending", "analyze_income", "analyze_merchants"])
    async def test_schema_declares_the_bounds(self, tool_name):
        top_n = (await _tool_schema(tool_name))["top_n"]

        assert (top_n["minimum"], top_n["maximum"], top_n["default"]) == (1, 100, 10)

    async def test_negative_top_n_is_a_tool_error_over_the_protocol(self, served_db: Database):
        async with create_connected_server_and_client_session(zm_server.server) as client:
            result = await client.call_tool("analyze_merchants", {"top_n": -1})

        assert result.isError
        assert "minimum of 1" in result.content[0].text


class TestIncludeTransfers:
    """``populated_db`` moves 50000 (transfer), 9000 (exchange) and 5000 (lent) out of acc-rub."""

    def test_transfers_stay_out_by_default(self, populated_db: Database):
        result = analyze_spending(populated_db)

        assert result["total_outcome"] == 5200.0
        assert "transfers_included" not in result

    def test_flag_adds_the_outgoing_side_of_transfers(self, populated_db: Database):
        result = analyze_spending(populated_db, include_transfers=True)

        assert result["transfers_included"] == {"amount": 64000.0, "count": 3}
        assert result["total_outcome"] == 69200.0

    def test_transfers_are_not_filed_under_categories(self, populated_db: Database):
        result = analyze_spending(populated_db, include_transfers=True)

        assert result["uncategorized"] == {"amount": 200.0, "count": 1}
        assert sum(c["amount"] for c in result["categories"]) == 5000.0

    def test_outgoing_side_is_converted_to_user_currency(self, populated_db: Database):
        insert_transaction(populated_db, date="2024-03-10", outcome=10.0, outcome_instrument=2,
                           outcome_account="acc-usd", income=900.0)

        result = analyze_spending(populated_db, period="2024-03", include_transfers=True)

        assert result["transfers_included"] == {"amount": 900.0, "count": 1}
        assert result["total_outcome"] == 900.0

    def test_merchant_view_counts_transfers_without_listing_them(self, populated_db: Database):
        result = analyze_spending(populated_db, group_by="merchant", include_transfers=True)

        assert result["total_outcome"] == 69200.0
        assert result["transfers_included"] == {"amount": 64000.0, "count": 3}
        assert "Паша" not in [m["name"] for m in result["merchants"]]


@pytest.fixture
def off_balance_db(populated_db: Database) -> Database:
    """March 2024: 100 spent and 1000 earned on a counted account, 40 and 300 on an off-balance one."""
    insert_account(populated_db, id="acc-off", title="Side account", in_balance=0)
    insert_transaction(populated_db, date="2024-03-05", outcome=100.0, payee="Corner shop")
    insert_transaction(populated_db, date="2024-03-06", outcome=40.0, payee="Corner shop",
                       outcome_account="acc-off")
    insert_transaction(populated_db, date="2024-03-07", income=1000.0, payee="Employer")
    insert_transaction(populated_db, date="2024-03-08", income=300.0, payee="Employer",
                       income_account="acc-off")
    return populated_db


class TestOffBalanceAccounts:
    """Off-balance accounts stay out by default (as in ZenMoney's reports), but visibly."""

    def test_spending_reports_what_it_left_out(self, off_balance_db: Database):
        result = analyze_spending(off_balance_db, period="2024-03")

        assert result["total_outcome"] == 100.0
        assert result["off_balance_excluded"] == {"amount": 40.0, "count": 1}

    def test_spending_includes_them_on_request(self, off_balance_db: Database):
        result = analyze_spending(off_balance_db, period="2024-03", include_off_balance=True)

        assert result["total_outcome"] == 140.0
        assert result["off_balance_excluded"] is None

    def test_spending_by_merchant_reports_and_includes_them(self, off_balance_db: Database):
        default = analyze_spending(off_balance_db, period="2024-03", group_by="merchant")
        included = analyze_spending(off_balance_db, period="2024-03", group_by="merchant",
                                    include_off_balance=True)

        assert default["off_balance_excluded"] == {"amount": 40.0, "count": 1}
        assert default["merchants"][0]["amount"] == 100.0
        assert included["off_balance_excluded"] is None
        assert included["merchants"][0]["amount"] == 140.0

    def test_nothing_excluded_is_null(self, populated_db: Database):
        assert analyze_spending(populated_db)["off_balance_excluded"] is None
        assert analyze_income(populated_db)["off_balance_excluded"] is None
        assert analyze_merchants(populated_db)["off_balance_excluded"] is None
        assert analyze_trends(populated_db, months=1)["off_balance_excluded"] is None

    def test_excluded_amount_is_in_user_currency(self, populated_db: Database):
        insert_account(populated_db, id="acc-off-usd", title="Side account", in_balance=0, instrument=2)
        insert_transaction(populated_db, date="2024-03-05", outcome=2.0, outcome_instrument=2,
                           outcome_account="acc-off-usd")

        result = analyze_spending(populated_db, period="2024-03")

        assert result["off_balance_excluded"] == {"amount": 180.0, "count": 1}

    def test_pending_charge_on_an_off_balance_account_is_reported_as_a_hold(self, off_balance_db: Database):
        insert_transaction(off_balance_db, date="2024-03-09", outcome=25.0, hold=1,
                           outcome_account="acc-off")

        default = analyze_spending(off_balance_db, period="2024-03")
        with_holds = analyze_spending(off_balance_db, period="2024-03", include_holds=True)

        # off_balance_excluded is exactly what include_off_balance would add to the total.
        assert default["holds_excluded"] == {"amount": 25.0, "count": 1}
        assert default["off_balance_excluded"] == {"amount": 40.0, "count": 1}
        assert with_holds["total_outcome"] == 100.0
        assert with_holds["off_balance_excluded"] == {"amount": 65.0, "count": 2}

    def test_income_reports_and_includes_them(self, off_balance_db: Database):
        default = analyze_income(off_balance_db, period="2024-03")
        included = analyze_income(off_balance_db, period="2024-03", include_off_balance=True)

        assert default["total_income"] == 1000.0
        assert default["off_balance_excluded"] == {"amount": 300.0, "count": 1}
        assert included["total_income"] == 1300.0
        assert included["off_balance_excluded"] is None

    def test_merchants_report_and_include_them(self, off_balance_db: Database):
        default = analyze_merchants(off_balance_db, period="2024-03")
        included = analyze_merchants(off_balance_db, period="2024-03", include_off_balance=True)

        assert default["total_outcome"] == 100.0
        assert default["off_balance_excluded"] == {"amount": 40.0, "count": 1}
        assert included["total_outcome"] == 140.0
        assert included["merchants"][0]["visits"] == 2
        assert included["off_balance_excluded"] is None


class TestOffBalanceAccountsInTrends:
    @pytest.fixture
    def trend_db(self, populated_db: Database) -> Database:
        """Last month: 100 spent and 1000 earned on a counted account, 40 and 300 off balance."""
        insert_account(populated_db, id="acc-off", title="Side account", in_balance=0)
        insert_transaction(populated_db, date=_month(1), outcome=100.0)
        insert_transaction(populated_db, date=_month(1), outcome=40.0, outcome_account="acc-off")
        insert_transaction(populated_db, date=_month(1), income=1000.0)
        insert_transaction(populated_db, date=_month(1), income=300.0, income_account="acc-off")
        return populated_db

    @staticmethod
    def _last_month(result: dict) -> float:
        return next(m["value"] for m in result["data"] if m["month"] == _month_key(1))

    def test_outcome_reports_and_includes_them(self, trend_db: Database):
        default = analyze_trends(trend_db, months=2, metric="outcome")
        included = analyze_trends(trend_db, months=2, metric="outcome", include_off_balance=True)

        assert self._last_month(default) == 100.0
        assert default["off_balance_excluded"] == {"amount": 40.0, "count": 1}
        assert self._last_month(included) == 140.0
        assert included["off_balance_excluded"] is None

    def test_income_reports_and_includes_them(self, trend_db: Database):
        default = analyze_trends(trend_db, months=2, metric="income")
        included = analyze_trends(trend_db, months=2, metric="income", include_off_balance=True)

        assert self._last_month(default) == 1000.0
        assert default["off_balance_excluded"] == {"amount": 300.0, "count": 1}
        assert self._last_month(included) == 1300.0
        assert included["off_balance_excluded"] is None

    def test_two_sided_metrics_report_both_sides(self, trend_db: Database):
        default = analyze_trends(trend_db, months=2, metric="net_cashflow")
        included = analyze_trends(trend_db, months=2, metric="net_cashflow", include_off_balance=True)

        assert self._last_month(default) == 900.0
        assert default["off_balance_excluded"] == {
            "outcome": {"amount": 40.0, "count": 1},
            "income": {"amount": 300.0, "count": 1},
        }
        assert self._last_month(included) == 1160.0
        assert included["off_balance_excluded"] is None


class TestOffBalanceFlagOnTheServer:
    TOOLS = ["analyze_spending", "analyze_income", "analyze_merchants", "analyze_trends"]

    @pytest.mark.parametrize("tool_name", TOOLS)
    async def test_schema_offers_the_flag(self, tool_name):
        flag = (await _tool_schema(tool_name))["include_off_balance"]

        assert flag["type"] == "boolean"
        assert flag["default"] is False

    @pytest.mark.parametrize("tool_name, total_key, expected", [
        ("analyze_spending", "total_outcome", 140.0),
        ("analyze_income", "total_income", 1300.0),
        ("analyze_merchants", "total_outcome", 140.0),
    ])
    async def test_flag_reaches_the_tool(self, off_balance_db: Database, tool_name, total_key, expected):
        result = await zm_server._run_tool(
            tool_name, {"period": "2024-03", "include_off_balance": True}, off_balance_db
        )

        assert result[total_key] == expected

    async def test_flag_reaches_analyze_trends(self, populated_db: Database):
        insert_account(populated_db, id="acc-off", title="Side account", in_balance=0)
        insert_transaction(populated_db, date=_month(0, day=1), outcome=40.0, outcome_account="acc-off")

        result = await zm_server._run_tool(
            "analyze_trends", {"months": 1, "include_off_balance": True}, populated_db
        )

        assert result["data"][-1]["value"] == 5240.0


@pytest.fixture
def refund_db(populated_db: Database) -> Database:
    """March 2024: 5000 salary, 400 spent on transport, 150 of it refunded.

    tag-transport is an expense-only category, tag-salary an income-only one.
    """
    insert_transaction(populated_db, date="2024-03-01", income=5000.0, tag=["tag-salary"], payee="Employer")
    insert_transaction(populated_db, date="2024-03-05", outcome=400.0, tag=["tag-transport"], payee="Railways")
    insert_transaction(populated_db, date="2024-03-09", income=150.0, tag=["tag-transport"], payee="Railways")
    return populated_db


class TestRefundsInIncome:
    """Income on an expense-only category is a refund, not earnings."""

    def test_total_keeps_refunds_and_says_how_much_they_are(self, refund_db: Database):
        result = analyze_income(refund_db, period="2024-03")

        assert result["total_income"] == 5150.0
        assert result["refunds_total"] == 150.0
        assert result["income_excluding_refunds"] == 5000.0

    def test_refund_categories_are_marked(self, refund_db: Database):
        result = analyze_income(refund_db, period="2024-03")

        categories = {c["name"]: c for c in result["categories"]}
        assert categories["Транспорт"]["is_refund"] is True
        assert "is_refund" not in categories["Зарплата"]

    def test_category_that_also_takes_income_is_not_a_refund(self, populated_db: Database):
        insert_tag(populated_db, id="tag-resale", title="Resale", show_income=1, show_outcome=1)
        insert_transaction(populated_db, date="2024-03-02", income=70.0, tag=["tag-resale"])
        insert_transaction(populated_db, date="2024-03-03", income=30.0)  # no category at all

        result = analyze_income(populated_db, period="2024-03")

        assert result["refunds_total"] == 0.0
        assert result["income_excluding_refunds"] == 100.0
        assert not any("is_refund" in c for c in result["categories"])

    def test_refund_on_an_off_balance_account_follows_the_flag(self, refund_db: Database):
        insert_account(refund_db, id="acc-off", title="Side account", in_balance=0)
        insert_transaction(refund_db, date="2024-03-10", income=20.0, tag=["tag-transport"],
                           income_account="acc-off")

        default = analyze_income(refund_db, period="2024-03")
        included = analyze_income(refund_db, period="2024-03", include_off_balance=True)

        assert default["refunds_total"] == 150.0
        assert included["refunds_total"] == 170.0
        assert included["income_excluding_refunds"] == 5000.0


class TestRefundsInSpending:
    def test_total_keeps_its_meaning_and_net_outcome_subtracts_refunds(self, refund_db: Database):
        result = analyze_spending(refund_db, period="2024-03")

        assert result["total_outcome"] == 400.0
        assert result["refunds_total"] == 150.0
        assert result["net_outcome"] == 250.0

    def test_category_shows_its_refunds(self, refund_db: Database):
        insert_transaction(refund_db, date="2024-03-06", outcome=90.0, tag=["tag-salary"])

        result = analyze_spending(refund_db, period="2024-03")

        categories = {c["name"]: c for c in result["categories"]}
        assert categories["Транспорт"]["amount"] == 400.0
        assert categories["Транспорт"]["refunds"] == 150.0
        assert "refunds" not in categories["Зарплата"]

    def test_no_refunds_is_zero_not_missing(self, populated_db: Database):
        result = analyze_spending(populated_db)

        assert result["refunds_total"] == 0.0
        assert result["net_outcome"] == result["total_outcome"] == 5200.0
        assert not any("refunds" in c for c in result["categories"])

    def test_refund_without_spending_in_the_period_still_has_a_row(self, populated_db: Database):
        # Bought earlier, refunded in April: nothing spent, 60 came back.
        insert_transaction(populated_db, date="2024-04-02", income=60.0, tag=["tag-transport"])

        result = analyze_spending(populated_db, period="2024-04")

        assert result["total_outcome"] == 0.0
        assert result["refunds_total"] == 60.0
        assert result["net_outcome"] == -60.0
        (row,) = result["categories"]
        assert (row["name"], row["amount"], row["count"], row["refunds"]) == ("Транспорт", 0.0, 0, 60.0)

    def test_drill_down_counts_only_that_category(self, refund_db: Database):
        insert_tag(refund_db, id="tag-clothes", title="Clothes")
        insert_transaction(refund_db, date="2024-03-11", outcome=300.0, tag=["tag-clothes"])
        insert_transaction(refund_db, date="2024-03-12", income=45.0, tag=["tag-clothes"])

        transport = analyze_spending(refund_db, period="2024-03", category_id="tag-transport")
        clothes = analyze_spending(refund_db, period="2024-03", category_id="tag-clothes")

        assert (transport["refunds_total"], transport["net_outcome"]) == (150.0, 250.0)
        assert (clothes["refunds_total"], clothes["net_outcome"]) == (45.0, 255.0)

    def test_drill_down_into_a_parent_counts_refunds_of_its_children(self, populated_db: Database):
        insert_transaction(populated_db, date="2024-03-05", outcome=200.0, tag=["tag-grocery"])
        insert_transaction(populated_db, date="2024-03-06", income=35.0, tag=["tag-grocery"])

        result = analyze_spending(populated_db, period="2024-03", category_id="tag-food")

        assert (result["total_outcome"], result["refunds_total"], result["net_outcome"]) == (200.0, 35.0, 165.0)

    def test_merchant_view_reports_the_totals(self, refund_db: Database):
        result = analyze_spending(refund_db, period="2024-03", group_by="merchant")

        assert result["refunds_total"] == 150.0
        assert result["net_outcome"] == 250.0

    def test_pending_and_off_balance_refunds_are_not_counted(self, refund_db: Database):
        insert_account(refund_db, id="acc-off", title="Side account", in_balance=0)
        insert_transaction(refund_db, date="2024-03-10", income=20.0, tag=["tag-transport"],
                           income_account="acc-off")
        insert_transaction(refund_db, date="2024-03-10", income=11.0, tag=["tag-transport"], hold=1)

        default = analyze_spending(refund_db, period="2024-03")
        included = analyze_spending(refund_db, period="2024-03", include_off_balance=True)

        assert default["refunds_total"] == 150.0
        assert included["refunds_total"] == 170.0

    def test_refund_in_a_foreign_currency_is_converted(self, populated_db: Database):
        insert_transaction(populated_db, date="2024-03-05", outcome=500.0, tag=["tag-transport"])
        insert_transaction(populated_db, date="2024-03-06", income=2.0, income_instrument=2,
                           income_account="acc-usd", tag=["tag-transport"])

        result = analyze_spending(populated_db, period="2024-03")

        assert result["refunds_total"] == 180.0
        assert result["net_outcome"] == 320.0

    def test_both_tools_agree_on_refunds(self, refund_db: Database):
        spending = analyze_spending(refund_db, period="2024-03")
        income = analyze_income(refund_db, period="2024-03")

        assert spending["refunds_total"] == income["refunds_total"] == 150.0


class TestSpendingRollsChildrenUp:
    """``populated_db``: Еда has two children with 3000 and 1500, Транспорт has 500, 200 is untagged."""

    def test_parent_includes_its_children(self, populated_db: Database):
        result = analyze_spending(populated_db)

        food = next(c for c in result["categories"] if c["name"] == "Еда")
        assert food["tag_id"] == "tag-food"
        assert food["amount"] == 4500.0
        assert food["count"] == 2
        assert food["share_pct"] == 86.5
        assert food["avg_check"] == 2250.0
        assert "parent_category" not in food

    def test_children_are_listed_as_subcategories(self, populated_db: Database):
        result = analyze_spending(populated_db)

        food = next(c for c in result["categories"] if c["name"] == "Еда")
        assert food["subcategories"] == [
            {"tag_id": "tag-restaurant", "name": "Рестораны", "amount": 3000.0, "count": 1},
            {"tag_id": "tag-grocery", "name": "Продукты", "amount": 1500.0, "count": 1},
        ]

    def test_top_level_list_has_parents_only(self, populated_db: Database):
        result = analyze_spending(populated_db)

        assert [c["name"] for c in result["categories"]] == ["Еда", "Транспорт"]
        assert result["total_categories"] == 2
        assert result["returned_count"] == 2

    def test_category_without_spending_in_children_has_no_breakdown(self, populated_db: Database):
        result = analyze_spending(populated_db)

        transport = next(c for c in result["categories"] if c["name"] == "Транспорт")
        assert transport["amount"] == 500.0
        assert transport["subcategories"] == []

    def test_parents_own_spending_is_one_of_the_lines(self, populated_db: Database):
        insert_transaction(populated_db, outcome=700.0, tag=["tag-food"])

        result = analyze_spending(populated_db)

        food = next(c for c in result["categories"] if c["name"] == "Еда")
        assert food["amount"] == 5200.0
        assert food["count"] == 3
        assert [(s["name"], s["amount"]) for s in food["subcategories"]] == [
            ("Рестораны", 3000.0), ("Продукты", 1500.0), ("Еда", 700.0),
        ]

    def test_top_n_counts_parents(self, populated_db: Database):
        result = analyze_spending(populated_db, top_n=1)

        assert [c["name"] for c in result["categories"]] == ["Еда"]
        assert result["returned_count"] == 1
        assert result["total_categories"] == 2

    def test_categories_and_uncategorized_add_up_to_the_total(self, populated_db: Database):
        result = analyze_spending(populated_db)

        parts = sum(c["amount"] for c in result["categories"]) + result["uncategorized"]["amount"]
        assert parts == pytest.approx(result["total_outcome"], abs=0.05)
        assert sum(c["share_pct"] for c in result["categories"]) == pytest.approx(96.2, abs=0.11)

    def test_refunds_of_a_child_roll_up_too(self, populated_db: Database):
        insert_transaction(populated_db, income=120.0, tag=["tag-grocery"])

        result = analyze_spending(populated_db)

        food = next(c for c in result["categories"] if c["name"] == "Еда")
        assert food["refunds"] == 120.0
        grocery = next(s for s in food["subcategories"] if s["name"] == "Продукты")
        assert grocery["refunds"] == 120.0
        assert "refunds" not in next(s for s in food["subcategories"] if s["name"] == "Рестораны")

    def test_drill_down_stays_a_flat_list(self, populated_db: Database):
        result = analyze_spending(populated_db, category_id="tag-food")

        assert [(c["name"], c["amount"], c["parent_category"]) for c in result["categories"]] == [
            ("Рестораны", 3000.0, "Еда"), ("Продукты", 1500.0, "Еда"),
        ]
        assert not any("subcategories" in c for c in result["categories"])

    def test_merchant_view_is_unchanged(self, populated_db: Database):
        result = analyze_spending(populated_db, group_by="merchant")

        assert result["total_merchants"] == 4
        assert not any("subcategories" in m for m in result["merchants"])


class TestIncomeRollsChildrenUp:
    @pytest.fixture
    def income_db(self, populated_db: Database) -> Database:
        """March 2024: a parent income category with two children, and a refund on a child of Еда."""
        insert_tag(populated_db, id="tag-work", title="Work", show_income=1, show_outcome=0)
        insert_tag(populated_db, id="tag-pay", title="Pay", parent="tag-work", show_income=1, show_outcome=0)
        insert_tag(populated_db, id="tag-bonus", title="Bonus", parent="tag-work", show_income=1, show_outcome=0)
        insert_transaction(populated_db, date="2024-03-01", income=5000.0, tag=["tag-pay"])
        insert_transaction(populated_db, date="2024-03-15", income=800.0, tag=["tag-bonus"])
        insert_transaction(populated_db, date="2024-03-20", income=90.0, tag=["tag-grocery"])
        return populated_db

    def test_parent_includes_its_children(self, income_db: Database):
        result = analyze_income(income_db, period="2024-03")

        work = next(c for c in result["categories"] if c["name"] == "Work")
        assert (work["tag_id"], work["amount"], work["count"]) == ("tag-work", 5800.0, 2)
        assert work["subcategories"] == [
            {"tag_id": "tag-pay", "name": "Pay", "amount": 5000.0, "count": 1},
            {"tag_id": "tag-bonus", "name": "Bonus", "amount": 800.0, "count": 1},
        ]
        assert "is_refund" not in work

    def test_counts_are_about_parents(self, income_db: Database):
        result = analyze_income(income_db, period="2024-03")

        assert [c["name"] for c in result["categories"]] == ["Work", "Еда"]
        assert result["total_categories"] == 2
        assert result["returned_categories"] == 2

    def test_refund_on_a_child_marks_the_line_and_its_parent(self, income_db: Database):
        result = analyze_income(income_db, period="2024-03")

        food = next(c for c in result["categories"] if c["name"] == "Еда")
        assert food["is_refund"] is True
        assert food["subcategories"] == [
            {"tag_id": "tag-grocery", "name": "Продукты", "amount": 90.0, "count": 1, "is_refund": True},
        ]
        assert result["refunds_total"] == 90.0
        assert result["income_excluding_refunds"] == 5800.0

    def test_category_without_income_in_children_has_no_breakdown(self, populated_db: Database):
        result = analyze_income(populated_db)

        salary = next(c for c in result["categories"] if c["name"] == "Зарплата")
        assert salary["subcategories"] == []

    def test_parent_with_earnings_and_refunds_is_not_a_refund_category(self, populated_db: Database):
        insert_tag(populated_db, id="tag-mixed", title="Mixed", show_income=1, show_outcome=1)
        insert_tag(populated_db, id="tag-earned", title="Earned", parent="tag-mixed", show_income=1, show_outcome=0)
        insert_tag(populated_db, id="tag-returned", title="Returned", parent="tag-mixed")
        insert_transaction(populated_db, date="2024-03-01", income=400.0, tag=["tag-earned"])
        insert_transaction(populated_db, date="2024-03-02", income=60.0, tag=["tag-returned"])

        result = analyze_income(populated_db, period="2024-03")

        (mixed,) = result["categories"]
        assert mixed["amount"] == 460.0
        assert "is_refund" not in mixed
        assert [(s["name"], s.get("is_refund")) for s in mixed["subcategories"]] == [
            ("Earned", None), ("Returned", True),
        ]
        assert result["refunds_total"] == 60.0
        assert result["income_excluding_refunds"] == 400.0


class TestToolsAgreeWithEachOther:
    @pytest.fixture
    def busy_db(self, populated_db: Database) -> Database:
        """Three months with everything the tools have to tell apart."""
        insert_account(populated_db, id="acc-off", title="Side account", in_balance=0)
        for months_ago, scale in ((2, 1.0), (1, 2.0), (0, 3.0)):
            day = _month(months_ago)
            # Counted: purchases in two currencies and on a child category, salary
            insert_transaction(populated_db, date=day, outcome=100.25 * scale, tag=["tag-grocery"], payee="Corner shop")
            insert_transaction(populated_db, date=day, outcome=3.0 * scale, outcome_instrument=2,
                               outcome_account="acc-usd", tag=["tag-transport"], payee="Railways")
            insert_transaction(populated_db, date=day, outcome=40.0 * scale, payee="Kiosk")
            insert_transaction(populated_db, date=day, income=5000.0 * scale, tag=["tag-salary"], payee="Employer")
            # A refund: income for both tools, shown separately
            insert_transaction(populated_db, date=day, income=15.5 * scale, tag=["tag-grocery"], payee="Corner shop")
            # Off balance: only with include_off_balance
            insert_transaction(populated_db, date=day, outcome=70.0 * scale, outcome_account="acc-off",
                               tag=["tag-transport"], payee="Railways")
            insert_transaction(populated_db, date=day, income=33.0 * scale, income_account="acc-off")
            # Never counted: deleted, pending, transfer, exchange, lent money
            insert_transaction(populated_db, date=day, outcome=900.0, deleted=1)
            insert_transaction(populated_db, date=day, income=900.0, deleted=1)
            insert_transaction(populated_db, date=day, outcome=800.0, hold=1)
            insert_transaction(populated_db, date=day, income=800.0, hold=1)
            insert_transaction(populated_db, date=day, outcome=700.0, income=700.0, income_account="acc-save")
            insert_transaction(populated_db, date=day, outcome=600.0, income=6.0, income_instrument=2,
                               income_account="acc-usd")
            insert_transaction(populated_db, date=day, outcome=500.0, income=500.0, income_account="acc-debt")
        return populated_db

    @pytest.mark.parametrize("include_off_balance", [False, True])
    def test_trend_months_equal_the_spending_and_income_totals(self, busy_db: Database, include_off_balance):
        flags = {"include_off_balance": include_off_balance}
        outcome = analyze_trends(busy_db, months=3, metric="outcome", **flags)["data"]
        income = analyze_trends(busy_db, months=3, metric="income", **flags)["data"]
        net = analyze_trends(busy_db, months=3, metric="net_cashflow", **flags)["data"]

        assert [m["month"] for m in outcome] == [_month_key(2), _month_key(1), _month_key(0)]
        for spent, earned, left in zip(outcome, income, net):
            month = spent["month"]
            spending = analyze_spending(busy_db, period=month, **flags)
            assert spent["value"] == spending["total_outcome"]
            assert spent["value"] == analyze_merchants(busy_db, period=month, **flags)["total_outcome"]
            assert earned["value"] == analyze_income(busy_db, period=month, **flags)["total_income"]
            assert left["value"] == pytest.approx(earned["value"] - spent["value"], abs=0.011)
            assert spending["refunds_total"] == analyze_income(busy_db, period=month, **flags)["refunds_total"]

    def test_expected_totals_of_the_oldest_month(self, busy_db: Database):
        month = _month_key(2)

        default = analyze_spending(busy_db, period=month)
        included = analyze_spending(busy_db, period=month, include_off_balance=True)

        # 100.25 + 3 USD * 90 + 40; the off-balance 70 only on request
        assert default["total_outcome"] == 410.25
        assert included["total_outcome"] == 480.25
        assert analyze_income(busy_db, period=month)["total_income"] == 5015.5
        assert analyze_income(busy_db, period=month, include_off_balance=True)["total_income"] == 5048.5

    @pytest.mark.parametrize("include_off_balance", [False, True])
    def test_trend_of_a_category_equals_its_drill_down(self, busy_db: Database, include_off_balance):
        flags = {"include_off_balance": include_off_balance}
        trend = analyze_trends(busy_db, months=3, category_id="tag-food", **flags)["data"]

        for month in trend:
            drill_down = analyze_spending(busy_db, period=month["month"], category_id="tag-food", **flags)
            assert month["value"] == drill_down["total_outcome"]

    @pytest.mark.parametrize("include_off_balance", [False, True])
    def test_categories_and_uncategorized_add_up_to_the_total(self, busy_db: Database, include_off_balance):
        result = analyze_spending(busy_db, period=_month_key(1), include_off_balance=include_off_balance)

        parts = sum(c["amount"] for c in result["categories"]) + result["uncategorized"]["amount"]
        assert parts == pytest.approx(result["total_outcome"], abs=0.05)
        for category in result["categories"]:
            if category["subcategories"]:
                assert sum(s["amount"] for s in category["subcategories"]) == pytest.approx(
                    category["amount"], abs=0.05
                )

    def test_excluded_off_balance_amount_is_what_the_flag_adds(self, busy_db: Database):
        month = _month_key(1)
        for tool, total_key in ((analyze_spending, "total_outcome"), (analyze_merchants, "total_outcome"),
                                (analyze_income, "total_income")):
            default = tool(busy_db, period=month)
            included = tool(busy_db, period=month, include_off_balance=True)

            assert included[total_key] == pytest.approx(
                default[total_key] + default["off_balance_excluded"]["amount"], abs=0.011
            )

    def test_end_date_is_inclusive(self, populated_db: Database):
        insert_transaction(populated_db, date="2024-03-10", outcome=10.0, payee="Kiosk")
        insert_transaction(populated_db, date="2024-03-20", outcome=20.0, payee="Kiosk")
        insert_transaction(populated_db, date="2024-03-21", outcome=40.0, payee="Kiosk")
        insert_transaction(populated_db, date="2024-03-20", income=300.0)
        insert_transaction(populated_db, date="2024-03-21", income=500.0)
        dates = {"start_date": "2024-03-10", "end_date": "2024-03-20"}

        assert analyze_spending(populated_db, **dates)["total_outcome"] == 30.0
        assert analyze_merchants(populated_db, **dates)["total_outcome"] == 30.0
        assert analyze_income(populated_db, **dates)["total_income"] == 300.0


class TestInvalidArguments:
    """A wrong argument is an error, never a silent fallback to the default."""

    @pytest.mark.parametrize("months", [0, -3, 2.5, "6", None, True])
    def test_trends_reject_months_that_are_not_a_positive_integer(self, populated_db: Database, months):
        with pytest.raises(ValueError, match="months must be an integer >= 1"):
            analyze_trends(populated_db, months=months)

    def test_trends_accept_a_whole_number_of_months_sent_as_a_float(self, populated_db: Database):
        insert_transaction(populated_db, date=_month(1), outcome=50.0)

        assert len(analyze_trends(populated_db, months=2.0)["data"]) == 2

    def test_trends_reject_an_unknown_metric(self, populated_db: Database):
        with pytest.raises(ValueError, match="Unknown metric 'spending'"):
            analyze_trends(populated_db, metric="spending")

    def test_spending_rejects_an_unknown_group_by(self, populated_db: Database):
        with pytest.raises(ValueError, match="Unknown group_by 'payee'"):
            analyze_spending(populated_db, group_by="payee")

    async def test_schema_declares_the_lower_bound_of_months(self):
        months = (await _tool_schema("analyze_trends"))["months"]

        assert (months["minimum"], months["default"]) == (1, 6)
