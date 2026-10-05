"""get_account_flow: the account's movements must reconcile with its balance."""

import json
from datetime import date

import pytest
from mcp.shared.memory import create_connected_server_and_client_session

from zenmoney_mcp import server as zm_server
from zenmoney_mcp.analytics import get_account_flow, get_period_dates
from zenmoney_mcp.database import Database

from .factories import insert_account, insert_instrument, insert_transaction

ACCOUNT = "acc-flow"
OTHER = "acc-save"


@pytest.fixture
def flow_db(populated_db: Database) -> Database:
    """An account with one movement of each kind in March 2025 and two later ones.

    March: +1000 income, -300 expense, +500 transfer in, -200 transfer out.
    April: -150 expense, +50 transfer in. Balance today: 1900.
    """
    insert_account(populated_db, id=ACCOUNT, title="Flow card", balance=1900.0)

    def own(day: str, **amounts: float) -> None:
        insert_transaction(
            populated_db, date=day, income_account=ACCOUNT, outcome_account=ACCOUNT, **amounts
        )

    own("2025-03-05", income=1000.0)
    own("2025-03-10", outcome=300.0)
    insert_transaction(
        populated_db, date="2025-03-15", income=500.0, income_account=ACCOUNT,
        outcome=500.0, outcome_account=OTHER,
    )
    insert_transaction(
        populated_db, date="2025-03-20", income=200.0, income_account=OTHER,
        outcome=200.0, outcome_account=ACCOUNT,
    )
    own("2025-04-02", outcome=150.0)
    insert_transaction(
        populated_db, date="2025-04-03", income=50.0, income_account=ACCOUNT,
        outcome=50.0, outcome_account=OTHER,
    )
    return populated_db


class TestNetChange:
    def test_net_change_counts_transfers_as_well_as_income_and_outcome(self, flow_db: Database):
        summary = get_account_flow(flow_db, ACCOUNT, "2025-03")["summary"]

        assert summary["income"] == 1000.0
        assert summary["outcome"] == 300.0
        assert summary["transfers_in"] == 500.0
        assert summary["transfers_out"] == 200.0
        assert summary["net_change"] == 1000.0


class TestBalances:
    def test_closing_balance_is_current_balance_minus_later_movements(self, flow_db: Database):
        summary = get_account_flow(flow_db, ACCOUNT, "2025-03")["summary"]

        # 1900 today, April moved the account by -150 + 50
        assert summary["closing_balance"] == 2000.0

    def test_opening_balance_plus_net_change_is_closing_balance(self, flow_db: Database):
        summary = get_account_flow(flow_db, ACCOUNT, "2025-03")["summary"]

        assert summary["opening_balance"] == 1000.0
        assert summary["opening_balance"] + summary["net_change"] == summary["closing_balance"]

    def test_period_reaching_today_closes_at_the_current_balance(self, flow_db: Database):
        result = get_account_flow(flow_db, ACCOUNT, "this_month", start_date="2025-04-01")

        assert result["summary"]["closing_balance"] == 1900.0
        assert result["summary"]["closing_balance"] == result["account"]["balance"]


class TestCurrency:
    def test_amounts_stay_in_the_account_currency_and_say_which(self, populated_db: Database):
        # acc-usd is a USD account; the user's currency is RUB
        insert_account(populated_db, id="acc-usd", title="Dollar cash", type="cash",
                       instrument=2, balance=130.0)
        insert_transaction(populated_db, date="2025-03-05", outcome=20.0, outcome_instrument=2,
                           outcome_account="acc-usd", income_instrument=2, income_account="acc-usd")
        insert_transaction(populated_db, date="2025-03-06", outcome=9000.0, outcome_instrument=1,
                           outcome_account="acc-rub", income=100.0, income_instrument=2,
                           income_account="acc-usd")

        result = get_account_flow(populated_db, "acc-usd", "2025-03")

        assert result["account"]["currency"] == "USD"
        assert result["summary"]["currency"] == "USD"
        assert result["summary"]["outcome"] == 20.0
        assert result["summary"]["transfers_in"] == 100.0
        assert result["summary"]["net_change"] == 80.0


class TestHoldsAndDeleted:
    """accounts.balance covers neither deleted rows nor pending holds, so the totals don't either."""

    @pytest.fixture
    def held_db(self, flow_db: Database) -> Database:
        for day, amount in (("2025-03-25", 70.0), ("2025-04-10", 30.0)):
            insert_transaction(flow_db, id=f"hold-{day}", date=day, hold=1, outcome=amount,
                               income_account=ACCOUNT, outcome_account=ACCOUNT)
        return flow_db

    def test_holds_do_not_move_totals_or_balances(self, held_db: Database):
        summary = get_account_flow(held_db, ACCOUNT, "2025-03")["summary"]

        assert summary["outcome"] == 300.0
        assert summary["net_change"] == 1000.0
        assert summary["closing_balance"] == 2000.0
        assert summary["opening_balance"] == 1000.0

    def test_holds_are_listed_flagged_and_counted_as_excluded(self, held_db: Database):
        result = get_account_flow(held_db, ACCOUNT, "2025-03")

        flags = {tx["id"]: tx["hold"] for tx in result["transactions"]}
        assert flags["hold-2025-03-25"] is True
        assert sum(flags.values()) == 1
        assert result["summary"]["holds_excluded"] == 1

    def test_deleted_rows_do_not_move_totals_or_balances(self, flow_db: Database):
        for day in ("2025-03-26", "2025-04-11"):
            insert_transaction(flow_db, date=day, deleted=1, outcome=40.0,
                               income_account=ACCOUNT, outcome_account=ACCOUNT)

        summary = get_account_flow(flow_db, ACCOUNT, "2025-03")["summary"]

        assert summary["outcome"] == 300.0
        assert summary["closing_balance"] == 2000.0


class TestDebtAccount:
    """A debt account is kept in the user's currency, but its rows carry the other side's."""

    @pytest.fixture
    def debt_db(self, populated_db: Database) -> Database:
        # Lent 10 USD in March, got 4 USD back in April; 1 USD = 90 RUB, the debt account is RUB
        insert_account(populated_db, id="acc-owed", title="Owed to me", type="debt",
                       instrument=1, balance=540.0)
        insert_transaction(populated_db, id="lent", date="2025-03-07",
                           income=10.0, income_instrument=2, income_account="acc-owed",
                           outcome=10.0, outcome_instrument=2, outcome_account="acc-usd")
        insert_transaction(populated_db, id="repaid", date="2025-04-07",
                           income=4.0, income_instrument=2, income_account="acc-usd",
                           outcome=4.0, outcome_instrument=2, outcome_account="acc-owed")
        return populated_db

    def test_foreign_currency_rows_are_converted_to_the_account_currency(self, debt_db: Database):
        summary = get_account_flow(debt_db, "acc-owed", "2025-03")["summary"]

        assert summary["currency"] == "RUB"
        assert summary["transfers_in"] == 900.0
        assert summary["net_change"] == 900.0
        assert summary["closing_balance"] == 900.0
        assert summary["opening_balance"] == 0.0

    def test_converted_row_keeps_its_original_amount_and_currency(self, debt_db: Database):
        (tx,) = get_account_flow(debt_db, "acc-owed", "2025-03")["transactions"]

        assert tx["amount"] == 900.0
        assert tx["original_amount"] == 10.0
        assert tx["original_currency"] == "USD"

    def test_row_in_the_account_currency_has_no_original_amount(self, flow_db: Database):
        transactions = get_account_flow(flow_db, ACCOUNT, "2025-03")["transactions"]

        assert all("original_amount" not in tx for tx in transactions)

    def test_rounded_figures_still_add_up(self, populated_db: Database):
        # 1 USD = 90 / 7 of the account currency: every converted amount has a long tail
        insert_instrument(populated_db, id=77, short_title="XTS", rate=7.0)
        insert_account(populated_db, id="acc-odd", type="debt", instrument=77, balance=100.0)
        for day in ("2025-03-07", "2025-04-07"):
            insert_transaction(populated_db, date=day,
                               income=1.0, income_instrument=2, income_account="acc-odd",
                               outcome=1.0, outcome_instrument=2, outcome_account="acc-usd")

        summary = get_account_flow(populated_db, "acc-odd", "2025-03")["summary"]

        assert summary["net_change"] == 12.86
        assert summary["closing_balance"] == 87.14
        assert round(summary["opening_balance"] + summary["net_change"], 2) == summary["closing_balance"]


class TestTransactionList:
    def test_long_list_is_cut_and_says_how_many_there_are(self, populated_db: Database):
        insert_account(populated_db, id="acc-busy", title="Busy card", balance=0.0)
        for _ in range(60):
            insert_transaction(populated_db, date="2025-03-10", outcome=10.0,
                               income_account="acc-busy", outcome_account="acc-busy")

        result = get_account_flow(populated_db, "acc-busy", "2025-03")

        assert len(result["transactions"]) == 50
        assert result["returned_count"] == 50
        assert result["total_count"] == 60
        assert result["summary"]["outcome"] == 600.0


class TestPeriod:
    def test_explicit_dates_need_no_period(self, flow_db: Database):
        result = get_account_flow(flow_db, ACCOUNT, start_date="2025-03-01", end_date="2025-03-31")

        assert result["period"] == {"start": "2025-03-01", "end": "2025-03-31"}
        assert result["summary"]["net_change"] == 1000.0

    def test_period_defaults_to_this_month(self, flow_db: Database):
        result = get_account_flow(flow_db, ACCOUNT)

        start, end = get_period_dates("this_month")
        assert result["period"] == {"start": start, "end": end}

    async def test_tool_call_with_dates_and_no_period_is_accepted(self, flow_db: Database, monkeypatch):
        monkeypatch.setenv("ZENMONEY_AUTO_SYNC_SECONDS", "0")
        monkeypatch.setattr(zm_server, "_db", flow_db)
        arguments = {"account_id": ACCOUNT, "start_date": "2025-03-01", "end_date": "2025-03-31"}

        async with create_connected_server_and_client_session(zm_server.server) as client:
            result = await client.call_tool("get_account_flow", arguments)

        assert not result.isError, result.content[0].text
        assert json.loads(result.content[0].text)["summary"]["net_change"] == 1000.0

    async def test_tool_call_without_period_or_dates_covers_this_month(self, flow_db: Database, monkeypatch):
        monkeypatch.setenv("ZENMONEY_AUTO_SYNC_SECONDS", "0")
        monkeypatch.setattr(zm_server, "_db", flow_db)

        async with create_connected_server_and_client_session(zm_server.server) as client:
            result = await client.call_tool("get_account_flow", {"account_id": ACCOUNT})

        assert not result.isError, result.content[0].text
        assert json.loads(result.content[0].text)["period"]["start"] == date.today().replace(day=1).isoformat()
