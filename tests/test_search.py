"""search_transactions: text search, limits, the period it reports, both sides of a transfer."""

import json

import pytest
from mcp.shared.memory import create_connected_server_and_client_session

from zenmoney_mcp import server as zm_server
from zenmoney_mcp.analytics import search_transactions
from zenmoney_mcp.database import Database

from .factories import insert_account, insert_merchant, insert_transaction


def _ids(result: dict) -> set[str]:
    return {tx["id"] for tx in result["transactions"]}


async def _call_tool(db: Database, monkeypatch, arguments: dict):
    """Call search_transactions through the MCP protocol, on the test DB and without syncing."""
    monkeypatch.setenv("ZENMONEY_AUTO_SYNC_SECONDS", "0")
    monkeypatch.setattr(zm_server, "_db", db)
    async with create_connected_server_and_client_session(zm_server.server) as client:
        return await client.call_tool("search_transactions", arguments)


class TestTextSearch:
    @pytest.mark.parametrize("query", ["Булочная", "булочная", "БУЛОЧНАЯ", "бУлОч"])
    def test_cyrillic_query_matches_whatever_its_case(self, populated_db: Database, query: str):
        insert_transaction(populated_db, id="bakery", outcome=120.0, payee="Булочная Корица")

        result = search_transactions(populated_db, payee_search=query)

        assert _ids(result) == {"bakery"}
        assert result["total_matching"] == 1

    def test_merchant_title_and_comment_are_matched_the_same_way(self, populated_db: Database):
        insert_merchant(populated_db, id="m-flowers", title="Цветы у Дома")
        insert_transaction(populated_db, id="by-merchant", outcome=50.0, merchant="m-flowers")
        insert_transaction(populated_db, id="by-comment", outcome=60.0, comment="Букет: ЦВЕТЫ и лента")

        result = search_transactions(populated_db, payee_search="цветы")

        assert _ids(result) == {"by-merchant", "by-comment"}

    def test_search_survives_a_reopened_connection(self, tmp_path):
        db = Database(tmp_path / "cache.db")
        db.init_schema()
        insert_transaction(db, id="bakery", outcome=120.0, payee="Булочная Корица")
        assert _ids(search_transactions(db, payee_search="булочная")) == {"bakery"}

        db.close()

        assert _ids(search_transactions(db, payee_search="булочная")) == {"bakery"}

    @pytest.mark.parametrize("query, expected", [("%", {"percent"}), ("_", {"underscore"})])
    def test_like_wildcards_are_searched_as_plain_text(
        self, populated_db: Database, query: str, expected: set[str]
    ):
        insert_transaction(populated_db, id="percent", outcome=10.0, payee="Cashback 5% promo")
        insert_transaction(populated_db, id="underscore", outcome=10.0, payee="SHOP_ONLINE")
        insert_transaction(populated_db, id="plain", outcome=10.0, payee="Corner shop")

        result = search_transactions(populated_db, payee_search=query)

        assert _ids(result) == expected


class TestLimit:
    @pytest.mark.parametrize("limit", [0, -1, 201, 100000])
    def test_limit_outside_1_to_200_is_rejected(self, populated_db: Database, limit: int):
        with pytest.raises(ValueError, match="limit"):
            search_transactions(populated_db, limit=limit)

    @pytest.mark.parametrize("limit", [2.5, "10", True, None])
    def test_limit_that_is_not_an_integer_is_rejected(self, populated_db: Database, limit):
        with pytest.raises(ValueError, match="limit"):
            search_transactions(populated_db, limit=limit)

    @pytest.mark.parametrize("limit", [1, 200])
    def test_limit_at_the_bounds_is_accepted(self, populated_db: Database, limit: int):
        result = search_transactions(populated_db, limit=limit)

        assert result["returned_count"] == min(limit, result["total_matching"])

    async def test_schema_states_the_bounds(self):
        tools = {tool.name: tool for tool in await zm_server.list_tools()}

        limit = tools["search_transactions"].inputSchema["properties"]["limit"]
        assert (limit["minimum"], limit["maximum"]) == (1, 200)

    async def test_tool_call_with_limit_out_of_range_is_an_error(self, populated_db, monkeypatch):
        result = await _call_tool(populated_db, monkeypatch, {"limit": -1})

        assert result.isError
        assert "-1" in result.content[0].text


class TestPeriod:
    @pytest.fixture
    def dated_db(self, populated_db: Database) -> Database:
        for tx_id, day in (("old", "2019-06-15"), ("march", "2025-03-10"), ("april", "2025-04-10")):
            insert_transaction(populated_db, id=tx_id, date=day, outcome=10.0, payee="Dated shop")
        return populated_db

    def test_named_period_is_echoed_as_dates(self, dated_db: Database):
        result = search_transactions(dated_db, period="2025-03", payee_search="Dated shop")

        assert result["period"] == {"start": "2025-03-01", "end": "2025-03-31"}
        assert _ids(result) == {"march"}

    def test_explicit_dates_are_echoed(self, dated_db: Database):
        result = search_transactions(
            dated_db, start_date="2025-03-01", end_date="2025-04-30", payee_search="Dated shop"
        )

        assert result["period"] == {"start": "2025-03-01", "end": "2025-04-30"}
        assert _ids(result) == {"march", "april"}

    def test_no_period_searches_the_whole_history_and_says_so(self, dated_db: Database):
        result = search_transactions(dated_db, payee_search="Dated shop")

        assert _ids(result) == {"old", "march", "april"}
        assert result["period"]["start"] is None
        assert result["period"]["end"] is None
        assert "whole history" in result["period"]["note"]

    def test_end_date_alone_searches_everything_up_to_it(self, dated_db: Database):
        result = search_transactions(dated_db, end_date="2025-03-31", payee_search="Dated shop")

        assert _ids(result) == {"old", "march"}
        assert result["period"]["start"] is None
        assert result["period"]["end"] == "2025-03-31"
        assert "up to" in result["period"]["note"]

    def test_malformed_end_date_alone_is_rejected(self, dated_db: Database):
        with pytest.raises(ValueError, match="end_date"):
            search_transactions(dated_db, end_date="31.03.2025")

    def test_unknown_period_is_rejected(self, dated_db: Database):
        with pytest.raises(ValueError, match="Unknown period"):
            search_transactions(dated_db, period="septembre")

    def test_end_date_alone_cannot_be_combined_with_a_period(self, dated_db: Database):
        # "March up to the 15th" or "everything up to the 15th"? Ask instead of guessing.
        with pytest.raises(ValueError, match="start_date"):
            search_transactions(dated_db, period="2025-03", end_date="2025-03-15")


class TestTransferSides:
    @pytest.fixture
    def exchange_db(self, populated_db: Database) -> Database:
        # 49 USD leave the dollar account, 45 EUR arrive on the euro one
        insert_account(populated_db, id="acc-usd", title="Dollar cash", type="cash", instrument=2)
        insert_account(populated_db, id="acc-eur", title="Euro wallet", instrument=3)
        insert_transaction(populated_db, id="fx", date="2025-03-10",
                           outcome=49.0, outcome_instrument=2, outcome_account="acc-usd",
                           income=45.0, income_instrument=3, income_account="acc-eur")
        return populated_db

    def test_exchange_shows_what_left_and_what_arrived(self, exchange_db: Database):
        (tx,) = search_transactions(exchange_db, period="2025-03", tx_type="transfer")["transactions"]

        assert tx["from"] == {"account": "Dollar cash", "amount": 49.0, "currency": "USD"}
        assert tx["to"] == {"account": "Euro wallet", "amount": 45.0, "currency": "EUR"}

    def test_plain_expense_has_no_transfer_sides(self, exchange_db: Database):
        insert_transaction(exchange_db, id="lunch", date="2025-03-11", outcome=300.0)

        (tx,) = search_transactions(exchange_db, period="2025-03", tx_type="outcome")["transactions"]

        assert (tx["amount"], tx["currency"]) == (300.0, "RUB")
        assert "from" not in tx and "to" not in tx
