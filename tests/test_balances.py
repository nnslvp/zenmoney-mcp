"""Tests for balance tools: net worth, liquidity, debts, currency conversion.

Fixture reminder (``populated_db``): user currency RUB (rate 1), USD rate 90, EUR rate 100.
Accounts: card 50 000 RUB with a 150 000 limit, cash 1 000 USD, deposit 500 000 RUB,
the system debt account (off-balance, 5 000 RUB) and one archived card.
"""

import json

import pytest

from zenmoney_mcp import server as zm_server
from zenmoney_mcp.analytics import (
    convert_currency,
    get_accounts_resource,
    get_debts,
    get_exchange_rates,
    get_liquidity,
    get_net_worth,
)
from zenmoney_mcp.database import Database

from .factories import insert_account, insert_instrument, insert_merchant, insert_transaction


def _titles(accounts: list[dict]) -> list[str]:
    return [account["title"] for account in accounts]


def _set_user_currency(db: Database, instrument_id: int) -> None:
    conn = db.connect()
    conn.execute("UPDATE users SET currency = ?", (instrument_id,))
    conn.commit()


def _counterparty(result: dict, name: str) -> dict:
    return next(cp for cp in result["by_counterparty"] if cp["counterparty"] == name)


def _borrow(db: Database, payee: str, amount: float, **overrides) -> None:
    """Money comes from the debt account to the user's card: the user received it."""
    row = dict(income_account="acc-rub", outcome_account="acc-debt") | overrides
    insert_transaction(db, payee=payee, income=amount, outcome=amount, **row)


def _lend(db: Database, payee: str, amount: float, **overrides) -> None:
    """Money goes from the user's card to the debt account: the user gave it away."""
    row = dict(income_account="acc-debt", outcome_account="acc-rub") | overrides
    insert_transaction(db, payee=payee, income=amount, outcome=amount, **row)


async def _tool_schema(name: str) -> dict:
    tools = {tool.name: tool for tool in await zm_server.list_tools()}
    return tools[name].inputSchema


class TestNetWorthClassification:
    """Accounts are grouped by type whether or not they count towards the balance."""

    def test_off_balance_accounts_are_grouped_by_type(self, populated_db: Database):
        insert_account(
            populated_db, title="Vacation fund", type="ccard", instrument=3,
            balance=2000.0, in_balance=0, savings=1,
        )

        breakdown = get_net_worth(populated_db)["breakdown"]

        assert _titles(breakdown["debts"]["accounts"]) == ["Долги"]
        assert breakdown["debts"]["total"] == 5000.0
        assert "Vacation fund" in _titles(breakdown["savings"]["accounts"])
        # deposit 500 000 + 2 000 EUR * 100
        assert breakdown["savings"]["total"] == 700000.0

    def test_every_account_counts_towards_net_worth_once(self, populated_db: Database):
        insert_account(
            populated_db, title="Vacation fund", type="ccard", instrument=3,
            balance=2000.0, in_balance=0, savings=1,
        )

        result = get_net_worth(populated_db)

        # card 50 000 + cash 90 000 + deposit 500 000 + Vacation fund 200 000 + debts 5 000
        assert result["net_worth"] == 845000.0
        assert result["net_worth"] == sum(group["total"] for group in result["breakdown"].values())

    def test_in_balance_and_off_balance_totals_are_reported(self, populated_db: Database):
        insert_account(
            populated_db, title="Vacation fund", type="ccard", instrument=3,
            balance=2000.0, in_balance=0, savings=1,
        )

        result = get_net_worth(populated_db)

        assert result["in_balance_total"] == 640000.0
        assert result["off_balance_total"] == 205000.0
        assert result["out_of_balance"]["total"] == 205000.0
        assert sorted(_titles(result["out_of_balance"]["accounts"])) == ["Vacation fund", "Долги"]

    def test_each_account_says_whether_it_is_in_balance(self, populated_db: Database):
        breakdown = get_net_worth(populated_db)["breakdown"]

        flags = {
            account["title"]: account["in_balance"]
            for group in breakdown.values()
            for account in group["accounts"]
        }
        assert flags == {
            "Тинькофф Black": True,
            "Наличные USD": True,
            "Накопительный Сбер": True,
            "Долги": False,
        }


class TestLiquiditySavings:
    """Savings are reported next to liquid funds, wherever they are kept."""

    def test_off_balance_savings_are_reported_with_account_names(self, populated_db: Database):
        insert_account(
            populated_db, title="Vacation fund", type="ccard", instrument=3,
            balance=2000.0, in_balance=0, savings=1,
        )

        result = get_liquidity(populated_db)

        assert result["liquid_own"] == 140000.0
        assert result["savings_accessible"] == 700000.0
        savings = {account["title"]: account for account in result["breakdown"]["savings_accounts"]}
        assert savings["Vacation fund"]["balance_converted"] == 200000.0
        assert savings["Vacation fund"]["in_balance"] is False
        assert savings["Накопительный Сбер"]["in_balance"] is True

    def test_in_balance_account_flagged_as_savings_is_savings_not_liquid(self, populated_db: Database):
        insert_account(populated_db, title="Rainy day", type="checking", balance=30000.0, savings=1)

        result = get_liquidity(populated_db)

        assert result["liquid_own"] == 140000.0
        assert result["savings_accessible"] == 530000.0
        assert "Rainy day" in _titles(result["breakdown"]["savings_accounts"])
        assert "Rainy day" not in _titles(result["breakdown"]["liquid_accounts"])

    def test_debt_and_loan_accounts_are_never_funds(self, populated_db: Database):
        insert_account(populated_db, title="Mortgage", type="loan", balance=250000.0, savings=1)
        insert_account(populated_db, title="Old debts", type="debt", balance=7000.0, in_balance=0, savings=1)

        result = get_liquidity(populated_db)

        assert result["liquid_own"] == 140000.0
        assert result["savings_accessible"] == 500000.0
        assert _titles(result["breakdown"]["savings_accounts"]) == ["Накопительный Сбер"]

    def test_off_balance_everyday_account_is_not_liquid(self, populated_db: Database):
        insert_account(populated_db, title="Shared wallet", type="cash", balance=40000.0, in_balance=0)

        result = get_liquidity(populated_db)

        assert result["liquid_own"] == 140000.0
        assert result["total_available"] == 640000.0


class TestLiquidityTargetCheck:
    """Fixture funds: 140 000 liquid, 500 000 savings."""

    def test_target_within_liquid_funds_needs_no_savings(self, populated_db: Database):
        check = get_liquidity(populated_db, target_amount=100000)["target_check"]

        assert check["verdict"] == "affordable_from_liquid"
        assert check["needed_from_savings"] == 0.0
        assert check["shortfall"] == 0.0

    def test_target_above_liquid_funds_says_how_much_comes_from_savings(self, populated_db: Database):
        # the deposit is kept off-balance
        insert_account(
            populated_db, id="acc-save", title="Deposit", type="deposit",
            balance=500000.0, in_balance=0, savings=1,
        )

        check = get_liquidity(populated_db, target_amount=200000)["target_check"]

        assert check["verdict"] == "affordable_with_savings"
        assert check["needed_from_savings"] == 60000.0
        assert check["shortfall"] == 0.0
        assert check["affordable_from_liquid"] is False
        assert check["affordable_with_savings"] is True
        assert "60000.0 RUB" in check["recommendation"]
        assert "savings" in check["recommendation"]

    def test_target_above_liquid_funds_and_savings_is_insufficient(self, populated_db: Database):
        check = get_liquidity(populated_db, target_amount=700000)["target_check"]

        assert check["verdict"] == "insufficient"
        assert check["shortfall"] == 60000.0
        assert check["needed_from_savings"] == 0.0
        assert "even with savings" in check["recommendation"]
        assert "60000.0 RUB" in check["recommendation"]

    @pytest.mark.parametrize("target", [-1, float("nan"), float("inf"), "5000", True])
    def test_invalid_target_is_rejected(self, populated_db: Database, target):
        with pytest.raises(ValueError, match="target_amount"):
            get_liquidity(populated_db, target_amount=target)

    def test_credit_is_mentioned_only_as_an_alternative_to_savings(self, populated_db: Database):
        # liquid funds plus the card's 150 000 limit give 290 000
        within_credit = get_liquidity(populated_db, target_amount=200000)["target_check"]
        beyond_credit = get_liquidity(populated_db, target_amount=350000)["target_check"]

        assert within_credit["verdict"] == "affordable_with_savings"
        assert "credit" in within_credit["recommendation"]
        assert beyond_credit["verdict"] == "affordable_with_savings"
        assert "credit" not in beyond_credit["recommendation"]

    async def test_schema_says_target_is_in_main_currency(self):
        schema = await _tool_schema("get_liquidity")

        assert "main currency" in schema["properties"]["target_amount"]["description"]


class TestLiquidityNegativeBalances:
    """Money owed on a card or an account reduces liquid funds the same way."""

    def test_overdrawn_card_and_overdrawn_account_both_reduce_liquid_funds(self, populated_db: Database):
        insert_account(populated_db, title="Overdrawn card", type="ccard", balance=-10000.0)
        insert_account(populated_db, title="Overdrawn account", type="checking", balance=-5000.0)

        result = get_liquidity(populated_db)

        assert result["liquid_own"] == 125000.0
        assert result["negative_balances_total"] == -15000.0
        # only the credit limit comes on top of own funds
        assert result["liquid_with_credit"] == 275000.0


class TestDebtsCurrency:
    """A debt operation is recorded in the currency of the non-debt account."""

    def test_debt_in_another_currency_is_converted_to_user_currency(self, populated_db: Database):
        # 40 USD received in cash; both instruments are the cash account's, as the API stores it
        _borrow(
            populated_db, "Dana", 40.0, date="2026-03-01",
            income_account="acc-usd", income_instrument=2, outcome_instrument=2,
        )

        result = get_debts(populated_db)

        dana = _counterparty(result, "Dana")
        assert dana["net_amount"] == -3600.0
        assert dana["transactions"][0]["amount"] == 3600.0
        assert dana["transactions"][0]["original_amount"] == 40.0
        assert dana["transactions"][0]["original_currency"] == "USD"
        assert result["summary"]["total_you_owe"] == 3600.0
        # fixture: 5 000 RUB lent
        assert result["summary"]["net_position"] == 1400.0

    def test_conversion_uses_the_rate_of_the_users_currency(self, populated_db: Database):
        _set_user_currency(populated_db, 3)  # EUR, rate 100
        insert_account(
            populated_db, id="acc-debt", title="Долги", type="debt", instrument=3,
            balance=14.0, in_balance=0,
        )
        _borrow(
            populated_db, "Dana", 40.0, date="2026-03-01",
            income_account="acc-usd", income_instrument=2, outcome_instrument=2,
        )

        result = get_debts(populated_db)

        assert result["currency"] == "EUR"
        # fixture: 5 000 RUB lent = 50 EUR; 40 USD borrowed = 36 EUR
        assert _counterparty(result, "Паша")["net_amount"] == 50.0
        assert _counterparty(result, "Dana")["net_amount"] == -36.0
        assert result["summary"]["total_owed_to_you"] == 50.0
        assert result["summary"]["total_you_owe"] == 36.0

    def test_net_position_reconciles_with_the_debt_account_balance(self, populated_db: Database):
        _set_user_currency(populated_db, 3)  # EUR, rate 100
        insert_account(
            populated_db, id="acc-debt", title="Долги", type="debt", instrument=3,
            balance=14.0, in_balance=0,
        )
        _borrow(
            populated_db, "Dana", 40.0, date="2026-03-01",
            income_account="acc-usd", income_instrument=2, outcome_instrument=2,
        )

        result = get_debts(populated_db)

        assert result["summary"]["net_position"] == 14.0
        assert sum(cp["net_amount"] for cp in result["by_counterparty"]) == 14.0

    def test_operation_in_user_currency_has_no_original_amount(self, populated_db: Database):
        result = get_debts(populated_db)

        transaction = _counterparty(result, "Паша")["transactions"][0]
        assert transaction["amount"] == 5000.0
        assert "original_amount" not in transaction
        assert "original_currency" not in transaction


def _history(result: dict, name: str) -> list[tuple[str, str]]:
    """Operations of one person as (date, type), in the order they are returned."""
    return [(tx["date"], tx["type"]) for tx in _counterparty(result, name)["transactions"]]


class TestDebtsLabels:
    """The debt account side gives the direction, the running balance tells a loan from a repayment."""

    def test_money_given_with_nothing_outstanding_is_a_loan(self, populated_db: Database):
        _lend(populated_db, "Lee", 3000.0, date="2026-03-01")

        assert _history(get_debts(populated_db), "Lee") == [("2026-03-01", "you_lent")]

    def test_money_received_with_nothing_outstanding_is_a_borrowing(self, populated_db: Database):
        _borrow(populated_db, "Kim", 750.0, date="2026-03-01")

        assert _history(get_debts(populated_db), "Kim") == [("2026-03-01", "you_borrowed")]

    def test_money_received_from_someone_who_owes_you_is_their_repayment(self, populated_db: Database):
        _lend(populated_db, "Lee", 3000.0, date="2026-03-01")
        _borrow(populated_db, "Lee", 3000.0, date="2026-03-10")

        result = get_debts(populated_db)

        assert _history(result, "Lee") == [("2026-03-10", "they_repaid"), ("2026-03-01", "you_lent")]
        assert _counterparty(result, "Lee")["status"] == "settled"

    def test_money_given_to_someone_you_owe_is_your_repayment(self, populated_db: Database):
        _borrow(populated_db, "Kim", 750.0, date="2026-03-01")
        _lend(populated_db, "Kim", 750.0, date="2026-03-02")

        result = get_debts(populated_db)

        assert _history(result, "Kim") == [("2026-03-02", "you_repaid"), ("2026-03-01", "you_borrowed")]
        assert _counterparty(result, "Kim")["status"] == "settled"

    def test_partial_repayment_leaves_the_rest_outstanding(self, populated_db: Database):
        _lend(populated_db, "Lee", 1000.0, date="2026-03-01")
        _borrow(populated_db, "Lee", 400.0, date="2026-03-05")

        result = get_debts(populated_db)

        assert _history(result, "Lee") == [("2026-03-05", "they_repaid"), ("2026-03-01", "you_lent")]
        assert _counterparty(result, "Lee")["net_amount"] == 600.0
        assert _counterparty(result, "Lee")["status"] == "they_owe_you"

    def test_same_day_operations_are_read_in_creation_order(self, populated_db: Database):
        # stored out of order on purpose: the repayment row is inserted first
        _borrow(populated_db, "Lee", 500.0, date="2026-03-01", created=2000, comment="evening")
        _lend(populated_db, "Lee", 500.0, date="2026-03-01", created=1000, comment="morning")

        transactions = _counterparty(get_debts(populated_db), "Lee")["transactions"]

        assert [(tx["comment"], tx["type"]) for tx in transactions] == [
            ("evening", "they_repaid"),
            ("morning", "you_lent"),
        ]

    def test_receiving_more_than_was_owed_to_you_is_a_new_borrowing(self, populated_db: Database):
        _lend(populated_db, "Lee", 100.0, date="2026-03-01")
        _borrow(populated_db, "Lee", 300.0, date="2026-03-05")

        result = get_debts(populated_db)

        assert _history(result, "Lee")[0] == ("2026-03-05", "you_borrowed")
        assert _counterparty(result, "Lee")["net_amount"] == -200.0
        assert _counterparty(result, "Lee")["status"] == "you_owe_them"

    def test_debt_repaid_in_full_is_settled_despite_float_error(self, populated_db: Database):
        _lend(populated_db, "Lee", 0.1, date="2026-03-01")
        _lend(populated_db, "Lee", 0.2, date="2026-03-02")
        _borrow(populated_db, "Lee", 0.3, date="2026-03-03")

        lee = _counterparty(get_debts(populated_db), "Lee")

        assert lee["net_amount"] == 0.0
        assert lee["status"] == "settled"

    def test_merchant_id_is_kept_when_an_earlier_operation_has_none(self, populated_db: Database):
        insert_merchant(populated_db, id="m-lee", title="Lee")
        _lend(populated_db, "Lee", 100.0, date="2026-03-01")
        _borrow(populated_db, "Lee", 100.0, date="2026-03-02", merchant="m-lee")

        result = get_debts(populated_db)

        assert _counterparty(result, "Lee")["merchant_id"] == "m-lee"
        assert len(_counterparty(result, "Lee")["transactions"]) == 2


class TestDebtsWithoutDebtAccount:
    def test_empty_summary_is_in_the_users_currency(self, populated_db: Database):
        conn = populated_db.connect()
        conn.execute("DELETE FROM accounts WHERE type = 'debt'")
        conn.commit()
        _set_user_currency(populated_db, 2)  # USD

        result = get_debts(populated_db)

        assert result["currency"] == "USD"
        assert result["by_counterparty"] == []
        assert result["summary"]["net_position"] == 0.0


class TestConvertCurrencyInput:
    def test_currency_codes_are_trimmed(self, populated_db: Database):
        result = convert_currency(populated_db, amount=100, from_currency=" usd ", to_currency="eur\n")

        assert result["from"]["currency"] == "USD"
        assert result["to"]["currency"] == "EUR"
        assert result["to"]["amount"] == 90.0

    @pytest.mark.parametrize("amount", [1e308, float("inf"), float("nan"), "100", True, None])
    def test_amount_that_is_not_a_sane_number_is_rejected(self, populated_db: Database, amount):
        with pytest.raises(ValueError, match="amount"):
            convert_currency(populated_db, amount=amount, from_currency="USD", to_currency="RUB")

    def test_largest_accepted_amount_still_gives_valid_json(self, populated_db: Database):
        result = convert_currency(populated_db, amount=1e15, from_currency="EUR", to_currency="RUB")

        assert result["to"]["amount"] == 1e17
        json.dumps(result, allow_nan=False)

    @pytest.mark.parametrize("code", ["", "   ", None, 840])
    def test_blank_or_non_text_currency_code_is_rejected(self, populated_db: Database, code):
        with pytest.raises(ValueError, match="from_currency"):
            convert_currency(populated_db, amount=1, from_currency=code, to_currency="RUB")


class TestConvertCurrencyPrecision:
    """Fixed decimals must not flatten the rate of a weak currency."""

    def test_tiny_rate_keeps_significant_digits(self, populated_db: Database):
        # 1 TNY = 0.00005 RUB, 1 USD = 90 RUB
        insert_instrument(populated_db, id=50, title="Tiny coin", short_title="TNY", symbol="t", rate=0.00005)

        result = convert_currency(populated_db, amount=1, from_currency="TNY", to_currency="USD")

        assert result["rate"] == 5.55556e-07
        assert result["inverse_rate"] == 1800000.0
        assert result["to"]["amount"] == 5.6e-07
        assert result["rate_description"] == "1 TNY = 5.556e-07 USD"

    def test_tiny_amount_in_user_currency_is_not_rounded_to_zero(self, populated_db: Database):
        insert_instrument(populated_db, id=50, title="Tiny coin", short_title="TNY", symbol="t", rate=0.00005)

        result = convert_currency(populated_db, amount=1, from_currency="TNY", to_currency="USD")

        assert result["in_user_currency"]["amount"] == 5e-05

    def test_ordinary_rates_and_amounts_keep_their_rounding(self, populated_db: Database):
        result = convert_currency(populated_db, amount=50, from_currency="EUR", to_currency="USD")

        assert result["rate"] == 1.111111
        assert result["inverse_rate"] == 0.9
        assert result["to"]["amount"] == 55.56
        assert result["rate_description"] == "1 EUR = 1.1111 USD"


class TestConvertCurrencyUnits:
    """ZenMoney keeps crypto in micro-units: the BTC instrument is one millionth of a bitcoin."""

    def test_instrument_titles_are_included(self, populated_db: Database):
        result = convert_currency(populated_db, amount=100, from_currency="USD", to_currency="EUR")

        assert result["from"]["title"] == "Доллар США"
        assert result["to"]["title"] == "Евро"

    def test_micro_unit_instrument_is_shown_as_is_not_rescaled(self, populated_db: Database):
        insert_instrument(populated_db, id=60, title="Bitcoin", short_title="BTC", symbol="μBTC", rate=9.0)

        result = convert_currency(populated_db, amount=1, from_currency="BTC", to_currency="USD")

        assert result["from"] == {"amount": 1, "currency": "BTC", "title": "Bitcoin", "symbol": "μBTC"}
        assert result["to"]["amount"] == 0.1
        assert result["rate_description"] == "1 μBTC = 0.1 USD"

    def test_rate_description_uses_plain_codes_for_ordinary_currencies(self, populated_db: Database):
        result = convert_currency(populated_db, amount=1, from_currency="USD", to_currency="RUB")

        assert result["rate_description"] == "1 USD = 90.0 RUB"


def _codes(result: dict) -> list[str]:
    return [entry["currency"] for entry in result["currencies"]]


class TestExchangeRatesInput:
    def test_unknown_codes_are_reported(self, populated_db: Database):
        result = get_exchange_rates(populated_db, currencies=["USD", "XXX", "nope"])

        assert result["unknown_currencies"] == ["XXX", "NOPE"]
        assert _codes(result) == ["USD"]

    def test_no_unknown_codes_gives_an_empty_list(self, populated_db: Database):
        assert get_exchange_rates(populated_db, currencies=["USD"])["unknown_currencies"] == []
        assert get_exchange_rates(populated_db)["unknown_currencies"] == []

    def test_codes_are_trimmed_and_deduplicated(self, populated_db: Database):
        result = get_exchange_rates(populated_db, currencies=["usd", "USD", " eur ", "EUR"])

        assert _codes(result) == ["EUR", "USD"]
        assert result["cross_rates"] == {"USD": {"EUR": 0.9}, "EUR": {"USD": 1.111111}}
        assert result["unknown_currencies"] == []

    @pytest.mark.parametrize("currencies", ["USD", ["USD", ""], ["USD", None], [840]])
    def test_anything_but_a_list_of_codes_is_rejected(self, populated_db: Database, currencies):
        with pytest.raises(ValueError, match="currencies"):
            get_exchange_rates(populated_db, currencies=currencies)


class TestExchangeRatesToUserCurrency:
    def test_rate_to_user_currency_is_given_even_when_it_was_not_requested(self, populated_db: Database):
        _set_user_currency(populated_db, 3)  # EUR, rate 100

        result = get_exchange_rates(populated_db, currencies=["USD"])

        assert result["user_currency"] == "EUR"
        assert result["currencies"][0]["rate_to_EUR"] == 0.9
        # the table itself stays limited to what was asked for
        assert _codes(result) == ["USD"]
        assert result["cross_rates"] == {"USD": {}}

    def test_user_currency_has_no_rate_to_itself(self, populated_db: Database):
        _set_user_currency(populated_db, 3)

        result = get_exchange_rates(populated_db, currencies=["USD", "EUR"])

        rates = {entry["currency"]: entry for entry in result["currencies"]}
        assert rates["USD"]["rate_to_EUR"] == 0.9
        assert "rate_to_EUR" not in rates["EUR"]


class TestExchangeRatesPrecision:
    def test_tiny_cross_rate_keeps_significant_digits(self, populated_db: Database):
        _set_user_currency(populated_db, 3)
        insert_instrument(populated_db, id=50, title="Tiny coin", short_title="TNY", symbol="t", rate=0.00005)

        result = get_exchange_rates(populated_db, currencies=["TNY", "USD"])

        assert result["cross_rates"]["TNY"]["USD"] == 5.55556e-07
        assert result["cross_rates"]["USD"]["TNY"] == 1800000.0
        assert result["currencies"][0]["rate_to_EUR"] == 5e-07


class TestAccountsResourceTotals:
    def test_totals_say_what_they_cover(self, populated_db: Database):
        insert_account(
            populated_db, title="Vacation fund", type="ccard", instrument=3,
            balance=2000.0, in_balance=0, savings=1,
        )

        result = get_accounts_resource(populated_db)

        assert result["in_balance_total"] == 640000.0
        assert result["off_balance_total"] == 205000.0
        assert result["user_currency"] == "RUB"
        assert "total_in_user_currency" not in result

    def test_totals_match_net_worth(self, populated_db: Database):
        resource = get_accounts_resource(populated_db)
        net_worth = get_net_worth(populated_db)

        assert resource["in_balance_total"] == net_worth["in_balance_total"]
        assert resource["off_balance_total"] == net_worth["off_balance_total"]
