"""Utility functions for ZenMoney MCP server."""

from .database import Database


def convert_to_user_currency(
    amount: float,
    instrument_id: int,
    db: Database,
    user_currency_id: int | None = None,
) -> float:
    """Convert amount from instrument currency to user's currency.

    Formula: amount_in_user = amount * instrument.rate / user_currency.rate

    Args:
        amount: Amount in the source currency.
        instrument_id: Source currency instrument ID.
        db: Database instance.
        user_currency_id: User's currency instrument ID. If None, will be looked up.

    Returns:
        Amount converted to user's currency.
    """
    if user_currency_id is None:
        user_currency_id = db.get_user_currency()
        if user_currency_id is None:
            # Fallback: return amount as-is if no user found
            return amount

    source_rate = db.get_instrument_rate(instrument_id)
    user_rate = db.get_instrument_rate(user_currency_id)

    if user_rate == 0:
        return amount

    # Convert to RUB first, then to user currency
    amount_in_rub = amount * source_rate
    amount_in_user = amount_in_rub / user_rate

    return amount_in_user


def classify_transaction(
    tx: dict,
    accounts: dict[str, dict] | None = None,
) -> str:
    """Classify transaction type.

    Args:
        tx: Transaction dict with income, outcome, incomeAccount, outcomeAccount.
        accounts: Optional dict of accounts by ID for debt detection.

    Returns:
        Transaction type: "income", "outcome", "transfer", "exchange", "debt_out", "debt_in"
    """
    income = tx.get("income", 0) or 0
    outcome = tx.get("outcome", 0) or 0

    # Pure income
    if income > 0 and outcome == 0:
        return "income"

    # Pure outcome
    if outcome > 0 and income == 0:
        return "outcome"

    # Both sides have values - it's a transfer, exchange, or debt operation
    if income > 0 and outcome > 0:
        income_account_id = tx.get("income_account") or tx.get("incomeAccount")
        outcome_account_id = tx.get("outcome_account") or tx.get("outcomeAccount")

        if accounts:
            income_acc = accounts.get(income_account_id, {})
            outcome_acc = accounts.get(outcome_account_id, {})

            income_is_debt = income_acc.get("type") == "debt"
            outcome_is_debt = outcome_acc.get("type") == "debt"

            # Debt operations
            if income_is_debt and not outcome_is_debt:
                return "debt_out"  # Gave money to debt account (lent money)
            if outcome_is_debt and not income_is_debt:
                return "debt_in"  # Took money from debt account (borrowed)

            # Check for currency exchange
            income_instrument = tx.get("income_instrument") or tx.get("incomeInstrument")
            outcome_instrument = tx.get("outcome_instrument") or tx.get("outcomeInstrument")

            if income_instrument != outcome_instrument:
                return "exchange"

        return "transfer"

    return "unknown"


def is_transfer(tx: dict) -> bool:
    """Check if transaction is a transfer (income > 0 AND outcome > 0).

    This is used for filtering out transfers from spending/income analysis.
    """
    income = tx.get("income", 0) or 0
    outcome = tx.get("outcome", 0) or 0
    return income > 0 and outcome > 0


def is_pure_expense(tx: dict) -> bool:
    """Check if transaction is a pure expense (outcome > 0 AND income == 0)."""
    income = tx.get("income", 0) or 0
    outcome = tx.get("outcome", 0) or 0
    return outcome > 0 and income == 0


def is_pure_income(tx: dict) -> bool:
    """Check if transaction is a pure income (income > 0 AND outcome == 0)."""
    income = tx.get("income", 0) or 0
    outcome = tx.get("outcome", 0) or 0
    return income > 0 and outcome == 0
