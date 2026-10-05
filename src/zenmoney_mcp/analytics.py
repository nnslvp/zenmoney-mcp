"""Analytics business logic for ZenMoney MCP tools."""

import calendar
import json
import re
from datetime import date, datetime, timedelta
from typing import Any

import httpx

from .database import Database
from .utils import convert_to_user_currency


PERIOD_FORMATS = (
    "'this_month', 'last_month', 'this_year', 'last_year', "
    "'last_N_days' (e.g. 'last_30_days'), 'YYYY-MM' or 'YYYY'"
)

_LAST_N_DAYS = re.compile(r"last_(\d+)_days")
_YEAR_MONTH = re.compile(r"(\d{4})-(\d{2})")
_YEAR = re.compile(r"\d{4}")


def parse_iso_date(value: str, field: str) -> date:
    """Parse a 'YYYY-MM-DD' date (a full ISO datetime is accepted, its time is dropped).

    Dates are compared as strings in SQL, so anything else must be rejected here.
    """
    try:
        return datetime.fromisoformat(value.strip()).date()
    except (ValueError, AttributeError):
        raise ValueError(f"Invalid {field} {value!r}: expected YYYY-MM-DD") from None


def _month_bounds(year: int, month: int) -> tuple[date, date]:
    return date(year, month, 1), date(year, month, calendar.monthrange(year, month)[1])


def _named_period_bounds(period: str, today: date) -> tuple[date, date] | None:
    if period == "this_month":
        return _month_bounds(today.year, today.month)
    if period == "last_month":
        last_day = today.replace(day=1) - timedelta(days=1)
        return _month_bounds(last_day.year, last_day.month)
    if period == "this_year":
        return date(today.year, 1, 1), date(today.year, 12, 31)
    if period == "last_year":
        return date(today.year - 1, 1, 1), date(today.year - 1, 12, 31)
    if match := _LAST_N_DAYS.fullmatch(period):
        days = int(match.group(1))
        if days >= 1:
            return today - timedelta(days=days - 1), today
    if match := _YEAR_MONTH.fullmatch(period):
        year, month = int(match.group(1)), int(match.group(2))
        if year >= 1 and 1 <= month <= 12:
            return _month_bounds(year, month)
    if _YEAR.fullmatch(period) and int(period) >= 1:
        return date(int(period), 1, 1), date(int(period), 12, 31)
    return None


def get_period_dates(
    period: str,
    start_date: str | None = None,
    end_date: str | None = None,
) -> tuple[str, str]:
    """Convert period string to start and end dates (both inclusive).

    Args:
        period: One of PERIOD_FORMATS.
        start_date: Optional explicit start date (ISO format). Overrides period.
        end_date: Optional explicit end date (ISO format). Defaults to today.

    Returns:
        Tuple of (start_date, end_date) as ISO strings.

    Raises:
        ValueError: If the period or a date cannot be parsed.
    """
    today = date.today()

    if start_date or end_date:
        if not start_date:
            raise ValueError("end_date requires start_date")
        start = parse_iso_date(start_date, "start_date")
        end = parse_iso_date(end_date, "end_date") if end_date else today
        if start > end:
            raise ValueError(f"start_date {start} is after end_date {end}")
        return start.isoformat(), end.isoformat()

    bounds = _named_period_bounds(period, today) if isinstance(period, str) else None
    if bounds is None:
        raise ValueError(
            f"Unknown period {period!r}. Use {PERIOD_FORMATS}, or pass start_date/end_date."
        )
    return bounds[0].isoformat(), bounds[1].isoformat()


def get_net_worth(db: Database) -> dict[str, Any]:
    """Calculate total net worth across all accounts.

    T1: "How much money do I have?"

    Every non-archived account is counted once, in the group of its type, whether
    or not it is in balance: the system debt account is always off-balance, and
    savings are often kept off-balance too.

    Returns:
        Dictionary with net_worth, its breakdown by account type, in_balance_total
        (the balance the ZenMoney app shows) and off_balance_total. out_of_balance
        lists the off-balance accounts again; it is already part of the breakdown.
    """
    conn = db.connect()

    # Get user currency
    user_currency_id = db.get_user_currency()
    if not user_currency_id:
        user_currency_id = 2  # Default to RUB

    # Get user currency info
    currency_row = conn.execute(
        "SELECT short_title, symbol FROM instruments WHERE id = ?",
        (user_currency_id,)
    ).fetchone()
    currency_code = currency_row["short_title"] if currency_row else "RUB"
    currency_symbol = currency_row["symbol"] if currency_row else "₽"

    # Get all active accounts, in balance or not
    rows = conn.execute("""
        SELECT a.id, a.title, a.type, a.balance, a.credit_limit,
               a.in_balance, a.savings, a.instrument,
               i.short_title as currency, i.symbol as currency_symbol, i.rate
        FROM accounts a
        LEFT JOIN instruments i ON i.id = a.instrument
        WHERE a.archive = 0
        ORDER BY a.type, a.balance DESC
    """).fetchall()

    # Group by type
    current_accounts = []  # cash, ccard, checking, emoney
    savings_accounts = []  # deposit, savings=1
    loans_accounts = []    # loan
    debts_accounts = []    # debt
    out_of_balance = []    # in_balance = 0, also listed in their type group

    current_total = 0.0
    savings_total = 0.0
    loans_total = 0.0
    debts_total = 0.0
    in_balance_total = 0.0
    out_of_balance_total = 0.0

    for row in rows:
        balance = row["balance"] or 0
        instrument_id = row["instrument"]

        # Convert to user currency
        if instrument_id and instrument_id != user_currency_id:
            converted = convert_to_user_currency(balance, instrument_id, db, user_currency_id)
        else:
            converted = balance

        account_info = {
            "id": row["id"],
            "title": row["title"],
            "balance": balance,
            "currency": row["currency"] or "???",
            "currency_symbol": row["currency_symbol"] or "?",
            "converted": round(converted, 2),
            "in_balance": bool(row["in_balance"]),
        }

        if row["in_balance"]:
            in_balance_total += converted
        else:
            out_of_balance.append(account_info)
            out_of_balance_total += converted

        acc_type = row["type"]
        is_savings = row["savings"]

        if acc_type == "debt":
            debts_accounts.append(account_info)
            debts_total += converted
        elif acc_type == "loan":
            loans_accounts.append(account_info)
            loans_total += converted
        elif acc_type == "deposit" or is_savings:
            savings_accounts.append(account_info)
            savings_total += converted
        else:  # cash, ccard, checking, emoney
            current_accounts.append(account_info)
            current_total += converted

    net_worth = current_total + savings_total + loans_total + debts_total

    return {
        "net_worth": round(net_worth, 2),
        "in_balance_total": round(in_balance_total, 2),
        "off_balance_total": round(out_of_balance_total, 2),
        "currency": currency_code,
        "currency_symbol": currency_symbol,
        "breakdown": {
            "current": {
                "total": round(current_total, 2),
                "accounts": current_accounts,
            },
            "savings": {
                "total": round(savings_total, 2),
                "accounts": savings_accounts,
            },
            "loans": {
                "total": round(loans_total, 2),
                "accounts": loans_accounts,
            },
            "debts": {
                "total": round(debts_total, 2),
                "accounts": debts_accounts,
            },
        },
        "out_of_balance": {
            "total": round(out_of_balance_total, 2),
            "accounts": out_of_balance,
        },
    }


def get_liquidity(
    db: Database,
    target_amount: float | None = None,
) -> dict[str, Any]:
    """Calculate liquid funds available for spending.

    T2: "How much liquid cash?", "Can I afford a purchase?"

    Liquid funds are in-balance cash, cards and checking accounts. Savings
    (deposits and accounts flagged as savings, in or off balance) are reported
    separately and are only the second source in the affordability check.

    liquid_own is a net figure: negative balances (overdrafts, used credit) are
    subtracted for every account type and summed up in negative_balances_total.

    Args:
        db: Database instance.
        target_amount: Optional target purchase amount to check affordability,
            in the user's main currency.

    Returns:
        Dictionary with liquid funds breakdown and affordability check.

    Raises:
        ValueError: If target_amount is negative or not a finite number.
    """
    if target_amount is not None:
        is_number = isinstance(target_amount, (int, float)) and not isinstance(target_amount, bool)
        if not is_number or not 0 <= target_amount < float("inf"):
            raise ValueError(
                f"Invalid target_amount {target_amount!r}: expected a non-negative number "
                "in the user's main currency"
            )

    conn = db.connect()

    # Get user currency
    user_currency_id = db.get_user_currency()
    if not user_currency_id:
        user_currency_id = 2  # Default to RUB

    currency_row = conn.execute(
        "SELECT short_title, symbol FROM instruments WHERE id = ?",
        (user_currency_id,)
    ).fetchone()
    currency_code = currency_row["short_title"] if currency_row else "RUB"
    currency_symbol = currency_row["symbol"] if currency_row else "₽"

    # Get all non-archived accounts that hold own money. Debt and loan accounts
    # are money owed by or to someone, never funds to spend.
    rows = conn.execute("""
        SELECT a.id, a.title, a.type, a.balance, a.credit_limit,
               a.in_balance, a.savings, a.instrument,
               i.short_title as currency, i.rate
        FROM accounts a
        LEFT JOIN instruments i ON i.id = a.instrument
        WHERE a.archive = 0 AND a.type NOT IN ('debt', 'loan')
        ORDER BY a.type, a.balance DESC
    """).fetchall()

    liquid_own = 0.0  # Own money in liquid accounts, net of negative balances
    liquid_with_credit = 0.0  # Including available credit
    savings_accessible = 0.0  # Savings (less liquid)
    negative_balances = 0.0  # Owed on overdrawn liquid accounts, part of liquid_own

    liquid_accounts = []
    credit_accounts = []
    savings_accounts = []

    for row in rows:
        balance = row["balance"] or 0
        credit_limit = row["credit_limit"] or 0
        instrument_id = row["instrument"]
        acc_type = row["type"]
        is_savings = row["savings"]

        # Convert to user currency
        if instrument_id and instrument_id != user_currency_id:
            converted_balance = convert_to_user_currency(balance, instrument_id, db, user_currency_id)
            converted_credit = convert_to_user_currency(credit_limit, instrument_id, db, user_currency_id)
        else:
            converted_balance = balance
            converted_credit = credit_limit

        account_info = {
            "id": row["id"],
            "title": row["title"],
            "type": acc_type,
            "balance": balance,
            "currency": row["currency"] or "???",
            "balance_converted": round(converted_balance, 2),
        }

        # Categorize accounts
        if acc_type == "deposit" or is_savings:
            # Savings: accessible but less liquid. Counted in or off balance,
            # savings are often kept off-balance.
            account_info["in_balance"] = bool(row["in_balance"])
            savings_accessible += converted_balance
            savings_accounts.append(account_info)

        elif row["in_balance"] and acc_type in ("cash", "ccard", "checking", "emoney"):
            # Liquid accounts. A negative balance is money already owed, so it
            # reduces own funds on a card just as on any other account; this is
            # also the net figure the ZenMoney app shows as the balance.
            liquid_own += converted_balance
            if converted_balance < 0:
                negative_balances += converted_balance

            if acc_type == "ccard":
                # Credit card: balance + available credit
                available_credit = converted_balance + converted_credit if converted_credit > 0 else converted_balance

                liquid_with_credit += available_credit

                account_info["available_credit"] = round(available_credit, 2)
                account_info["credit_limit"] = round(converted_credit, 2)

                if converted_credit > 0:
                    credit_accounts.append(account_info)
                else:
                    liquid_accounts.append(account_info)
            else:
                # Cash, checking, emoney: all balance is liquid
                liquid_with_credit += converted_balance
                liquid_accounts.append(account_info)

    total_available = liquid_own + savings_accessible

    result = {
        "liquid_own": round(liquid_own, 2),
        "liquid_with_credit": round(liquid_with_credit, 2),
        "negative_balances_total": round(negative_balances, 2),
        "savings_accessible": round(savings_accessible, 2),
        "total_available": round(total_available, 2),
        "currency": currency_code,
        "currency_symbol": currency_symbol,
        "breakdown": {
            "liquid_accounts": liquid_accounts,
            "credit_accounts": credit_accounts,
            "savings_accounts": savings_accounts,
        },
    }

    # Target affordability check: own money first (liquid, then savings).
    # Credit is borrowed money, so it is only mentioned as an alternative.
    if target_amount is not None:
        needed_from_savings = 0.0
        shortfall = 0.0

        if liquid_own >= target_amount:
            verdict = "affordable_from_liquid"
            recommendation = "Affordable from liquid funds"
        elif total_available >= target_amount:
            verdict = "affordable_with_savings"
            needed_from_savings = target_amount - liquid_own
            recommendation = (
                f"Affordable only by using savings: "
                f"{round(needed_from_savings, 2)} {currency_code} must come from savings"
            )
        else:
            verdict = "insufficient"
            shortfall = target_amount - total_available
            recommendation = (
                f"Insufficient funds even with savings (short {round(shortfall, 2)} {currency_code})"
            )

        if verdict != "affordable_from_liquid" and liquid_with_credit >= target_amount:
            recommendation += ". Alternatively, available credit covers it"

        result["target_check"] = {
            "target": target_amount,
            "verdict": verdict,
            "needed_from_savings": round(needed_from_savings, 2),
            "shortfall": round(shortfall, 2),
            "affordable_from_liquid": liquid_own >= target_amount,
            "affordable_with_credit": liquid_with_credit >= target_amount,
            "affordable_with_savings": total_available >= target_amount,
            "recommendation": recommendation,
        }

    return result


def analyze_spending(
    db: Database,
    period: str = "this_month",
    category_id: str | None = None,
    top_n: int = 10,
    include_transfers: bool = False,
    include_holds: bool = False,
    start_date: str | None = None,
    end_date: str | None = None,
    group_by: str = "category",
) -> dict[str, Any]:
    """Analyze spending by categories.

    T3: "Where does my money go?", "What do I spend the most on?"

    Args:
        db: Database instance.
        period: Time period ("this_month", "last_month", "last_30_days", "YYYY-MM").
        category_id: Optional category filter (includes children).
        top_n: Number of top categories to return.
        include_transfers: Include transfers in analysis.
        include_holds: Include hold transactions.
        start_date: Optional explicit start date (ISO). Overrides period.
        end_date: Optional explicit end date (ISO). Used with start_date.
        group_by: Aggregation mode: "category" (default) or "merchant".

    Returns:
        Dictionary with spending breakdown by categories.
    """
    conn = db.connect()
    start_date, end_date = get_period_dates(period, start_date=start_date, end_date=end_date)

    # Get user currency
    user_currency_id = db.get_user_currency()
    if not user_currency_id:
        user_currency_id = 2  # Default to RUB

    currency_row = conn.execute(
        "SELECT short_title, symbol, rate FROM instruments WHERE id = ?",
        (user_currency_id,)
    ).fetchone()
    currency_code = currency_row["short_title"] if currency_row else "RUB"
    user_rate = currency_row["rate"] if currency_row else 1.0

    # Build category filter if specified
    category_ids = []
    if category_id:
        category_ids.append(category_id)
        # Add children
        children = conn.execute(
            "SELECT id FROM tags WHERE parent = ?", (category_id,)
        ).fetchall()
        category_ids.extend(row["id"] for row in children)

    # Base query for expenses (get all, filter holds in Python to track excluded)
    query = """
        SELECT
            t.id,
            t.outcome,
            t.outcome_instrument,
            t.tag,
            t.hold,
            t.merchant,
            t.payee,
            m.title as merchant_title
        FROM transactions t
        LEFT JOIN accounts a ON a.id = t.outcome_account
        LEFT JOIN merchants m ON m.id = t.merchant
        WHERE t.deleted = 0
          AND t.date BETWEEN ? AND ?
          AND t.outcome > 0
          AND t.income = 0
          AND (a.in_balance = 1 OR a.in_balance IS NULL)
    """
    params: list[Any] = [start_date, end_date]

    if not include_transfers:
        query += " AND NOT (t.income > 0 AND t.outcome > 0)"

    rows = conn.execute(query, params).fetchall()

    # Aggregate by category
    category_totals: dict[str | None, dict[str, Any]] = {}
    merchant_totals: dict[str | None, dict[str, Any]] = {}
    holds_excluded = {"amount": 0.0, "count": 0}

    for row in rows:
        # Filter holds in Python to track excluded amount
        is_hold = row["hold"]

        tag_json = row["tag"]
        if tag_json:
            try:
                tags = json.loads(tag_json)
                primary_tag = tags[0] if tags else None
            except (json.JSONDecodeError, IndexError):
                primary_tag = None
        else:
            primary_tag = None

        # Filter by category if specified
        if category_ids and primary_tag not in category_ids:
            continue

        # Convert amount to user currency
        amount = row["outcome"]
        instrument_id = row["outcome_instrument"]
        if instrument_id and instrument_id != user_currency_id:
            source_rate = db.get_instrument_rate(instrument_id)
            amount = amount * source_rate / user_rate if user_rate else amount

        # Track holds separately if not included
        if row["hold"] and not include_holds:
            holds_excluded["amount"] += amount
            holds_excluded["count"] += 1
            continue

        if primary_tag not in category_totals:
            category_totals[primary_tag] = {
                "tag_id": primary_tag,
                "amount": 0.0,
                "count": 0,
            }

        category_totals[primary_tag]["amount"] += amount
        category_totals[primary_tag]["count"] += 1

        # Track merchant totals (for group_by=merchant or drill-down)
        merchant_name = row["merchant_title"] or row["payee"] or None
        merchant_key = row["merchant"] or merchant_name
        if merchant_key not in merchant_totals:
            merchant_totals[merchant_key] = {
                "merchant_id": row["merchant"],
                "name": merchant_name or "Unknown",
                "amount": 0.0,
                "count": 0,
            }
        merchant_totals[merchant_key]["amount"] += amount
        merchant_totals[merchant_key]["count"] += 1

    # Calculate total
    total_outcome = sum(cat["amount"] for cat in category_totals.values())

    # If group_by == "merchant", return merchant aggregation
    if group_by == "merchant":
        merchants_list = []
        for data in merchant_totals.values():
            merchants_list.append({
                "merchant_id": data["merchant_id"],
                "name": data["name"],
                "amount": round(data["amount"], 2),
                "share_pct": round(data["amount"] / total_outcome * 100, 1) if total_outcome > 0 else 0,
                "count": data["count"],
                "avg_check": round(data["amount"] / data["count"], 2) if data["count"] > 0 else 0,
            })
        merchants_list.sort(key=lambda x: x["amount"], reverse=True)

        return {
            "period": {"start": start_date, "end": end_date},
            "total_outcome": round(total_outcome, 2),
            "currency": currency_code,
            "group_by": "merchant",
            "merchants": merchants_list[:top_n],
            "returned_count": min(len(merchants_list), top_n),
            "total_merchants": len(merchants_list),
            "holds_excluded": holds_excluded if holds_excluded["count"] > 0 else None,
        }

    # Get category names and parent info
    tag_info = {}
    if category_totals:
        tag_ids = [t for t in category_totals.keys() if t]
        if tag_ids:
            placeholders = ",".join("?" * len(tag_ids))
            tag_rows = conn.execute(
                f"SELECT id, title, parent FROM tags WHERE id IN ({placeholders})",
                tag_ids
            ).fetchall()
            for tr in tag_rows:
                tag_info[tr["id"]] = {"title": tr["title"], "parent": tr["parent"]}

            # Get parent titles
            parent_ids = [ti["parent"] for ti in tag_info.values() if ti["parent"]]
            if parent_ids:
                placeholders = ",".join("?" * len(parent_ids))
                parent_rows = conn.execute(
                    f"SELECT id, title FROM tags WHERE id IN ({placeholders})",
                    parent_ids
                ).fetchall()
                parent_titles = {pr["id"]: pr["title"] for pr in parent_rows}
                for ti in tag_info.values():
                    if ti["parent"]:
                        ti["parent_title"] = parent_titles.get(ti["parent"])

    # Calculate percentages
    categories = []
    for tag_id, data in category_totals.items():
        info = tag_info.get(tag_id, {})
        name = info.get("title", "Uncategorized") if tag_id else "Uncategorized"
        parent_title = info.get("parent_title")

        cat_data = {
            "tag_id": tag_id,
            "name": name,
            "amount": round(data["amount"], 2),
            "share_pct": round(data["amount"] / total_outcome * 100, 1) if total_outcome > 0 else 0,
            "count": data["count"],
            "avg_check": round(data["amount"] / data["count"], 2) if data["count"] > 0 else 0,
        }
        if parent_title:
            cat_data["parent_category"] = parent_title

        categories.append(cat_data)

    # Sort by amount and limit
    categories.sort(key=lambda x: x["amount"], reverse=True)

    # Separate uncategorized
    uncategorized = None
    categorized = []
    for cat in categories:
        if cat["tag_id"] is None:
            uncategorized = {"amount": cat["amount"], "count": cat["count"]}
        else:
            categorized.append(cat)

    result = {
        "period": {"start": start_date, "end": end_date},
        "total_outcome": round(total_outcome, 2),
        "currency": currency_code,
        "categories": categorized[:top_n],
        "returned_count": min(len(categorized), top_n),
        "total_categories": len(categorized),
        "uncategorized": uncategorized,
        "holds_excluded": holds_excluded if holds_excluded["count"] > 0 else None,
    }

    # Add top_merchants when in drill-down mode (category_id is set)
    if category_id and merchant_totals:
        top_merchants = []
        for data in merchant_totals.values():
            top_merchants.append({
                "merchant_id": data["merchant_id"],
                "name": data["name"],
                "amount": round(data["amount"], 2),
                "count": data["count"],
            })
        top_merchants.sort(key=lambda x: x["amount"], reverse=True)
        result["top_merchants"] = top_merchants[:10]

    return result


def analyze_income(
    db: Database,
    period: str = "this_month",
    top_n: int = 10,
    start_date: str | None = None,
    end_date: str | None = None,
) -> dict[str, Any]:
    """Analyze income by categories and sources.

    T4: "Where does my money come from?", "How much did I earn?"

    Args:
        db: Database instance.
        period: Time period ("this_month", "last_month", "last_30_days", "YYYY-MM").
        top_n: Number of top categories/sources to return.
        start_date: Optional explicit start date (ISO). Overrides period.
        end_date: Optional explicit end date (ISO). Used with start_date.

    Returns:
        Dictionary with income breakdown by categories and sources.
    """
    conn = db.connect()
    start_date, end_date = get_period_dates(period, start_date=start_date, end_date=end_date)

    # Get user currency
    user_currency_id = db.get_user_currency()
    if not user_currency_id:
        user_currency_id = 2  # Default to RUB

    currency_row = conn.execute(
        "SELECT short_title, symbol, rate FROM instruments WHERE id = ?",
        (user_currency_id,)
    ).fetchone()
    currency_code = currency_row["short_title"] if currency_row else "RUB"
    user_rate = currency_row["rate"] if currency_row else 1.0

    # Query for income transactions (pure income only, no transfers)
    query = """
        SELECT
            t.id,
            t.income,
            t.income_instrument,
            t.tag,
            t.merchant,
            t.payee,
            t.original_payee
        FROM transactions t
        LEFT JOIN accounts a ON a.id = t.income_account
        WHERE t.deleted = 0
          AND (t.hold IS NULL OR t.hold = 0)
          AND t.date BETWEEN ? AND ?
          AND t.income > 0
          AND t.outcome = 0
          AND (a.in_balance = 1 OR a.in_balance IS NULL)
    """
    params: list[Any] = [start_date, end_date]

    rows = conn.execute(query, params).fetchall()

    # Aggregate by category
    category_totals: dict[str | None, dict[str, Any]] = {}
    # Aggregate by source (merchant/payee)
    source_totals: dict[str, dict[str, Any]] = {}

    for row in rows:
        # Parse primary tag
        tag_json = row["tag"]
        if tag_json:
            try:
                tags = json.loads(tag_json)
                primary_tag = tags[0] if tags else None
            except (json.JSONDecodeError, IndexError):
                primary_tag = None
        else:
            primary_tag = None

        # Convert amount to user currency
        amount = row["income"]
        instrument_id = row["income_instrument"]
        if instrument_id and instrument_id != user_currency_id:
            source_rate = db.get_instrument_rate(instrument_id)
            amount = amount * source_rate / user_rate if user_rate else amount

        # Aggregate by category
        if primary_tag not in category_totals:
            category_totals[primary_tag] = {
                "tag_id": primary_tag,
                "amount": 0.0,
                "count": 0,
            }
        category_totals[primary_tag]["amount"] += amount
        category_totals[primary_tag]["count"] += 1

        # Aggregate by source (merchant or payee)
        source_key = row["merchant"] or row["payee"] or "Unknown source"
        if source_key not in source_totals:
            source_totals[source_key] = {
                "merchant_id": row["merchant"],
                "payee": row["payee"],
                "amount": 0.0,
                "count": 0,
            }
        source_totals[source_key]["amount"] += amount
        source_totals[source_key]["count"] += 1

    # Get category names
    tag_info = {}
    if category_totals:
        tag_ids = [t for t in category_totals.keys() if t]
        if tag_ids:
            placeholders = ",".join("?" * len(tag_ids))
            tag_rows = conn.execute(
                f"SELECT id, title, parent FROM tags WHERE id IN ({placeholders})",
                tag_ids
            ).fetchall()
            for tr in tag_rows:
                tag_info[tr["id"]] = {"title": tr["title"], "parent": tr["parent"]}

    # Get merchant names
    merchant_ids = [s["merchant_id"] for s in source_totals.values() if s["merchant_id"]]
    merchant_titles = {}
    if merchant_ids:
        placeholders = ",".join("?" * len(merchant_ids))
        merchant_rows = conn.execute(
            f"SELECT id, title FROM merchants WHERE id IN ({placeholders})",
            merchant_ids
        ).fetchall()
        merchant_titles = {mr["id"]: mr["title"] for mr in merchant_rows}

    # Calculate totals
    total_income = sum(cat["amount"] for cat in category_totals.values())

    # Format categories
    categories = []
    for tag_id, data in category_totals.items():
        info = tag_info.get(tag_id, {})
        name = info.get("title", "Uncategorized") if tag_id else "Uncategorized"

        categories.append({
            "tag_id": tag_id,
            "name": name,
            "amount": round(data["amount"], 2),
            "share_pct": round(data["amount"] / total_income * 100, 1) if total_income > 0 else 0,
            "count": data["count"],
        })

    categories.sort(key=lambda x: x["amount"], reverse=True)

    # Format sources
    sources = []
    for source_key, data in source_totals.items():
        merchant_id = data["merchant_id"]
        name = merchant_titles.get(merchant_id) if merchant_id else data["payee"]
        if not name:
            name = "Unknown source"

        sources.append({
            "name": name,
            "merchant_id": merchant_id,
            "amount": round(data["amount"], 2),
            "share_pct": round(data["amount"] / total_income * 100, 1) if total_income > 0 else 0,
            "count": data["count"],
        })

    sources.sort(key=lambda x: x["amount"], reverse=True)

    return {
        "period": {"start": start_date, "end": end_date},
        "total_income": round(total_income, 2),
        "currency": currency_code,
        "categories": categories[:top_n],
        "sources": sources[:top_n],
        "returned_categories": min(len(categories), top_n),
        "total_categories": len(categories),
        "returned_sources": min(len(sources), top_n),
        "total_sources": len(sources),
    }


def _compute_budget_period(
    month_start_day: int, target_year: int, target_month: int
) -> tuple[date, date]:
    """Compute budget period start/end for a given budget month.

    Args:
        month_start_day: Day of month when budget period starts (1-31).
        target_year: Year of the budget month.
        target_month: Month number (1-12) of the budget month.

    Returns:
        (period_start, period_end) dates.
    """
    import calendar

    # Clamp start day to actual days in target month
    max_day = calendar.monthrange(target_year, target_month)[1]
    clamped_start = min(month_start_day, max_day)
    period_start = date(target_year, target_month, clamped_start)

    # End = day before the start of the next budget period
    if target_month == 12:
        next_year, next_mon = target_year + 1, 1
    else:
        next_year, next_mon = target_year, target_month + 1

    max_day_next = calendar.monthrange(next_year, next_mon)[1]
    next_period_start_day = min(month_start_day, max_day_next)
    next_period_start = date(next_year, next_mon, next_period_start_day)
    period_end = next_period_start - timedelta(days=1)

    return period_start, period_end


def _current_budget_month(month_start_day: int, today: date) -> tuple[int, int]:
    """Determine which budget month 'today' falls into.

    Returns (year, month) of the budget month.
    """
    if month_start_day <= 1 or today.day >= month_start_day:
        return today.year, today.month
    # Before the start day → previous budget month
    if today.month == 1:
        return today.year - 1, 12
    return today.year, today.month - 1


def check_budget_health(
    db: Database,
    month: str | None = None,
) -> dict[str, Any]:
    """Check budget health: plan vs actual spending.

    T5: "Am I within budget?", "Where am I overspending?"

    Args:
        db: Database instance.
        month: Month in "YYYY-MM" format. If None, uses current month.

    Returns:
        Dictionary with budget health status for each category.
    """
    conn = db.connect()
    today = date.today()
    month_start_day = db.get_user_month_start_day()

    # Determine period and budget month
    if month:
        try:
            target_year, target_month = map(int, month.split("-"))
            date(target_year, target_month, 1)  # validate
        except (ValueError, AttributeError):
            target_year, target_month = _current_budget_month(month_start_day, today)
        period_start, period_end = _compute_budget_period(
            month_start_day, target_year, target_month
        )
    else:
        target_year, target_month = _current_budget_month(month_start_day, today)
        period_start, period_end = _compute_budget_period(
            month_start_day, target_year, target_month
        )

    # Budget records are always keyed by YYYY-MM-01
    budget_date = date(target_year, target_month, 1).isoformat()

    # Calculate days progress
    days_total = (period_end - period_start).days + 1

    if period_start <= today <= period_end:
        days_elapsed = (today - period_start).days + 1
        month_status = "current"
    elif today > period_end:
        days_elapsed = days_total
        month_status = "completed"
    else:
        days_elapsed = 0
        month_status = "future"

    days_remaining = max(0, days_total - days_elapsed)

    # Get user currency
    user_currency_id = db.get_user_currency()
    if not user_currency_id:
        user_currency_id = 2  # Default to RUB

    currency_row = conn.execute(
        "SELECT short_title, rate FROM instruments WHERE id = ?",
        (user_currency_id,)
    ).fetchone()
    currency_code = currency_row["short_title"] if currency_row else "RUB"
    user_rate = currency_row["rate"] if currency_row else 1.0

    # Load budgets for this month
    budget_rows = conn.execute("""
        SELECT b.tag, b.outcome, b.outcome_lock, t.title as tag_title
        FROM budgets b
        LEFT JOIN tags t ON t.id = b.tag
        WHERE b.date = ?
    """, (budget_date,)).fetchall()

    if not budget_rows:
        return {
            "month": f"{target_year:04d}-{target_month:02d}",
            "period_start": period_start.isoformat(),
            "period_end": period_end.isoformat(),
            "status": month_status,
            "days_elapsed": days_elapsed,
            "days_total": days_total,
            "message": "No budgets configured for this month",
            "categories": [],
        }

    # Date range for actuals
    month_start = period_start.isoformat()
    month_end = period_end.isoformat()

    categories = []
    overall_planned = 0.0
    overall_actual = 0.0

    for budget_row in budget_rows:
        tag_id = budget_row["tag"]
        tag_title = budget_row["tag_title"]
        budget_outcome = budget_row["outcome"] or 0
        outcome_lock = budget_row["outcome_lock"]

        # Special handling for total budget and null category
        if tag_id == "00000000-0000-0000-0000-000000000000":
            tag_title = "Monthly total"
            is_total = True
        elif not tag_title:
            tag_title = "Uncategorized"
            is_total = False
        else:
            is_total = False

        # Calculate planned amount
        if outcome_lock:
            planned = budget_outcome
        else:
            # Include planned reminder markers
            reminder_sum = conn.execute("""
                SELECT COALESCE(SUM(outcome), 0) as total
                FROM reminder_markers
                WHERE state = 'planned'
                  AND date >= ? AND date <= ?
                  AND tag = ?
            """, (month_start, month_end, tag_id)).fetchone()["total"]
            planned = budget_outcome + reminder_sum

        # Clamp negative planned to 0
        planned_warning = None
        if planned < 0:
            planned_warning = f"Computed planned was {round(planned, 2)}, clamped to 0"
            planned = 0

        # Calculate actual spending for this tag
        # Include children tags
        tag_ids = [tag_id] if tag_id else []
        if tag_id and not is_total:
            children = conn.execute(
                "SELECT id FROM tags WHERE parent = ?", (tag_id,)
            ).fetchall()
            tag_ids.extend(row["id"] for row in children)

        if tag_ids:
            placeholders = ",".join("?" * len(tag_ids))
            actual_query = f"""
                SELECT t.outcome, t.outcome_instrument
                FROM transactions t
                LEFT JOIN accounts a ON a.id = t.outcome_account
                WHERE t.deleted = 0
                  AND (t.hold IS NULL OR t.hold = 0)
                  AND NOT (t.income > 0 AND t.outcome > 0)
                  AND t.outcome > 0
                  AND t.income = 0
                  AND t.date >= ? AND t.date <= ?
                  AND json_extract(t.tag, '$[0]') IN ({placeholders})
                  AND (a.in_balance = 1 OR a.in_balance IS NULL)
            """
            params = [month_start, month_end] + tag_ids
            actual_rows = conn.execute(actual_query, params).fetchall()

            actual = 0.0
            for row in actual_rows:
                amount = row["outcome"]
                instrument_id = row["outcome_instrument"]
                if instrument_id and instrument_id != user_currency_id:
                    source_rate = db.get_instrument_rate(instrument_id)
                    amount = amount * source_rate / user_rate if user_rate else amount
                actual += amount
        else:
            actual = 0.0

        # Skip if both planned and actual are zero (except for total)
        if not is_total and planned == 0 and actual == 0:
            continue

        # Calculate metrics
        remaining = planned - actual
        pct_used = (actual / planned * 100) if planned > 0 else 0
        daily_remaining = (remaining / days_remaining) if days_remaining > 0 else 0

        # Determine status
        if pct_used < 80:
            status = "on_track"
        elif pct_used < 100:
            status = "warning"
        else:
            status = "overspent"

        # Calculate pace
        month_progress = days_elapsed / days_total if days_total > 0 else 0
        spend_progress = pct_used / 100 if planned > 0 else 0

        if spend_progress > month_progress * 1.1:
            pace = "ahead_of_pace"
        elif spend_progress < month_progress * 0.9:
            pace = "behind_pace"
        else:
            pace = "on_pace"

        # Generate insight
        insight = None
        if status == "overspent":
            overspend = actual - planned
            insight = f"Overspent by {round(overspend, 2)} {currency_code}"
        elif status == "warning" and pace == "ahead_of_pace" and days_remaining > 0:
            days_until_depleted = int(remaining / (actual / days_elapsed)) if actual > 0 else days_remaining
            if days_until_depleted < days_remaining:
                insight = f"At current pace, budget will be exhausted in {days_until_depleted} days"

        cat_data = {
            "tag_id": tag_id,
            "name": tag_title,
            "planned": round(planned, 2),
            "actual": round(actual, 2),
            "remaining": round(remaining, 2),
            "pct_used": round(pct_used, 1),
            "daily_remaining": round(daily_remaining, 2) if days_remaining > 0 else 0,
            "status": status,
            "pace": pace,
        }

        if insight:
            cat_data["insight"] = insight
        if planned_warning:
            cat_data["warning"] = planned_warning

        if is_total:
            overall_data = cat_data.copy()
            overall_data.pop("tag_id", None)
            overall_data.pop("name", None)
        else:
            categories.append(cat_data)
            if not is_total:
                overall_planned += planned
                overall_actual += actual

    # Sort categories by pct_used descending (most critical first)
    categories.sort(key=lambda x: x["pct_used"], reverse=True)

    result = {
        "month": f"{target_year:04d}-{target_month:02d}",
        "period_start": period_start.isoformat(),
        "period_end": period_end.isoformat(),
        "status": month_status,
        "days_elapsed": days_elapsed,
        "days_total": days_total,
        "currency": currency_code,
        "categories": categories,
    }

    # Build overall from accumulated totals (not from the "000..." total budget row
    # which has no real transactions mapped to it)
    if overall_planned > 0 or overall_actual > 0:
        overall_remaining = overall_planned - overall_actual
        overall_pct_used = (overall_actual / overall_planned * 100) if overall_planned > 0 else 0
        overall_daily_remaining = (overall_remaining / days_remaining) if days_remaining > 0 else 0

        if overall_pct_used < 80:
            overall_status = "on_track"
        elif overall_pct_used < 100:
            overall_status = "warning"
        else:
            overall_status = "overspent"

        month_progress = days_elapsed / days_total if days_total > 0 else 0
        spend_progress = overall_pct_used / 100 if overall_planned > 0 else 0
        if spend_progress > month_progress * 1.1:
            overall_pace = "ahead_of_pace"
        elif spend_progress < month_progress * 0.9:
            overall_pace = "behind_pace"
        else:
            overall_pace = "on_pace"

        result["overall"] = {
            "planned": round(overall_planned, 2),
            "actual": round(overall_actual, 2),
            "remaining": round(overall_remaining, 2),
            "pct_used": round(overall_pct_used, 1),
            "daily_remaining": round(overall_daily_remaining, 2) if days_remaining > 0 else 0,
            "status": overall_status,
            "pace": overall_pace,
        }

    return result


def analyze_merchants(
    db: Database,
    period: str = "this_month",
    category_id: str | None = None,
    top_n: int = 10,
    start_date: str | None = None,
    end_date: str | None = None,
) -> dict[str, Any]:
    """Analyze spending by merchants/payees.

    T7: "Where do I spend the most?", "Top merchants"

    Args:
        db: Database instance.
        period: Time period ("this_month", "last_month", "last_30_days", "YYYY-MM").
        category_id: Optional category filter (includes children).
        top_n: Number of top merchants to return.
        start_date: Optional explicit start date (ISO). Overrides period.
        end_date: Optional explicit end date (ISO). Used with start_date.

    Returns:
        Dictionary with spending breakdown by merchants.
    """
    conn = db.connect()
    start_date, end_date = get_period_dates(period, start_date=start_date, end_date=end_date)

    # Get user currency
    user_currency_id = db.get_user_currency()
    if not user_currency_id:
        user_currency_id = 2  # Default to RUB

    currency_row = conn.execute(
        "SELECT short_title, symbol, rate FROM instruments WHERE id = ?",
        (user_currency_id,)
    ).fetchone()
    currency_code = currency_row["short_title"] if currency_row else "RUB"
    user_rate = currency_row["rate"] if currency_row else 1.0

    # Build category filter if specified
    category_ids = []
    if category_id:
        category_ids.append(category_id)
        # Add children
        children = conn.execute(
            "SELECT id FROM tags WHERE parent = ?", (category_id,)
        ).fetchall()
        category_ids.extend(row["id"] for row in children)

    # Query for expense transactions
    query = """
        SELECT
            t.id,
            t.date,
            t.outcome,
            t.outcome_instrument,
            t.tag,
            t.merchant,
            t.payee,
            m.title as merchant_title
        FROM transactions t
        LEFT JOIN accounts a ON a.id = t.outcome_account
        LEFT JOIN merchants m ON m.id = t.merchant
        WHERE t.deleted = 0
          AND t.date BETWEEN ? AND ?
          AND (t.hold IS NULL OR t.hold = 0)
          AND NOT (t.income > 0 AND t.outcome > 0)
          AND t.outcome > 0
          AND t.income = 0
          AND (a.in_balance = 1 OR a.in_balance IS NULL)
    """
    params: list[Any] = [start_date, end_date]

    rows = conn.execute(query, params).fetchall()

    # Aggregate by merchant
    merchant_totals: dict[str, dict[str, Any]] = {}

    for row in rows:
        # Filter by category if specified
        if category_ids:
            tag_json = row["tag"]
            if tag_json:
                try:
                    tags = json.loads(tag_json)
                    primary_tag = tags[0] if tags else None
                except (json.JSONDecodeError, IndexError):
                    primary_tag = None
            else:
                primary_tag = None

            if primary_tag not in category_ids:
                continue

        # Convert amount to user currency
        amount = row["outcome"]
        instrument_id = row["outcome_instrument"]
        if instrument_id and instrument_id != user_currency_id:
            source_rate = db.get_instrument_rate(instrument_id)
            amount = amount * source_rate / user_rate if user_rate else amount

        # Determine merchant key (merchant_id or payee)
        merchant_id = row["merchant"]
        merchant_title = row["merchant_title"]
        payee = row["payee"]

        # Use merchant title if available, otherwise payee
        display_name = merchant_title or payee or "Unknown"
        merchant_key = merchant_id or payee or "unknown"

        if merchant_key not in merchant_totals:
            merchant_totals[merchant_key] = {
                "merchant_id": merchant_id,
                "name": display_name,
                "amount": 0.0,
                "count": 0,
                "last_visit": row["date"],
            }

        merchant_totals[merchant_key]["amount"] += amount
        merchant_totals[merchant_key]["count"] += 1

        # Track last visit (latest date)
        if row["date"] > merchant_totals[merchant_key]["last_visit"]:
            merchant_totals[merchant_key]["last_visit"] = row["date"]

    # Calculate totals and percentages
    total_outcome = sum(m["amount"] for m in merchant_totals.values())

    merchants = []
    for key, data in merchant_totals.items():
        merchants.append({
            "merchant_id": data["merchant_id"],
            "name": data["name"],
            "total": round(data["amount"], 2),
            "visits": data["count"],
            "avg_check": round(data["amount"] / data["count"], 2) if data["count"] > 0 else 0,
            "last_visit": data["last_visit"],
            "share_pct": round(data["amount"] / total_outcome * 100, 1) if total_outcome > 0 else 0,
        })

    # Sort by total amount and limit
    merchants.sort(key=lambda x: x["total"], reverse=True)

    return {
        "period": {"start": start_date, "end": end_date},
        "total_outcome": round(total_outcome, 2),
        "currency": currency_code,
        "merchants": merchants[:top_n],
        "returned_count": min(len(merchants), top_n),
        "total_merchants": len(merchants),
    }


def detect_recurring(
    db: Database,
    lookback_months: int = 3,
    tolerance_pct: int = 10,
) -> dict[str, Any]:
    """Detect recurring payments (subscriptions, regular bills).

    T6: "What subscriptions?", "Recurring payments?", "What can I cancel?"

    Args:
        db: Database instance.
        lookback_months: Number of months to analyze (default 3).
        tolerance_pct: Tolerance for amount variation in % (default 10).

    Returns:
        Dictionary with detected recurring payments.
    """
    conn = db.connect()

    # Get user currency
    user_currency_id = db.get_user_currency()
    if not user_currency_id:
        user_currency_id = 2  # Default to RUB

    currency_row = conn.execute(
        "SELECT short_title, rate FROM instruments WHERE id = ?",
        (user_currency_id,)
    ).fetchone()
    currency_code = currency_row["short_title"] if currency_row else "RUB"
    user_rate = currency_row["rate"] if currency_row else 1.0

    # Calculate date range
    today = date.today()
    start_date = today - timedelta(days=lookback_months * 30)

    # Query expense transactions
    rows = conn.execute("""
        SELECT
            t.id, t.date, t.outcome, t.outcome_instrument, t.outcome_account,
            t.merchant, t.payee, t.tag, t.mcc,
            m.title as merchant_title,
            a.title as account_title
        FROM transactions t
        LEFT JOIN merchants m ON m.id = t.merchant
        LEFT JOIN accounts a ON a.id = t.outcome_account
        WHERE t.deleted = 0
          AND (t.hold IS NULL OR t.hold = 0)
          AND NOT (t.income > 0 AND t.outcome > 0)
          AND t.outcome > 0
          AND t.income = 0
          AND t.date >= ?
          AND (a.in_balance = 1 OR a.in_balance IS NULL)
        ORDER BY t.date ASC
    """, (start_date.isoformat(),)).fetchall()

    # Group transactions by (payee/merchant, tag, account)
    groups = {}
    for row in rows:
        # Convert to user currency
        amount = row["outcome"]
        instrument_id = row["outcome_instrument"]
        if instrument_id and instrument_id != user_currency_id:
            source_rate = db.get_instrument_rate(instrument_id)
            converted_amount = amount * source_rate / user_rate if user_rate else amount
        else:
            converted_amount = amount

        # Group key
        payee_key = row["merchant_title"] or row["payee"] or "unknown"
        tag_json = row["tag"]
        if tag_json:
            try:
                tags = json.loads(tag_json)
                tag_key = tags[0] if tags else None
            except (json.JSONDecodeError, IndexError):
                tag_key = None
        else:
            tag_key = None

        account_key = row["outcome_account"]

        # Round amount to nearest 100 for grouping (tolerance for minor variations)
        amount_bucket = round(converted_amount / 100) * 100

        group_key = (payee_key, tag_key, account_key, amount_bucket)

        if group_key not in groups:
            groups[group_key] = {
                "payee": payee_key,
                "merchant_id": row["merchant"],
                "tag": tag_key,
                "account": account_key,
                "account_title": row["account_title"],
                "mcc": row["mcc"],
                "transactions": [],
            }

        groups[group_key]["transactions"].append({
            "id": row["id"],
            "date": row["date"],
            "amount": converted_amount,
        })

    # Analyze each group for recurring patterns
    recurring = []

    for group_key, group_data in groups.items():
        txs = group_data["transactions"]

        # Need at least 2 transactions to detect pattern
        if len(txs) < 2:
            continue

        # Sort by date
        txs.sort(key=lambda x: x["date"])

        # Calculate intervals between transactions
        intervals = []
        for i in range(1, len(txs)):
            date1 = date.fromisoformat(txs[i-1]["date"])
            date2 = date.fromisoformat(txs[i]["date"])
            interval_days = (date2 - date1).days
            intervals.append(interval_days)

        if not intervals:
            continue

        # Determine average interval
        avg_interval = sum(intervals) / len(intervals)

        # Classify frequency
        if 25 <= avg_interval <= 35:
            frequency = "monthly"
            interval_days = 30
        elif 6 <= avg_interval <= 8:
            frequency = "weekly"
            interval_days = 7
        elif 12 <= avg_interval <= 16:
            frequency = "biweekly"
            interval_days = 14
        elif 85 <= avg_interval <= 95:
            frequency = "quarterly"
            interval_days = 90
        elif 360 <= avg_interval <= 370:
            frequency = "yearly"
            interval_days = 365
        else:
            # Not a clear pattern
            continue

        # Check amount stability
        amounts = [tx["amount"] for tx in txs]
        avg_amount = sum(amounts) / len(amounts)
        max_amount = max(amounts)
        min_amount = min(amounts)

        if avg_amount > 0:
            variation_pct = ((max_amount - min_amount) / avg_amount) * 100
        else:
            variation_pct = 0

        # Skip if amounts vary too much
        if variation_pct > tolerance_pct:
            continue

        # Check consistency (at least 2 occurrences expected within lookback period)
        expected_occurrences = (lookback_months * 30) / interval_days
        actual_occurrences = len(txs)
        confidence = min(actual_occurrences / max(expected_occurrences, 1), 1.0)

        # Skip if confidence too low
        if confidence < 0.5:
            continue

        # Get tag name
        tag_name = None
        if group_data["tag"]:
            tag_row = conn.execute(
                "SELECT title FROM tags WHERE id = ?", (group_data["tag"],)
            ).fetchone()
            if tag_row:
                tag_name = tag_row["title"]

        # Classify type based on MCC and tag
        mcc = group_data["mcc"]
        if tag_name:
            tag_lower = tag_name.lower()
            if any(word in tag_lower for word in ["подписка", "subscription", "сервис"]):
                payment_type = "subscription"
            elif any(word in tag_lower for word in ["жкх", "коммунал", "utility"]):
                payment_type = "utility"
            elif any(word in tag_lower for word in ["кредит", "loan"]):
                payment_type = "loan"
            elif any(word in tag_lower for word in ["страхов", "insurance"]):
                payment_type = "insurance"
            else:
                payment_type = "other"
        else:
            payment_type = "other"

        # Calculate next expected payment
        last_payment_date = date.fromisoformat(txs[-1]["date"])
        next_expected = last_payment_date + timedelta(days=int(interval_days))

        # Calculate yearly cost
        yearly_cost = avg_amount * (365 / interval_days)

        recurring.append({
            "name": group_data["payee"],
            "merchant_id": group_data["merchant_id"],
            "avg_amount": round(avg_amount, 2),
            "frequency": frequency,
            "interval_days": interval_days,
            "category": tag_name,
            "account": group_data["account_title"],
            "last_payment": txs[-1]["date"],
            "next_expected": next_expected.isoformat() if next_expected <= today + timedelta(days=60) else None,
            "confidence": round(confidence, 2),
            "source": "detected",
            "type": payment_type,
            "occurrences": actual_occurrences,
            "yearly_cost": round(yearly_cost, 2),
        })

    # Build set of detected names for dedup
    detected_names = {r["name"].strip().lower() for r in recurring if r.get("name")}

    # Add reminders with interval != null
    reminder_rows = conn.execute("""
        SELECT r.id, r.interval, r.step, r.outcome, r.payee, r.tag,
               r.outcome_account, t.title as tag_title, a.title as account_title
        FROM reminders r
        LEFT JOIN tags t ON t.id = json_extract(r.tag, '$[0]')
        LEFT JOIN accounts a ON a.id = r.outcome_account
        WHERE r.interval IS NOT NULL AND r.outcome > 0
    """).fetchall()

    for row in reminder_rows:
        # Skip if already detected from transactions
        reminder_name = (row["payee"] or "Unknown").strip().lower()
        if reminder_name in detected_names:
            continue

        frequency_map = {
            "day": "daily",
            "week": "weekly",
            "month": "monthly",
            "year": "yearly",
        }
        frequency = frequency_map.get(row["interval"], row["interval"])

        # Compute yearly_cost from amount and frequency
        interval_days_map = {
            "daily": 1,
            "weekly": 7,
            "month": 30,
            "monthly": 30,
            "year": 365,
            "yearly": 365,
        }
        interval_d = interval_days_map.get(frequency, 30)
        yearly_cost = row["outcome"] * (365 / interval_d)

        recurring.append({
            "name": row["payee"] or "Unknown",
            "merchant_id": None,
            "avg_amount": round(row["outcome"], 2),
            "frequency": frequency,
            "category": row["tag_title"],
            "account": row["account_title"],
            "confidence": 1.0,
            "source": "reminder",
            "type": "other",
            "yearly_cost": round(yearly_cost, 2),
        })

    # Sort by yearly cost descending
    recurring.sort(key=lambda x: x.get("yearly_cost", 0), reverse=True)

    # Calculate totals
    total_monthly = sum(r.get("yearly_cost", 0) / 12 for r in recurring)
    total_yearly = sum(r.get("yearly_cost", 0) for r in recurring)

    return {
        "total_monthly_estimate": round(total_monthly, 2),
        "total_yearly_estimate": round(total_yearly, 2),
        "currency": currency_code,
        "recurring": recurring,
        "total_found": len(recurring),
    }


def analyze_trends(
    db: Database,
    months: int = 6,
    category_id: str | None = None,
    metric: str = "outcome",
) -> dict[str, Any]:
    """Analyze spending/income trends over time.

    T8: "How did spending change?", "Am I spending more?"

    Args:
        db: Database instance.
        months: Number of months to analyze (default 6).
        category_id: Optional category filter.
        metric: Metric to track ("outcome", "income", "savings_rate", "net_cashflow").

    Returns:
        Dictionary with monthly data and trend analysis.
    """
    conn = db.connect()

    # Get user currency
    user_currency_id = db.get_user_currency()
    if not user_currency_id:
        user_currency_id = 2  # Default to RUB

    currency_row = conn.execute(
        "SELECT short_title, rate FROM instruments WHERE id = ?",
        (user_currency_id,)
    ).fetchone()
    currency_code = currency_row["short_title"] if currency_row else "RUB"
    user_rate = currency_row["rate"] if currency_row else 1.0

    # Calculate month range
    today = date.today()
    current_month_start = today.replace(day=1)

    monthly_data = []
    values = []

    for i in range(months - 1, -1, -1):
        # Calculate month start
        month_offset = i
        if current_month_start.month > month_offset:
            month_start = current_month_start.replace(month=current_month_start.month - month_offset)
        else:
            year_offset = (month_offset - current_month_start.month + 12) // 12
            new_month = (current_month_start.month - month_offset) % 12
            if new_month == 0:
                new_month = 12
            month_start = current_month_start.replace(year=current_month_start.year - year_offset, month=new_month)

        # Calculate month end
        if month_start.month == 12:
            month_end = date(month_start.year + 1, 1, 1) - timedelta(days=1)
        else:
            month_end = date(month_start.year, month_start.month + 1, 1) - timedelta(days=1)

        month_key = month_start.strftime("%Y-%m")
        is_partial = (month_start.year == today.year and month_start.month == today.month)

        # Build category filter
        category_ids = []
        if category_id:
            category_ids.append(category_id)
            children = conn.execute(
                "SELECT id FROM tags WHERE parent = ?", (category_id,)
            ).fetchall()
            category_ids.extend(row["id"] for row in children)

        # Calculate outcome for this month
        outcome_query = """
            SELECT t.outcome, t.outcome_instrument
            FROM transactions t
            LEFT JOIN accounts a ON a.id = t.outcome_account
            WHERE t.deleted = 0
              AND (t.hold IS NULL OR t.hold = 0)
              AND NOT (t.income > 0 AND t.outcome > 0)
              AND t.outcome > 0
              AND t.income = 0
              AND t.date >= ? AND t.date <= ?
              AND (a.in_balance = 1 OR a.in_balance IS NULL)
        """
        outcome_params: list[Any] = [month_start.isoformat(), month_end.isoformat()]

        if category_ids:
            placeholders = ",".join("?" * len(category_ids))
            outcome_query += f" AND json_extract(t.tag, '$[0]') IN ({placeholders})"
            outcome_params.extend(category_ids)

        outcome_rows = conn.execute(outcome_query, outcome_params).fetchall()
        total_outcome = 0.0
        for row in outcome_rows:
            amount = row["outcome"]
            instrument_id = row["outcome_instrument"]
            if instrument_id and instrument_id != user_currency_id:
                source_rate = db.get_instrument_rate(instrument_id)
                amount = amount * source_rate / user_rate if user_rate else amount
            total_outcome += amount

        # Calculate income for this month (if needed for metric)
        if metric in ("income", "savings_rate", "net_cashflow"):
            income_query = """
                SELECT t.income, t.income_instrument
                FROM transactions t
                LEFT JOIN accounts a ON a.id = t.income_account
                WHERE t.deleted = 0
                  AND (t.hold IS NULL OR t.hold = 0)
                  AND t.income > 0
                  AND t.outcome = 0
                  AND t.date >= ? AND t.date <= ?
                  AND (a.in_balance = 1 OR a.in_balance IS NULL)
            """
            income_params: list[Any] = [month_start.isoformat(), month_end.isoformat()]

            if category_ids:
                placeholders = ",".join("?" * len(category_ids))
                income_query += f" AND json_extract(t.tag, '$[0]') IN ({placeholders})"
                income_params.extend(category_ids)

            income_rows = conn.execute(income_query, income_params).fetchall()
            total_income = 0.0
            for row in income_rows:
                amount = row["income"]
                instrument_id = row["income_instrument"]
                if instrument_id and instrument_id != user_currency_id:
                    source_rate = db.get_instrument_rate(instrument_id)
                    amount = amount * source_rate / user_rate if user_rate else amount
                total_income += amount
        else:
            total_income = 0.0

        # Calculate metric value
        if metric == "outcome":
            value = total_outcome
        elif metric == "income":
            value = total_income
        elif metric == "savings_rate":
            value = ((total_income - total_outcome) / total_income * 100) if total_income > 0 else 0
        elif metric == "net_cashflow":
            value = total_income - total_outcome
        else:
            value = total_outcome

        month_data = {
            "month": month_key,
            "value": round(value, 2),
        }
        if is_partial:
            month_data["partial"] = True

        monthly_data.append(month_data)
        if not is_partial:  # Don't include partial month in trend calculation
            values.append(value)

    # Calculate statistics
    if values:
        avg_value = sum(values) / len(values)
        min_value = min(values)
        max_value = max(values)

        # Find min/max months
        min_month = next((m for m in monthly_data if m["value"] == min_value), None)
        max_month = next((m for m in monthly_data if m["value"] == max_value), None)

        # Calculate trend direction (simple linear regression slope)
        if len(values) >= 2:
            n = len(values)
            x_values = list(range(n))
            x_mean = sum(x_values) / n
            y_mean = avg_value

            numerator = sum((x - x_mean) * (y - y_mean) for x, y in zip(x_values, values))
            denominator = sum((x - x_mean) ** 2 for x in x_values)

            if denominator != 0:
                slope = numerator / denominator
                pct_change_per_month = (slope / y_mean * 100) if y_mean != 0 else 0

                if abs(pct_change_per_month) < 2:
                    trend_direction = "stable"
                elif pct_change_per_month > 0:
                    trend_direction = "rising"
                else:
                    trend_direction = "falling"
            else:
                slope = 0
                pct_change_per_month = 0
                trend_direction = "stable"
        else:
            slope = 0
            pct_change_per_month = 0
            trend_direction = "stable"

        # Detect anomalies (values > 2 standard deviations from mean)
        if len(values) >= 3:
            variance = sum((v - avg_value) ** 2 for v in values) / len(values)
            stddev = variance ** 0.5

            anomalies = []
            for month_data in monthly_data:
                if month_data.get("partial"):
                    continue
                value = month_data["value"]
                if abs(value - avg_value) > 2 * stddev:
                    deviation_pct = ((value - avg_value) / avg_value * 100) if avg_value != 0 else 0
                    anomalies.append({
                        "month": month_data["month"],
                        "value": value,
                        "deviation": f"{deviation_pct:+.1f}%",
                    })
        else:
            anomalies = []

        summary = {
            "average": round(avg_value, 2),
            "min": {"month": min_month["month"] if min_month else None, "value": round(min_value, 2)},
            "max": {"month": max_month["month"] if max_month else None, "value": round(max_value, 2)},
            "trend_direction": trend_direction,
            "trend_pct_change_per_month": round(pct_change_per_month, 1),
        }

        if anomalies:
            summary["anomalies"] = anomalies
    else:
        summary = {
            "message": "Insufficient data for analysis"
        }

    # Get category name if specified
    category_name = None
    if category_id:
        cat_row = conn.execute("SELECT title FROM tags WHERE id = ?", (category_id,)).fetchone()
        if cat_row:
            category_name = cat_row["title"]

    return {
        "metric": metric,
        "category": category_name,
        "currency": currency_code if metric in ("outcome", "income", "net_cashflow") else None,
        "data": monthly_data,
        "summary": summary,
    }


def get_upcoming_payments(
    db: Database,
    days_ahead: int = 30,
) -> dict[str, Any]:
    """Get upcoming planned payments from reminder markers.

    T12: "What payments are coming up?", "What bills are due?"

    Args:
        db: Database instance.
        days_ahead: Planning horizon in days (default 30).

    Returns:
        Dictionary with upcoming payments and weekly/monthly load.
    """
    conn = db.connect()

    # Get user currency
    user_currency_id = db.get_user_currency()
    if not user_currency_id:
        user_currency_id = 2  # Default to RUB

    currency_row = conn.execute(
        "SELECT short_title, rate FROM instruments WHERE id = ?",
        (user_currency_id,)
    ).fetchone()
    currency_code = currency_row["short_title"] if currency_row else "RUB"
    user_rate = currency_row["rate"] if currency_row else 1.0

    # Calculate date range
    today = date.today()
    end_date = (today + timedelta(days=days_ahead)).isoformat()

    # Query upcoming reminder markers
    rows = conn.execute("""
        SELECT
            rm.id,
            rm.date,
            rm.income,
            rm.outcome,
            rm.income_account,
            rm.outcome_account,
            rm.tag,
            rm.merchant,
            rm.payee,
            rm.comment,
            rm.reminder,
            ia.title as income_account_title,
            oa.title as outcome_account_title,
            oa.instrument as outcome_instrument,
            ia.instrument as income_instrument,
            t.title as tag_title,
            m.title as merchant_title
        FROM reminder_markers rm
        LEFT JOIN accounts ia ON ia.id = rm.income_account
        LEFT JOIN accounts oa ON oa.id = rm.outcome_account
        LEFT JOIN tags t ON t.id = json_extract(rm.tag, '$[0]')
        LEFT JOIN merchants m ON m.id = rm.merchant
        WHERE rm.state = 'planned'
          AND rm.date >= ?
          AND rm.date <= ?
        ORDER BY rm.date ASC
    """, (today.isoformat(), end_date)).fetchall()

    upcoming = []
    total_income = 0.0
    total_outcome = 0.0

    for row in rows:
        income = row["income"] or 0
        outcome = row["outcome"] or 0

        # Determine type and convert amount
        if outcome > 0:
            tx_type = "outcome"
            amount = outcome
            instrument_id = row["outcome_instrument"]
            account = row["outcome_account_title"]
        elif income > 0:
            tx_type = "income"
            amount = income
            instrument_id = row["income_instrument"]
            account = row["income_account_title"]
        else:
            continue  # Skip zero-amount markers

        # Convert to user currency
        if instrument_id and instrument_id != user_currency_id:
            source_rate = db.get_instrument_rate(instrument_id)
            converted_amount = amount * source_rate / user_rate if user_rate else amount
        else:
            converted_amount = amount

        # Aggregate totals
        if tx_type == "outcome":
            total_outcome += converted_amount
        else:
            total_income += converted_amount

        # Get category name
        category = row["tag_title"]

        # Get payee/merchant
        payee = row["merchant_title"] or row["payee"] or "Unknown"

        upcoming.append({
            "id": row["id"],
            "date": row["date"],
            "type": tx_type,
            "amount": round(converted_amount, 2),
            "currency": currency_code,
            "account": account,
            "category": category,
            "payee": payee,
            "comment": row["comment"],
            "reminder_id": row["reminder"],
        })

    # Calculate weekly load
    weekly_load = []
    if upcoming:
        # Group by weeks
        weeks = {}
        for payment in upcoming:
            payment_date = date.fromisoformat(payment["date"])
            # Get week start (Monday)
            week_start = payment_date - timedelta(days=payment_date.weekday())
            week_key = week_start.isoformat()

            if week_key not in weeks:
                weeks[week_key] = {"start": week_key, "amount": 0.0}

            if payment["type"] == "outcome":
                weeks[week_key]["amount"] += payment["amount"]

        # Format weekly load
        for week_data in sorted(weeks.values(), key=lambda x: x["start"]):
            week_start_date = date.fromisoformat(week_data["start"])
            week_end_date = week_start_date + timedelta(days=6)
            weekly_load.append({
                "week": f"{week_start_date.strftime('%m-%d')} — {week_end_date.strftime('%m-%d')}",
                "amount": round(week_data["amount"], 2),
            })

    return {
        "upcoming": upcoming,
        "total_upcoming_outcome": round(total_outcome, 2),
        "total_upcoming_income": round(total_income, 2),
        "currency": currency_code,
        "period": {
            "start": today.isoformat(),
            "end": end_date,
            "days": days_ahead,
        },
        "weekly_load": weekly_load,
    }


def get_debts(db: Database) -> dict[str, Any]:
    """Get debts summary (who owes whom).

    T11: "Who owes me?", "Who do I owe?", "Debt summary"

    All amounts are in the user's currency. Each operation is labelled from the
    user's point of view: money given is "you_lent", or "you_repaid" if it reduces
    what the user owed; money received is "you_borrowed", or "they_repaid" if it
    reduces what the person owed.

    Returns:
        Dictionary with debts breakdown by counterparty.
    """
    conn = db.connect()

    # Get user currency
    user_currency_id = db.get_user_currency()
    currency_row = conn.execute(
        "SELECT short_title, rate FROM instruments WHERE id = ?",
        (user_currency_id,)
    ).fetchone()
    currency_code = currency_row["short_title"] if currency_row else "RUB"
    user_rate = currency_row["rate"] if currency_row else None

    # Find debt accounts
    debt_accounts = conn.execute(
        "SELECT id, title, balance, instrument FROM accounts WHERE type = 'debt' AND archive = 0"
    ).fetchall()

    if not debt_accounts:
        return {
            "currency": currency_code,
            "summary": {
                "total_owed_to_you": 0.0,
                "total_you_owe": 0.0,
                "net_position": 0.0,
            },
            "by_counterparty": [],
        }

    counterparties_data = {}

    for debt_acc in debt_accounts:
        account_id = debt_acc["id"]

        # Get all transactions for this debt account, oldest first: the label of
        # an operation depends on the balance before it. A debt operation is
        # recorded in the currency of the non-debt account, on both sides.
        rows = conn.execute("""
            SELECT t.id, t.date, t.income, t.outcome,
                   t.income_account, t.outcome_account,
                   t.merchant, t.payee, t.comment,
                   m.title as merchant_title,
                   ii.short_title as income_currency, ii.rate as income_rate,
                   oi.short_title as outcome_currency, oi.rate as outcome_rate
            FROM transactions t
            LEFT JOIN merchants m ON m.id = t.merchant
            LEFT JOIN instruments ii ON ii.id = t.income_instrument
            LEFT JOIN instruments oi ON oi.id = t.outcome_instrument
            WHERE t.deleted = 0
              AND (t.income_account = ? OR t.outcome_account = ?)
            ORDER BY t.date, t.created
        """, (account_id, account_id)).fetchall()

        for row in rows:
            # Determine counterparty
            merchant_title = row["merchant_title"]
            payee = row["payee"]
            counterparty = merchant_title or payee or "Unknown"

            if counterparty not in counterparties_data:
                counterparties_data[counterparty] = {
                    "name": counterparty,
                    "merchant_id": row["merchant"],
                    "balance": 0.0,
                    "history": [],
                }
            elif row["merchant"]:
                counterparties_data[counterparty]["merchant_id"] = row["merchant"]

            # Both sides of a debt operation are > 0; the direction is given by
            # which side is the debt account.
            given = row["income_account"] == account_id
            if given:
                # Money came into debt account (I lent money or I returned)
                original_amount = row["income"]
                original_currency = row["income_currency"]
                rate = row["income_rate"]
            else:
                # Money went out from debt account (They lent me or they returned)
                original_amount = row["outcome"]
                original_currency = row["outcome_currency"]
                rate = row["outcome_rate"]

            # Convert to user currency. An operation without a known instrument is
            # taken as is: the debt account itself is always in the user's currency.
            is_foreign = bool(
                original_currency and original_currency != currency_code and rate and user_rate
            )
            amount = original_amount * rate / user_rate if is_foreign else original_amount

            # Update balance (positive: they owe me). An operation that reduces
            # what is outstanding is a repayment, any other one is a new loan.
            balance_before = counterparties_data[counterparty]["balance"]
            balance_after = balance_before + amount if given else balance_before - amount
            counterparties_data[counterparty]["balance"] = balance_after

            is_repayment = abs(balance_after) < abs(balance_before)
            if given:
                tx_type = "you_repaid" if is_repayment else "you_lent"
            else:
                tx_type = "they_repaid" if is_repayment else "you_borrowed"

            history_entry = {
                "date": row["date"],
                "amount": round(amount, 2),
                "type": tx_type,
                "comment": row["comment"],
            }
            if is_foreign:
                history_entry["original_amount"] = round(original_amount, 2)
                history_entry["original_currency"] = original_currency
            counterparties_data[counterparty]["history"].append(history_entry)

    # Format counterparties
    by_counterparty = []
    for cp_data in counterparties_data.values():
        # Rounded first: converted amounts leave float dust on a repaid debt
        net_balance = round(cp_data["balance"], 2)

        if net_balance > 0:
            status = "they_owe_you"
        elif net_balance < 0:
            status = "you_owe_them"
        else:
            status = "settled"

        # Get last activity (history is oldest first)
        history = cp_data["history"][::-1]
        if history:
            last_activity = history[0]["date"]
        else:
            last_activity = None

        by_counterparty.append({
            "counterparty": cp_data["name"],
            "merchant_id": cp_data["merchant_id"],
            "net_amount": net_balance,
            "status": status,
            "last_activity": last_activity,
            "transactions": history[:10],  # Last 10 transactions
        })

    # Sort by absolute balance descending
    by_counterparty.sort(key=lambda x: abs(x["net_amount"]), reverse=True)

    # Calculate totals
    total_owed_to_you = sum(cp["net_amount"] for cp in by_counterparty if cp["status"] == "they_owe_you")
    total_you_owe = sum(abs(cp["net_amount"]) for cp in by_counterparty if cp["status"] == "you_owe_them")

    return {
        "currency": currency_code,
        "summary": {
            "total_owed_to_you": round(total_owed_to_you, 2),
            "total_you_owe": round(total_you_owe, 2),
            "net_position": round(total_owed_to_you - total_you_owe, 2),
        },
        "by_counterparty": by_counterparty,
    }


def analyze_transfers(
    db: Database,
    period: str = "this_month",
    top_n: int = 15,
) -> dict[str, Any]:
    """Analyze transfers between accounts.

    T9: "What transfers?", "Currency exchanges"

    Args:
        db: Database instance.
        period: Time period.
        top_n: Number of top transfers to return.

    Returns:
        Dictionary with transfers breakdown.
    """
    conn = db.connect()
    start_date, end_date = get_period_dates(period)

    # Get user currency
    user_currency_id = db.get_user_currency()
    currency_row = conn.execute(
        "SELECT short_title FROM instruments WHERE id = ?",
        (user_currency_id,)
    ).fetchone()
    currency_code = currency_row["short_title"] if currency_row else "RUB"

    # Query transfer transactions (income > 0 AND outcome > 0)
    rows = conn.execute("""
        SELECT
            t.id, t.date, t.income, t.outcome, t.comment,
            t.income_instrument, t.outcome_instrument,
            t.income_account, t.outcome_account,
            ia.title as income_account_title, ia.type as income_account_type,
            oa.title as outcome_account_title, oa.type as outcome_account_type,
            ii.short_title as income_currency,
            oi.short_title as outcome_currency
        FROM transactions t
        LEFT JOIN accounts ia ON ia.id = t.income_account
        LEFT JOIN accounts oa ON oa.id = t.outcome_account
        LEFT JOIN instruments ii ON ii.id = t.income_instrument
        LEFT JOIN instruments oi ON oi.id = t.outcome_instrument
        WHERE t.deleted = 0
          AND t.income > 0
          AND t.outcome > 0
          AND t.date >= ? AND t.date <= ?
        ORDER BY t.date DESC
    """, (start_date, end_date)).fetchall()

    transfers = []
    total_amount = 0.0
    by_type = {}

    for row in rows:
        # Classify transfer type
        income_is_debt = row["income_account_type"] == "debt"
        outcome_is_debt = row["outcome_account_type"] == "debt"
        is_currency_exchange = row["income_currency"] != row["outcome_currency"]

        if income_is_debt or outcome_is_debt:
            transfer_type = "debt"
        elif is_currency_exchange:
            transfer_type = "currency_exchange"
        else:
            transfer_type = "own_transfer"

        # Convert to user currency
        amount_user = convert_to_user_currency(
            row["outcome"], row["outcome_instrument"], db, user_currency_id
        )

        transfer_data = {
            "date": row["date"],
            "from": row["outcome_account_title"],
            "to": row["income_account_title"],
            "amount_outcome": round(row["outcome"], 2),
            "amount_income": round(row["income"], 2),
            "amount_user": round(amount_user, 2),
            "currency_outcome": row["outcome_currency"],
            "currency_income": row["income_currency"],
            "type": transfer_type,
            "comment": row["comment"],
        }

        if is_currency_exchange:
            # Quote the rate the way people say it: the price of one unit of the
            # dearer currency ("1 USD = 3.8 PLN"), whichever way the money went.
            base, quote = (
                ("outcome", "income") if row["outcome"] <= row["income"] else ("income", "outcome")
            )
            rate = round(row[quote] / row[base], 4)
            transfer_data["effective_rate"] = rate
            transfer_data["rate_description"] = (
                f"1 {row[f'{base}_currency']} = {rate} {row[f'{quote}_currency']}"
            )

        transfers.append(transfer_data)
        total_amount += amount_user

        # Aggregate by type
        if transfer_type not in by_type:
            by_type[transfer_type] = {"count": 0, "total": 0.0}
        by_type[transfer_type]["count"] += 1
        by_type[transfer_type]["total"] += amount_user

    # Format by_type for output
    by_type_list = [
        {
            "type": t,
            "count": stats["count"],
            "total": round(stats["total"], 2),
        }
        for t, stats in by_type.items()
    ]
    by_type_list.sort(key=lambda x: x["total"], reverse=True)

    return {
        "period": {"start": start_date, "end": end_date},
        "currency": currency_code,
        "summary": {
            "total_count": len(transfers),
            "total_amount": round(total_amount, 2),
        },
        "by_type": by_type_list,
        "transfers": transfers[:top_n],
    }


def detect_anomalies(
    db: Database,
    period: str = "this_month",
    category_id: str | None = None,
    z_threshold: float = 2.0,
    start_date: str | None = None,
    end_date: str | None = None,
) -> dict[str, Any]:
    """Detect anomalous transactions.

    T10: "Unusual spending?", "Duplicates?", "Suspicious transactions?"

    Args:
        db: Database instance.
        period: Time period to analyze.
        category_id: Optional category filter.
        z_threshold: Z-score threshold (standard deviations). Minimum 1.5.
        start_date: Optional explicit start date (ISO). Overrides period.
        end_date: Optional explicit end date (ISO). Used with start_date.

    Returns:
        Dictionary with detected anomalies.
    """
    # Enforce minimum z_threshold
    z_threshold = max(z_threshold, 1.5)

    conn = db.connect()
    start_date, end_date = get_period_dates(period, start_date=start_date, end_date=end_date)

    # Get user currency for conversion
    user_currency_id = db.get_user_currency()
    if not user_currency_id:
        user_currency_id = 2  # Default to RUB
    currency_row = conn.execute(
        "SELECT short_title, rate FROM instruments WHERE id = ?",
        (user_currency_id,)
    ).fetchone()
    currency_code = currency_row["short_title"] if currency_row else "RUB"
    user_rate = currency_row["rate"] if currency_row else 1.0

    # Build query with optional category filter
    query = """
        SELECT
            t.id, t.date, t.outcome, t.outcome_instrument, t.tag, t.merchant, t.payee,
            t.comment,
            m.title as merchant_title,
            tag.title as tag_title
        FROM transactions t
        LEFT JOIN merchants m ON m.id = t.merchant
        LEFT JOIN tags tag ON tag.id = json_extract(t.tag, '$[0]')
        WHERE t.deleted = 0
          AND (t.hold IS NULL OR t.hold = 0)
          AND NOT (t.income > 0 AND t.outcome > 0)
          AND t.outcome > 0
          AND t.income = 0
          AND t.date >= ? AND t.date <= ?
    """
    params: list[Any] = [start_date, end_date]

    # Add category filter (with children)
    if category_id:
        category_ids = [category_id]
        children = conn.execute(
            "SELECT id FROM tags WHERE parent = ?", (category_id,)
        ).fetchall()
        category_ids.extend(row["id"] for row in children)

        placeholders = ",".join("?" * len(category_ids))
        query += f" AND json_extract(t.tag, '$[0]') IN ({placeholders})"
        params.extend(category_ids)

    rows = conn.execute(query, params).fetchall()

    outliers = []
    duplicates = []

    # Detect amount outliers by category
    category_stats = {}
    for row in rows:
        tag_json = row["tag"]
        if tag_json:
            try:
                tags = json.loads(tag_json)
                category = tags[0] if tags else None
            except:
                category = None
        else:
            category = None

        if category not in category_stats:
            category_stats[category] = []
        amount = row["outcome"]
        instrument_id = row["outcome_instrument"]
        if instrument_id and instrument_id != user_currency_id:
            source_rate = db.get_instrument_rate(instrument_id)
            amount = amount * source_rate / user_rate if user_rate else amount
        category_stats[category].append(amount)

    # Calculate stats for each category
    for category, amounts in category_stats.items():
        if len(amounts) < 3:
            continue  # Need at least 3 for meaningful stats

        mean = sum(amounts) / len(amounts)
        variance = sum((x - mean) ** 2 for x in amounts) / len(amounts)
        stddev = variance ** 0.5

        if stddev == 0:
            continue

        # Find outliers
        for row in rows:
            tag_json = row["tag"]
            if tag_json:
                try:
                    tags = json.loads(tag_json)
                    row_category = tags[0] if tags else None
                except:
                    row_category = None
            else:
                row_category = None

            if row_category != category:
                continue

            amount = row["outcome"]
            instrument_id = row["outcome_instrument"]
            if instrument_id and instrument_id != user_currency_id:
                source_rate = db.get_instrument_rate(instrument_id)
                amount = amount * source_rate / user_rate if user_rate else amount

            z_score = abs(amount - mean) / stddev
            if z_score > z_threshold:
                outliers.append({
                    "transaction_id": row["id"],
                    "date": row["date"],
                    "amount": round(amount, 2),
                    "category": row["tag_title"] or "Uncategorized",
                    "payee": row["merchant_title"] or row["payee"],
                    "z_score": round(z_score, 2),
                    "mean": round(mean, 2),
                    "stddev": round(stddev, 2),
                    "severity": "high" if z_score >= 3.0 else ("medium" if z_score >= 2.0 else "low"),
                })

    # Detect possible duplicates (same amount, date ±1 day, same payee)
    checked_pairs = set()
    for i, row1 in enumerate(rows):
        for row2 in rows[i+1:]:
            pair_key = tuple(sorted([row1["id"], row2["id"]]))
            if pair_key in checked_pairs:
                continue
            checked_pairs.add(pair_key)

            # Convert amounts to user currency for comparison
            amount1 = row1["outcome"]
            instr1 = row1["outcome_instrument"]
            if instr1 and instr1 != user_currency_id:
                source_rate = db.get_instrument_rate(instr1)
                amount1 = amount1 * source_rate / user_rate if user_rate else amount1

            amount2 = row2["outcome"]
            instr2 = row2["outcome_instrument"]
            if instr2 and instr2 != user_currency_id:
                source_rate = db.get_instrument_rate(instr2)
                amount2 = amount2 * source_rate / user_rate if user_rate else amount2

            # Check if amounts are close
            if abs(amount1 - amount2) < 0.01:
                # Check if dates are close
                date1 = date.fromisoformat(row1["date"])
                date2 = date.fromisoformat(row2["date"])
                if abs((date1 - date2).days) <= 1:
                    # Check if payees match
                    payee1 = row1["merchant_title"] or row1["payee"] or ""
                    payee2 = row2["merchant_title"] or row2["payee"] or ""
                    if payee1 and payee2 and payee1 == payee2:
                        duplicates.append({
                            "transactions": [row1["id"], row2["id"]],
                            "date": row1["date"],
                            "amount": round(amount1, 2),
                            "payee": payee1,
                            "severity": "medium",
                        })

    return {
        "period": {"start": start_date, "end": end_date},
        "currency": currency_code,
        "summary": {
            "outliers_count": len(outliers),
            "duplicates_count": len(duplicates),
            "total_transactions_analyzed": len(rows),
        },
        "outliers": outliers[:15],
        "possible_duplicates": duplicates[:15],
    }


# What a transaction adds to (credited) or takes from (debited) one account, in
# that account's currency. A side's instrument is the account's own, except on a
# debt account: its rows carry the currency of the other account (see Transaction
# in the API doc). Those are converted at the current rates, which is also how
# the debt account's balance is kept.
_ACCOUNT_CREDITED_SQL = """
    CASE WHEN t.income_account = :account THEN t.income * CASE
        WHEN t.income_instrument = :instrument THEN 1.0
        ELSE COALESCE(ii.rate / :rate, 1.0) END
    ELSE 0 END"""
_ACCOUNT_DEBITED_SQL = """
    CASE WHEN t.outcome_account = :account THEN t.outcome * CASE
        WHEN t.outcome_instrument = :instrument THEN 1.0
        ELSE COALESCE(oi.rate / :rate, 1.0) END
    ELSE 0 END"""


def get_account_flow(
    db: Database,
    account_id: str,
    period: str = "this_month",
    start_date: str | None = None,
    end_date: str | None = None,
) -> dict[str, Any]:
    """Get cash flow for a specific account.

    T14: "What happened on my card?", "Cash flow details"

    All amounts are in the account's own currency. net_change covers income,
    outcome and transfers both ways, so opening_balance + net_change =
    closing_balance.

    Args:
        db: Database instance.
        account_id: Account ID (UUID).
        period: Time period ("this_month", "last_month", "last_30_days", "YYYY-MM").
        start_date: Optional explicit start date (ISO). Overrides period.
        end_date: Optional explicit end date (ISO). Used with start_date.

    Returns:
        Dictionary with account flow breakdown.
    """
    conn = db.connect()
    start_date, end_date = get_period_dates(period, start_date=start_date, end_date=end_date)

    # Get account info. Every amount below is in this account's own currency:
    # it is one account, so nothing is converted to the user's currency.
    account_row = conn.execute("""
        SELECT a.title, a.type, a.balance, a.instrument,
               i.short_title AS currency, i.rate
        FROM accounts a
        LEFT JOIN instruments i ON i.id = a.instrument
        WHERE a.id = ?
    """, (account_id,)).fetchone()

    if not account_row:
        raise ValueError(f"Account {account_id} not found")

    account_title = account_row["title"]
    account_type = account_row["type"]
    account_currency = account_row["currency"]
    current_balance = account_row["balance"] or 0
    account_params = {
        "account": account_id,
        "instrument": account_row["instrument"],
        "rate": account_row["rate"],
    }

    # Query all transactions involving this account
    rows = conn.execute(f"""
        SELECT
            t.id, t.date, t.income, t.outcome, t.hold, t.comment,
            t.income_account, t.outcome_account,
            t.income_instrument, t.outcome_instrument,
            t.tag, t.merchant, t.payee,
            {_ACCOUNT_CREDITED_SQL} AS credited,
            {_ACCOUNT_DEBITED_SQL} AS debited,
            m.title as merchant_title,
            tag.title as tag_title,
            ia.title as income_account_title,
            oa.title as outcome_account_title,
            ii.short_title as income_currency,
            oi.short_title as outcome_currency
        FROM transactions t
        LEFT JOIN merchants m ON m.id = t.merchant
        LEFT JOIN tags tag ON tag.id = json_extract(t.tag, '$[0]')
        LEFT JOIN accounts ia ON ia.id = t.income_account
        LEFT JOIN accounts oa ON oa.id = t.outcome_account
        LEFT JOIN instruments ii ON ii.id = t.income_instrument
        LEFT JOIN instruments oi ON oi.id = t.outcome_instrument
        WHERE t.deleted = 0
          AND t.date >= :start AND t.date <= :end
          AND (t.income_account = :account OR t.outcome_account = :account)
        ORDER BY t.date DESC
    """, {**account_params, "start": start_date, "end": end_date}).fetchall()

    # Categorize transactions
    totals = {"income": 0.0, "outcome": 0.0, "transfer_in": 0.0, "transfer_out": 0.0}
    holds_excluded = 0
    by_category_map = {}
    transactions = []

    for row in rows:
        income = row["income"] or 0
        outcome = row["outcome"] or 0
        income_account = row["income_account"]
        outcome_account = row["outcome_account"]

        # Determine transaction type based on income/outcome values
        # In ZenMoney, for simple expenses/incomes, both accounts are the same
        if income > 0 and outcome == 0:
            # Pure income
            if income_account == account_id:
                tx_type = "income"
            else:
                continue
        elif income == 0 and outcome > 0:
            # Pure expense
            if outcome_account == account_id:
                tx_type = "outcome"
            else:
                continue
        elif income > 0 and outcome > 0:
            # Transfer or exchange
            if income_account == account_id and outcome_account != account_id:
                tx_type = "transfer_in"
            elif outcome_account == account_id and income_account != account_id:
                tx_type = "transfer_out"
            else:
                continue
        else:
            continue

        # The account's side of the transaction, in the account's currency
        side = "income" if tx_type in ["income", "transfer_in"] else "outcome"
        amount = row["credited"] if side == "income" else row["debited"]

        # Totals and balances are built from the rows accounts.balance itself
        # consists of: not deleted and not on hold (checked against real balances -
        # a pending hold is not in the balance yet). Holds stay in the list,
        # flagged, so that pending card payments remain visible.
        on_hold = bool(row["hold"])
        if on_hold:
            holds_excluded += 1
        else:
            totals[tx_type] += amount

        # Category aggregation (only for income/outcome, not transfers)
        if tx_type in ["income", "outcome"] and not on_hold:
            category = row["tag_title"] or "Uncategorized"
            if category not in by_category_map:
                by_category_map[category] = {"type": tx_type, "total": 0.0, "count": 0}
            by_category_map[category]["total"] += amount
            by_category_map[category]["count"] += 1

        # Transaction list
        transaction = {
            "id": row["id"],
            "date": row["date"],
            "type": tx_type,
            "amount": round(amount, 2),
            "category": row["tag_title"],
            "payee": row["merchant_title"] or row["payee"],
            "comment": row["comment"],
            "counterparty": (
                row["outcome_account_title"] if tx_type in ["transfer_in", "income"]
                else row["income_account_title"]
            ),
            "hold": on_hold,
        }
        if row[f"{side}_instrument"] != account_row["instrument"]:
            # Converted above: keep what the transaction itself says
            transaction["original_amount"] = round(row[side], 2)
            transaction["original_currency"] = row[f"{side}_currency"]
        transactions.append(transaction)

    # Net change covers every movement of the account, transfers included:
    # only then does it match how the balance moved. The figures are rounded
    # before they are combined, so that the reported ones add up to the cent.
    totals = {kind: round(total, 2) for kind, total in totals.items()}
    net_change = round(
        totals["income"] + totals["transfer_in"] - totals["outcome"] - totals["transfer_out"], 2
    )

    # The cache keeps only today's balance, so the balance at the end of the
    # period is today's balance with every later movement rolled back.
    moved_after_period = conn.execute(f"""
        SELECT COALESCE(SUM({_ACCOUNT_CREDITED_SQL} - {_ACCOUNT_DEBITED_SQL}), 0) AS total
        FROM transactions t
        LEFT JOIN instruments ii ON ii.id = t.income_instrument
        LEFT JOIN instruments oi ON oi.id = t.outcome_instrument
        WHERE t.deleted = 0
          AND (t.hold IS NULL OR t.hold = 0)
          AND t.date > :end
          AND (t.income_account = :account OR t.outcome_account = :account)
    """, {**account_params, "end": end_date}).fetchone()["total"]
    closing_balance = round(current_balance - moved_after_period, 2)
    opening_balance = round(closing_balance - net_change, 2)

    # Format by_category
    by_category = [
        {
            "category": cat,
            "type": stats["type"],
            "total": round(stats["total"], 2),
            "count": stats["count"],
        }
        for cat, stats in by_category_map.items()
    ]
    by_category.sort(key=lambda x: x["total"], reverse=True)

    returned = transactions[:50]  # Limit to 50; the summary covers all of them

    return {
        "account": {
            "id": account_id,
            "title": account_title,
            "type": account_type,
            "currency": account_currency,
            "balance": round(current_balance, 2),
        },
        "period": {"start": start_date, "end": end_date},
        "summary": {
            "currency": account_currency,
            "income": totals["income"],
            "outcome": totals["outcome"],
            "transfers_in": totals["transfer_in"],
            "transfers_out": totals["transfer_out"],
            "net_change": net_change,
            "opening_balance": opening_balance,
            "closing_balance": closing_balance,
            "holds_excluded": holds_excluded,
            "by_category": by_category,
        },
        "total_count": len(transactions),
        "returned_count": len(returned),
        "transactions": returned,
    }


async def suggest_category(
    payee: str,
    token: str,
    db: Database,
) -> dict[str, Any]:
    """Suggest category for a payee using ZenMoney API.

    T15: "Suggest category for McDonalds", "How to classify this transaction?"

    Args:
        payee: Payee/merchant name.
        token: ZenMoney OAuth token.
        db: Database instance for enrichment.

    Returns:
        Dictionary with suggestions.
    """
    # Make API request
    url = "https://api.zenmoney.ru/v8/suggest/"

    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(
                url,
                json={"payee": payee},
                headers={
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json",
                },
                timeout=10.0,
            )
        except Exception as e:
            return {
                "error": f"HTTP error: {e}",
                "original_payee": payee,
            }

    if response.status_code != 200:
        return {
            "error": f"API returned status {response.status_code}",
            "original_payee": payee,
        }

    try:
        data = response.json()
    except ValueError:
        return {
            "error": "Invalid JSON response",
            "original_payee": payee,
        }

    # Enrich suggested tags with titles from local cache
    suggested_tags = data.get("tag", [])
    if isinstance(suggested_tags, str):
        suggested_tags = [suggested_tags]

    conn = db.connect()
    tag_titles = []

    if suggested_tags:
        placeholders = ",".join("?" * len(suggested_tags))
        tag_rows = conn.execute(
            f"SELECT id, title FROM tags WHERE id IN ({placeholders})",
            suggested_tags
        ).fetchall()
        tag_map = {row["id"]: row["title"] for row in tag_rows}

        for tag_id in suggested_tags:
            tag_titles.append({
                "tag_id": tag_id,
                "name": tag_map.get(tag_id, tag_id),
            })

    # The API names the merchant by id only; the title comes from the local cache
    merchant_id = data.get("merchant")
    merchant_title = None
    if merchant_id:
        merchant_row = conn.execute(
            "SELECT title FROM merchants WHERE id = ?", (merchant_id,)
        ).fetchone()
        merchant_title = merchant_row["title"] if merchant_row else None

    return {
        "original_payee": payee,
        "normalized_payee": data.get("payee", payee),
        "suggested_merchant": merchant_title,
        "suggested_merchant_id": merchant_id,
        "suggested_categories": tag_titles,
    }


_SEARCH_MAX_LIMIT = 200


def _casefold(value: Any) -> Any:
    """Case folding for any script, for use in SQL (SQLite's LOWER and LIKE fold ASCII only)."""
    return value.casefold() if isinstance(value, str) else value


def search_transactions(
    db: Database,
    period: str | None = None,
    category_id: str | None = None,
    account_id: str | None = None,
    merchant_id: str | None = None,
    payee_search: str | None = None,
    min_amount: float | None = None,
    max_amount: float | None = None,
    tx_type: str | None = None,
    limit: int = 50,
    start_date: str | None = None,
    end_date: str | None = None,
) -> dict[str, Any]:
    """Search transactions with filters.

    T13: "Show recent spending", "Search transactions"

    Args:
        db: Database instance.
        period: Optional time period filter. Without it and without dates the
            whole history is searched.
        category_id: Filter by category (includes children).
        account_id: Filter by account.
        merchant_id: Filter by merchant.
        payee_search: Search in payee, comment, merchant title.
        min_amount: Minimum transaction amount.
        max_amount: Maximum transaction amount.
        tx_type: Transaction type ("income", "outcome", "transfer").
        limit: Maximum results to return (1 to 200).
        start_date: Optional explicit start date (ISO). Overrides period.
        end_date: Optional explicit end date (ISO). Without start_date (and
            without period) everything up to this date is searched.

    Returns:
        Dictionary with matching transactions and the period searched.

    Raises:
        ValueError: If limit is out of range or the period/dates are invalid.
    """
    # SQLite reads a negative LIMIT as "no limit", so the range is enforced here
    if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= _SEARCH_MAX_LIMIT:
        raise ValueError(f"Invalid limit {limit!r}: expected an integer from 1 to {_SEARCH_MAX_LIMIT}")

    conn = db.connect()

    # Get user currency for display
    user_currency_id = db.get_user_currency()

    # Build query
    query = """
        SELECT
            t.id, t.date, t.income, t.outcome, t.hold, t.deleted,
            t.income_instrument, t.outcome_instrument,
            t.income_account, t.outcome_account,
            t.tag, t.merchant, t.payee, t.original_payee, t.comment,
            m.title as merchant_title,
            ia.title as income_account_title,
            oa.title as outcome_account_title,
            ii.short_title as income_currency,
            oi.short_title as outcome_currency
        FROM transactions t
        LEFT JOIN merchants m ON m.id = t.merchant
        LEFT JOIN accounts ia ON ia.id = t.income_account
        LEFT JOIN accounts oa ON oa.id = t.outcome_account
        LEFT JOIN instruments ii ON ii.id = t.income_instrument
        LEFT JOIN instruments oi ON oi.id = t.outcome_instrument
        WHERE t.deleted = 0
    """
    params: list[Any] = []

    # Period filter. Unlike the reports, a search may be open-ended: with no
    # period at all it covers the whole history, with end_date alone everything
    # up to that date. The output says which dates were actually searched.
    period_info: dict[str, Any]
    if start_date or period:
        sd, ed = get_period_dates(period or "this_month", start_date=start_date, end_date=end_date)
        query += " AND t.date BETWEEN ? AND ?"
        params.extend([sd, ed])
        period_info = {"start": sd, "end": ed}
    elif end_date:
        ed = parse_iso_date(end_date, "end_date").isoformat()
        query += " AND t.date <= ?"
        params.append(ed)
        period_info = {
            "start": None,
            "end": ed,
            "note": "No start_date given: everything up to end_date was searched",
        }
    else:
        period_info = {
            "start": None,
            "end": None,
            "note": "No period or dates given: the whole history was searched",
        }

    # Category filter (with children)
    if category_id:
        category_ids = [category_id]
        children = conn.execute(
            "SELECT id FROM tags WHERE parent = ?", (category_id,)
        ).fetchall()
        category_ids.extend(row["id"] for row in children)

        placeholders = ",".join("?" * len(category_ids))
        query += f" AND json_extract(t.tag, '$[0]') IN ({placeholders})"
        params.extend(category_ids)

    # Account filter
    if account_id:
        query += " AND (t.income_account = ? OR t.outcome_account = ?)"
        params.extend([account_id, account_id])

    # Merchant filter
    if merchant_id:
        query += " AND t.merchant = ?"
        params.append(merchant_id)

    # Payee search (substring of payee, original_payee, comment, merchant.title).
    # Case is folded in Python: SQLite on its own would miss "кофе" in "Кофе".
    # Database caches one connection, so registering the function on every
    # call is cheap and also covers a connection re-opened after close().
    # instr() rather than LIKE: the query is plain text, "%" and "_" included.
    if payee_search:
        conn.create_function("zm_casefold", 1, _casefold, deterministic=True)
        query += """ AND (
            instr(zm_casefold(t.payee), ?) > 0 OR
            instr(zm_casefold(t.original_payee), ?) > 0 OR
            instr(zm_casefold(t.comment), ?) > 0 OR
            instr(zm_casefold(m.title), ?) > 0
        )"""
        params.extend([_casefold(payee_search)] * 4)

    # Amount filters — compare in the user's currency so thresholds are meaningful
    # across multi-currency accounts. Transaction amounts are stored in each
    # account's own currency; convert via amount * instrument.rate / user_rate.
    # COALESCE(rate, user_rate) makes a missing rate fall back to the raw amount.
    user_rate = db.get_instrument_rate(user_currency_id) if user_currency_id else 0
    if min_amount is not None:
        if user_rate:
            query += (
                " AND ( (t.outcome > 0 AND t.outcome * COALESCE(oi.rate, ?) / ? >= ?)"
                " OR (t.income > 0 AND t.income * COALESCE(ii.rate, ?) / ? >= ?) )"
            )
            params.extend([user_rate, user_rate, min_amount, user_rate, user_rate, min_amount])
        else:
            query += " AND (t.outcome >= ? OR t.income >= ?)"
            params.extend([min_amount, min_amount])

    if max_amount is not None:
        if user_rate:
            query += (
                " AND ( (t.outcome > 0 AND t.outcome * COALESCE(oi.rate, ?) / ? <= ?)"
                " OR (t.income > 0 AND t.income * COALESCE(ii.rate, ?) / ? <= ?)"
                " OR (t.outcome = 0 AND t.income = 0) )"
            )
            params.extend([user_rate, user_rate, max_amount, user_rate, user_rate, max_amount])
        else:
            query += " AND (t.outcome <= ? OR t.income <= ? OR (t.outcome = 0 AND t.income = 0))"
            params.extend([max_amount, max_amount])

    # Type filter
    if tx_type == "income":
        query += " AND t.income > 0 AND t.outcome = 0"
    elif tx_type == "outcome":
        query += " AND t.outcome > 0 AND t.income = 0"
    elif tx_type == "transfer":
        query += " AND t.income > 0 AND t.outcome > 0"

    # Count total before limit
    count_query = f"SELECT COUNT(*) as total FROM ({query})"
    total_count = conn.execute(count_query, params).fetchone()["total"]

    # Add ordering and limit
    query += " ORDER BY t.date DESC, t.changed DESC LIMIT ?"
    params.append(limit)

    rows = conn.execute(query, params).fetchall()

    # Get tag titles
    tag_ids = set()
    for row in rows:
        if row["tag"]:
            try:
                tags = json.loads(row["tag"])
                tag_ids.update(tags)
            except json.JSONDecodeError:
                pass

    tag_titles = {}
    if tag_ids:
        placeholders = ",".join("?" * len(tag_ids))
        tag_rows = conn.execute(
            f"SELECT id, title FROM tags WHERE id IN ({placeholders})",
            list(tag_ids)
        ).fetchall()
        tag_titles = {tr["id"]: tr["title"] for tr in tag_rows}

    # Format results
    transactions = []
    for row in rows:
        income = row["income"] or 0
        outcome = row["outcome"] or 0

        # Determine type
        if income > 0 and outcome == 0:
            tx_type_str = "income"
            amount = income
            currency = row["income_currency"]
            account = row["income_account_title"]
        elif outcome > 0 and income == 0:
            tx_type_str = "outcome"
            amount = outcome
            currency = row["outcome_currency"]
            account = row["outcome_account_title"]
        else:
            tx_type_str = "transfer"
            amount = outcome  # Show outcome side
            currency = row["outcome_currency"]
            account = f"{row['outcome_account_title']} → {row['income_account_title']}"

        # Get category name
        category = None
        if row["tag"]:
            try:
                tags = json.loads(row["tag"])
                if tags:
                    category = tag_titles.get(tags[0], tags[0])
            except json.JSONDecodeError:
                pass

        # Get payee/merchant
        payee = row["merchant_title"] or row["payee"] or row["comment"]

        transaction = {
            "id": row["id"],
            "date": row["date"],
            "type": tx_type_str,
            "amount": amount,
            "currency": currency,
            "account": account,
            "category": category,
            "payee": payee,
            "comment": row["comment"] if row["comment"] != payee else None,
            "hold": bool(row["hold"]),
        }
        if tx_type_str == "transfer":
            # "amount" above is the outgoing side only; an exchange arrives as a
            # different amount in a different currency, so spell out both sides
            transaction["from"] = {
                "account": row["outcome_account_title"],
                "amount": outcome,
                "currency": row["outcome_currency"],
            }
            transaction["to"] = {
                "account": row["income_account_title"],
                "amount": income,
                "currency": row["income_currency"],
            }
        transactions.append(transaction)

    return {
        "period": period_info,
        "transactions": transactions,
        "returned_count": len(transactions),
        "total_matching": total_count,
    }


# ============================================================================
# Resources
# ============================================================================

def get_accounts_resource(db: Database) -> dict[str, Any]:
    """R1: Get accounts list for LLM context."""
    conn = db.connect()

    user_currency_id = db.get_user_currency()
    currency_row = conn.execute(
        "SELECT short_title FROM instruments WHERE id = ?",
        (user_currency_id,)
    ).fetchone()
    user_currency = currency_row["short_title"] if currency_row else "RUB"

    rows = conn.execute("""
        SELECT a.id, a.title, a.type, a.balance, a.credit_limit,
               a.in_balance, a.savings, a.archive,
               i.short_title as currency, i.symbol as currency_symbol
        FROM accounts a
        LEFT JOIN instruments i ON i.id = a.instrument
        WHERE a.archive = 0
        ORDER BY a.in_balance DESC, a.balance DESC
    """).fetchall()

    in_balance_total = 0.0  # what the ZenMoney app shows as the balance
    off_balance_total = 0.0
    accounts = []
    for row in rows:
        balance = row["balance"] or 0
        instrument_row = conn.execute(
            "SELECT id FROM instruments WHERE short_title = ?",
            (row["currency"],)
        ).fetchone()
        if instrument_row:
            converted = convert_to_user_currency(balance, instrument_row["id"], db, user_currency_id)
            if row["in_balance"]:
                in_balance_total += converted
            else:
                off_balance_total += converted

        accounts.append({
            "id": row["id"],
            "title": row["title"],
            "type": row["type"],
            "balance": balance,
            "currency": row["currency"],
            "currency_symbol": row["currency_symbol"],
            "credit_limit": row["credit_limit"],
            "in_balance": bool(row["in_balance"]),
            "savings": bool(row["savings"]),
        })

    return {
        "accounts": accounts,
        "in_balance_total": round(in_balance_total, 2),
        "off_balance_total": round(off_balance_total, 2),
        "user_currency": user_currency,
    }


def get_categories_resource(db: Database) -> dict[str, Any]:
    """R2: Get categories tree for LLM context."""
    conn = db.connect()

    rows = conn.execute("""
        SELECT id, title, parent, show_income, show_outcome, budget_outcome
        FROM tags
        ORDER BY title
    """).fetchall()

    # Build tree
    tags_by_id = {row["id"]: dict(row) for row in rows}

    expense_categories = []
    income_categories = []

    for tag_id, tag in tags_by_id.items():
        if tag["parent"]:
            continue  # Skip children, they'll be added to parents

        children = [
            {
                "id": t["id"],
                "title": t["title"],
                "budget_tracked": bool(t["budget_outcome"]),
            }
            for t in tags_by_id.values()
            if t["parent"] == tag_id
        ]

        cat_info = {
            "id": tag_id,
            "title": tag["title"],
            "parent": None,
            "budget_tracked": bool(tag["budget_outcome"]),
            "children": children,
        }

        if tag["show_outcome"]:
            expense_categories.append(cat_info)
        if tag["show_income"]:
            income_categories.append(cat_info)

    return {
        "expense_categories": expense_categories,
        "income_categories": income_categories,
    }


def get_current_budgets_resource(db: Database) -> dict[str, Any]:
    """R3: Get current month budgets for LLM context."""
    conn = db.connect()

    # Resolve the user's current BUDGET month (respects month_start_day), matching
    # check_budget_health. Using the calendar month would return the wrong budget
    # near the month boundary when month_start_day != 1.
    today = date.today()
    month_start_day = db.get_user_month_start_day()
    target_year, target_month = _current_budget_month(month_start_day, today)
    budget_date = date(target_year, target_month, 1).isoformat()

    rows = conn.execute("""
        SELECT b.tag, b.outcome, b.outcome_lock, b.income, b.income_lock,
               t.title as tag_title
        FROM budgets b
        LEFT JOIN tags t ON t.id = b.tag
        WHERE b.date = ?
        ORDER BY b.outcome DESC
    """, (budget_date,)).fetchall()

    budgets = []
    for row in rows:
        tag_id = row["tag"]
        tag_title = row["tag_title"]

        # Special case for total budget
        if tag_id == "00000000-0000-0000-0000-000000000000":
            tag_title = "Monthly total"
        elif not tag_title:
            tag_title = "Uncategorized"

        budgets.append({
            "tag_id": tag_id,
            "tag_title": tag_title,
            "planned_outcome": row["outcome"],
            "outcome_locked": bool(row["outcome_lock"]),
            "planned_income": row["income"],
            "income_locked": bool(row["income_lock"]),
        })

    return {
        "month": f"{target_year:04d}-{target_month:02d}",
        "budgets": budgets,
    }


def get_merchants_resource(db: Database) -> dict[str, Any]:
    """R4: Get merchants list for LLM context."""
    conn = db.connect()

    rows = conn.execute("""
        SELECT id, title
        FROM merchants
        ORDER BY title
    """).fetchall()

    merchants = []
    for row in rows:
        merchants.append({
            "id": row["id"],
            "title": row["title"],
        })

    return {
        "merchants": merchants,
        "total": len(merchants),
    }


def get_instruments_resource(db: Database) -> dict[str, Any]:
    """R5: Get instruments (currencies) with exchange rates."""
    conn = db.connect()

    rows = conn.execute("""
        SELECT id, title, short_title, symbol, rate
        FROM instruments
        ORDER BY id
    """).fetchall()

    instruments = []
    for row in rows:
        instruments.append({
            "id": row["id"],
            "title": row["title"],
            "code": row["short_title"],
            "symbol": row["symbol"],
            "rate": row["rate"],
        })

    return {
        "instruments": instruments,
    }


_MAX_CONVERT_AMOUNT = 1e15


def _round_keep_small(value: float, decimals: int) -> float:
    """Round to `decimals` places; a value below 1 keeps `decimals` significant digits instead.

    Fixed decimals flatten the rate of a weak currency: 1 IRR is about 0.0000006 USD,
    which rounds to 0.000001 or, in a description, to 0.0.
    """
    if abs(value) >= 1:
        return round(value, decimals)
    return float(f"{value:.{decimals}g}")


def _unit_label(code: str, symbol: str | None) -> str:
    """Unit to name in a rate: the code, or the symbol when it is a scaled form of the code.

    ZenMoney keeps crypto in micro-units: the instrument with code BTC has the
    symbol μBTC, and "1 BTC = 0.09 USD" would be off by a factor of a million.
    """
    if symbol and symbol != code and symbol.endswith(code):
        return symbol
    return code


def convert_currency(
    db: Database,
    amount: float,
    from_currency: str,
    to_currency: str,
) -> dict[str, Any]:
    """T16: Convert amount between currencies using ZenMoney rates.

    Uses real exchange rates from ZenMoney (synced from banks).
    All rates are stored relative to RUB, so cross-rates are calculated as:
    amount_to = amount * from_rate / to_rate

    Amounts are in the instrument's own unit and are never rescaled; title and
    symbol are returned so the unit is visible (crypto is kept in micro-units).

    Args:
        db: Database instance.
        amount: Amount to convert.
        from_currency: Source currency code (e.g. "USD", "EUR", "PLN").
        to_currency: Target currency code.

    Returns:
        Conversion result with rate and converted amount.

    Raises:
        ValueError: If the amount is not a finite number of a sane size, or a
            currency code is blank.
    """
    # NaN and infinity fail the comparison too; a huge amount would overflow into
    # Infinity, which is not valid JSON.
    is_number = isinstance(amount, (int, float)) and not isinstance(amount, bool)
    if not is_number or not abs(amount) <= _MAX_CONVERT_AMOUNT:
        raise ValueError(
            f"Invalid amount {amount!r}: expected a finite number, "
            f"at most {_MAX_CONVERT_AMOUNT:g} in absolute value"
        )
    for field, code in (("from_currency", from_currency), ("to_currency", to_currency)):
        if not isinstance(code, str) or not code.strip():
            raise ValueError(f"Invalid {field} {code!r}: expected a currency code like 'USD'")

    conn = db.connect()

    from_row = conn.execute(
        "SELECT id, title, short_title, symbol, rate FROM instruments WHERE short_title = ?",
        (from_currency.strip().upper(),),
    ).fetchone()

    to_row = conn.execute(
        "SELECT id, title, short_title, symbol, rate FROM instruments WHERE short_title = ?",
        (to_currency.strip().upper(),),
    ).fetchone()

    if not from_row:
        return {"error": f"Currency '{from_currency}' not found"}
    if not to_row:
        return {"error": f"Currency '{to_currency}' not found"}

    from_rate = from_row["rate"]  # cost of 1 unit in RUB
    to_rate = to_row["rate"]      # cost of 1 unit in RUB

    if to_rate == 0:
        return {"error": f"Rate for {to_currency} is 0, conversion not possible"}

    cross_rate = from_rate / to_rate
    converted = _round_keep_small(amount * cross_rate, 2)

    # Also get user currency for context
    user_currency_id = db.get_user_currency()
    user_row = None
    if user_currency_id:
        user_row = conn.execute(
            "SELECT short_title, symbol, rate FROM instruments WHERE id = ?",
            (int(user_currency_id),),
        ).fetchone()

    result: dict[str, Any] = {
        "from": {
            "amount": amount,
            "currency": from_row["short_title"],
            "title": from_row["title"],
            "symbol": from_row["symbol"],
        },
        "to": {
            "amount": converted,
            "currency": to_row["short_title"],
            "title": to_row["title"],
            "symbol": to_row["symbol"],
        },
        "rate": _round_keep_small(cross_rate, 6),
        "inverse_rate": _round_keep_small(1 / cross_rate, 6) if cross_rate != 0 else None,
        "rate_description": (
            f"1 {_unit_label(from_row['short_title'], from_row['symbol'])} = "
            f"{_round_keep_small(cross_rate, 4)} {_unit_label(to_row['short_title'], to_row['symbol'])}"
        ),
    }

    if user_row and user_row["short_title"] not in (from_row["short_title"], to_row["short_title"]):
        user_rate = from_rate / user_row["rate"] if user_row["rate"] != 0 else 0
        result["in_user_currency"] = {
            "amount": _round_keep_small(amount * user_rate, 2),
            "currency": user_row["short_title"],
            "symbol": user_row["symbol"],
        }

    return result


def get_exchange_rates(db: Database, currencies: list[str] | None = None) -> dict[str, Any]:
    """T17: Get current exchange rates and cross-rate table.

    If currencies list is provided, returns cross-rates only for those.
    Otherwise returns rates for currencies used in user's accounts.

    Args:
        db: Database instance.
        currencies: Optional list of currency codes to include.

    Returns:
        Exchange rate table with cross-rates. Requested codes that ZenMoney does
        not know are listed in unknown_currencies.

    Raises:
        ValueError: If currencies is not a list of currency codes.
    """
    conn = db.connect()

    if currencies:
        is_code_list = isinstance(currencies, (list, tuple)) and all(
            isinstance(c, str) and c.strip() for c in currencies
        )
        if not is_code_list:
            raise ValueError(
                f"Invalid currencies {currencies!r}: expected a list of currency codes like ['USD', 'EUR']"
            )
        # Normalize and de-duplicate, keeping the requested order
        codes = list(dict.fromkeys(c.strip().upper() for c in currencies))
    else:
        # Get currencies from user's active accounts
        rows = conn.execute("""
            SELECT DISTINCT i.short_title
            FROM accounts a
            JOIN instruments i ON a.instrument = i.id
            WHERE a.archive = 0
            ORDER BY i.short_title
        """).fetchall()
        codes = [r["short_title"] for r in rows]

    if not codes:
        return {"error": "No currencies to display"}

    # Fetch rates for these currencies
    placeholders = ",".join("?" for _ in codes)
    instruments = conn.execute(
        f"SELECT short_title, symbol, rate, title FROM instruments WHERE short_title IN ({placeholders})",
        codes,
    ).fetchall()

    instr_map = {r["short_title"]: r for r in instruments}
    unknown_codes = [code for code in codes if code not in instr_map]

    # Get user currency
    user_currency_id = db.get_user_currency()
    user_code = None
    user_rate = None
    if user_currency_id:
        user_row = conn.execute(
            "SELECT short_title, rate FROM instruments WHERE id = ?",
            (int(user_currency_id),),
        ).fetchone()
        if user_row:
            user_code = user_row["short_title"]
            user_rate = user_row["rate"]

    # Build cross-rate table
    cross_rates = {}
    for code in codes:
        if code not in instr_map:
            continue
        rate_from = instr_map[code]["rate"]
        rates = {}
        for other_code in codes:
            if other_code == code or other_code not in instr_map:
                continue
            rate_to = instr_map[other_code]["rate"]
            if rate_to != 0:
                rates[other_code] = _round_keep_small(rate_from / rate_to, 6)
        cross_rates[code] = rates

    # Build summary list
    rate_list = []
    for code in sorted(codes):
        if code not in instr_map:
            continue
        r = instr_map[code]
        entry: dict[str, Any] = {
            "currency": code,
            "symbol": r["symbol"],
            "title": r["title"],
            "rate_to_rub": r["rate"],
        }
        # Always given, whether or not the user's currency was requested
        if user_rate and code != user_code:
            entry[f"rate_to_{user_code}"] = _round_keep_small(r["rate"] / user_rate, 6)
        rate_list.append(entry)

    return {
        "user_currency": user_code,
        "currencies": rate_list,
        "cross_rates": cross_rates,
        "unknown_currencies": unknown_codes,
        "rate_source": "cbr",
        "note": "Rates from ZenMoney (Central Bank of Russia, updated on sync). rate_to_rub = cost of 1 unit in RUB.",
    }


def get_sync_status_resource(db: Database) -> dict[str, Any]:
    """R6: Get sync status and cache statistics."""
    conn = db.connect()

    # Get server timestamp and last sync time
    server_timestamp = db.get_meta("server_timestamp") or "0"
    last_sync_time = db.get_meta("last_sync_time")

    # Get cache stats
    cache_stats = {}
    tables = ["transactions", "accounts", "tags", "merchants", "budgets", "reminders", "reminder_markers"]

    for table in tables:
        count = conn.execute(f"SELECT COUNT(*) as cnt FROM {table}").fetchone()["cnt"]
        cache_stats[table] = count

    # Calculate staleness
    if last_sync_time:
        try:
            last_sync = int(last_sync_time)
            current_time = int(datetime.now().timestamp())
            age_seconds = current_time - last_sync

            if age_seconds < 300:  # 5 minutes
                staleness = "fresh"
            elif age_seconds < 3600:  # 1 hour
                staleness = "slightly_stale"
            else:
                staleness = "stale"
        except (ValueError, TypeError):
            staleness = "unknown"
    else:
        staleness = "never_synced"

    # Format last sync time
    if last_sync_time:
        try:
            dt = datetime.fromtimestamp(int(last_sync_time))
            last_sync_formatted = dt.isoformat()
        except (ValueError, TypeError):
            last_sync_formatted = None
    else:
        last_sync_formatted = None

    return {
        "last_server_timestamp": int(server_timestamp) if server_timestamp else 0,
        "last_sync_time": last_sync_formatted,
        "cache_stats": cache_stats,
        "staleness": staleness,
    }
