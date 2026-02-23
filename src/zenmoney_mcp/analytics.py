"""Analytics business logic for ZenMoney MCP tools."""

import json
from datetime import date, datetime, timedelta
from typing import Any

import httpx

from .database import Database
from .utils import convert_to_user_currency


def get_period_dates(period: str) -> tuple[str, str]:
    """Convert period string to start and end dates.

    Args:
        period: One of "this_month", "last_month", "last_30_days", or "YYYY-MM"

    Returns:
        Tuple of (start_date, end_date) as ISO strings.
    """
    today = date.today()

    if period == "this_month":
        start = today.replace(day=1)
        # End of month
        if today.month == 12:
            end = today.replace(year=today.year + 1, month=1, day=1) - timedelta(days=1)
        else:
            end = today.replace(month=today.month + 1, day=1) - timedelta(days=1)
    elif period == "last_month":
        first_of_this_month = today.replace(day=1)
        end = first_of_this_month - timedelta(days=1)
        start = end.replace(day=1)
    elif period == "last_30_days":
        end = today
        start = today - timedelta(days=30)
    else:
        # Assume YYYY-MM format
        try:
            year, month = map(int, period.split("-"))
            start = date(year, month, 1)
            if month == 12:
                end = date(year + 1, 1, 1) - timedelta(days=1)
            else:
                end = date(year, month + 1, 1) - timedelta(days=1)
        except (ValueError, AttributeError):
            # Fallback to this month
            start = today.replace(day=1)
            if today.month == 12:
                end = today.replace(year=today.year + 1, month=1, day=1) - timedelta(days=1)
            else:
                end = today.replace(month=today.month + 1, day=1) - timedelta(days=1)

    return start.isoformat(), end.isoformat()


def get_net_worth(db: Database) -> dict[str, Any]:
    """Calculate total net worth across all accounts.

    T1: "How much money do I have?"

    Returns:
        Dictionary with net_worth breakdown by account type.
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

    # Get all active accounts in balance
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
    out_of_balance = []    # in_balance = 0

    current_total = 0.0
    savings_total = 0.0
    loans_total = 0.0
    debts_total = 0.0

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
        }

        if not row["in_balance"]:
            out_of_balance.append(account_info)
            continue

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
        "out_of_balance": out_of_balance,
    }


def get_liquidity(
    db: Database,
    target_amount: float | None = None,
) -> dict[str, Any]:
    """Calculate liquid funds available for spending.

    T2: "How much liquid cash?", "Can I afford a purchase?"

    Args:
        db: Database instance.
        target_amount: Optional target purchase amount to check affordability.

    Returns:
        Dictionary with liquid funds breakdown and affordability check.
    """
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

    # Get all non-archived accounts
    rows = conn.execute("""
        SELECT a.id, a.title, a.type, a.balance, a.credit_limit,
               a.in_balance, a.savings, a.instrument,
               i.short_title as currency, i.rate
        FROM accounts a
        LEFT JOIN instruments i ON i.id = a.instrument
        WHERE a.archive = 0 AND a.in_balance = 1
        ORDER BY a.type, a.balance DESC
    """).fetchall()

    liquid_own = 0.0  # Own money in liquid accounts
    liquid_with_credit = 0.0  # Including available credit
    savings_accessible = 0.0  # Savings (less liquid)

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
        if acc_type in ("cash", "ccard", "checking", "emoney"):
            # Liquid accounts
            if acc_type == "ccard":
                # Credit card: own funds (if positive) + available credit
                own_funds = max(0, converted_balance)
                available_credit = converted_balance + converted_credit if converted_credit > 0 else converted_balance

                liquid_own += own_funds
                liquid_with_credit += available_credit

                account_info["own_funds"] = round(own_funds, 2)
                account_info["available_credit"] = round(available_credit, 2)
                account_info["credit_limit"] = round(converted_credit, 2)

                if converted_credit > 0:
                    credit_accounts.append(account_info)
                else:
                    liquid_accounts.append(account_info)
            else:
                # Cash, checking, emoney: all balance is liquid
                liquid_own += converted_balance
                liquid_with_credit += converted_balance
                liquid_accounts.append(account_info)

        elif acc_type == "deposit" or is_savings:
            # Savings: accessible but less liquid
            savings_accessible += converted_balance
            savings_accounts.append(account_info)

    total_available = liquid_own + savings_accessible

    result = {
        "liquid_own": round(liquid_own, 2),
        "liquid_with_credit": round(liquid_with_credit, 2),
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

    # Target affordability check
    if target_amount is not None:
        result["target_check"] = {
            "target": target_amount,
            "affordable_from_liquid": liquid_own >= target_amount,
            "affordable_with_credit": liquid_with_credit >= target_amount,
            "affordable_with_savings": total_available >= target_amount,
        }

        # Add recommendation
        if liquid_own >= target_amount:
            result["target_check"]["recommendation"] = "Affordable from own liquid funds"
        elif liquid_with_credit >= target_amount:
            shortfall = target_amount - liquid_own
            result["target_check"]["recommendation"] = f"Need credit ({round(shortfall, 2)} {currency_code} shortfall)"
        elif total_available >= target_amount:
            shortfall = target_amount - liquid_with_credit
            result["target_check"]["recommendation"] = f"Need to use savings ({round(shortfall, 2)} {currency_code} shortfall)"
        else:
            shortfall = target_amount - total_available
            result["target_check"]["recommendation"] = f"Insufficient funds (short {round(shortfall, 2)} {currency_code})"

    return result


def analyze_spending(
    db: Database,
    period: str = "this_month",
    category_id: str | None = None,
    top_n: int = 10,
    include_transfers: bool = False,
    include_holds: bool = False,
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

    Returns:
        Dictionary with spending breakdown by categories.
    """
    conn = db.connect()
    start_date, end_date = get_period_dates(period)

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
            t.hold
        FROM transactions t
        LEFT JOIN accounts a ON a.id = t.outcome_account
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

    # Calculate totals and percentages
    total_outcome = sum(cat["amount"] for cat in category_totals.values())

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

    return {
        "period": {"start": start_date, "end": end_date},
        "total_outcome": round(total_outcome, 2),
        "currency": currency_code,
        "categories": categorized[:top_n],
        "returned_count": min(len(categorized), top_n),
        "total_categories": len(categorized),
        "uncategorized": uncategorized,
        "holds_excluded": holds_excluded if holds_excluded["count"] > 0 else None,
    }


def analyze_income(
    db: Database,
    period: str = "this_month",
    top_n: int = 10,
) -> dict[str, Any]:
    """Analyze income by categories and sources.

    T4: "Where does my money come from?", "How much did I earn?"

    Args:
        db: Database instance.
        period: Time period ("this_month", "last_month", "last_30_days", "YYYY-MM").
        top_n: Number of top categories/sources to return.

    Returns:
        Dictionary with income breakdown by categories and sources.
    """
    conn = db.connect()
    start_date, end_date = get_period_dates(period)

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

    # Determine month
    if month:
        try:
            year, mon = map(int, month.split("-"))
            target_date = date(year, mon, 1)
        except (ValueError, AttributeError):
            target_date = date.today().replace(day=1)
    else:
        target_date = date.today().replace(day=1)

    month_start = target_date.isoformat()

    # Calculate days in month and days elapsed
    today = date.today()
    if target_date.year == today.year and target_date.month == today.month:
        days_elapsed = today.day
    else:
        # For past/future months, assume full month
        days_elapsed = 1

    # Days in month
    if target_date.month == 12:
        next_month = date(target_date.year + 1, 1, 1)
    else:
        next_month = date(target_date.year, target_date.month + 1, 1)
    days_total = (next_month - target_date).days
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
    """, (month_start,)).fetchall()

    if not budget_rows:
        return {
            "month": target_date.strftime("%Y-%m"),
            "days_elapsed": days_elapsed,
            "days_total": days_total,
            "message": "No budgets configured for this month",
            "categories": [],
        }

    # Get month date range for actuals
    month_end = (next_month - timedelta(days=1)).isoformat()

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
        "month": target_date.strftime("%Y-%m"),
        "days_elapsed": days_elapsed,
        "days_total": days_total,
        "currency": currency_code,
        "categories": categories,
    }

    # Add overall if we have it
    if "overall_data" in locals():
        result["overall"] = overall_data

    return result


def analyze_merchants(
    db: Database,
    period: str = "this_month",
    category_id: str | None = None,
    top_n: int = 10,
) -> dict[str, Any]:
    """Analyze spending by merchants/payees.

    T7: "Where do I spend the most?", "Top merchants"

    Args:
        db: Database instance.
        period: Time period ("this_month", "last_month", "last_30_days", "YYYY-MM").
        category_id: Optional category filter (includes children).
        top_n: Number of top merchants to return.

    Returns:
        Dictionary with spending breakdown by merchants.
    """
    conn = db.connect()
    start_date, end_date = get_period_dates(period)

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
        frequency_map = {
            "day": "daily",
            "week": "weekly",
            "month": "monthly",
            "year": "yearly",
        }
        frequency = frequency_map.get(row["interval"], row["interval"])

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

    Returns:
        Dictionary with debts breakdown by counterparty.
    """
    conn = db.connect()

    # Find debt accounts
    debt_accounts = conn.execute(
        "SELECT id, title, balance, instrument FROM accounts WHERE type = 'debt' AND archive = 0"
    ).fetchall()

    if not debt_accounts:
        return {
            "currency": "RUB",
            "summary": {
                "total_owed_to_you": 0.0,
                "total_you_owe": 0.0,
                "net_position": 0.0,
            },
            "by_counterparty": [],
        }

    # Get user currency
    user_currency_id = db.get_user_currency()
    currency_row = conn.execute(
        "SELECT short_title FROM instruments WHERE id = ?",
        (user_currency_id,)
    ).fetchone()
    currency_code = currency_row["short_title"] if currency_row else "RUB"

    counterparties_data = {}

    for debt_acc in debt_accounts:
        account_id = debt_acc["id"]
        account_balance = debt_acc["balance"] or 0

        # Get all transactions for this debt account
        rows = conn.execute("""
            SELECT t.id, t.date, t.income, t.outcome,
                   t.income_account, t.outcome_account,
                   t.merchant, t.payee, t.comment,
                   m.title as merchant_title
            FROM transactions t
            LEFT JOIN merchants m ON m.id = t.merchant
            WHERE t.deleted = 0
              AND (t.income_account = ? OR t.outcome_account = ?)
            ORDER BY t.date DESC
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

            # Determine transaction type and update balance
            if row["income_account"] == account_id:
                # Money came into debt account (I lent money or they returned)
                amount = row["income"]
                tx_type = "lent" if row["outcome"] > 0 else "received"
                counterparties_data[counterparty]["balance"] += amount
            else:
                # Money went out from debt account (They lent me or I returned)
                amount = row["outcome"]
                tx_type = "borrowed" if row["income"] > 0 else "returned"
                counterparties_data[counterparty]["balance"] -= amount

            counterparties_data[counterparty]["history"].append({
                "date": row["date"],
                "amount": round(amount, 2),
                "type": tx_type,
                "comment": row["comment"],
            })

    # Format counterparties
    by_counterparty = []
    for cp_data in counterparties_data.values():
        net_balance = cp_data["balance"]

        if net_balance > 0:
            status = "they_owe_you"
        elif net_balance < 0:
            status = "you_owe_them"
        else:
            status = "settled"

        # Get last activity
        if cp_data["history"]:
            last_activity = cp_data["history"][0]["date"]
        else:
            last_activity = None

        by_counterparty.append({
            "counterparty": cp_data["name"],
            "merchant_id": cp_data["merchant_id"],
            "net_amount": round(net_balance, 2),
            "status": status,
            "last_activity": last_activity,
            "transactions": cp_data["history"][:10],  # Last 10 transactions
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

        if is_currency_exchange and row["income"] > 0:
            transfer_data["effective_rate"] = round(row["outcome"] / row["income"], 4)

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
) -> dict[str, Any]:
    """Detect anomalous transactions.

    T10: "Unusual spending?", "Duplicates?", "Suspicious transactions?"

    Args:
        db: Database instance.
        period: Time period to analyze.
        category_id: Optional category filter.
        z_threshold: Z-score threshold (standard deviations).

    Returns:
        Dictionary with detected anomalies.
    """
    conn = db.connect()
    start_date, end_date = get_period_dates(period)

    # Build query with optional category filter
    query = """
        SELECT
            t.id, t.date, t.outcome, t.tag, t.merchant, t.payee,
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
        category_stats[category].append(row["outcome"])

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

            z_score = abs(row["outcome"] - mean) / stddev
            if z_score > z_threshold:
                outliers.append({
                    "transaction_id": row["id"],
                    "date": row["date"],
                    "amount": round(row["outcome"], 2),
                    "category": row["tag_title"] or "Uncategorized",
                    "payee": row["merchant_title"] or row["payee"],
                    "z_score": round(z_score, 2),
                    "mean": round(mean, 2),
                    "stddev": round(stddev, 2),
                    "severity": "high" if z_score > z_threshold * 1.5 else "medium",
                })

    # Detect possible duplicates (same amount, date ±1 day, same payee)
    checked_pairs = set()
    for i, row1 in enumerate(rows):
        for row2 in rows[i+1:]:
            pair_key = tuple(sorted([row1["id"], row2["id"]]))
            if pair_key in checked_pairs:
                continue
            checked_pairs.add(pair_key)

            # Check if amounts are close
            if abs(row1["outcome"] - row2["outcome"]) < 0.01:
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
                            "amount": round(row1["outcome"], 2),
                            "payee": payee1,
                            "severity": "medium",
                        })

    return {
        "period": {"start": start_date, "end": end_date},
        "summary": {
            "outliers_count": len(outliers),
            "duplicates_count": len(duplicates),
            "total_transactions_analyzed": len(rows),
        },
        "outliers": outliers[:15],
        "possible_duplicates": duplicates[:15],
    }


def get_account_flow(
    db: Database,
    account_id: str,
    period: str,
) -> dict[str, Any]:
    """Get cash flow for a specific account.

    T14: "What happened on my card?", "Cash flow details"

    Args:
        db: Database instance.
        account_id: Account ID (UUID).
        period: Time period ("this_month", "last_month", "last_30_days", "YYYY-MM").

    Returns:
        Dictionary with account flow breakdown.
    """
    conn = db.connect()
    start_date, end_date = get_period_dates(period)

    # Get account info
    account_row = conn.execute(
        "SELECT title, type, balance, instrument FROM accounts WHERE id = ?",
        (account_id,)
    ).fetchone()

    if not account_row:
        raise ValueError(f"Account {account_id} not found")

    account_title = account_row["title"]
    account_type = account_row["type"]
    current_balance = account_row["balance"] or 0

    # Get user currency
    user_currency_id = db.get_user_currency()
    if not user_currency_id:
        user_currency_id = 2  # Default to RUB

    currency_row = conn.execute(
        "SELECT short_title FROM instruments WHERE id = ?",
        (user_currency_id,)
    ).fetchone()
    currency_code = currency_row["short_title"] if currency_row else "RUB"

    # Query all transactions involving this account
    rows = conn.execute("""
        SELECT
            t.id, t.date, t.income, t.outcome, t.comment,
            t.income_account, t.outcome_account,
            t.income_instrument, t.outcome_instrument,
            t.tag, t.merchant, t.payee,
            m.title as merchant_title,
            tag.title as tag_title,
            ia.title as income_account_title,
            oa.title as outcome_account_title
        FROM transactions t
        LEFT JOIN merchants m ON m.id = t.merchant
        LEFT JOIN tags tag ON tag.id = json_extract(t.tag, '$[0]')
        LEFT JOIN accounts ia ON ia.id = t.income_account
        LEFT JOIN accounts oa ON oa.id = t.outcome_account
        WHERE t.deleted = 0
          AND t.date >= ? AND t.date <= ?
          AND (t.income_account = ? OR t.outcome_account = ?)
        ORDER BY t.date DESC
    """, (start_date, end_date, account_id, account_id)).fetchall()

    # Categorize transactions
    income_total = 0.0
    outcome_total = 0.0
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
                amount = income
                income_total += income
            else:
                continue
        elif income == 0 and outcome > 0:
            # Pure expense
            if outcome_account == account_id:
                tx_type = "outcome"
                amount = outcome
                outcome_total += outcome
            else:
                continue
        elif income > 0 and outcome > 0:
            # Transfer or exchange
            if income_account == account_id and outcome_account != account_id:
                tx_type = "transfer_in"
                amount = income
            elif outcome_account == account_id and income_account != account_id:
                tx_type = "transfer_out"
                amount = outcome
            else:
                continue
        else:
            continue

        # Category aggregation (only for income/outcome, not transfers)
        if tx_type in ["income", "outcome"]:
            category = row["tag_title"] or "Uncategorized"
            if category not in by_category_map:
                by_category_map[category] = {"type": tx_type, "total": 0.0, "count": 0}
            by_category_map[category]["total"] += amount
            by_category_map[category]["count"] += 1

        # Transaction list
        transactions.append({
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
        })

    # Calculate net change
    net_change = income_total - outcome_total

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

    return {
        "account": {
            "id": account_id,
            "title": account_title,
            "type": account_type,
            "balance": round(current_balance, 2),
        },
        "period": {"start": start_date, "end": end_date},
        "summary": {
            "total_income": round(income_total, 2),
            "total_outcome": round(outcome_total, 2),
            "net_change": round(net_change, 2),
            "transaction_count": len(transactions),
            "by_category": by_category,
        },
        "transactions": transactions[:50],  # Limit to 50
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

    return {
        "original_payee": payee,
        "normalized_payee": data.get("payee", payee),
        "suggested_merchant_id": data.get("merchant"),
        "suggested_categories": tag_titles,
    }


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
) -> dict[str, Any]:
    """Search transactions with filters.

    T13: "Show recent spending", "Search transactions"

    Args:
        db: Database instance.
        period: Optional time period filter.
        category_id: Filter by category (includes children).
        account_id: Filter by account.
        merchant_id: Filter by merchant.
        payee_search: Search in payee, comment, merchant title.
        min_amount: Minimum transaction amount.
        max_amount: Maximum transaction amount.
        tx_type: Transaction type ("income", "outcome", "transfer").
        limit: Maximum results to return.

    Returns:
        Dictionary with matching transactions.
    """
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

    # Period filter
    if period:
        start_date, end_date = get_period_dates(period)
        query += " AND t.date BETWEEN ? AND ?"
        params.extend([start_date, end_date])

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

    # Payee search (LIKE on payee, original_payee, comment, merchant.title)
    if payee_search:
        search_pattern = f"%{payee_search}%"
        query += """ AND (
            t.payee LIKE ? OR
            t.original_payee LIKE ? OR
            t.comment LIKE ? OR
            m.title LIKE ?
        )"""
        params.extend([search_pattern] * 4)

    # Amount filters
    if min_amount is not None:
        query += " AND (t.outcome >= ? OR t.income >= ?)"
        params.extend([min_amount, min_amount])

    if max_amount is not None:
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

        transactions.append({
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
        })

    return {
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

    total_converted = 0.0
    accounts = []
    for row in rows:
        balance = row["balance"] or 0
        if row["in_balance"]:
            instrument_row = conn.execute(
                "SELECT id FROM instruments WHERE short_title = ?",
                (row["currency"],)
            ).fetchone()
            if instrument_row:
                converted = convert_to_user_currency(balance, instrument_row["id"], db, user_currency_id)
                total_converted += converted

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
        "total_in_user_currency": round(total_converted, 2),
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

    # Current month first day
    today = date.today()
    month_start = today.replace(day=1).isoformat()

    rows = conn.execute("""
        SELECT b.tag, b.outcome, b.outcome_lock, b.income, b.income_lock,
               t.title as tag_title
        FROM budgets b
        LEFT JOIN tags t ON t.id = b.tag
        WHERE b.date = ?
        ORDER BY b.outcome DESC
    """, (month_start,)).fetchall()

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
        "month": today.strftime("%Y-%m"),
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

    Args:
        db: Database instance.
        amount: Amount to convert.
        from_currency: Source currency code (e.g. "USD", "EUR", "PLN").
        to_currency: Target currency code.

    Returns:
        Conversion result with rate and converted amount.
    """
    conn = db.connect()

    from_row = conn.execute(
        "SELECT id, title, short_title, symbol, rate FROM instruments WHERE short_title = ?",
        (from_currency.upper(),),
    ).fetchone()

    to_row = conn.execute(
        "SELECT id, title, short_title, symbol, rate FROM instruments WHERE short_title = ?",
        (to_currency.upper(),),
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
    converted = round(amount * cross_rate, 2)

    # Also get user currency for context
    user_currency_id = db.get_meta("user_currency")
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
            "symbol": from_row["symbol"],
        },
        "to": {
            "amount": converted,
            "currency": to_row["short_title"],
            "symbol": to_row["symbol"],
        },
        "rate": round(cross_rate, 6),
        "inverse_rate": round(1 / cross_rate, 6) if cross_rate != 0 else None,
        "rate_description": f"1 {from_row['short_title']} = {round(cross_rate, 4)} {to_row['short_title']}",
    }

    if user_row and user_row["short_title"] not in (from_row["short_title"], to_row["short_title"]):
        user_rate = from_rate / user_row["rate"] if user_row["rate"] != 0 else 0
        result["in_user_currency"] = {
            "amount": round(amount * user_rate, 2),
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
        Exchange rate table with cross-rates.
    """
    conn = db.connect()

    if currencies:
        codes = [c.upper() for c in currencies]
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

    # Get user currency
    user_currency_id = db.get_meta("user_currency")
    user_code = None
    if user_currency_id:
        user_row = conn.execute(
            "SELECT short_title FROM instruments WHERE id = ?",
            (int(user_currency_id),),
        ).fetchone()
        if user_row:
            user_code = user_row["short_title"]

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
                rates[other_code] = round(rate_from / rate_to, 6)
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
        if user_code and user_code in cross_rates.get(code, {}):
            entry[f"rate_to_{user_code}"] = cross_rates[code][user_code]
        rate_list.append(entry)

    return {
        "user_currency": user_code,
        "currencies": rate_list,
        "cross_rates": cross_rates,
        "note": "Rates from ZenMoney (updated on sync). rate_to_rub = cost of 1 unit in RUB.",
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
