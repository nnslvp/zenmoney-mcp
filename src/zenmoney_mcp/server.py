"""MCP Server for ZenMoney financial analytics."""

import asyncio
import json
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Any

from mcp.server import Server
from mcp.types import Resource, TextContent, Tool
from pydantic import AnyUrl

from .analytics import (
    PERIOD_FORMATS,
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
    get_sync_status_resource,
    get_upcoming_payments,
    search_transactions,
    suggest_category,
)
from .database import Database
from .sync_engine import SyncEngine, SyncError


# Initialize MCP server
server = Server("zenmoney-mcp")

PERIOD_DESCRIPTION = f"Period: {PERIOD_FORMATS}"

# Global state
_db: Database | None = None
_sync_engine: SyncEngine | None = None


def get_db() -> Database:
    """Get or create database instance."""
    global _db
    if _db is None:
        # Default to user's cache directory
        cache_dir = Path.home() / ".cache" / "zenmoney-mcp"
        cache_dir.mkdir(parents=True, exist_ok=True)
        db_path = cache_dir / "zenmoney.db"

        _db = Database(db_path)
        _db.init_schema()
    return _db


def get_sync_engine() -> SyncEngine:
    """Get or create sync engine instance."""
    global _sync_engine
    if _sync_engine is None:
        token = os.environ.get("ZENMONEY_TOKEN")
        if not token:
            raise ValueError(
                "ZENMONEY_TOKEN environment variable is required. "
                "Get your token at https://zerro.app/token"
            )
        _sync_engine = SyncEngine(get_db(), token)
    return _sync_engine


def init_for_testing(db: Database, token: str = "test_token") -> None:
    """Initialize server with test database and token.

    Args:
        db: Database instance to use.
        token: OAuth token (can be dummy for testing without API).
    """
    global _db, _sync_engine
    _db = db
    _sync_engine = SyncEngine(db, token)


# Tools answer from the local cache; a cache older than this is synced first
DEFAULT_AUTO_SYNC_SECONDS = 600

_sync_lock = asyncio.Lock()


def _auto_sync_seconds() -> int:
    """Max cache age before a request triggers a sync. 0 disables auto-sync."""
    try:
        return int(os.environ.get("ZENMONEY_AUTO_SYNC_SECONDS", DEFAULT_AUTO_SYNC_SECONDS))
    except ValueError:
        return DEFAULT_AUTO_SYNC_SECONDS


def _last_sync_time(db: Database) -> int | None:
    value = db.get_meta("last_sync_time")
    return int(value) if value else None


async def ensure_fresh(db: Database) -> str | None:
    """Sync the cache if it is older than the auto-sync threshold.

    Returns:
        A warning when the sync failed and the answer comes from a stale cache.

    Raises:
        SyncError, ValueError: If the sync failed and there is no cache to fall back on.
    """
    max_age = _auto_sync_seconds()
    if max_age <= 0:
        return None

    async with _sync_lock:
        last_sync = _last_sync_time(db)
        if last_sync is not None and time.time() - last_sync < max_age:
            return None
        try:
            await get_sync_engine().sync()
        except (SyncError, ValueError) as e:
            if last_sync is None:
                raise
            return f"Could not sync with ZenMoney ({e}). Showing cached data."
    return None


def _freshness(db: Database, sync_warning: str | None) -> dict[str, Any]:
    """Fields that tell the reader how current the answer is."""
    last_sync = _last_sync_time(db)
    info: dict[str, Any] = {
        "data_synced_at": datetime.fromtimestamp(last_sync).isoformat() if last_sync else None,
    }
    if sync_warning:
        info["sync_warning"] = sync_warning
    return info


# ============================================================================
# Tools
# ============================================================================

@server.list_tools()
async def list_tools() -> list[Tool]:
    """List available tools."""
    return [
        Tool(
            name="sync_data",
            description="Sync data with ZenMoney. Use to refresh data before analysis.",
            inputSchema={
                "type": "object",
                "properties": {
                    "force_full": {
                        "type": "boolean",
                        "description": "Force full sync (reset cache)",
                        "default": False,
                    }
                },
            },
        ),
        Tool(
            name="get_net_worth",
            description="Get total net worth: sum of all accounts broken down by type (current, savings, loans, debts).",
            inputSchema={
                "type": "object",
                "properties": {},
            },
        ),
        Tool(
            name="get_liquidity",
            description="Get liquid funds: how much cash is available. Answers: 'Can I afford this purchase?', 'How much cash do I have?'",
            inputSchema={
                "type": "object",
                "properties": {
                    "target_amount": {
                        "type": "number",
                        "description": "Target purchase amount to check affordability",
                    },
                },
            },
        ),
        Tool(
            name="analyze_spending",
            description="Analyze spending by category. Answers: 'Where does my money go?', 'What do I spend the most on?'",
            inputSchema={
                "type": "object",
                "properties": {
                    "period": {
                        "type": "string",
                        "description": PERIOD_DESCRIPTION,
                        "default": "this_month",
                    },
                    "start_date": {
                        "type": "string",
                        "description": "Custom start date (ISO, e.g. '2026-01-01'). Overrides period.",
                    },
                    "end_date": {
                        "type": "string",
                        "description": "Custom end date (ISO). If omitted with start_date, defaults to today.",
                    },
                    "category_id": {
                        "type": "string",
                        "description": "Category UUID for drill-down (includes subcategories)",
                    },
                    "top_n": {
                        "type": "integer",
                        "description": "Number of top categories to return",
                        "default": 10,
                    },
                    "include_transfers": {
                        "type": "boolean",
                        "description": "Include transfers between own accounts",
                        "default": False,
                    },
                    "include_holds": {
                        "type": "boolean",
                        "description": "Include hold transactions (pre-authorizations)",
                        "default": False,
                    },
                    "group_by": {
                        "type": "string",
                        "enum": ["category", "merchant"],
                        "description": "Aggregation mode: 'category' (default) or 'merchant'",
                        "default": "category",
                    },
                },
            },
        ),
        Tool(
            name="analyze_income",
            description="Analyze income by category and source. Answers: 'Where does my money come from?', 'How much did I earn?'",
            inputSchema={
                "type": "object",
                "properties": {
                    "period": {
                        "type": "string",
                        "description": PERIOD_DESCRIPTION,
                        "default": "this_month",
                    },
                    "start_date": {
                        "type": "string",
                        "description": "Custom start date (ISO). Overrides period.",
                    },
                    "end_date": {
                        "type": "string",
                        "description": "Custom end date (ISO). If omitted with start_date, defaults to today.",
                    },
                    "top_n": {
                        "type": "integer",
                        "description": "Number of top categories/sources to return",
                        "default": 10,
                    },
                },
            },
        ),
        Tool(
            name="analyze_merchants",
            description="Analyze spending by merchant/store. Answers: 'Where do I spend the most?', 'Top stores'",
            inputSchema={
                "type": "object",
                "properties": {
                    "period": {
                        "type": "string",
                        "description": PERIOD_DESCRIPTION,
                        "default": "this_month",
                    },
                    "start_date": {
                        "type": "string",
                        "description": "Custom start date (ISO). Overrides period.",
                    },
                    "end_date": {
                        "type": "string",
                        "description": "Custom end date (ISO). If omitted with start_date, defaults to today.",
                    },
                    "category_id": {
                        "type": "string",
                        "description": "Category UUID to filter (includes subcategories)",
                    },
                    "top_n": {
                        "type": "integer",
                        "description": "Number of top merchants to return",
                        "default": 10,
                    },
                },
            },
        ),
        Tool(
            name="check_budget_health",
            description="Check budget health: planned vs actual spending. Answers: 'Am I within budget?', 'Where am I overspending?'",
            inputSchema={
                "type": "object",
                "properties": {
                    "month": {
                        "type": "string",
                        "description": "Month in 'YYYY-MM' format. Defaults to current budget period.",
                    },
                },
            },
        ),
        Tool(
            name="get_upcoming_payments",
            description="Get upcoming payments from reminders. Answers: 'What payments are coming up?', 'What bills are due?'",
            inputSchema={
                "type": "object",
                "properties": {
                    "days_ahead": {
                        "type": "integer",
                        "description": "Planning horizon in days",
                        "default": 30,
                    },
                },
            },
        ),
        Tool(
            name="analyze_trends",
            description="Analyze spending/income trends over multiple months. Answers: 'How did my spending change?', 'Am I spending more?'",
            inputSchema={
                "type": "object",
                "properties": {
                    "months": {
                        "type": "integer",
                        "description": "Number of months to analyze",
                        "default": 6,
                    },
                    "category_id": {
                        "type": "string",
                        "description": "Category UUID to filter",
                    },
                    "metric": {
                        "type": "string",
                        "enum": ["outcome", "income", "savings_rate", "net_cashflow"],
                        "description": "Metric: outcome (spending), income, savings_rate (% saved), net_cashflow",
                        "default": "outcome",
                    },
                },
            },
        ),
        Tool(
            name="detect_recurring",
            description="Detect recurring payments (subscriptions, bills). Answers: 'What subscriptions do I have?', 'What can I cancel?'",
            inputSchema={
                "type": "object",
                "properties": {
                    "lookback_months": {
                        "type": "integer",
                        "description": "Analysis depth in months",
                        "default": 3,
                    },
                    "tolerance_pct": {
                        "type": "integer",
                        "description": "Amount variation tolerance in %",
                        "default": 10,
                    },
                },
            },
        ),
        Tool(
            name="get_account_flow",
            description="Get money flow for a specific account, in the account's own currency: income, outcome, transfers in and out, net change, opening and closing balance, and the transactions (first 50). Answers: 'What happened on my card?', 'Cash flow details'",
            inputSchema={
                "type": "object",
                "properties": {
                    "account_id": {
                        "type": "string",
                        "description": "Account UUID",
                    },
                    "period": {
                        "type": "string",
                        "description": PERIOD_DESCRIPTION,
                        "default": "this_month",
                    },
                    "start_date": {
                        "type": "string",
                        "description": "Custom start date (ISO). Overrides period.",
                    },
                    "end_date": {
                        "type": "string",
                        "description": "Custom end date (ISO). If omitted with start_date, defaults to today.",
                    },
                },
                "required": ["account_id"],
            },
        ),
        Tool(
            name="analyze_transfers",
            description="Analyze transfers between accounts and currency exchanges. Answers: 'Where did I transfer money?', 'Currency exchanges'",
            inputSchema={
                "type": "object",
                "properties": {
                    "period": {
                        "type": "string",
                        "description": PERIOD_DESCRIPTION,
                        "default": "this_month",
                    },
                    "top_n": {
                        "type": "integer",
                        "description": "Number of top transfers to return",
                        "default": 15,
                    },
                },
            },
        ),
        Tool(
            name="detect_anomalies",
            description="Detect anomalous spending (outliers, suspicious duplicates). Answers: 'Any unusual spending?', 'Suspicious transactions?'. Severity: z>=3.0 high, z>=2.0 medium, else low. Minimum z_threshold is 1.5.",
            inputSchema={
                "type": "object",
                "properties": {
                    "period": {
                        "type": "string",
                        "description": PERIOD_DESCRIPTION,
                        "default": "this_month",
                    },
                    "start_date": {
                        "type": "string",
                        "description": "Custom start date (ISO). Overrides period.",
                    },
                    "end_date": {
                        "type": "string",
                        "description": "Custom end date (ISO). If omitted with start_date, defaults to today.",
                    },
                    "category_id": {
                        "type": "string",
                        "description": "Category UUID to filter",
                    },
                    "z_threshold": {
                        "type": "number",
                        "description": "Z-score threshold for outlier detection (minimum 1.5)",
                        "default": 2.0,
                    },
                },
            },
        ),
        Tool(
            name="get_debts",
            description="Get debt summary: who owes whom. Answers: 'My debts?', 'Who owes me?'",
            inputSchema={
                "type": "object",
                "properties": {},
            },
        ),
        Tool(
            name="suggest_category",
            description="Suggest a category for a transaction via ZenMoney API. Answers: 'What category for McDonalds?'",
            inputSchema={
                "type": "object",
                "properties": {
                    "payee": {
                        "type": "string",
                        "description": "Payee/merchant name or description",
                    },
                },
                "required": ["payee"],
            },
        ),
        Tool(
            name="convert_currency",
            description="Convert amount between currencies using real ZenMoney exchange rates. Answers: 'How much is 100 USD in EUR?'",
            inputSchema={
                "type": "object",
                "properties": {
                    "amount": {
                        "type": "number",
                        "description": "Amount to convert",
                    },
                    "from_currency": {
                        "type": "string",
                        "description": "Source currency code (USD, EUR, PLN, BYN, RUB, RON, CZK, HUF, GBP, etc.)",
                    },
                    "to_currency": {
                        "type": "string",
                        "description": "Target currency code",
                    },
                },
                "required": ["amount", "from_currency", "to_currency"],
            },
        ),
        Tool(
            name="get_exchange_rates",
            description="Get current exchange rates with cross-rate table. Defaults to currencies from your accounts. Use for any currency rate questions.",
            inputSchema={
                "type": "object",
                "properties": {
                    "currencies": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "List of currency codes (e.g. ['USD', 'EUR', 'PLN']). If omitted, uses currencies from your accounts.",
                    },
                },
            },
        ),
        Tool(
            name="search_transactions",
            description="Search transactions by various criteria: date, category, account, amount, payee. Without period and dates the whole history is searched; the answer states the period used. Transfers and exchanges show both sides.",
            inputSchema={
                "type": "object",
                "properties": {
                    "period": {
                        "type": "string",
                        "description": PERIOD_DESCRIPTION + ". Omit it (and the dates) to search the whole history.",
                    },
                    "start_date": {
                        "type": "string",
                        "description": "Custom start date (ISO). Overrides period.",
                    },
                    "end_date": {
                        "type": "string",
                        "description": "Custom end date (ISO). If omitted with start_date, defaults to today. Alone (no start_date, no period): everything up to this date.",
                    },
                    "category_id": {
                        "type": "string",
                        "description": "Category UUID (includes subcategories)",
                    },
                    "account_id": {
                        "type": "string",
                        "description": "Account UUID",
                    },
                    "merchant_id": {
                        "type": "string",
                        "description": "Merchant UUID",
                    },
                    "payee_search": {
                        "type": "string",
                        "description": "Text to find in payee, comment, or merchant name (case-insensitive, taken literally)",
                    },
                    "min_amount": {
                        "type": "number",
                        "description": "Minimum amount",
                    },
                    "max_amount": {
                        "type": "number",
                        "description": "Maximum amount",
                    },
                    "type": {
                        "type": "string",
                        "enum": ["income", "outcome", "transfer"],
                        "description": "Transaction type",
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum results (1-200)",
                        "default": 50,
                        "minimum": 1,
                        "maximum": 200,
                    },
                },
            },
        ),
    ]


@server.call_tool()
async def call_tool(name: str, arguments: dict[str, Any]) -> list[TextContent]:
    """Handle tool calls."""
    db = get_db()

    if name == "sync_data":
        result = await get_sync_engine().sync(force_full=arguments.get("force_full", False))
    else:
        sync_warning = await ensure_fresh(db)
        result = await _run_tool(name, arguments, db)
        result.update(_freshness(db, sync_warning))

    return [TextContent(type="text", text=json.dumps(result, ensure_ascii=False, indent=2))]


async def _run_tool(name: str, arguments: dict[str, Any], db: Database) -> dict[str, Any]:
    """Run an analytics tool against the local cache."""
    if name == "get_net_worth":
        return get_net_worth(db)

    elif name == "get_liquidity":
        return get_liquidity(
            db,
            target_amount=arguments.get("target_amount"),
        )

    elif name == "analyze_spending":
        return analyze_spending(
            db,
            period=arguments.get("period", "this_month"),
            category_id=arguments.get("category_id"),
            top_n=arguments.get("top_n", 10),
            include_transfers=arguments.get("include_transfers", False),
            include_holds=arguments.get("include_holds", False),
            start_date=arguments.get("start_date"),
            end_date=arguments.get("end_date"),
            group_by=arguments.get("group_by", "category"),
        )

    elif name == "analyze_income":
        return analyze_income(
            db,
            period=arguments.get("period", "this_month"),
            top_n=arguments.get("top_n", 10),
            start_date=arguments.get("start_date"),
            end_date=arguments.get("end_date"),
        )

    elif name == "analyze_merchants":
        return analyze_merchants(
            db,
            period=arguments.get("period", "this_month"),
            category_id=arguments.get("category_id"),
            top_n=arguments.get("top_n", 10),
            start_date=arguments.get("start_date"),
            end_date=arguments.get("end_date"),
        )

    elif name == "check_budget_health":
        return check_budget_health(
            db,
            month=arguments.get("month"),
        )

    elif name == "get_upcoming_payments":
        return get_upcoming_payments(
            db,
            days_ahead=arguments.get("days_ahead", 30),
        )

    elif name == "analyze_trends":
        return analyze_trends(
            db,
            months=arguments.get("months", 6),
            category_id=arguments.get("category_id"),
            metric=arguments.get("metric", "outcome"),
        )

    elif name == "detect_recurring":
        return detect_recurring(
            db,
            lookback_months=arguments.get("lookback_months", 3),
            tolerance_pct=arguments.get("tolerance_pct", 10),
        )

    elif name == "get_account_flow":
        return get_account_flow(
            db,
            account_id=arguments.get("account_id"),
            period=arguments.get("period", "this_month"),
            start_date=arguments.get("start_date"),
            end_date=arguments.get("end_date"),
        )

    elif name == "analyze_transfers":
        return analyze_transfers(
            db,
            period=arguments.get("period", "this_month"),
            top_n=arguments.get("top_n", 15),
        )

    elif name == "detect_anomalies":
        return detect_anomalies(
            db,
            period=arguments.get("period", "this_month"),
            category_id=arguments.get("category_id"),
            z_threshold=arguments.get("z_threshold", 2.0),
            start_date=arguments.get("start_date"),
            end_date=arguments.get("end_date"),
        )

    elif name == "get_debts":
        return get_debts(db)

    elif name == "suggest_category":
        engine = get_sync_engine()
        return await suggest_category(
            payee=arguments.get("payee"),
            token=engine.token,
            db=db,
        )

    elif name == "convert_currency":
        return convert_currency(
            db,
            amount=arguments.get("amount"),
            from_currency=arguments.get("from_currency"),
            to_currency=arguments.get("to_currency"),
        )

    elif name == "get_exchange_rates":
        return get_exchange_rates(
            db,
            currencies=arguments.get("currencies"),
        )

    elif name == "search_transactions":
        return search_transactions(
            db,
            period=arguments.get("period"),
            category_id=arguments.get("category_id"),
            account_id=arguments.get("account_id"),
            merchant_id=arguments.get("merchant_id"),
            payee_search=arguments.get("payee_search"),
            min_amount=arguments.get("min_amount"),
            max_amount=arguments.get("max_amount"),
            tx_type=arguments.get("type"),
            limit=arguments.get("limit", 50),
            start_date=arguments.get("start_date"),
            end_date=arguments.get("end_date"),
        )

    else:
        raise ValueError(f"Unknown tool: {name}")


# ============================================================================
# Resources
# ============================================================================

@server.list_resources()
async def list_resources() -> list[Resource]:
    """List available resources."""
    return [
        Resource(
            uri="zenmoney://accounts",
            name="Accounts",
            description="Active accounts with balances",
            mimeType="application/json",
        ),
        Resource(
            uri="zenmoney://categories",
            name="Categories",
            description="Expense and income category tree",
            mimeType="application/json",
        ),
        Resource(
            uri="zenmoney://budgets/current",
            name="Budgets",
            description="Budget limits for the current month",
            mimeType="application/json",
        ),
        Resource(
            uri="zenmoney://merchants",
            name="Merchants",
            description="Merchant directory",
            mimeType="application/json",
        ),
        Resource(
            uri="zenmoney://instruments",
            name="Currencies",
            description="Currency reference with exchange rates",
            mimeType="application/json",
        ),
        Resource(
            uri="zenmoney://sync-status",
            name="Sync Status",
            description="Sync state and cache statistics",
            mimeType="application/json",
        ),
    ]


@server.read_resource()
async def read_resource(uri: AnyUrl | str) -> str:
    """Read resource content."""
    db = get_db()
    await ensure_fresh(db)
    uri = str(uri)  # the SDK passes an AnyUrl, which never equals a str

    if uri == "zenmoney://accounts":
        result = get_accounts_resource(db)
    elif uri == "zenmoney://categories":
        result = get_categories_resource(db)
    elif uri == "zenmoney://budgets/current":
        result = get_current_budgets_resource(db)
    elif uri == "zenmoney://merchants":
        result = get_merchants_resource(db)
    elif uri == "zenmoney://instruments":
        result = get_instruments_resource(db)
    elif uri == "zenmoney://sync-status":
        result = get_sync_status_resource(db)
    else:
        raise ValueError(f"Unknown resource: {uri}")

    return json.dumps(result, ensure_ascii=False, indent=2)


# ============================================================================
# Main
# ============================================================================

def main() -> None:
    """Run the MCP server."""
    import asyncio

    from mcp.server.stdio import stdio_server

    async def run():
        async with stdio_server() as (read_stream, write_stream):
            await server.run(read_stream, write_stream, server.create_initialization_options())

    asyncio.run(run())


if __name__ == "__main__":
    main()
