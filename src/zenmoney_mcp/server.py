"""MCP Server for ZenMoney financial analytics."""

import json
import os
from pathlib import Path
from typing import Any

from mcp.server import Server
from mcp.types import Resource, TextContent, Tool

from .analytics import (
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
from .sync_engine import SyncEngine


# Initialize MCP server
server = Server("zenmoney-mcp")

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
                        "description": "Period: 'this_month', 'last_month', 'last_30_days' or 'YYYY-MM'",
                        "default": "this_month",
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
                        "description": "Period: 'this_month', 'last_month', 'last_30_days' or 'YYYY-MM'",
                        "default": "this_month",
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
                        "description": "Period: 'this_month', 'last_month', 'last_30_days' or 'YYYY-MM'",
                        "default": "this_month",
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
                        "description": "Month in 'YYYY-MM' format. Defaults to current month if not specified.",
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
            description="Get money flow for a specific account. Answers: 'What happened on my card?', 'Cash flow details'",
            inputSchema={
                "type": "object",
                "properties": {
                    "account_id": {
                        "type": "string",
                        "description": "Account UUID",
                    },
                    "period": {
                        "type": "string",
                        "description": "Period: 'this_month', 'last_month', 'last_30_days' or 'YYYY-MM'",
                    },
                },
                "required": ["account_id", "period"],
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
                        "description": "Period: 'this_month', 'last_month', 'last_30_days' or 'YYYY-MM'",
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
            description="Detect anomalous spending (outliers, suspicious duplicates). Answers: 'Any unusual spending?', 'Suspicious transactions?'",
            inputSchema={
                "type": "object",
                "properties": {
                    "period": {
                        "type": "string",
                        "description": "Period: 'this_month', 'last_month', 'last_30_days' or 'YYYY-MM'",
                        "default": "this_month",
                    },
                    "category_id": {
                        "type": "string",
                        "description": "Category UUID to filter",
                    },
                    "z_threshold": {
                        "type": "number",
                        "description": "Z-score threshold for outlier detection (standard deviations)",
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
            description="Search transactions by various criteria: date, category, account, amount, payee.",
            inputSchema={
                "type": "object",
                "properties": {
                    "period": {
                        "type": "string",
                        "description": "Period: 'this_month', 'last_month', 'last_30_days' or 'YYYY-MM'",
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
                        "description": "Search by payee, comment, or merchant name",
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
                        "description": "Maximum results",
                        "default": 50,
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
        engine = get_sync_engine()
        force_full = arguments.get("force_full", False)
        result = await engine.sync(force_full=force_full)
        return [TextContent(type="text", text=json.dumps(result, ensure_ascii=False, indent=2))]

    elif name == "get_net_worth":
        result = get_net_worth(db)
        return [TextContent(type="text", text=json.dumps(result, ensure_ascii=False, indent=2))]

    elif name == "get_liquidity":
        result = get_liquidity(
            db,
            target_amount=arguments.get("target_amount"),
        )
        return [TextContent(type="text", text=json.dumps(result, ensure_ascii=False, indent=2))]

    elif name == "analyze_spending":
        result = analyze_spending(
            db,
            period=arguments.get("period", "this_month"),
            category_id=arguments.get("category_id"),
            top_n=arguments.get("top_n", 10),
            include_transfers=arguments.get("include_transfers", False),
            include_holds=arguments.get("include_holds", False),
        )
        return [TextContent(type="text", text=json.dumps(result, ensure_ascii=False, indent=2))]

    elif name == "analyze_income":
        result = analyze_income(
            db,
            period=arguments.get("period", "this_month"),
            top_n=arguments.get("top_n", 10),
        )
        return [TextContent(type="text", text=json.dumps(result, ensure_ascii=False, indent=2))]

    elif name == "analyze_merchants":
        result = analyze_merchants(
            db,
            period=arguments.get("period", "this_month"),
            category_id=arguments.get("category_id"),
            top_n=arguments.get("top_n", 10),
        )
        return [TextContent(type="text", text=json.dumps(result, ensure_ascii=False, indent=2))]

    elif name == "check_budget_health":
        result = check_budget_health(
            db,
            month=arguments.get("month"),
        )
        return [TextContent(type="text", text=json.dumps(result, ensure_ascii=False, indent=2))]

    elif name == "get_upcoming_payments":
        result = get_upcoming_payments(
            db,
            days_ahead=arguments.get("days_ahead", 30),
        )
        return [TextContent(type="text", text=json.dumps(result, ensure_ascii=False, indent=2))]

    elif name == "analyze_trends":
        result = analyze_trends(
            db,
            months=arguments.get("months", 6),
            category_id=arguments.get("category_id"),
            metric=arguments.get("metric", "outcome"),
        )
        return [TextContent(type="text", text=json.dumps(result, ensure_ascii=False, indent=2))]

    elif name == "detect_recurring":
        result = detect_recurring(
            db,
            lookback_months=arguments.get("lookback_months", 3),
            tolerance_pct=arguments.get("tolerance_pct", 10),
        )
        return [TextContent(type="text", text=json.dumps(result, ensure_ascii=False, indent=2))]

    elif name == "get_account_flow":
        result = get_account_flow(
            db,
            account_id=arguments.get("account_id"),
            period=arguments.get("period"),
        )
        return [TextContent(type="text", text=json.dumps(result, ensure_ascii=False, indent=2))]

    elif name == "analyze_transfers":
        result = analyze_transfers(
            db,
            period=arguments.get("period", "this_month"),
            top_n=arguments.get("top_n", 15),
        )
        return [TextContent(type="text", text=json.dumps(result, ensure_ascii=False, indent=2))]

    elif name == "detect_anomalies":
        result = detect_anomalies(
            db,
            period=arguments.get("period", "this_month"),
            category_id=arguments.get("category_id"),
            z_threshold=arguments.get("z_threshold", 2.0),
        )
        return [TextContent(type="text", text=json.dumps(result, ensure_ascii=False, indent=2))]

    elif name == "get_debts":
        result = get_debts(db)
        return [TextContent(type="text", text=json.dumps(result, ensure_ascii=False, indent=2))]

    elif name == "suggest_category":
        engine = get_sync_engine()
        result = await suggest_category(
            payee=arguments.get("payee"),
            token=engine.token,
            db=db,
        )
        return [TextContent(type="text", text=json.dumps(result, ensure_ascii=False, indent=2))]

    elif name == "convert_currency":
        result = convert_currency(
            db,
            amount=arguments.get("amount"),
            from_currency=arguments.get("from_currency"),
            to_currency=arguments.get("to_currency"),
        )
        return [TextContent(type="text", text=json.dumps(result, ensure_ascii=False, indent=2))]

    elif name == "get_exchange_rates":
        result = get_exchange_rates(
            db,
            currencies=arguments.get("currencies"),
        )
        return [TextContent(type="text", text=json.dumps(result, ensure_ascii=False, indent=2))]

    elif name == "search_transactions":
        result = search_transactions(
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
        )
        return [TextContent(type="text", text=json.dumps(result, ensure_ascii=False, indent=2))]

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
async def read_resource(uri: str) -> str:
    """Read resource content."""
    db = get_db()

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
