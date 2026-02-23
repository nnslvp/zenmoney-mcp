# ZenMoney MCP Server

## What is this

Read-only MCP server for personal finance analytics over ZenMoney API.

## Tech stack

- Python 3.11+
- `mcp` (official MCP SDK by Anthropic) — tools and resources
- `httpx` — HTTP requests to ZenMoney API
- `sqlite3` (stdlib) — local cache
- `pytest` — testing
- **Do NOT add:** pandas, sqlalchemy, heavy ORMs. SQLite + stdlib only.

## Project structure

```
zenmoney-mcp/
├── CLAUDE.md
├── pyproject.toml
├── src/
│   └── zenmoney_mcp/
│       ├── __init__.py
│       ├── server.py          # MCP server, tool/resource registration
│       ├── database.py        # SQLite: schema, CRUD
│       ├── sync_engine.py     # Sync via /v8/diff/
│       ├── analytics.py       # Business logic for all tools (~3000 lines)
│       └── utils.py           # Currency conversion, transaction classification
├── tests/
│   ├── conftest.py            # Fixtures: in-memory SQLite with test data
│   ├── test_database.py
│   ├── test_sync.py
│   ├── test_tools.py          # Tests for all 18 tools + 6 resources
│   ├── test_utils.py
│   └── test_integration.py    # Smoke test with real API (needs ZENMONEY_TOKEN)
└── README.md
```

## Configuration

```bash
export ZENMONEY_TOKEN=your_token_here  # get at https://zerro.app/token
```

## ZenMoney API reference

Official docs: https://github.com/zenmoney/ZenPlugins/wiki/ZenMoney-API

Consult when unclear about: entity structure, `/v8/diff/` format, Budget lock flags, transaction types (expense/income/transfer/debt), `Reminder` vs `ReminderMarker`, `Account.type = debt`.

## Architecture (do not change)

1. **Read-only:** server NEVER writes to ZenMoney. Only `/v8/diff/` (read) and `/v8/suggest/` (suggestions).

2. **Transfer filtering:** `income > 0 AND outcome > 0` → excluded from spending/income by default (these are transfers, exchanges, debts).

3. **Base WHERE for expenses:**
   ```sql
   WHERE deleted = 0
     AND (hold IS NULL OR hold = 0)
     AND NOT (income > 0 AND outcome > 0)
     AND outcome > 0 AND income = 0
   ```

4. **Enrichment via JOIN, not Python dicts.** UUID → human-readable names.

5. **LIMIT on all responses.** search_transactions: 50, analyze_spending: 15. Always return `total_count` + `returned_count`.

6. **Currency conversion:** `amount_user = amount * instrument.rate / user_currency.rate`

7. **Tag hierarchy:** queries on parent category always include children.

## Tools (18)

| # | Tool | Purpose |
|---|------|---------|
| T0 | `sync_data` | Sync with ZenMoney API |
| T1 | `get_net_worth` | Total capital by account type |
| T2 | `get_liquidity` | Liquid funds, affordability check |
| T3 | `analyze_spending` | Spending by category |
| T4 | `analyze_income` | Income by source |
| T5 | `check_budget_health` | Budget plan vs actual |
| T6 | `detect_recurring` | Subscriptions, recurring payments |
| T7 | `analyze_merchants` | Top merchants by spending |
| T8 | `analyze_trends` | Monthly trends (spending/income/savings) |
| T9 | `analyze_transfers` | Transfers between accounts |
| T10 | `detect_anomalies` | Unusual spending (Z-score) |
| T11 | `get_debts` | Debt summary |
| T12 | `get_upcoming_payments` | Future reminder payments |
| T13 | `search_transactions` | Search with filters |
| T14 | `get_account_flow` | Account movement details |
| T15 | `suggest_category` | Category suggestion via ZenMoney API |
| T16 | `convert_currency` | Currency conversion with real rates |
| T17 | `get_exchange_rates` | Cross-rate table for account currencies |

## Resources (6)

| # | URI | Content |
|---|-----|---------|
| R1 | `zenmoney://accounts` | Active accounts with balances |
| R2 | `zenmoney://categories` | Tag hierarchy (parent-child) |
| R3 | `zenmoney://budgets/current` | Current month budget limits |
| R4 | `zenmoney://merchants` | Merchant directory |
| R5 | `zenmoney://instruments` | Currencies with rates |
| R6 | `zenmoney://sync-status` | Sync state and cache stats |

## Testing

After any change:
```bash
pytest tests/ -v --ignore=tests/test_integration.py
```

All 175 tests must pass before committing.

## Common mistakes (avoid)

- `SELECT * FROM transactions` without WHERE and LIMIT
- Summing amounts in different currencies without conversion
- Counting transfers between own accounts as expenses
- Missing `deleted = 0` filter
- Showing UUIDs instead of category/merchant names
- Adding pandas or other heavy dependencies
