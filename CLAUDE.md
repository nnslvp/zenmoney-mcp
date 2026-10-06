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
│   ├── factories.py           # Row factories: insert_transaction(db, outcome=..) etc.
│   ├── test_server.py         # Protocol-level: tools/resources via an in-memory MCP client, auto-sync
│   ├── test_database.py
│   ├── test_sync.py
│   ├── test_tools.py          # Tests for all 18 tools + 6 resources
│   ├── test_<area>.py         # Per-area tests: balances, budget, spending, search, recurring, ...
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

5. **LIMIT on all responses.** search_transactions: 50 (max 200), top_n reports: 10 (max 100). Always return `total_count` + `returned_count`.

6. **Currency conversion:** `amount_user = amount * instrument.rate / user_currency.rate`. Reminders and reminder markers have no instrument: their amounts are in the currency of their account (JOIN accounts). A debt operation is in the currency of the non-debt account.

7. **Tag hierarchy:** queries on parent category always include children; overviews fold children into the parent (`subcategories`).

8. **Off-balance accounts** (`in_balance = 0`) are left out of spending/income/budget reports, as in ZenMoney's own reports, but never silently: report `off_balance_excluded` and accept `include_off_balance`. Balances and liquidity count every account and flag it.

9. **Invalid input is an error, never a silent fallback.** Periods and dates go through `get_period_dates` / `parse_iso_date`; raise `ValueError` with a clear message, the server turns it into a tool error.

10. **Freshness:** tools and resources answer from the cache; `server.ensure_fresh` syncs first when it is older than `ZENMONEY_AUTO_SYNC_SECONDS` (default 600). Every tool answer carries `data_synced_at`.

11. **Budget plan** (`_load_budget_plan`): locked row = exact; unlocked row = stored amount + scheduled operations (reminder markers, states planned+processed, first tag, converted); a parent's plan = own + children (ZenMoney stores the parent as the remainder over its children); the month-total row follows the same rule one level up. Deleting a reminder leaves its planned markers in the cache: sync retires them.

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

All tests must pass before committing. `tests/test_server.py` drives the server through a real MCP client session; add a test there when a tool's schema or dispatch changes.

## Common mistakes (avoid)

- `SELECT * FROM transactions` without WHERE and LIMIT
- Summing amounts in different currencies without conversion
- Counting transfers between own accounts as expenses
- Missing `deleted = 0` filter
- Showing UUIDs instead of category/merchant names
- Adding pandas or other heavy dependencies
