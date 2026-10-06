# ZenMoney MCP Server

Read-only MCP server for personal finance analytics over [ZenMoney](https://zenmoney.ru/) API.

Connects your ZenMoney data to AI assistants (Claude, Cursor, etc.) via the [Model Context Protocol](https://modelcontextprotocol.io/). Ask questions about your finances in natural language.

## What can it do?

| Question | Tool |
|----------|------|
| "How much money do I have?" | `get_net_worth` |
| "Can I afford a 100k purchase?" | `get_liquidity` |
| "Where does my money go?" | `analyze_spending` |
| "Where does my income come from?" | `analyze_income` |
| "Where do I spend the most?" | `analyze_merchants` |
| "Am I within budget?" | `check_budget_health` |
| "What subscriptions do I have?" | `detect_recurring` |
| "Spending trend over 6 months" | `analyze_trends` |
| "Transfers between accounts?" | `analyze_transfers` |
| "Any unusual spending?" | `detect_anomalies` |
| "Who owes me money?" | `get_debts` |
| "What payments are coming up?" | `get_upcoming_payments` |
| "Show recent transactions" | `search_transactions` |
| "What's happening on my PKO card?" | `get_account_flow` |
| "How much is 100 USD in EUR?" | `convert_currency` |
| "Exchange rates" | `get_exchange_rates` |
| "Category for McDonalds?" | `suggest_category` |

Plus 6 resources: accounts, categories, budgets, merchants, currencies, sync status.

## Installation

```bash
# Clone into any directory (example: ~/zenmoney-mcp)
git clone https://github.com/nnslvp/zenmoney-mcp.git ~/zenmoney-mcp
cd ~/zenmoney-mcp

# Install with uv (recommended; uses the locked dependency versions)
uv sync --extra dev

# Or with pip
python3 -m venv .venv && .venv/bin/pip install -e ".[dev]"
```

## Setup

1. Get your ZenMoney token at https://zerro.app/token

2. Configure your MCP client. Use the path where you cloned the repo; if you followed the install steps above, the binary is `~/zenmoney-mcp/.venv/bin/zenmoney-mcp`.

**Claude Desktop** (`~/Library/Application Support/Claude/claude_desktop_config.json`):
```json
{
  "mcpServers": {
    "zenmoney": {
      "command": "~/zenmoney-mcp/.venv/bin/zenmoney-mcp",
      "env": {
        "ZENMONEY_TOKEN": "paste_token_from_step_1"
      }
    }
  }
}
```

**Cursor** (`.cursor/mcp.json` in your project, or global MCP settings):
```json
{
  "mcpServers": {
    "zenmoney": {
      "command": "~/zenmoney-mcp/.venv/bin/zenmoney-mcp",
      "env": {
        "ZENMONEY_TOKEN": "paste_token_from_step_1"
      }
    }
  }
}
```

**Claude Code**:
```bash
claude mcp add zenmoney ~/zenmoney-mcp/.venv/bin/zenmoney-mcp -e ZENMONEY_TOKEN=paste_token_from_step_1
```

## How it works

1. **Sync**: downloads your ZenMoney data via `/v8/diff/` API into a local SQLite cache (`~/.cache/zenmoney-mcp/zenmoney.db`). A cache older than 10 minutes is refreshed before answering (`ZENMONEY_AUTO_SYNC_SECONDS` changes the threshold, `0` disables auto-sync); every answer carries `data_synced_at`, and `sync_data` forces a refresh.
2. **Analyze**: 18 tools run SQL queries against the local cache — no data leaves your machine
3. **Read-only**: the server never writes anything back to ZenMoney

## Key features

- Multi-currency support with real exchange rates from ZenMoney
- Transfer detection (excludes transfers between own accounts from spending)
- Category hierarchy (parent + child categories in analytics)
- Periods: `this_month`, `last_month`, `this_year`, `last_year`, `last_N_days`, `YYYY-MM`, `YYYY`, or explicit `start_date`/`end_date`
- Off-balance accounts are left out of reports, as in ZenMoney itself, but never silently: the amount left out is reported, and `include_off_balance` includes them
- Budget tracking with pace and overspend alerts
- Recurring payment detection (subscriptions, bills)
- Anomaly detection (unusual spending via Z-score)
- Currency converter with cross-rates

## Testing

```bash
# Unit tests (no API required)
pytest tests/ -v --ignore=tests/test_integration.py

# Integration tests (requires token)
ZENMONEY_TOKEN=xxx pytest tests/test_integration.py -v
```

## Tech stack

- Python 3.11+
- `mcp` — official MCP SDK by Anthropic
- `httpx` — HTTP client
- `sqlite3` — local cache (stdlib, no heavy ORMs)
- `pytest` — testing

## License

MIT
