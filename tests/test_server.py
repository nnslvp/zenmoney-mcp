"""Protocol-level tests: the server is driven through a real MCP client session."""

import json
import time
from datetime import datetime
from unittest.mock import AsyncMock

import pytest
from mcp.shared.exceptions import McpError
from mcp.shared.memory import create_connected_server_and_client_session
from pydantic import AnyUrl

from zenmoney_mcp import server as zm_server
from zenmoney_mcp.database import Database
from zenmoney_mcp.sync_engine import SyncEngine, SyncError

RESOURCE_URIS = [
    "zenmoney://accounts",
    "zenmoney://categories",
    "zenmoney://budgets/current",
    "zenmoney://merchants",
    "zenmoney://instruments",
    "zenmoney://sync-status",
]


@pytest.fixture
def sync_mock(populated_db: Database, monkeypatch) -> AsyncMock:
    """Wire the server to the test DB and replace the network sync with a mock.

    The mock records a successful sync the way the real engine does.
    """
    async def fake_sync(*args, **kwargs):
        populated_db.set_meta("last_sync_time", str(int(time.time())))
        return {"status": "synced"}

    mock = AsyncMock(side_effect=fake_sync)
    monkeypatch.setattr(SyncEngine, "sync", mock)
    monkeypatch.delenv("ZENMONEY_AUTO_SYNC_SECONDS", raising=False)
    zm_server.init_for_testing(populated_db)
    yield mock
    zm_server._db = None
    zm_server._sync_engine = None


def _mark_synced(db: Database, seconds_ago: int = 0) -> int:
    synced_at = int(time.time()) - seconds_ago
    db.set_meta("last_sync_time", str(synced_at))
    return synced_at


async def _call(name: str, arguments: dict | None = None):
    async with create_connected_server_and_client_session(zm_server.server) as client:
        return await client.call_tool(name, arguments or {})


def _payload(result) -> dict:
    assert not result.isError, result.content[0].text
    return json.loads(result.content[0].text)


class TestResourcesOverProtocol:
    """The SDK hands read_resource an AnyUrl, not a str."""

    @pytest.mark.parametrize("uri", RESOURCE_URIS)
    async def test_listed_resource_is_readable(self, populated_db, sync_mock, uri):
        _mark_synced(populated_db)
        async with create_connected_server_and_client_session(zm_server.server) as client:
            result = await client.read_resource(AnyUrl(uri))

        assert isinstance(json.loads(result.contents[0].text), dict)

    async def test_every_listed_resource_is_covered(self, populated_db, sync_mock):
        async with create_connected_server_and_client_session(zm_server.server) as client:
            listed = await client.list_resources()

        assert sorted(str(r.uri) for r in listed.resources) == sorted(RESOURCE_URIS)

    async def test_unknown_resource_is_an_error(self, populated_db, sync_mock):
        _mark_synced(populated_db)
        async with create_connected_server_and_client_session(zm_server.server) as client:
            with pytest.raises(McpError, match="Unknown resource"):
                await client.read_resource(AnyUrl("zenmoney://nope"))

    async def test_stale_cache_is_refreshed_before_reading(self, populated_db, sync_mock):
        _mark_synced(populated_db, seconds_ago=3600)
        async with create_connected_server_and_client_session(zm_server.server) as client:
            await client.read_resource(AnyUrl("zenmoney://accounts"))

        assert sync_mock.await_count == 1


class TestToolsOverProtocol:
    async def test_tool_returns_json(self, populated_db, sync_mock):
        _mark_synced(populated_db)

        payload = _payload(await _call("get_net_worth"))

        assert "net_worth" in payload

    async def test_malformed_date_is_reported_as_tool_error(self, populated_db, sync_mock):
        _mark_synced(populated_db)

        result = await _call(
            "analyze_spending", {"start_date": "01.09.2026", "end_date": "30.09.2026"}
        )

        assert result.isError
        assert "start_date" in result.content[0].text


class TestAutoSync:
    """Tools answer from the local cache, so the cache must not silently go stale."""

    async def test_stale_cache_is_synced_before_answering(self, populated_db, sync_mock):
        _mark_synced(populated_db, seconds_ago=3600)

        _payload(await _call("get_net_worth"))

        assert sync_mock.await_count == 1

    async def test_never_synced_cache_is_synced_before_answering(self, populated_db, sync_mock):
        _payload(await _call("get_net_worth"))

        assert sync_mock.await_count == 1

    async def test_fresh_cache_is_not_synced_again(self, populated_db, sync_mock):
        _mark_synced(populated_db, seconds_ago=30)

        _payload(await _call("get_net_worth"))

        assert sync_mock.await_count == 0

    async def test_answer_reports_when_data_was_synced(self, populated_db, sync_mock):
        synced_at = _mark_synced(populated_db, seconds_ago=30)

        payload = _payload(await _call("get_net_worth"))

        assert payload["data_synced_at"] == datetime.fromtimestamp(synced_at).isoformat()

    async def test_failed_sync_falls_back_to_cache_with_warning(self, populated_db, sync_mock):
        synced_at = _mark_synced(populated_db, seconds_ago=3600)
        sync_mock.side_effect = SyncError("network down")

        payload = _payload(await _call("get_net_worth"))

        assert "network down" in payload["sync_warning"]
        assert payload["data_synced_at"] == datetime.fromtimestamp(synced_at).isoformat()
        assert "net_worth" in payload

    async def test_failed_first_sync_is_an_error_not_zeros(self, populated_db, sync_mock):
        sync_mock.side_effect = SyncError("network down")

        result = await _call("get_net_worth")

        assert result.isError
        assert "network down" in result.content[0].text

    async def test_auto_sync_can_be_disabled(self, populated_db, sync_mock, monkeypatch):
        monkeypatch.setenv("ZENMONEY_AUTO_SYNC_SECONDS", "0")
        _mark_synced(populated_db, seconds_ago=3600)

        payload = _payload(await _call("get_net_worth"))

        assert sync_mock.await_count == 0
        assert "sync_warning" not in payload

    async def test_explicit_sync_does_not_sync_twice(self, populated_db, sync_mock):
        _mark_synced(populated_db, seconds_ago=3600)

        payload = _payload(await _call("sync_data"))

        assert sync_mock.await_count == 1
        assert payload["status"] == "synced"
