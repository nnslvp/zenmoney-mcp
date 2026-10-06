"""suggest_category: the API answers with ids, the tool must answer with names."""

from unittest.mock import AsyncMock, Mock, patch

from zenmoney_mcp.analytics import suggest_category
from zenmoney_mcp.database import Database

from .factories import insert_merchant


async def _suggest(db: Database, api_answer: dict) -> dict:
    """Run suggest_category against a mocked /v8/suggest/ answer."""
    response = Mock(status_code=200)
    response.json.return_value = api_answer
    with patch("httpx.AsyncClient") as client:
        client.return_value.__aenter__.return_value.post = AsyncMock(return_value=response)
        return await suggest_category(payee="corner bakery", token="test_token", db=db)


class TestSuggestedMerchant:
    async def test_merchant_id_is_resolved_to_its_title(self, populated_db: Database):
        insert_merchant(populated_db, id="m-bakery", title="Corner Bakery")

        result = await _suggest(populated_db, {"payee": "Corner Bakery", "merchant": "m-bakery"})

        assert result["suggested_merchant"] == "Corner Bakery"
        assert result["suggested_merchant_id"] == "m-bakery"

    async def test_merchant_missing_from_the_cache_has_no_title(self, populated_db: Database):
        result = await _suggest(populated_db, {"payee": "Corner Bakery", "merchant": "m-not-synced"})

        assert result["suggested_merchant"] is None
        assert result["suggested_merchant_id"] == "m-not-synced"

    async def test_answer_without_a_merchant(self, populated_db: Database):
        result = await _suggest(populated_db, {"payee": "Corner Bakery", "tag": ["tag-restaurant"]})

        assert result["suggested_merchant"] is None
        assert result["suggested_merchant_id"] is None
        assert result["suggested_categories"] == [{"tag_id": "tag-restaurant", "name": "Рестораны"}]
