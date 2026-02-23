"""Sync engine for ZenMoney API using /v8/diff/ protocol."""

import time
from typing import Any

import httpx

from .database import Database


ZENMONEY_API_URL = "https://api.zenmoney.ru/v8/diff/"

# Entity names in API response mapped to database upsert methods and table names
ENTITY_MAPPING = {
    "instrument": ("upsert_instruments", "instruments"),
    "company": ("upsert_companies", "companies"),
    "user": ("upsert_users", "users"),
    "account": ("upsert_accounts", "accounts"),
    "tag": ("upsert_tags", "tags"),
    "merchant": ("upsert_merchants", "merchants"),
    "transaction": ("upsert_transactions", "transactions"),
    "budget": ("upsert_budgets", "budgets"),
    "reminder": ("upsert_reminders", "reminders"),
    "reminderMarker": ("upsert_reminder_markers", "reminder_markers"),
}


class SyncError(Exception):
    """Error during synchronization with ZenMoney API."""

    pass


class SyncEngine:
    """Synchronization engine for ZenMoney data."""

    def __init__(self, db: Database, token: str):
        """Initialize sync engine.

        Args:
            db: Database instance for storing synced data.
            token: ZenMoney OAuth2 bearer token.
        """
        self.db = db
        self.token = token

    async def sync(self, force_full: bool = False) -> dict[str, Any]:
        """Perform synchronization with ZenMoney API.

        Args:
            force_full: If True, perform full sync (serverTimestamp=0).
                        If False, perform incremental sync.

        Returns:
            Dictionary with sync results including updated and deleted counts.

        Raises:
            SyncError: If API request fails.
        """
        start_time = time.time()

        # Get server timestamp for incremental sync
        server_timestamp = 0 if force_full else self.db.get_server_timestamp()

        # Current client timestamp
        current_timestamp = int(time.time())

        # Make API request
        request_body = {
            "currentClientTimestamp": current_timestamp,
            "serverTimestamp": server_timestamp,
        }

        async with httpx.AsyncClient() as client:
            try:
                response = await client.post(
                    ZENMONEY_API_URL,
                    json=request_body,
                    headers={
                        "Authorization": f"Bearer {self.token}",
                        "Content-Type": "application/json",
                    },
                    timeout=60.0,
                )
            except httpx.HTTPError as e:
                raise SyncError(f"HTTP error during sync: {e}") from e

        if response.status_code != 200:
            raise SyncError(
                f"API returned status {response.status_code}: {response.text}"
            )

        try:
            diff_data = response.json()
        except ValueError as e:
            raise SyncError(f"Invalid JSON response: {e}") from e

        # Process the diff response
        result = self._apply_diff(diff_data)

        # Save new server timestamp
        new_timestamp = diff_data.get("serverTimestamp", current_timestamp)
        self.db.set_server_timestamp(new_timestamp)

        # Save last sync time
        self.db.set_meta("last_sync_time", str(int(time.time())))

        result["new_server_timestamp"] = new_timestamp
        result["sync_duration_ms"] = int((time.time() - start_time) * 1000)
        result["status"] = "synced"

        return result

    def _apply_diff(self, diff_data: dict[str, Any]) -> dict[str, Any]:
        """Apply diff data to database.

        Args:
            diff_data: Response from /v8/diff/ API.

        Returns:
            Dictionary with counts of updated and deleted records.
        """
        updated: dict[str, int] = {}
        deleted: dict[str, int] = {}

        # Process each entity type
        for entity_name, (upsert_method, table_name) in ENTITY_MAPPING.items():
            items = diff_data.get(entity_name, [])
            if items:
                method = getattr(self.db, upsert_method)
                count = method(items)
                if count > 0:
                    updated[table_name] = count

        # Process deletions
        deletion_list = diff_data.get("deletion", [])
        for deletion in deletion_list:
            obj_type = deletion.get("object")
            obj_id = deletion.get("id")

            if not obj_type or obj_id is None:
                continue

            # Map API object type to table name
            if obj_type in ENTITY_MAPPING:
                _, table_name = ENTITY_MAPPING[obj_type]
                count = self.db.delete_by_ids(table_name, [obj_id])
                if count > 0:
                    deleted[table_name] = deleted.get(table_name, 0) + count

        return {"updated": updated, "deleted": deleted}

    def apply_diff_data(self, diff_data: dict[str, Any]) -> dict[str, Any]:
        """Apply diff data directly (for testing without HTTP).

        Args:
            diff_data: Simulated diff response data.

        Returns:
            Dictionary with counts of updated and deleted records.
        """
        result = self._apply_diff(diff_data)

        # Save server timestamp if present
        new_timestamp = diff_data.get("serverTimestamp")
        if new_timestamp is not None:
            self.db.set_server_timestamp(new_timestamp)

        result["status"] = "synced"
        return result
