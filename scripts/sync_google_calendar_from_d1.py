#!/usr/bin/env python3
"""
Sync events from D1 to Google Calendar.

Reads events from D1 that are marked for publishing and creates/updates
them in Google Calendar. Stores Google event IDs back in D1 for idempotency.

Environment variables:
    D1_DB_NAME: Name of the D1 database (required)
    GOOGLE_CALENDAR_ID: Google Calendar ID to sync to (required)
    GOOGLE_SERVICE_ACCOUNT_JSON: Service account credentials JSON (required)
    DRY_RUN: Set to "1" to log actions without executing (optional)

Usage:
    python scripts/sync_google_calendar_from_d1.py
"""

from __future__ import annotations

import json
import logging
import os
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

# Add scripts directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from d1 import D1Client, D1Error

# Google API imports - optional for dry-run mode
try:
    from google.oauth2 import service_account
    from googleapiclient.discovery import build
    from googleapiclient.errors import HttpError
    GOOGLE_API_AVAILABLE = True
except ImportError:
    GOOGLE_API_AVAILABLE = False

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


# Event kinds that are published by default (fallback if no publish_to_google flag)
DEFAULT_PUBLISH_KINDS = ("session", "training", "ladies", "tournament", "social", "other")
DEFAULT_PUBLISH_STATUS = "scheduled"


def get_google_calendar_service():
    """Create Google Calendar API service using service account credentials."""
    if not GOOGLE_API_AVAILABLE:
        raise RuntimeError(
            "Google API libraries not installed. "
            "Run: pip install google-auth google-api-python-client"
        )

    creds_json = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
    if not creds_json:
        raise ValueError("GOOGLE_SERVICE_ACCOUNT_JSON environment variable is required")

    try:
        creds_data = json.loads(creds_json)
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in GOOGLE_SERVICE_ACCOUNT_JSON: {e}")

    credentials = service_account.Credentials.from_service_account_info(
        creds_data,
        scopes=["https://www.googleapis.com/auth/calendar"]
    )

    return build("calendar", "v3", credentials=credentials)


def build_events_query() -> str:
    """
    Build SQL query to get events for Google Calendar sync.

    Checks for publish_to_google column first; falls back to kind/status filtering.
    """
    # Calculate date range: 14 days ago to future
    cutoff = (datetime.now(timezone.utc) - timedelta(days=14)).isoformat()

    # TODO: Detect if publish_to_google column exists and use it preferentially
    # For now, use fallback logic based on kind and status

    # This query assumes the events table exists with these columns:
    # id, title, description, location, starts_at_utc, ends_at_utc, timezone,
    # kind, status, google_event_id, google_last_synced_at
    #
    # Fallback logic:
    # - kind IN ('session','training','ladies','tournament','social','other')
    # - status = 'scheduled'
    # - starts_at_utc >= 14 days ago

    query = f"""
    SELECT
        id,
        title,
        description,
        location,
        starts_at_utc,
        ends_at_utc,
        timezone,
        kind,
        status,
        google_event_id,
        google_last_synced_at
    FROM events
    WHERE
        (
            -- Prefer publish_to_google if column exists
            -- TODO: Add check for publish_to_google column
            (kind IN ('session', 'training', 'ladies', 'tournament', 'social', 'other'))
            AND (status = 'scheduled' OR status IS NULL)
        )
        AND starts_at_utc >= '{cutoff}'
    ORDER BY starts_at_utc ASC
    """
    return query.strip()


def format_google_event(event: dict[str, Any], calendar_timezone: str) -> dict[str, Any]:
    """Format a D1 event for Google Calendar API."""
    # Parse UTC times
    start_utc = event.get("starts_at_utc")
    end_utc = event.get("ends_at_utc")

    # Use event timezone or fallback
    tz = event.get("timezone") or calendar_timezone

    google_event = {
        "summary": event.get("title", "Untitled Event"),
        "description": event.get("description", ""),
        "location": event.get("location", ""),
    }

    # Format start time
    if start_utc:
        google_event["start"] = {
            "dateTime": start_utc,
            "timeZone": tz,
        }

    # Format end time (default to 1 hour after start if not specified)
    if end_utc:
        google_event["end"] = {
            "dateTime": end_utc,
            "timeZone": tz,
        }
    elif start_utc:
        # Default 1 hour duration
        try:
            start_dt = datetime.fromisoformat(start_utc.replace("Z", "+00:00"))
            end_dt = start_dt + timedelta(hours=1)
            google_event["end"] = {
                "dateTime": end_dt.isoformat(),
                "timeZone": tz,
            }
        except Exception:
            google_event["end"] = google_event["start"]

    return google_event


def sync_event_to_google(
    service,
    calendar_id: str,
    event: dict[str, Any],
    dry_run: bool = False
) -> tuple[str | None, bool]:
    """
    Sync a single event to Google Calendar.

    Returns:
        (google_event_id, created) - ID of the Google event and whether it was newly created
    """
    event_id = event.get("id")
    google_event_id = event.get("google_event_id")
    title = event.get("title", "Unknown")

    google_event = format_google_event(event, "Europe/London")

    if dry_run:
        if google_event_id:
            logger.info(f"[DRY_RUN] Would UPDATE Google event {google_event_id}: {title}")
        else:
            logger.info(f"[DRY_RUN] Would CREATE Google event for D1 event {event_id}: {title}")
        return google_event_id, False

    try:
        if google_event_id:
            # Update existing event
            logger.info(f"Updating Google event {google_event_id}: {title}")
            result = service.events().update(
                calendarId=calendar_id,
                eventId=google_event_id,
                body=google_event
            ).execute()
            return result.get("id"), False
        else:
            # Create new event
            logger.info(f"Creating Google event for D1 event {event_id}: {title}")
            result = service.events().insert(
                calendarId=calendar_id,
                body=google_event
            ).execute()
            return result.get("id"), True

    except HttpError as e:
        if e.resp.status == 404 and google_event_id:
            # Event was deleted in Google, recreate it
            logger.warning(f"Google event {google_event_id} not found, recreating: {title}")
            try:
                result = service.events().insert(
                    calendarId=calendar_id,
                    body=google_event
                ).execute()
                return result.get("id"), True
            except HttpError as e2:
                logger.error(f"Failed to recreate event {event_id}: {e2}")
                return None, False
        else:
            logger.error(f"Failed to sync event {event_id}: {e}")
            return None, False


def escape_sql_string(s: str | None) -> str:
    """Escape a string for SQL, handling None."""
    if s is None:
        return "NULL"
    escaped = s.replace("'", "''")
    return f"'{escaped}'"


def generate_update_sql(updates: list[tuple[str, str]]) -> str:
    """
    Generate SQL to update google_event_id and google_last_synced_at.

    Args:
        updates: List of (event_id, google_event_id) tuples
    """
    if not updates:
        return "-- No updates"

    now = datetime.now(timezone.utc).isoformat()
    statements = []

    for event_id, google_event_id in updates:
        sql = f"""UPDATE events
SET google_event_id = {escape_sql_string(google_event_id)},
    google_last_synced_at = {escape_sql_string(now)}
WHERE id = {escape_sql_string(event_id)};"""
        statements.append(sql)

    return "\n\n".join(statements)


def main() -> int:
    dry_run = os.environ.get("DRY_RUN", "").lower() in ("1", "true", "yes")

    logger.info("Starting Google Calendar sync from D1")

    if dry_run:
        logger.info("DRY_RUN mode enabled")

    # Get configuration
    calendar_id = os.environ.get("GOOGLE_CALENDAR_ID")
    if not calendar_id:
        logger.error("GOOGLE_CALENDAR_ID environment variable is required")
        return 1

    # Initialize D1 client
    try:
        d1_client = D1Client(dry_run=dry_run)
    except D1Error as e:
        logger.error(f"Failed to initialize D1 client: {e}")
        return 1

    # Initialize Google Calendar service (unless dry-run)
    service = None
    if not dry_run:
        try:
            service = get_google_calendar_service()
        except Exception as e:
            logger.error(f"Failed to initialize Google Calendar service: {e}")
            return 1

    # Step 1: Query events from D1
    logger.info("Step 1: Querying events from D1...")
    query = build_events_query()

    try:
        events = d1_client.query_json(query)
    except D1Error as e:
        logger.error(f"Failed to query events from D1: {e}")
        return 1

    if not events:
        logger.info("No events to sync")
        return 0

    logger.info(f"Found {len(events)} events to sync")

    # Step 2: Sync each event to Google Calendar
    logger.info("Step 2: Syncing events to Google Calendar...")
    updates = []  # List of (event_id, google_event_id) for D1 update
    created_count = 0
    updated_count = 0
    failed_count = 0

    for event in events:
        event_id = event.get("id")
        google_event_id, created = sync_event_to_google(
            service, calendar_id, event, dry_run=dry_run
        )

        if google_event_id:
            # Only add to updates if this is a new mapping or we need to refresh
            if created or event.get("google_event_id") != google_event_id:
                updates.append((event_id, google_event_id))

            if created:
                created_count += 1
            else:
                updated_count += 1
        else:
            failed_count += 1

    logger.info(f"Sync complete: {created_count} created, {updated_count} updated, {failed_count} failed")

    # Step 3: Update D1 with Google event IDs
    if updates:
        logger.info(f"Step 3: Updating {len(updates)} events in D1 with Google IDs...")

        update_sql = generate_update_sql(updates)
        sql_file = os.path.join(tempfile.gettempdir(), "google_calendar_updates.sql")

        with open(sql_file, "w", encoding="utf-8") as f:
            f.write(update_sql)

        try:
            d1_client.execute_file(sql_file)
            logger.info(f"Updated {len(updates)} events in D1")
        except D1Error as e:
            logger.error(f"Failed to update D1: {e}")
            return 1
    else:
        logger.info("Step 3: No D1 updates needed")

    logger.info("Google Calendar sync completed successfully")
    return 0


if __name__ == "__main__":
    sys.exit(main())
