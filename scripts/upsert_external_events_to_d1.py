#!/usr/bin/env python3
"""
Upsert external events to Cloudflare D1.

Fetches events from GBUWH website and upserts them into the
external_events table in D1.

Environment variables:
    D1_DB_NAME: Name of the D1 database (required)
    DRY_RUN: Set to "1" to log SQL without executing (optional)

Usage:
    python scripts/upsert_external_events_to_d1.py
"""

from __future__ import annotations

import logging
import os
import sys
import tempfile
import uuid
from datetime import datetime, timezone
from pathlib import Path

# Add scripts directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from d1 import D1Client, D1Error
from extract_external_events import extract_all_events

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


def escape_sql_string(s: str | None) -> str:
    """Escape a string for SQL, handling None."""
    if s is None:
        return "NULL"
    # Escape single quotes by doubling them
    escaped = s.replace("'", "''")
    return f"'{escaped}'"


def generate_upsert_sql(events: list[dict]) -> str:
    """
    Generate SQL for upserting events into external_events table.

    Uses INSERT ... ON CONFLICT DO UPDATE for idempotent upserts.
    """
    if not events:
        return "-- No events to upsert"

    now = datetime.now(timezone.utc).isoformat()

    statements = []
    for event in events:
        # Generate a stable UUID for the id based on source + source_event_id
        event_id = str(uuid.uuid5(
            uuid.NAMESPACE_URL,
            f"{event['source']}:{event['source_event_id']}"
        ))

        sql = f"""INSERT INTO external_events (
    id, source, source_event_id, title, description, location,
    starts_at_utc, ends_at_utc, timezone, url,
    visibility, origin, created_by_person_id, updated_by_person_id,
    updated_at, created_at
) VALUES (
    {escape_sql_string(event_id)},
    {escape_sql_string(event['source'])},
    {escape_sql_string(event['source_event_id'])},
    {escape_sql_string(event['title'])},
    {escape_sql_string(event['description'])},
    {escape_sql_string(event['location'])},
    {escape_sql_string(event['starts_at_utc'])},
    {escape_sql_string(event['ends_at_utc'])},
    {escape_sql_string(event['timezone'])},
    {escape_sql_string(event['url'])},
    'public',
    'import',
    NULL,
    NULL,
    {escape_sql_string(now)},
    {escape_sql_string(now)}
)
ON CONFLICT(source, source_event_id) DO UPDATE SET
    title = excluded.title,
    description = excluded.description,
    location = excluded.location,
    starts_at_utc = excluded.starts_at_utc,
    ends_at_utc = excluded.ends_at_utc,
    timezone = excluded.timezone,
    url = excluded.url,
    updated_at = excluded.updated_at;"""
        statements.append(sql)

    return "\n\n".join(statements)


def main() -> int:
    logger.info("Starting external events upsert pipeline")

    # Step 1: Extract events from GBUWH
    logger.info("Step 1: Extracting events from GBUWH website...")
    try:
        events = extract_all_events()
    except Exception as e:
        logger.error(f"Failed to extract events: {e}")
        return 1

    if not events:
        logger.warning("No events extracted, nothing to upsert")
        return 0

    logger.info(f"Fetched {len(events)} events")

    # Step 2: Generate upsert SQL
    logger.info("Step 2: Generating upsert SQL...")
    sql = generate_upsert_sql(events)

    # Step 3: Write SQL to temp file
    sql_file = os.path.join(tempfile.gettempdir(), "external_events_upsert.sql")
    with open(sql_file, "w", encoding="utf-8") as f:
        f.write(sql)
    logger.info(f"Wrote SQL to {sql_file} ({len(sql)} chars)")

    # Step 4: Execute via wrangler
    logger.info("Step 3: Executing upsert via Wrangler...")
    try:
        client = D1Client()
        client.execute_file(sql_file)
    except D1Error as e:
        logger.error(f"Failed to execute upsert: {e}")
        return 1

    logger.info(f"Successfully upserted {len(events)} events to D1")
    return 0


if __name__ == "__main__":
    sys.exit(main())
