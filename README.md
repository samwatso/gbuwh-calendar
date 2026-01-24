# GBUWH Calendar

Automated pipelines for ingesting UK underwater hockey events and syncing to Google Calendar.

## Overview

This repo provides two automated pipelines:

### A) Ingest Pipeline (Monthly)

Scrapes UK-wide events from [GBUWH](https://www.gbuwh.co.uk/events) and upserts them into a Cloudflare D1 database.

- **Workflow:** `.github/workflows/ingest_external_events.yml`
- **Schedule:** Monthly (1st of each month at 06:00 UTC)
- **Script:** `scripts/upsert_external_events_to_d1.py`

### B) Google Calendar Sync Pipeline (Every 6 hours)

Reads events from D1 that admins have selected for publishing and syncs them to Google Calendar.

- **Workflow:** `.github/workflows/sync_google_calendar.yml`
- **Schedule:** Every 6 hours
- **Script:** `scripts/sync_google_calendar_from_d1.py`

## Required Secrets

Configure these in your GitHub repository settings:

| Secret | Description |
|--------|-------------|
| `CLOUDFLARE_API_TOKEN` | Cloudflare API token with D1 edit permissions |
| `CLOUDFLARE_ACCOUNT_ID` | Your Cloudflare account ID |
| `D1_DB_NAME` | Name of your D1 database (e.g., `wwuwh-prod`) |
| `GOOGLE_CALENDAR_ID` | Google Calendar ID to sync events to |
| `GOOGLE_SERVICE_ACCOUNT_JSON` | Google service account credentials JSON |

## D1 Database Schema

The pipelines expect these tables in your D1 database:

### `external_events`

Stores scraped external events:

```sql
CREATE TABLE external_events (
    id TEXT PRIMARY KEY,
    source TEXT NOT NULL,
    source_event_id TEXT NOT NULL,
    title TEXT NOT NULL,
    description TEXT,
    location TEXT,
    starts_at_utc TEXT NOT NULL,
    ends_at_utc TEXT,
    timezone TEXT DEFAULT 'Europe/London',
    url TEXT,
    updated_at TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE(source, source_event_id)
);
```

### `events`

Your club's events table (managed by your main app):

```sql
-- Expected columns for Google Calendar sync
-- id, title, description, location, starts_at_utc, ends_at_utc, timezone
-- kind, status, google_event_id, google_last_synced_at
```

The sync script uses fallback logic if `publish_to_google` column doesn't exist:
- `kind` IN ('session', 'training', 'ladies', 'tournament', 'social', 'other')
- `status` = 'scheduled'
- `starts_at_utc` >= 14 days ago

## Scripts

### `scripts/extract_external_events.py`

Scrapes GBUWH events and outputs normalized JSON.

```bash
# Output to stdout
python scripts/extract_external_events.py

# Output to file
python scripts/extract_external_events.py --output events.json
```

### `scripts/d1.py`

Wrapper for Wrangler D1 CLI commands.

```bash
# Execute SQL command
python scripts/d1.py --command "SELECT * FROM events LIMIT 5"

# Execute SQL file
python scripts/d1.py --file schema.sql

# Query with JSON output
python scripts/d1.py --query "SELECT * FROM events"
```

### `scripts/upsert_external_events_to_d1.py`

Fetches events from GBUWH and upserts to D1.

```bash
D1_DB_NAME=wwuwh-prod python scripts/upsert_external_events_to_d1.py
```

### `scripts/sync_google_calendar_from_d1.py`

Syncs events from D1 to Google Calendar.

```bash
D1_DB_NAME=wwuwh-prod \
GOOGLE_CALENDAR_ID=your-calendar@group.calendar.google.com \
GOOGLE_SERVICE_ACCOUNT_JSON='{"type":"service_account",...}' \
python scripts/sync_google_calendar_from_d1.py
```

## Dry Run Mode

Set `DRY_RUN=1` to log actions without executing:

```bash
DRY_RUN=1 python scripts/upsert_external_events_to_d1.py
DRY_RUN=1 python scripts/sync_google_calendar_from_d1.py
```

## Wrangler CLI Notes

The D1 wrapper uses these Wrangler flags for CI compatibility:

- `--remote` - Execute against remote D1 database
- `--yes` - Non-interactive mode (skips prompts)
- `--json` - Output results as JSON (for queries)

Example:
```bash
npx wrangler d1 execute $D1_DB_NAME --remote --yes --file=script.sql
npx wrangler d1 execute $D1_DB_NAME --remote --yes --json --command="SELECT * FROM events"
```

## Google Calendar Setup

1. Create a Google Cloud project
2. Enable the Google Calendar API
3. Create a service account and download the JSON credentials
4. Share your Google Calendar with the service account email (give "Make changes to events" permission)
5. Add the JSON credentials as the `GOOGLE_SERVICE_ACCOUNT_JSON` secret

## Legacy ICS Feed

The original ICS feed workflow is still available at `.github/workflows/publish.yml` and publishes to GitHub Pages weekly.

## Development

```bash
# Install dependencies
pip install -r requirements.txt

# Test extraction
python scripts/extract_external_events.py

# Dry run upsert
DRY_RUN=1 D1_DB_NAME=test python scripts/upsert_external_events_to_d1.py
```
