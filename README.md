# GBUWH Calendar Sync

Automated pipelines for syncing underwater hockey events between GBUWH, Cloudflare D1, and Google Calendar.

## What It Does

```
GBUWH Website ──► D1 (external_events) ──► Your App ──► D1 (events) ──► Google Calendar
     │                                         │
     └── Ingest Pipeline (monthly)             └── Sync Pipeline (every 6 hours)
```

### 1. Ingest External Events (Monthly)

Scrapes national UK events from [gbuwh.co.uk/events](https://www.gbuwh.co.uk/events) and stores them in your D1 database for your app to display.

- **Runs:** 1st of each month at 06:00 UTC (+ manual trigger)
- **Source:** GBUWH website
- **Destination:** `external_events` table in D1

### 2. Sync to Google Calendar (Every 6 Hours)

Syncs your club's events from D1 to Google Calendar. Only syncs events that match the publish criteria (sessions, training, tournaments, etc.).

- **Runs:** Every 6 hours (+ manual trigger)
- **Source:** `events` table in D1
- **Destination:** Google Calendar

## Setup

### 1. Add GitHub Secrets

Go to your repo → Settings → Secrets and variables → Actions → New repository secret:

| Secret | Description |
|--------|-------------|
| `CLOUDFLARE_API_TOKEN` | Cloudflare API token with D1 edit permissions |
| `CLOUDFLARE_ACCOUNT_ID` | Your Cloudflare account ID |
| `D1_DB_NAME` | Name of your D1 database (e.g. `wwuwh-prod`) |
| `GOOGLE_CALENDAR_ID` | Your Google Calendar ID (e.g. `you@gmail.com`) |
| `GOOGLE_SERVICE_ACCOUNT_JSON` | Full JSON contents of your service account key |

### 2. Set Up Google Calendar Access

1. Go to [Google Cloud Console](https://console.cloud.google.com)
2. Create a project and enable the **Google Calendar API**
3. Create a **Service Account** → Keys → Add Key → JSON
4. Copy the JSON contents into `GOOGLE_SERVICE_ACCOUNT_JSON` secret
5. Share your Google Calendar with the service account email (found in the JSON as `client_email`)
6. Give it **"Make changes to events"** permission

### 3. Add D1 Columns (if needed)

The sync script needs these columns on your `events` table:

```sql
ALTER TABLE events ADD COLUMN google_event_id TEXT;
ALTER TABLE events ADD COLUMN google_last_synced_at TEXT;
```

## D1 Tables

### `external_events` (populated by ingest)

| Column | Type | Description |
|--------|------|-------------|
| id | TEXT | Primary key (UUID) |
| source | TEXT | Always `"gbuwh"` |
| source_event_id | TEXT | Original event ID from source |
| title | TEXT | Event title |
| description | TEXT | Event description |
| location | TEXT | Venue |
| starts_at_utc | TEXT | ISO 8601 UTC timestamp |
| ends_at_utc | TEXT | ISO 8601 UTC timestamp |
| timezone | TEXT | Original timezone |
| url | TEXT | Link to event page |

### `events` (your club events, synced to Google Calendar)

The sync script looks for events where:
- `kind` IN ('session', 'training', 'ladies', 'tournament', 'social', 'other')
- `status` = 'scheduled'
- `starts_at_utc` >= 14 days ago

## Manual Triggers

Go to Actions tab → select workflow → "Run workflow" to trigger manually.

## Dry Run Mode

Test without making changes by setting `DRY_RUN=1`:

```bash
DRY_RUN=1 python scripts/upsert_external_events_to_d1.py
DRY_RUN=1 python scripts/sync_google_calendar_from_d1.py
```

## Local Development

```bash
pip install -r requirements.txt

# Test scraping
python scripts/extract_external_events.py

# Test with dry run
DRY_RUN=1 D1_DB_NAME=your-db python scripts/upsert_external_events_to_d1.py
```
