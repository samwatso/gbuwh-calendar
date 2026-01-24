#!/usr/bin/env python3
"""
Extract external events from GBUWH website.

Scrapes https://www.gbuwh.co.uk/events and outputs normalized JSON
with UTC timestamps suitable for upserting to D1.

Usage:
    python scripts/extract_external_events.py
    python scripts/extract_external_events.py --output events.json
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import uuid
from datetime import datetime, timezone
from typing import Any
from zoneinfo import ZoneInfo

import requests
from bs4 import BeautifulSoup
from dateutil import parser as dtparser

# Configuration
BASE_URL = "https://www.gbuwh.co.uk"
EVENTS_URL = os.getenv("EVENTS_URL", "https://www.gbuwh.co.uk/events")
SOURCE_TZ = ZoneInfo(os.getenv("TIMEZONE", "Europe/London"))
SOURCE_NAME = "gbuwh"  # Stable source identifier for D1

USER_AGENT = {"User-Agent": "gbuwh-calendar-bot/1.0 (+GitHub Actions)"}

# Labels used on the detail pages
DETAIL_LABELS = {
    "Type of event",
    "Location",
    "Is this a BOA event?",
    "Event Owner",
    "Start Date",
    "End Date",
    "Add to Calendar",
    "Tier",
    "No. of teams",
    "Age Categories:",
    "Team Registration & Edit Deadlines",
    "Event overview",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


def clean_lines(html: str) -> list[str]:
    """Extract clean text lines from HTML."""
    soup = BeautifulSoup(html, "html.parser")
    lines = [ln.strip() for ln in soup.get_text("\n").splitlines()]
    return [ln for ln in lines if ln]


def parse_datetime(s: str, tz: ZoneInfo) -> datetime | None:
    """Parse datetime string (UK day-first format) and return UTC datetime."""
    if not s:
        return None
    try:
        dt = dtparser.parse(s, dayfirst=True)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=tz)
        return dt.astimezone(timezone.utc)
    except Exception as e:
        logger.warning(f"Failed to parse datetime '{s}': {e}")
        return None


def parse_event_detail(detail_url: str) -> dict[str, Any] | None:
    """Parse a single event detail page and return normalized event dict."""
    try:
        r = requests.get(detail_url, headers=USER_AGENT, timeout=30)
        r.raise_for_status()
    except requests.RequestException as e:
        logger.error(f"Failed to fetch {detail_url}: {e}")
        return None

    lines = clean_lines(r.text)
    soup = BeautifulSoup(r.text, "html.parser")

    # Title from H1
    h1 = soup.find("h1")
    title = h1.get_text(strip=True) if h1 else (lines[0] if lines else "Unknown Event")

    def get_value(label: str) -> str:
        try:
            i = lines.index(label)
        except ValueError:
            return ""
        for j in range(i + 1, min(i + 15, len(lines))):
            if lines[j] in DETAIL_LABELS:
                break
            if lines[j]:
                return lines[j]
        return ""

    location = get_value("Location")
    start_s = get_value("Start Date")
    end_s = get_value("End Date")
    event_type = get_value("Type of event")

    # Overview text
    overview = ""
    if "Event overview" in lines:
        i = lines.index("Event overview")
        buff = []
        for j in range(i + 1, len(lines)):
            if "Back to Events" in lines[j]:
                break
            if lines[j] in DETAIL_LABELS or lines[j] == "* * *":
                continue
            buff.append(lines[j])
        overview = "\n".join(buff).strip()

    # Parse datetimes to UTC
    start_dt = parse_datetime(start_s, SOURCE_TZ)
    end_dt = parse_datetime(end_s, SOURCE_TZ)

    if start_dt is None:
        logger.warning(f"Skipping event without start date: {detail_url}")
        return None

    # Extract stable event ID from URL: /events/detail/813 -> "813"
    m = re.search(r"/events/detail/(\d+)", detail_url)
    source_event_id = m.group(1) if m else str(uuid.uuid4())

    # Build description
    desc_parts = [detail_url]
    if event_type:
        desc_parts.append(f"Type: {event_type}")
    if overview:
        desc_parts.append(overview)
    description = "\n\n".join(desc_parts)

    return {
        "source": SOURCE_NAME,
        "source_event_id": source_event_id,
        "title": title,
        "description": description,
        "location": location or None,
        "starts_at_utc": start_dt.isoformat(),
        "ends_at_utc": end_dt.isoformat() if end_dt else None,
        "timezone": str(SOURCE_TZ),
        "url": detail_url,
    }


def get_event_detail_links() -> list[str]:
    """Fetch all event detail page URLs from the events listing."""
    try:
        r = requests.get(EVENTS_URL, headers=USER_AGENT, timeout=30)
        r.raise_for_status()
    except requests.RequestException as e:
        logger.error(f"Failed to fetch events listing: {e}")
        return []

    soup = BeautifulSoup(r.text, "html.parser")
    links = set()
    for a in soup.select('a[href^="/events/detail/"]'):
        href = a.get("href")
        if href:
            links.add(BASE_URL + href)

    return sorted(links)


def extract_all_events() -> list[dict[str, Any]]:
    """Extract all events from GBUWH website."""
    logger.info(f"Fetching events from {EVENTS_URL}")

    detail_links = get_event_detail_links()
    logger.info(f"Found {len(detail_links)} event detail links")

    events = []
    for url in detail_links:
        logger.info(f"Parsing: {url}")
        event = parse_event_detail(url)
        if event:
            events.append(event)

    logger.info(f"Successfully extracted {len(events)} events")
    return events


def main() -> int:
    parser = argparse.ArgumentParser(description="Extract GBUWH events")
    parser.add_argument(
        "--output", "-o",
        type=str,
        default=None,
        help="Output JSON file path (default: stdout)"
    )
    args = parser.parse_args()

    events = extract_all_events()

    output_json = json.dumps(events, indent=2, ensure_ascii=False)

    if args.output:
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(output_json)
        logger.info(f"Wrote {len(events)} events to {args.output}")
    else:
        print(output_json)

    return 0


if __name__ == "__main__":
    sys.exit(main())
