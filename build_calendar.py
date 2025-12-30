import os
import re
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import requests
from bs4 import BeautifulSoup
from dateutil import parser as dtparser
from icalendar import Calendar, Event


BASE = "https://www.gbuwh.co.uk"
EVENTS_URL = os.getenv("EVENTS_URL", "https://www.gbuwh.co.uk/events")
TZ = ZoneInfo(os.getenv("TIMEZONE", "Europe/London"))
OUT_DIR = Path("site")

# Labels used on the detail pages (we use these to pick the “next line” as the value)
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

UA = {"User-Agent": "gbuwh-calendar-bot/1.0 (+GitHub Actions)"}


def clean_lines(html: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    lines = [ln.strip() for ln in soup.get_text("\n").splitlines()]
    return [ln for ln in lines if ln]


def parse_detail(detail_url: str) -> dict:
    r = requests.get(detail_url, headers=UA, timeout=30)
    r.raise_for_status()

    lines = clean_lines(r.text)

    # Title: first non-empty line after breadcrumb usually includes it, but safest is the first H1
    soup = BeautifulSoup(r.text, "html.parser")
    h1 = soup.find("h1")
    title = h1.get_text(strip=True) if h1 else lines[0]

    def get_value(label: str) -> str:
        try:
            i = lines.index(label)
        except ValueError:
            return ""
        # value is typically the next line that isn't another label
        for j in range(i + 1, min(i + 15, len(lines))):
            if lines[j] in DETAIL_LABELS:
                break
            if lines[j]:
                return lines[j]
        return ""

    location = get_value("Location")
    start_s = get_value("Start Date")
    end_s = get_value("End Date")

    # Overview text: collect everything after "Event overview" until “Back to Events”
    overview = ""
    if "Event overview" in lines:
        i = lines.index("Event overview")
        buff = []
        for j in range(i + 1, len(lines)):
            if "Back to Events" in lines[j]:
                break
            # ignore separator lines
            if lines[j] in DETAIL_LABELS or lines[j] == "* * *":
                continue
            buff.append(lines[j])
        overview = "\n".join(buff).strip()

    # Parse datetime strings like: 24/01/2026, 12:30 (UK day-first)
    def parse_dt(s: str) -> datetime | None:
        if not s:
            return None
        dt = dtparser.parse(s, dayfirst=True)
        return dt.replace(tzinfo=TZ) if dt.tzinfo is None else dt

    start_dt = parse_dt(start_s)
    end_dt = parse_dt(end_s)

    # Stable event id from /events/detail/813
    m = re.search(r"/events/detail/(\d+)", detail_url)
    event_id = m.group(1) if m else detail_url

    cancelled = title.lower().startswith("cancelled")

    return {
        "id": event_id,
        "title": title,
        "url": detail_url,
        "location": location,
        "start": start_dt,
        "end": end_dt,
        "overview": overview,
        "cancelled": cancelled,
    }


def get_detail_links() -> list[str]:
    r = requests.get(EVENTS_URL, headers=UA, timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    links = set()
    for a in soup.select('a[href^="/events/detail/"]'):
        href = a.get("href")
        if href:
            links.add(BASE + href)

    return sorted(links)


def build_ics(events: list[dict]) -> bytes:
    cal = Calendar()
    cal.add("prodid", "-//GBUWH Events Feed//gbuwh-calendar//EN")
    cal.add("version", "2.0")
    cal.add("x-wr-calname", "GBUWH Events")
    cal.add("x-wr-timezone", "Europe/London")

    now = datetime.now(tz=TZ)

    for e in events:
        if not e["start"]:
            continue

        ve = Event()
        ve.add("uid", f'{e["id"]}@gbuwh-calendar')
        ve.add("dtstamp", now)
        ve.add("summary", e["title"])
        ve.add("dtstart", e["start"])
        if e["end"]:
            ve.add("dtend", e["end"])
        if e["location"]:
            ve.add("location", e["location"])
        ve.add("url", e["url"])

        desc_parts = [e["url"]]
        if e["overview"]:
            desc_parts.append(e["overview"])
        ve.add("description", "\n\n".join(desc_parts))

        if e["cancelled"]:
            ve.add("status", "CANCELLED")

        cal.add_component(ve)

    return cal.to_ical()


def write_site(ics_bytes: bytes, count: int):
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    (OUT_DIR / "calendar.ics").write_bytes(ics_bytes)
    (OUT_DIR / "index.html").write_text(
        f"""<!doctype html>
<html><head><meta charset="utf-8"><title>GBUWH Events Feed</title></head>
<body>
  <h1>GBUWH Events Feed</h1>
  <p><a href="calendar.ics">calendar.ics</a></p>
  <p>Events: {count}</p>
  <p>Last generated: {datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")}</p>
</body></html>
""",
        encoding="utf-8",
    )


def main():
    detail_links = get_detail_links()
    print(f"Found {len(detail_links)} event detail links")

    events = [parse_detail(url) for url in detail_links]
    events = [e for e in events if e["start"] is not None]

    ics = build_ics(events)
    write_site(ics, len(events))
    print(f"Wrote site/calendar.ics with {len(events)} events")


if __name__ == "__main__":
    main()
