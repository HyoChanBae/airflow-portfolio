import hashlib
import re
from datetime import date, datetime
from zoneinfo import ZoneInfo

import requests
from icalendar import Calendar
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from macro_calendar.models import MacroCalendarEvent


BLS_ICS_URL = "https://www.bls.gov/schedule/news_release/bls.ics"
ET = ZoneInfo("America/New_York")
UTC = ZoneInfo("UTC")

INDICATOR_MAP = {
    "consumer price index": {
        "code": "CPI",
        "name": "Consumer Price Index",
        "category": "INFLATION",
        "importance": 5,
        "url": "https://www.bls.gov/cpi/",
    },
    "producer price index": {
        "code": "PPI",
        "name": "Producer Price Index",
        "category": "INFLATION",
        "importance": 4,
        "url": "https://www.bls.gov/ppi/",
    },
    "employment situation": {
        "code": "NFP",
        "name": "Employment Situation",
        "category": "EMPLOYMENT",
        "importance": 5,
        "url": "https://www.bls.gov/ces/",
    },
    "job openings and labor turnover survey": {
        "code": "JOLTS",
        "name": "Job Openings and Labor Turnover Survey",
        "category": "EMPLOYMENT",
        "importance": 4,
        "url": "https://www.bls.gov/jlt/",
    },
}

REFERENCE_PERIOD_PATTERN = re.compile(
    r"\b(?:January|February|March|April|May|June|July|August|September|"
    r"October|November|December)\s+\d{4}\b|\bQ[1-4]\s+\d{4}\b",
    re.IGNORECASE,
)


def _build_session() -> requests.Session:
    retry = Retry(
        total=3,
        connect=3,
        read=3,
        backoff_factor=1,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET"}),
    )
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


def _match_indicator(summary: str) -> dict | None:
    normalized_summary = summary.casefold()
    for keyword, config in INDICATOR_MAP.items():
        if keyword in normalized_summary:
            return config
    return None


def _reference_period(summary: str, description: str) -> str | None:
    match = REFERENCE_PERIOD_PATTERN.search(f"{summary} {description}")
    return match.group(0) if match else None


def _event_key(source_event_id: str) -> str:
    return hashlib.sha256(f"BLS|{source_event_id}".encode("utf-8")).hexdigest()


def collect_bls_events(user_agent: str) -> list[dict]:
    headers = {
        "Accept": "text/calendar",
        "User-Agent": user_agent,
    }
    with _build_session() as session:
        response = session.get(BLS_ICS_URL, headers=headers, timeout=(10, 30))
        response.raise_for_status()

    if b"BEGIN:VCALENDAR" not in response.content[:1024]:
        content_type = response.headers.get("Content-Type", "unknown")
        raise ValueError(
            "BLS response is not an iCalendar document "
            f"(Content-Type: {content_type})"
        )

    calendar = Calendar.from_ical(response.content)
    events = []

    for component in calendar.walk("VEVENT"):
        summary = str(component.get("summary", "")).strip()
        indicator = _match_indicator(summary)
        if not indicator:
            continue

        release_time = component.decoded("dtstart")
        if isinstance(release_time, date) and not isinstance(release_time, datetime):
            continue
        if release_time.tzinfo is None:
            release_time = release_time.replace(tzinfo=ET)
        release_at_utc = release_time.astimezone(UTC)

        uid = str(component.get("uid", "")).strip()
        if not uid:
            uid = f"{indicator['code']}|{release_at_utc.isoformat()}"

        description = str(component.get("description", "")).strip()
        component_url = str(component.get("url", "")).strip()

        event = MacroCalendarEvent(
            event_key=_event_key(uid),
            source_event_id=uid,
            indicator_code=indicator["code"],
            indicator_name=indicator["name"],
            category=indicator["category"],
            source="BLS",
            reference_period=_reference_period(summary, description),
            release_at_utc=release_at_utc,
            importance=indicator["importance"],
            event_url=component_url or indicator["url"],
            status="SCHEDULED",
            raw_title=summary,
        )
        events.append(event.to_dict())

    if not events:
        raise ValueError("BLS calendar returned no supported release events")

    return events
