"""Google Calendar parser — fetches events via Calendar API (read-only).

Requires:
  - google-api-python-client, google-auth-oauthlib (install with `pip install parsers[google]`)
  - OAuth credentials at ~/.config/refs/credentials.json

Yields one record per calendar event. Deduplicates recurring events.
"""

import logging
from collections.abc import Iterator
from pathlib import Path

log = logging.getLogger("parsers")

_SCOPES = ["https://www.googleapis.com/auth/calendar.readonly"]
_CONFIG_DIR = Path.home() / ".config" / "refs"
_CREDENTIALS_FILE = _CONFIG_DIR / "credentials.json"
_TOKEN_FILE = _CONFIG_DIR / "calendar_token.json"

_SKIP_CALENDARS = {"holidays", "Holidays", "Birthdays", "Other calendars"}


def _get_credentials():
    from google.auth.transport.requests import Request
    from google.oauth2.credentials import Credentials
    from google_auth_oauthlib.flow import InstalledAppFlow

    creds = None
    if _TOKEN_FILE.exists():
        creds = Credentials.from_authorized_user_file(str(_TOKEN_FILE), _SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            if not _CREDENTIALS_FILE.exists():
                raise FileNotFoundError(f"OAuth credentials not found at {_CREDENTIALS_FILE}")
            flow = InstalledAppFlow.from_client_secrets_file(str(_CREDENTIALS_FILE), _SCOPES)
            creds = flow.run_local_server(port=0)
        _TOKEN_FILE.parent.mkdir(parents=True, exist_ok=True)
        _TOKEN_FILE.write_text(creds.to_json())
    return creds


def parse(path: Path | None = None, **kwargs) -> Iterator[dict]:
    """Yield one record per calendar event.

    Args:
        path: If given, read from JSON file (calendar_parsed.json or calendar_derived.json).
              If None, fetch from Google Calendar API.
    """
    if path and path.exists():
        import json
        with open(path) as f:
            data = json.load(f)

        events = data.get("events", [])
        # Dedup recurring: keep latest instance per title
        seen_recurring: set[str] = set()
        for ev in sorted(events, key=lambda e: e.get("start", ""), reverse=True):
            if ev.get("recurring"):
                title = ev.get("summary", "")
                if title in seen_recurring:
                    continue
                seen_recurring.add(title)

            summary = ev.get("summary", "(no title)")
            description = ev.get("description", "")
            location = ev.get("location", "")
            attendees = ", ".join(ev.get("attendees", []))
            cal_name = ev.get("calendar_name", "")

            text_parts = [summary]
            if description:
                text_parts.append(description[:1000])
            if location:
                text_parts.append(f"Location: {location}")
            if attendees:
                text_parts.append(f"With: {attendees}")

            yield {
                "id": f"calendar_{ev['id']}",
                "source": "calendar",
                "title": f"[{cal_name}] {summary}"[:120],
                "date": ev.get("start", "")[:10],
                "text": " | ".join(text_parts),
                "metadata": {
                    "start": ev.get("start", ""),
                    "end": ev.get("end", ""),
                    "location": location,
                    "calendar": cal_name,
                    "recurring": ev.get("recurring", False),
                    "channel": "operational",
                },
            }
        return

    # API mode
    from googleapiclient.discovery import build

    creds = _get_credentials()
    service = build("calendar", "v3", credentials=creds)

    # List calendars
    calendars = service.calendarList().list().execute().get("items", [])
    count = 0

    for cal in calendars:
        cal_name = cal.get("summary", "")
        if cal_name in _SKIP_CALENDARS:
            continue

        page_token = None
        while True:
            result = service.events().list(
                calendarId=cal["id"], maxResults=250,
                singleEvents=True, orderBy="startTime",
                pageToken=page_token,
            ).execute()

            for ev in result.get("items", []):
                if ev.get("status") == "cancelled":
                    continue

                start = ev.get("start", {})
                end = ev.get("end", {})
                start_str = start.get("dateTime", start.get("date", ""))
                summary = ev.get("summary", "(no title)")
                description = ev.get("description", "")
                location = ev.get("location", "")
                attendees = [a.get("email", "") for a in ev.get("attendees", [])]

                text_parts = [summary]
                if description:
                    text_parts.append(description[:1000])
                if location:
                    text_parts.append(f"Location: {location}")

                yield {
                    "id": f"calendar_{ev['id']}",
                    "source": "calendar",
                    "title": f"[{cal_name}] {summary}"[:120],
                    "date": start_str[:10],
                    "text": " | ".join(text_parts),
                    "metadata": {
                        "start": start_str,
                        "end": end.get("dateTime", end.get("date", "")),
                        "location": location,
                        "calendar": cal_name,
                        "channel": "operational",
                    },
                }
                count += 1

            page_token = result.get("nextPageToken")
            if not page_token:
                break

    log.info(f"Calendar: emitted {count} events")
