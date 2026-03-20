"""Gmail parser — fetches emails via Gmail API (read-only).

Requires:
  - google-api-python-client, google-auth-oauthlib (install with `pip install parsers[google]`)
  - OAuth credentials at ~/.config/refs/credentials.json
  - First run opens browser for consent (gmail.readonly scope)

Yields one record per email thread (grouped) or per message (raw).
"""

import base64
import logging
import re
from collections.abc import Iterator
from pathlib import Path

log = logging.getLogger("parsers")

_SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]
_CONFIG_DIR = Path.home() / ".config" / "refs"
_CREDENTIALS_FILE = _CONFIG_DIR / "credentials.json"
_TOKEN_FILE = _CONFIG_DIR / "gmail_token.json"

_SKIP_LABELS = {"SPAM", "TRASH", "CATEGORY_PROMOTIONS", "CATEGORY_SOCIAL", "CATEGORY_UPDATES", "CATEGORY_FORUMS"}
_NOREPLY = re.compile(r"(noreply|no-reply|donotreply|mailer-daemon|notifications?@)", re.I)


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
                raise FileNotFoundError(
                    f"OAuth credentials not found at {_CREDENTIALS_FILE}. "
                    "Download from Google Cloud Console."
                )
            flow = InstalledAppFlow.from_client_secrets_file(str(_CREDENTIALS_FILE), _SCOPES)
            creds = flow.run_local_server(port=0)
        _TOKEN_FILE.parent.mkdir(parents=True, exist_ok=True)
        _TOKEN_FILE.write_text(creds.to_json())
    return creds


def _get_header(headers: list, name: str) -> str:
    for h in headers:
        if h["name"].lower() == name.lower():
            return h["value"]
    return ""


def _extract_body(payload: dict) -> str:
    if payload.get("mimeType") == "text/plain" and payload.get("body", {}).get("data"):
        return base64.urlsafe_b64decode(payload["body"]["data"]).decode("utf-8", errors="replace")
    for part in payload.get("parts", []):
        if part.get("mimeType") == "text/plain" and part.get("body", {}).get("data"):
            return base64.urlsafe_b64decode(part["body"]["data"]).decode("utf-8", errors="replace")
        if part.get("parts"):
            result = _extract_body(part)
            if result:
                return result
    return ""


def parse(path: Path | None = None, *, max_results: int = 2000, **kwargs) -> Iterator[dict]:
    """Yield one record per Gmail message.

    Args:
        path: If given, read from a JSON file (gmail_parsed.json format).
              If None, fetch from Gmail API.
        max_results: Max messages to fetch from API (default 2000).
    """
    if path and path.exists():
        import json
        with open(path) as f:
            data = json.load(f)
        # Thread format
        for thread in data.get("threads", []):
            if thread.get("message_count", 0) < 2:
                continue
            subject = thread.get("subject", "(no subject)")
            participants = ", ".join(thread.get("participants", [])[:5])
            preview = thread.get("combined_preview", "")[:2000]
            direction = thread.get("direction", "unknown")
            date_range = thread.get("date_range", {})
            date = date_range.get("last", date_range.get("first", ""))[:10]
            yield {
                "id": f"gmail_thread_{thread['thread_id']}",
                "source": "gmail",
                "title": f"[{direction}] {subject}"[:120],
                "date": date,
                "text": f"Subject: {subject} | Participants: {participants} | {preview}",
                "metadata": {
                    "participants": thread.get("participants", []),
                    "message_count": thread.get("message_count", 0),
                    "direction": direction,
                    "channel": "operational",
                },
            }
        # Raw message format fallback
        for msg in data.get("messages", []):
            subject = msg.get("subject", "(no subject)")
            from_name = msg.get("from_name", "")
            body = msg.get("body_preview", "") or msg.get("snippet", "")
            direction = "sent" if msg.get("is_sent") else "received"
            yield {
                "id": f"gmail_{msg['id']}",
                "source": "gmail",
                "title": f"[{direction}] {subject}"[:120],
                "date": msg.get("date", "")[:10],
                "text": f"From: {from_name} | Subject: {subject} | {body[:2000]}",
                "metadata": {"from": msg.get("from", ""), "channel": "operational"},
            }
        return

    # API mode
    from googleapiclient.discovery import build

    creds = _get_credentials()
    service = build("gmail", "v1", credentials=creds)

    # Fetch message IDs
    msg_ids = []
    page_token = None
    while len(msg_ids) < max_results:
        result = service.users().messages().list(
            userId="me", maxResults=min(100, max_results - len(msg_ids)),
            pageToken=page_token,
        ).execute()
        msg_ids.extend(m["id"] for m in result.get("messages", []))
        page_token = result.get("nextPageToken")
        if not page_token:
            break

    log.info(f"Gmail: fetching {len(msg_ids)} messages")

    # Fetch and parse each message
    count = 0
    for msg_id in msg_ids:
        try:
            msg = service.users().messages().get(userId="me", id=msg_id, format="full").execute()
            payload = msg.get("payload", {})
            headers = payload.get("headers", [])
            labels = set(msg.get("labelIds", []))

            if labels & _SKIP_LABELS:
                continue
            from_addr = _get_header(headers, "From")
            if _NOREPLY.search(from_addr):
                continue
            if _get_header(headers, "List-Unsubscribe"):
                continue

            subject = _get_header(headers, "Subject") or "(no subject)"
            body = _extract_body(payload)[:3000]
            body = re.sub(r"\n{3,}", "\n\n", body)
            body = re.sub(r"--\s*\n.*", "", body, flags=re.DOTALL)

            from_name = from_addr.split("<")[0].strip().strip('"') if "<" in from_addr else from_addr
            is_sent = "SENT" in labels

            import email.utils
            date = ""
            date_str = _get_header(headers, "Date")
            if date_str:
                try:
                    date = email.utils.parsedate_to_datetime(date_str).strftime("%Y-%m-%d")
                except Exception:
                    pass

            yield {
                "id": f"gmail_{msg['id']}",
                "source": "gmail",
                "title": f"[{'sent' if is_sent else 'received'}] {subject}"[:120],
                "date": date,
                "text": f"From: {from_name}\nSubject: {subject}\n\n{body}",
                "metadata": {
                    "from": from_addr,
                    "is_sent": is_sent,
                    "thread_id": msg.get("threadId", ""),
                    "channel": "operational",
                },
            }
            count += 1
        except Exception:
            log.exception(f"Failed to fetch message {msg_id}")

    log.info(f"Gmail: emitted {count} messages")
