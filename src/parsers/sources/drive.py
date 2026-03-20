"""Google Drive parser — fetches files via Drive API (read-only).

Requires:
  - google-api-python-client, google-auth-oauthlib (install with `pip install parsers[google]`)
  - OAuth credentials at ~/.config/refs/credentials.json

Lists files, exports Google Docs/Sheets as text. Skips binary and trashed items.
"""

import io
import logging
from collections.abc import Iterator
from pathlib import Path

log = logging.getLogger("parsers")

_SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]
_CONFIG_DIR = Path.home() / ".config" / "refs"
_CREDENTIALS_FILE = _CONFIG_DIR / "credentials.json"
_TOKEN_FILE = _CONFIG_DIR / "drive_token.json"

_EXPORTABLE = {
    "application/vnd.google-apps.document": ("text/plain", "Google Doc"),
    "application/vnd.google-apps.spreadsheet": ("text/csv", "Google Sheet"),
    "application/vnd.google-apps.presentation": ("text/plain", "Google Slides"),
}
_SKIP_TYPES = {
    "application/vnd.google-apps.folder", "application/vnd.google-apps.shortcut",
    "application/vnd.google-apps.form", "application/vnd.google-apps.map",
}


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


def parse(path: Path | None = None, *, max_results: int = 1000, **kwargs) -> Iterator[dict]:
    """Yield one record per Drive file with text content.

    Args:
        path: If given, read from JSON file (drive_parsed.json format).
              If None, fetch from Google Drive API.
        max_results: Max files to fetch from API (default 1000).
    """
    if path and path.exists():
        import json
        with open(path) as f:
            data = json.load(f)

        for f_entry in data.get("files", []):
            name = f_entry.get("name", "")
            folder = f_entry.get("folder_path", "")
            content = f_entry.get("content_preview", "")
            description = f_entry.get("description", "")
            file_type = f_entry.get("file_type", "other")

            if not content and not description:
                continue

            text_parts = [name]
            if folder:
                text_parts.append(f"Folder: {folder}")
            if description:
                text_parts.append(description)
            if content:
                text_parts.append(content[:1500])

            title = f"[{file_type}] {folder}/{name}" if folder else f"[{file_type}] {name}"
            yield {
                "id": f"drive_{f_entry['id']}",
                "source": "drive",
                "title": title[:120],
                "date": f_entry.get("modified", "")[:10],
                "text": " | ".join(text_parts),
                "metadata": {
                    "mime_type": f_entry.get("mime_type", ""),
                    "folder_path": folder,
                    "file_type": file_type,
                    "channel": "operational",
                },
            }
        return

    # API mode
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaIoBaseDownload

    creds = _get_credentials()
    service = build("drive", "v3", credentials=creds)

    page_token = None
    count = 0

    while count < max_results:
        result = service.files().list(
            q="trashed = false",
            fields="nextPageToken, files(id, name, mimeType, modifiedTime, description, parents, size)",
            pageSize=min(100, max_results - count),
            pageToken=page_token,
            orderBy="modifiedTime desc",
        ).execute()

        for f in result.get("files", []):
            mime = f.get("mimeType", "")
            if mime in _SKIP_TYPES:
                continue

            name = f.get("name", "")
            file_id = f["id"]
            modified = f.get("modifiedTime", "")
            description = f.get("description", "")

            # Try to get text content
            content = ""
            if mime in _EXPORTABLE:
                export_mime, file_type = _EXPORTABLE[mime]
                try:
                    request = service.files().export_media(fileId=file_id, mimeType=export_mime)
                    buf = io.BytesIO()
                    downloader = MediaIoBaseDownload(buf, request)
                    done = False
                    while not done:
                        _, done = downloader.next_chunk()
                    content = buf.getvalue().decode("utf-8", errors="replace")[:2000]
                except Exception:
                    pass
            else:
                file_type = mime.split("/")[-1] if "/" in mime else "other"

            if not content and not description:
                continue

            text_parts = [name]
            if description:
                text_parts.append(description)
            if content:
                text_parts.append(content)

            yield {
                "id": f"drive_{file_id}",
                "source": "drive",
                "title": f"[{file_type}] {name}"[:120],
                "date": modified[:10],
                "text": " | ".join(text_parts),
                "metadata": {
                    "mime_type": mime,
                    "file_type": file_type,
                    "channel": "operational",
                },
            }
            count += 1

        page_token = result.get("nextPageToken")
        if not page_token:
            break

    log.info(f"Drive: emitted {count} files")
