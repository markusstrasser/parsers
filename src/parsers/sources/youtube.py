"""YouTube parser — fetches liked videos and playlists via YouTube Data API.

Requires:
  - google-api-python-client, google-auth-oauthlib (install with `pip install parsers[google]`)
  - OAuth credentials at ~/.config/refs/credentials.json

Note: Watch history is NOT available via API (use Google Takeout instead).
"""

import logging
from collections.abc import Iterator
from pathlib import Path

log = logging.getLogger("parsers")

_SCOPES = ["https://www.googleapis.com/auth/youtube.readonly"]
_CONFIG_DIR = Path.home() / ".config" / "refs"
_CREDENTIALS_FILE = _CONFIG_DIR / "credentials.json"
_TOKEN_FILE = _CONFIG_DIR / "youtube_token.json"


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


def parse(path: Path | None = None, *, max_results: int = 500, **kwargs) -> Iterator[dict]:
    """Yield one record per YouTube video (liked or from playlists).

    Args:
        path: If given, read from JSON file (youtube_parsed.json format).
              If None, fetch from YouTube Data API.
        max_results: Max items per category (default 500).
    """
    if path and path.exists():
        import json
        with open(path) as f:
            data = json.load(f)

        for item in data.get("items", []):
            item_type = item.get("type", "video")
            if item_type == "subscription":
                continue

            title = item.get("title", "")
            channel = item.get("channel", "")
            description = item.get("description", "")

            text_parts = [title]
            if channel:
                text_parts.append(f"Channel: {channel}")
            if description:
                text_parts.append(description[:1000])

            prefix = "[liked]" if item_type == "liked" else "[playlist]"
            yield {
                "id": f"youtube_{item['id']}",
                "source": "youtube",
                "title": f"{prefix} {title}"[:120],
                "date": item.get("published_at", "")[:10],
                "text": " | ".join(text_parts),
                "metadata": {
                    "channel_name": channel,
                    "type": item_type,
                    "url": item.get("url", ""),
                    "channel": "curated",
                },
            }
        return

    # API mode
    from googleapiclient.discovery import build

    creds = _get_credentials()
    service = build("youtube", "v3", credentials=creds)

    # Get liked videos playlist
    channels = service.channels().list(part="contentDetails", mine=True).execute()
    liked_playlist = channels["items"][0]["contentDetails"]["relatedPlaylists"].get("likes")

    count = 0
    if liked_playlist:
        page_token = None
        while count < max_results:
            result = service.playlistItems().list(
                part="snippet,contentDetails",
                playlistId=liked_playlist,
                maxResults=min(50, max_results - count),
                pageToken=page_token,
            ).execute()

            for item in result.get("items", []):
                snippet = item["snippet"]
                video_id = snippet.get("resourceId", {}).get("videoId", "")
                title = snippet.get("title", "")
                channel_name = snippet.get("videoOwnerChannelTitle", "")
                description = snippet.get("description", "")
                published = snippet.get("publishedAt", "")

                text_parts = [title]
                if channel_name:
                    text_parts.append(f"Channel: {channel_name}")
                if description:
                    text_parts.append(description[:1000])

                yield {
                    "id": f"youtube_{video_id}",
                    "source": "youtube",
                    "title": f"[liked] {title}"[:120],
                    "date": published[:10],
                    "text": " | ".join(text_parts),
                    "metadata": {
                        "channel_name": channel_name,
                        "type": "liked",
                        "url": f"https://youtube.com/watch?v={video_id}",
                        "channel": "curated",
                    },
                }
                count += 1

            page_token = result.get("nextPageToken")
            if not page_token:
                break

    log.info(f"YouTube: emitted {count} videos")
