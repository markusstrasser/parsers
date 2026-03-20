"""Raycast export parser — reads decrypted Raycast JSON export.

Extracts AI chats and snippets from Raycast data export.
"""

import json
import logging
from collections.abc import Iterator
from pathlib import Path

log = logging.getLogger("parsers")


def parse(path: Path | None = None, **kwargs) -> Iterator[dict]:
    """Yield records from Raycast export (AI chats + snippets).

    Args:
        path: Path to decrypted Raycast JSON file.
    """
    if path is None:
        raise ValueError("raycast parser requires path to Raycast export JSON")
    if not path.exists():
        raise FileNotFoundError(f"Raycast export not found: {path}")

    with open(path) as f:
        data = json.load(f)

    count = 0

    # AI chats
    openai_data = data.get("builtin_package_open-ai", {})
    for chat in openai_data.get("aiChats", []):
        try:
            if not isinstance(chat, dict) or "record" not in chat:
                continue
            record = chat["record"]
            messages = chat.get("messages", [])
            texts = [m.get("text", "") for m in messages if isinstance(m, dict) and m.get("text")]
            if not texts:
                continue

            title = record.get("title", "").strip() or (texts[0][:100] + "...")
            date_raw = record.get("modifiedAt", record.get("createdAt", ""))
            date = date_raw[:10] if date_raw else ""

            yield {
                "id": f"raycast_chat_{record.get('id', '')[:8]}",
                "source": "raycast",
                "title": f"[AI Chat] {title[:100]}",
                "date": date,
                "text": f"{title}\n\n" + "\n\n".join(texts),
                "metadata": {
                    "type": "ai-chat",
                    "model": record.get("model", ""),
                    "channel": "ai_conversation",
                },
            }
            count += 1
        except Exception:
            log.exception("Failed to parse Raycast AI chat")

    # Snippets
    snippets_data = data.get("builtin_package_snippets", {})
    for snippet in snippets_data.get("snippets", []):
        text = snippet.get("text", "")
        if len(text) < 20:
            continue
        yield {
            "id": f"raycast_snippet_{snippet.get('keyword', 'unknown')}",
            "source": "raycast",
            "title": f"[Snippet] {snippet.get('name', 'Untitled')}",
            "date": "",
            "text": text,
            "metadata": {
                "type": "snippet",
                "keyword": snippet.get("keyword", ""),
                "channel": "authored",
            },
        }
        count += 1

    log.info(f"Raycast: emitted {count} items")
