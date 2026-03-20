"""iMessage parser — reads macOS chat.db directly.

Merges individual messages into per-conversation records (grouped by
contact/chat). One record per conversation, 12K char cap.

Default path: ~/Library/Messages/chat.db
Platform: macOS only (requires Full Disk Access for chat.db).
"""

import logging
import sqlite3
from collections import defaultdict
from collections.abc import Iterator
from datetime import datetime, timezone
from pathlib import Path

log = logging.getLogger("parsers")

DEFAULT_DB = Path.home() / "Library" / "Messages" / "chat.db"

# Apple's epoch offset (2001-01-01 vs 1970-01-01)
_APPLE_EPOCH = 978307200

# Short codes and spam numbers to filter
_SPAM_HANDLES = {"40404", "7277", "30300", "88877", "22000", "55555", "266278"}


def _apple_to_iso(apple_date: int) -> str:
    """Convert Apple Core Data timestamp (nanoseconds since 2001) to ISO date."""
    if not apple_date:
        return ""
    try:
        unix_ts = apple_date / 1_000_000_000 + _APPLE_EPOCH
        return datetime.fromtimestamp(unix_ts, tz=timezone.utc).strftime("%Y-%m-%d")
    except (OSError, ValueError):
        return ""


def _apple_to_datetime_str(apple_date: int) -> str:
    """Convert Apple timestamp to readable datetime for conversation text."""
    if not apple_date:
        return ""
    try:
        unix_ts = apple_date / 1_000_000_000 + _APPLE_EPOCH
        return datetime.fromtimestamp(unix_ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
    except (OSError, ValueError):
        return ""


def parse(path: Path | None = None, **kwargs) -> Iterator[dict]:
    """Yield one record per conversation from iMessage database.

    Args:
        path: Path to chat.db. Defaults to ~/Library/Messages/chat.db.
    """
    db_path = path or DEFAULT_DB
    if not db_path.exists():
        raise FileNotFoundError(f"iMessage database not found: {db_path}")

    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row

    # Chat display names for group chats
    chat_names: dict[int, str] = {}
    for row in conn.execute("SELECT ROWID, display_name, chat_identifier FROM chat"):
        chat_names[row["ROWID"]] = row["display_name"] or row["chat_identifier"] or ""

    # Message → chat mapping
    msg_to_chat: dict[int, int] = {}
    for row in conn.execute("SELECT message_id, chat_id FROM chat_message_join"):
        msg_to_chat[row["message_id"]] = row["chat_id"]

    query = """
        SELECT m.ROWID as msg_id, m.text, m.is_from_me, m.date,
               h.id as contact_id, m.cache_roomnames
        FROM message m
        LEFT JOIN handle h ON m.handle_id = h.ROWID
        WHERE m.text IS NOT NULL AND length(m.text) > 0
          AND m.associated_message_type = 0
        ORDER BY m.date ASC
    """

    # Group messages by conversation
    by_chat: dict[str, list[dict]] = defaultdict(list)
    skipped = 0

    for row in conn.execute(query):
        contact = row["contact_id"] or ""

        # Resolve contact from chat if missing
        if not contact:
            chat_id = msg_to_chat.get(row["msg_id"])
            contact = chat_names.get(chat_id, "") if chat_id else ""

        # Filter spam
        clean = contact.replace("+", "").replace("-", "").replace(" ", "")
        if clean in _SPAM_HANDLES or (clean.isdigit() and len(clean) <= 5):
            skipped += 1
            continue

        date_str = _apple_to_iso(row["date"])
        if not date_str:
            continue

        chat_id = msg_to_chat.get(row["msg_id"])
        chat_name = chat_names.get(chat_id, "") if chat_id else ""
        chat_key = chat_name or contact

        by_chat[chat_key].append({
            "contact": contact,
            "is_from_me": bool(row["is_from_me"]),
            "text": row["text"],
            "date": date_str,
            "datetime": _apple_to_datetime_str(row["date"]),
            "is_group": bool(row["cache_roomnames"]),
        })

    conn.close()
    log.info(f"iMessage: {sum(len(v) for v in by_chat.values())} messages in {len(by_chat)} conversations (skipped {skipped} spam)")

    # Yield one record per conversation
    for chat_key, msgs in by_chat.items():
        if len(msgs) < 2:
            continue

        lines = []
        for m in msgs:
            sender = "me" if m["is_from_me"] else m["contact"]
            lines.append(f"[{m['datetime']}] {sender}: {m['text']}")

        text = "\n".join(lines)
        if not text:
            continue
        if len(text) > 12000:
            text = text[:12000]

        contact = msgs[0]["contact"]
        is_group = msgs[0]["is_group"]
        chat_type = "group" if is_group else "dm"

        try:
            yield {
                "id": f"imessage_{chat_key[:20]}",
                "source": "imessage",
                "title": f"[{chat_type}] {contact}"[:120],
                "date": msgs[-1]["date"],
                "text": text,
                "metadata": {
                    "contact": contact,
                    "chat_type": chat_type,
                    "message_count": len(msgs),
                    "first_date": msgs[0]["date"],
                    "last_date": msgs[-1]["date"],
                    "channel": "operational",
                },
            }
        except Exception:
            log.exception(f"Failed to emit record for chat {chat_key!r}")
