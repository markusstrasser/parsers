"""Hinge dating app parser — reads matches JSON export.

Expects the Hinge data export JSON containing matches with chat messages.
"""

import json
import logging
from collections.abc import Iterator
from pathlib import Path

log = logging.getLogger("parsers")


def parse(path: Path | None = None, **kwargs) -> Iterator[dict]:
    """Yield one record per Hinge conversation (matches with messages).

    Args:
        path: Path to Hinge matches JSON file.
    """
    if path is None:
        raise ValueError("hinge parser requires path to matches JSON file")
    if not path.exists():
        raise FileNotFoundError(f"Hinge export not found: {path}")

    with open(path) as f:
        matches = json.load(f)

    count = 0
    for i, match in enumerate(matches):
        try:
            chats = match.get("chats", [])
            if not chats:
                continue

            chats_sorted = sorted(chats, key=lambda c: c.get("timestamp", ""))
            messages = [c.get("body", "") for c in chats_sorted if c.get("body")]
            if not messages:
                continue

            text = "\n".join(messages)
            first_ts = chats_sorted[0].get("timestamp", "")
            date = first_ts[:10] if first_ts else ""

            yield {
                "id": f"hinge_{i}",
                "source": "hinge",
                "title": f"Hinge conversation ({len(messages)} messages, {date})",
                "date": date,
                "text": text,
                "metadata": {
                    "message_count": len(messages),
                    "channel": "curated",
                },
            }
            count += 1
        except Exception:
            log.exception(f"Failed to parse match {i}")

    log.info(f"Hinge: emitted {count} conversations")
