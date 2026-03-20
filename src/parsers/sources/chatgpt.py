"""ChatGPT conversations parser.

Reads `conversations.json` from a ChatGPT data export.
Supports both the old format (mapping tree, pre-2024) and the new format
(chat_messages array, 2024+). Yields one record per conversation.
"""

import json
import logging
from collections.abc import Iterator
from datetime import datetime
from pathlib import Path

log = logging.getLogger("parsers")

_ROLE_MAP = {"human": "user", "assistant": "assistant"}


def _extract_messages_mapping(mapping: dict) -> list[dict]:
    """Old format: conversation tree with nested mapping nodes."""
    messages = []
    for node in mapping.values():
        msg = node.get("message")
        if not msg:
            continue

        role = msg.get("author", {}).get("role", "")
        if role not in ("user", "assistant"):
            continue

        content = msg.get("content", {})
        content_type = content.get("content_type", "")

        text = ""
        if content_type == "text":
            parts = content.get("parts", [])
            text = " ".join(str(p) for p in parts if p)
        elif content_type == "code":
            text = content.get("text", "")
        elif content_type == "user_editable_context":
            continue
        elif content_type == "multimodal_text":
            parts = content.get("parts", [])
            text = " ".join(
                p if isinstance(p, str) else p.get("text", "")
                for p in parts
                if isinstance(p, (str, dict))
            )

        if text.strip():
            messages.append({
                "role": role,
                "text": text.strip(),
                "create_time": msg.get("create_time"),
            })

    messages.sort(key=lambda x: x["create_time"] or 0)
    return messages


def _extract_messages_flat(chat_messages: list) -> list[dict]:
    """New format (2024+): flat array with sender/text fields."""
    messages = []
    for m in chat_messages:
        sender = m.get("sender", "")
        role = _ROLE_MAP.get(sender, sender)
        if role not in ("user", "assistant"):
            continue

        text = m.get("text", "").strip()
        if not text:
            continue

        messages.append({
            "role": role,
            "text": text,
            "create_time": m.get("created_at", ""),
        })
    return messages


def _parse_date(conv: dict) -> str:
    """Extract YYYY-MM-DD from either format."""
    # New format: ISO string
    created_at = conv.get("created_at", "")
    if created_at and isinstance(created_at, str):
        return created_at[:10]

    # Old format: epoch timestamp
    create_time = conv.get("create_time", 0)
    if create_time:
        try:
            return datetime.fromtimestamp(create_time).strftime("%Y-%m-%d")
        except (OSError, ValueError):
            pass
    return ""


def parse(path: Path | None = None, **kwargs) -> Iterator[dict]:
    """Yield one record per ChatGPT conversation.

    Args:
        path: Path to conversations.json from ChatGPT data export.
    """
    if path is None:
        raise ValueError("chatgpt parser requires path to conversations.json")
    if not path.exists():
        raise FileNotFoundError(f"ChatGPT export not found: {path}")

    with open(path, "r", encoding="utf-8") as f:
        conversations = json.load(f)

    log.info(f"ChatGPT: processing {len(conversations)} conversations")

    for conv in conversations:
        try:
            # Detect format and extract messages
            if "mapping" in conv:
                messages = _extract_messages_mapping(conv["mapping"])
                conv_id = conv.get("id", "")
                title = conv.get("title", "Untitled")
            elif "chat_messages" in conv:
                messages = _extract_messages_flat(conv["chat_messages"])
                conv_id = conv.get("uuid", "")
                title = conv.get("name", "") or conv.get("summary", "") or "Untitled"
            else:
                continue

            if not messages:
                continue

            text = "\n".join(
                f"{m['role']}: {m['text']}" for m in messages if m.get("text")
            )
            if not text:
                continue
            if len(text) > 12000:
                text = text[:12000]

            date = _parse_date(conv)

            yield {
                "id": f"chatgpt_{conv_id[:12]}",
                "source": "chatgpt",
                "title": title,
                "date": date,
                "text": text,
                "metadata": {
                    "conversation_id": conv_id,
                    "total_messages": len(messages),
                    "channel": "ai_conversation",
                },
            }
        except Exception:
            log.exception(f"Failed to parse conversation {conv.get('id', conv.get('uuid', '?'))}")
