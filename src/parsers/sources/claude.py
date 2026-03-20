"""Claude (Anthropic) conversation export parser.

Reads conversations.json from Claude data export (ZIP or directory).
Supports the 2024+ export format with chat_messages array.
"""

import json
import logging
import zipfile
from collections.abc import Iterator
from pathlib import Path
from tempfile import TemporaryDirectory

log = logging.getLogger("parsers")


def _extract_text(msg: dict) -> str:
    """Extract text from a Claude message, handling content blocks."""
    text = msg.get("text", "")
    if text and text.strip():
        return text.strip()

    content = msg.get("content", [])
    if isinstance(content, list):
        parts = []
        for block in content:
            if isinstance(block, dict) and block.get("type") == "text":
                parts.append(block.get("text", ""))
            elif isinstance(block, str):
                parts.append(block)
        if parts:
            return "\n".join(p for p in parts if p).strip()
    return ""


def parse(path: Path | None = None, **kwargs) -> Iterator[dict]:
    """Yield one record per Claude conversation.

    Args:
        path: Path to export ZIP, extracted directory, or conversations.json.
    """
    if path is None:
        raise ValueError("claude parser requires path to export ZIP, directory, or conversations.json")
    if not path.exists():
        raise FileNotFoundError(f"Claude export not found: {path}")

    # Resolve to conversations.json
    if path.suffix == ".zip":
        tmpdir = TemporaryDirectory()
        with zipfile.ZipFile(path) as zf:
            zf.extractall(tmpdir.name)
        conv_path = Path(tmpdir.name) / "conversations.json"
    elif path.is_dir():
        conv_path = path / "conversations.json"
    else:
        conv_path = path

    if not conv_path.exists():
        raise FileNotFoundError(f"conversations.json not found in {path}")

    with open(conv_path, "r", encoding="utf-8") as f:
        conversations = json.load(f)

    log.info(f"Claude: processing {len(conversations)} conversations")

    for conv in conversations:
        try:
            messages = conv.get("chat_messages", [])
            if not messages:
                continue

            parsed = []
            for msg in messages:
                text = _extract_text(msg)
                if not text:
                    continue
                role = msg.get("sender", "unknown")
                parsed.append({"role": role, "text": text})

            if not parsed:
                continue

            uuid = conv.get("uuid", "")
            name = conv.get("name", "").strip()
            summary = conv.get("summary", "").strip()
            title = name or summary or (parsed[0]["text"][:100] if parsed else "Untitled")

            text = "\n".join(f"{m['role']}: {m['text']}" for m in parsed if m["text"])
            if not text:
                continue
            if len(text) > 12000:
                text = text[:12000]

            date_raw = conv.get("created_at", "")
            date = date_raw[:10] if date_raw else ""

            yield {
                "id": f"claude_{uuid[:12]}",
                "source": "claude",
                "title": title,
                "date": date,
                "text": text,
                "metadata": {
                    "uuid": uuid,
                    "summary": summary,
                    "total_messages": len(parsed),
                    "channel": "ai_conversation",
                },
            }
        except Exception:
            log.exception(f"Failed to parse conversation {conv.get('uuid', '?')}")
