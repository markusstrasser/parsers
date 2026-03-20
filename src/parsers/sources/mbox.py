"""Mbox email parser — reads standard mbox files (e.g. Gmail Takeout).

Filters spam, newsletters, and noise by Gmail labels and headers.
"""

import email.header
import email.utils
import hashlib
import logging
import mailbox
import re
from collections.abc import Iterator
from pathlib import Path

log = logging.getLogger("parsers")

_SKIP_LABELS = {
    "spam", "trash", "promotions", "social", "updates", "forums",
    "category_promotions", "category_social", "category_updates", "category_forums",
}
_NOREPLY = re.compile(r"(noreply|no-reply|donotreply|mailer-daemon|notifications?@|bounce|postmaster)", re.I)


def _decode_header(value: str) -> str:
    if not value:
        return ""
    parts = []
    for part, charset in email.header.decode_header(value):
        if isinstance(part, bytes):
            parts.append(part.decode(charset or "utf-8", errors="replace"))
        else:
            parts.append(part)
    return " ".join(parts)


def _extract_body(msg) -> str:
    if msg.is_multipart():
        for part in msg.walk():
            if part.get_content_type() == "text/plain":
                payload = part.get_payload(decode=True)
                if payload:
                    return payload.decode(part.get_content_charset() or "utf-8", errors="replace")
    else:
        payload = msg.get_payload(decode=True)
        if payload:
            return payload.decode(msg.get_content_charset() or "utf-8", errors="replace")
    return ""


def _is_newsletter(msg, from_addr: str) -> bool:
    if _NOREPLY.search(from_addr):
        return True
    if msg.get("List-Unsubscribe"):
        return True
    if msg.get("Precedence", "").lower() in ("bulk", "list", "junk"):
        return True
    return False


def parse(path: Path | None = None, **kwargs) -> Iterator[dict]:
    """Yield one record per email from mbox file.

    Args:
        path: Path to .mbox file.
    """
    if path is None:
        raise ValueError("mbox parser requires path to .mbox file")
    if not path.exists():
        raise FileNotFoundError(f"Mbox file not found: {path}")

    mbox = mailbox.mbox(str(path))
    count = 0

    for msg in mbox:
        try:
            labels = {l.strip().lower() for l in msg.get("X-Gmail-Labels", "").split(",") if l.strip()}
            if labels & _SKIP_LABELS:
                continue

            from_addr = _decode_header(msg.get("From", ""))
            if _is_newsletter(msg, from_addr):
                continue

            subject = _decode_header(msg.get("Subject", "")) or "(no subject)"
            body = _extract_body(msg)[:3000]
            body = re.sub(r"\n{3,}", "\n\n", body)
            body = re.sub(r"--\s*\n.*", "", body, flags=re.DOTALL)

            if not body.strip():
                continue

            date_str = msg.get("Date", "")
            date = ""
            if date_str:
                try:
                    parsed = email.utils.parsedate_to_datetime(date_str)
                    date = parsed.strftime("%Y-%m-%d")
                except Exception:
                    pass

            msg_id = msg.get("Message-ID", "").strip("<>").strip()
            hash_key = msg_id or f"{from_addr}{date_str}{subject}"
            stable_id = hashlib.md5(hash_key.encode()).hexdigest()[:16]

            from_name = from_addr.split("<")[0].strip().strip('"') if "<" in from_addr else from_addr
            is_sent = "sent" in labels or "sent mail" in labels

            yield {
                "id": f"mbox_{stable_id}",
                "source": "mbox",
                "title": subject[:120],
                "date": date,
                "text": f"From: {from_name}\nSubject: {subject}\n\n{body}",
                "metadata": {
                    "from": from_addr,
                    "is_sent": is_sent,
                    "channel": "operational",
                },
            }
            count += 1
        except Exception:
            log.exception("Failed to parse email message")

    log.info(f"Mbox: emitted {count} emails")
