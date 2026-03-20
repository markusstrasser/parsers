"""WhatsApp parser — reads WhatsApp Desktop SQLite database.

Default path: ~/Library/Group Containers/group.net.whatsapp.WhatsApp.shared/ChatStorage.sqlite
Platform: macOS only (requires Full Disk Access).
"""

import logging
import shutil
import sqlite3
from collections.abc import Iterator
from datetime import datetime
from pathlib import Path

log = logging.getLogger("parsers")

DEFAULT_DB = (
    Path.home() / "Library" / "Group Containers"
    / "group.net.whatsapp.WhatsApp.shared" / "ChatStorage.sqlite"
)

# WhatsApp epoch offset (Core Data uses 2001-01-01)
_WA_EPOCH = 978307200


def parse(path: Path | None = None, **kwargs) -> Iterator[dict]:
    """Yield one record per chat conversation from WhatsApp Desktop DB.

    Args:
        path: Path to ChatStorage.sqlite. Defaults to macOS WhatsApp location.
    """
    db_path = path or DEFAULT_DB
    if not db_path.exists():
        raise FileNotFoundError(f"WhatsApp database not found: {db_path}")

    # Copy to avoid locking the live database
    tmp_db = Path("/tmp/whatsapp_parsers.sqlite")
    shutil.copy2(db_path, tmp_db)

    try:
        conn = sqlite3.connect(tmp_db)
        conn.row_factory = sqlite3.Row

        chats = conn.execute("""
            SELECT cs.Z_PK, cs.ZCONTACTJID, cs.ZPARTNERNAME,
                   COUNT(m.Z_PK) as msg_count,
                   SUM(CASE WHEN m.ZTEXT IS NOT NULL AND m.ZTEXT != '' THEN 1 ELSE 0 END) as text_count,
                   MIN(m.ZMESSAGEDATE) as first_date,
                   MAX(m.ZMESSAGEDATE) as last_date
            FROM ZWACHATSESSION cs
            JOIN ZWAMESSAGE m ON m.ZCHATSESSION = cs.Z_PK
            GROUP BY cs.Z_PK
            HAVING text_count > 0
            ORDER BY last_date DESC
        """).fetchall()

        count = 0
        for chat in chats:
            try:
                partner = chat["ZPARTNERNAME"] or chat["ZCONTACTJID"] or "Unknown"
                is_group = "@g.us" in (chat["ZCONTACTJID"] or "")

                messages = conn.execute("""
                    SELECT m.ZMESSAGEDATE, m.ZISFROMME, m.ZTEXT, m.ZFROMJID
                    FROM ZWAMESSAGE m
                    WHERE m.ZCHATSESSION = ? AND m.ZTEXT IS NOT NULL AND m.ZTEXT != ''
                    ORDER BY m.ZMESSAGEDATE ASC
                """, (chat["Z_PK"],)).fetchall()

                if not messages:
                    continue

                lines = []
                for msg in messages:
                    ts = datetime.utcfromtimestamp(msg["ZMESSAGEDATE"] + _WA_EPOCH)
                    date_str = ts.strftime("%Y-%m-%d %H:%M")
                    if msg["ZISFROMME"]:
                        sender = "me"
                    elif is_group and msg["ZFROMJID"]:
                        sender = msg["ZFROMJID"].split("@")[0]
                    else:
                        sender = partner
                    lines.append(f"[{date_str}] {sender}: {msg['ZTEXT']}")

                text = "\n".join(lines)
                if len(text) > 12000:
                    text = text[:12000]

                first_date = datetime.utcfromtimestamp(chat["first_date"] + _WA_EPOCH).strftime("%Y-%m-%d")
                last_date = datetime.utcfromtimestamp(chat["last_date"] + _WA_EPOCH).strftime("%Y-%m-%d")
                chat_type = "group" if is_group else "dm"
                jid = chat["ZCONTACTJID"] or ""

                yield {
                    "id": f"whatsapp_{jid[:20]}",
                    "source": "whatsapp",
                    "title": f"[{chat_type}] {partner}",
                    "date": last_date,
                    "text": text,
                    "metadata": {
                        "partner": partner,
                        "chat_type": chat_type,
                        "message_count": chat["msg_count"],
                        "first_date": first_date,
                        "last_date": last_date,
                        "channel": "operational",
                    },
                }
                count += 1
            except Exception:
                log.exception(f"Failed to parse chat {chat['Z_PK']}")

        conn.close()
        log.info(f"WhatsApp: emitted {count} conversations")
    finally:
        tmp_db.unlink(missing_ok=True)
