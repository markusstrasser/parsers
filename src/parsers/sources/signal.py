"""Signal parser — exports via sigexport, groups into conversations.

Requires: `uvx --from signal-export sigexport` available on PATH.
Platform: any with Signal Desktop installed.
"""

import json
import logging
import shutil
import subprocess
from collections import defaultdict
from collections.abc import Iterator
from pathlib import Path

log = logging.getLogger("parsers")


def parse(path: Path | None = None, **kwargs) -> Iterator[dict]:
    """Yield one record per Signal conversation.

    Args:
        path: Optional path to pre-exported sigexport directory. If None,
              runs sigexport automatically.
    """
    if path and path.is_dir():
        export_dir = path
        cleanup = False
    else:
        export_dir = Path("/tmp/signal_parsers_export")
        if export_dir.exists():
            shutil.rmtree(export_dir)
        log.info("Exporting Signal via sigexport...")
        result = subprocess.run(
            ["uvx", "--from", "signal-export", "sigexport", str(export_dir),
             "--json", "--no-attachments"],
            capture_output=True, text=True, timeout=120,
        )
        if result.returncode != 0:
            raise RuntimeError(f"sigexport failed: {result.stderr}")
        cleanup = True

    try:
        # Read all messages, group by contact
        by_contact: dict[str, list[dict]] = defaultdict(list)

        for chat_dir in sorted(export_dir.iterdir()):
            data_file = chat_dir / "data.json"
            if not data_file.exists():
                continue
            chat_name = chat_dir.name
            with open(data_file) as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        msg = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    body = msg.get("body", "").strip()
                    if not body:
                        continue
                    by_contact[chat_name].append({
                        "date": msg.get("date", ""),
                        "sender": msg.get("sender", ""),
                        "text": body,
                    })

        count = 0
        for contact, msgs in by_contact.items():
            if len(msgs) < 2:
                continue

            msgs.sort(key=lambda m: m.get("date", ""))
            lines = []
            for m in msgs:
                sender = "me" if m["sender"] == "Me" else contact
                date_part = m["date"][:16].replace("T", " ") if m["date"] else ""
                lines.append(f"[{date_part}] {sender}: {m['text']}")

            text = "\n".join(lines)
            if len(text) > 12000:
                text = text[:12000]

            first_date = msgs[0]["date"][:10] if msgs[0]["date"] else ""
            last_date = msgs[-1]["date"][:10] if msgs[-1]["date"] else ""

            yield {
                "id": f"signal_{contact[:20]}",
                "source": "signal",
                "title": f"Signal: {contact}",
                "date": last_date,
                "text": text,
                "metadata": {
                    "contact": contact,
                    "message_count": len(msgs),
                    "first_date": first_date,
                    "last_date": last_date,
                    "channel": "operational",
                },
            }
            count += 1

        log.info(f"Signal: emitted {count} conversations")
    finally:
        if cleanup and export_dir.exists():
            shutil.rmtree(export_dir, ignore_errors=True)
