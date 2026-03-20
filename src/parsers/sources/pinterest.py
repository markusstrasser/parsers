"""Pinterest parser — reads gallery-dl JSON metadata files.

Pinterest has no export — pins are scraped via gallery-dl which saves
per-pin JSON alongside images. Scans directories for .json pin metadata.
"""

import json
import logging
from collections.abc import Iterator
from pathlib import Path

log = logging.getLogger("parsers")

_SKIP_STEMS = {
    "pinterest_before", "pinterest_after",
    "pinterest_snapshot_before_reorg", "pinterest_snapshot_after_reorg",
    "pinterest_reorganization_report", "embeddings_cache",
}


def parse(path: Path | None = None, **kwargs) -> Iterator[dict]:
    """Yield one record per Pinterest pin.

    Args:
        path: Directory containing gallery-dl JSON metadata files.
              Can also accept multiple directories separated by colons.
    """
    if path is None:
        raise ValueError("pinterest parser requires path to pin metadata directory")

    # Support multiple directories via colon separator
    dirs = [Path(p) for p in str(path).split(":")] if ":" in str(path) else [path]
    existing = [d for d in dirs if d.is_dir()]
    if not existing:
        raise FileNotFoundError(f"Pinterest directory not found: {path}")

    seen_ids: set[str] = set()
    count = 0

    for pin_dir in existing:
        for json_file in pin_dir.glob("**/*.json"):
            if json_file.stem in _SKIP_STEMS:
                continue

            try:
                with open(json_file) as f:
                    pin = json.load(f)
            except Exception:
                continue

            description = pin.get("description", "") or ""
            title = pin.get("title", "") or ""
            alt_text = pin.get("seo_alt_text", "") or pin.get("auto_alt_text", "") or ""
            note = pin.get("unified_user_note", "") or ""
            board_name = pin.get("board", {}).get("name", "") if isinstance(pin.get("board"), dict) else ""

            text_parts = [p for p in [board_name, title, description, alt_text, note] if p.strip()]
            text = " | ".join(text_parts)

            if len(text.strip()) < 15:
                continue

            pin_id = str(pin.get("id", json_file.stem))
            if pin_id in seen_ids:
                continue
            seen_ids.add(pin_id)

            yield {
                "id": f"pinterest_{pin_id}",
                "source": "pinterest",
                "title": f"[{board_name}] {title or description or alt_text}"[:120],
                "date": "",
                "text": text,
                "metadata": {
                    "board": board_name,
                    "link": pin.get("link", ""),
                    "domain": pin.get("domain", ""),
                    "channel": "curated",
                },
            }
            count += 1

    log.info(f"Pinterest: emitted {count} pins (deduped)")
