"""Bear notes parser — reads exported markdown files.

Expects a directory of .md files from Bear export.
"""

import logging
import re
from collections.abc import Iterator
from pathlib import Path

log = logging.getLogger("parsers")


def _extract_date(filename: str, content: str) -> str:
    """Try to extract date from filename (common Bear pattern: DD.MM.YYYY)."""
    for pattern in [r"(\d{2})\.(\d{2})\.(\d{4})", r"(\d{2})\.\s*(\d{2})\.\s*(\d{4})"]:
        m = re.match(pattern, filename)
        if m:
            day, month, year = m.groups()
            return f"{year}-{month}-{day}"

    m = re.search(r"#\s*(\d{2})\.(\d{2})\.(\d{4})", content[:200])
    if m:
        day, month, year = m.groups()
        return f"{year}-{month}-{day}"
    return ""


def parse(path: Path | None = None, **kwargs) -> Iterator[dict]:
    """Yield one record per Bear note.

    Args:
        path: Directory containing exported .md files.
    """
    if path is None:
        raise ValueError("bear parser requires path to notes directory")
    if not path.is_dir():
        raise FileNotFoundError(f"Bear notes directory not found: {path}")

    count = 0
    for md_file in sorted(path.glob("*.md")):
        try:
            content = md_file.read_text(encoding="utf-8")
        except Exception:
            continue

        if len(content.strip()) < 10:
            continue

        title = md_file.stem
        date = _extract_date(title, content)
        text = content.strip()
        if text.startswith(f"# {title}"):
            text = text[len(f"# {title}"):].strip()

        yield {
            "id": f"bear_{md_file.stem}",
            "source": "bear",
            "title": title[:120],
            "date": date,
            "text": text,
            "metadata": {"channel": "authored"},
        }
        count += 1

    log.info(f"Bear: emitted {count} notes")
