"""Logseq graph parser.

Reads a Logseq graph directory (pages/ and journals/ subdirectories).
Yields one record per page or journal entry.
"""

import logging
import re
from collections.abc import Iterator
from pathlib import Path

log = logging.getLogger("parsers")


def _parse_markdown(file_path: Path) -> dict:
    """Parse a Logseq markdown file into blocks, links, and tags."""
    content = file_path.read_text(encoding="utf-8")
    title = file_path.stem

    # Parse blocks (lines starting with "- ")
    blocks: list[str] = []
    current: list[str] = []
    for line in content.split("\n"):
        stripped = line.strip()
        if stripped.startswith("- "):
            if current:
                blocks.append("\n".join(current))
            current = [stripped[2:]]
        elif stripped and current:
            current.append(stripped)
        elif not stripped and current:
            blocks.append("\n".join(current))
            current = []
    if current:
        blocks.append("\n".join(current))

    links = list(set(re.findall(r"\[\[([^\]]+)\]\]", content)))
    tags = list(set(re.findall(r"#(\w+)", content)))
    is_journal = bool(re.match(r"\d{4}[_-]\d{2}[_-]\d{2}", title))

    # Try to extract date from journal filename
    date = ""
    date_match = re.match(r"(\d{4})[_-](\d{2})[_-](\d{2})", title)
    if date_match:
        date = f"{date_match.group(1)}-{date_match.group(2)}-{date_match.group(3)}"

    return {
        "title": title,
        "content": content,
        "blocks": blocks,
        "links": links,
        "tags": tags,
        "is_journal": is_journal,
        "date": date,
        "type": "journal" if is_journal else "page",
    }


def parse(path: Path | None = None, **kwargs) -> Iterator[dict]:
    """Yield one record per Logseq page/journal.

    Args:
        path: Path to Logseq graph directory (containing pages/ and/or journals/).
    """
    if path is None:
        raise ValueError("logseq parser requires path to graph directory")
    if not path.is_dir():
        raise FileNotFoundError(f"Logseq graph directory not found: {path}")

    count = 0
    for subdir in ("pages", "journals"):
        dir_path = path / subdir
        if not dir_path.exists():
            continue

        for md_file in sorted(dir_path.glob("*.md")):
            try:
                page = _parse_markdown(md_file)

                # Build combined text: title + first blocks
                parts = [page["title"]]
                if page["blocks"]:
                    parts.extend(page["blocks"][:5])
                text = "\n".join(parts)
                if len(text) > 2000:
                    text = text[:2000]

                if not text.strip():
                    continue

                yield {
                    "id": f"logseq_{count}",
                    "source": "logseq",
                    "title": page["title"],
                    "date": page["date"],
                    "text": text,
                    "metadata": {
                        "type": page["type"],
                        "is_journal": page["is_journal"],
                        "tags": page["tags"],
                        "block_count": len(page["blocks"]),
                        "channel": "authored",
                    },
                }
                count += 1
            except Exception:
                log.exception(f"Failed to parse {md_file}")

    log.info(f"Logseq: emitted {count} pages/journals")
