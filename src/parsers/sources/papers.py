"""Papers parser — reads a directory of markdown paper files.

Expects markdown files with optional YAML frontmatter (title, arxiv_id, etc.).
"""

import logging
from collections.abc import Iterator
from datetime import datetime
from pathlib import Path

log = logging.getLogger("parsers")


def _parse_frontmatter(content: str) -> tuple[dict, str]:
    """Extract YAML frontmatter and remaining content."""
    if not content.startswith("---"):
        return {}, content

    end = content.find("---", 3)
    if end == -1:
        return {}, content

    fm = {}
    for line in content[3:end].split("\n"):
        if ":" in line:
            key, _, val = line.partition(":")
            fm[key.strip()] = val.strip()
    return fm, content[end + 3:].strip()


def parse(path: Path | None = None, **kwargs) -> Iterator[dict]:
    """Yield one record per paper.

    Args:
        path: Directory containing markdown paper files.
    """
    if path is None:
        raise ValueError("papers parser requires path to papers directory")
    if not path.is_dir():
        raise FileNotFoundError(f"Papers directory not found: {path}")

    count = 0
    for md_file in sorted(path.rglob("*.md")):
        try:
            content = md_file.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue

        fm, body = _parse_frontmatter(content)
        title = fm.get("title", md_file.stem)
        arxiv_id = fm.get("arxiv_id", "")
        date = fm.get("ingested_date", "")
        if isinstance(date, datetime):
            date = date.strftime("%Y-%m-%d")
        elif date:
            date = str(date)[:10]

        text = f"{title}\n"
        if arxiv_id:
            text += f"ArXiv ID: {arxiv_id}\n"
        text += f"\n{body[:8000]}"

        yield {
            "id": f"paper_{count}",
            "source": "papers",
            "title": title,
            "date": date,
            "text": text,
            "metadata": {
                "arxiv_id": arxiv_id or None,
                "channel": "curated",
            },
        }
        count += 1

    log.info(f"Papers: emitted {count} papers")
