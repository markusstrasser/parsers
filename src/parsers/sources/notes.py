"""Apple Notes parser — reads a snapshot directory of markdown files.

Expects a directory tree of .md files (e.g. from notes-snapshot-*).
"""

import logging
import re
from collections.abc import Iterator
from pathlib import Path

log = logging.getLogger("parsers")


def parse(path: Path | None = None, **kwargs) -> Iterator[dict]:
    """Yield one record per note.

    Args:
        path: Path to notes snapshot directory.
    """
    if path is None:
        raise ValueError("notes parser requires path to snapshot directory")
    if not path.is_dir():
        raise FileNotFoundError(f"Notes directory not found: {path}")

    count = 0
    for md_file in sorted(path.rglob("*.md")):
        try:
            content = md_file.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue

        title = md_file.stem
        rel_path = md_file.relative_to(path)
        folders = list(rel_path.parts[:-1])
        tags = list(set(re.findall(r"#(\w+)", content)))

        path_str = " > ".join(folders) if folders else ""
        text = f"{title}\nPath: {path_str}\n\n{content[:2000]}" if path_str else f"{title}\n\n{content[:2000]}"

        if not text.strip():
            continue

        yield {
            "id": f"notes_{count}",
            "source": "notes",
            "title": title,
            "date": "",
            "text": text,
            "metadata": {
                "folders": folders,
                "tags": tags,
                "channel": "authored",
            },
        }
        count += 1

    log.info(f"Notes: emitted {count} notes")
