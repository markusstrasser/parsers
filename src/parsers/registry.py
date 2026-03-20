"""Source registry — maps source names to parse functions."""

import importlib
import logging
from collections.abc import Iterator
from pathlib import Path
from typing import Any

log = logging.getLogger("parsers")

# name → module path under parsers.sources
_SOURCES: dict[str, str] = {
    "bear": "parsers.sources.bear",
    "chatgpt": "parsers.sources.chatgpt",
    "claude": "parsers.sources.claude",
    "films": "parsers.sources.films",
    "git": "parsers.sources.git",
    "healthkit": "parsers.sources.healthkit",
    "hinge": "parsers.sources.hinge",
    "imessage": "parsers.sources.imessage",
    "instagram": "parsers.sources.instagram",
    "logseq": "parsers.sources.logseq",
    "mbox": "parsers.sources.mbox",
    "notes": "parsers.sources.notes",
    "papers": "parsers.sources.papers",
    "pinterest": "parsers.sources.pinterest",
    "raycast": "parsers.sources.raycast",
    "signal": "parsers.sources.signal",
    "twitter": "parsers.sources.twitter",
    "whatsapp": "parsers.sources.whatsapp",
    "yfull": "parsers.sources.yfull",
}


def list_sources() -> list[str]:
    """Return sorted list of available source names."""
    return sorted(_SOURCES)


def parse(source: str, path: str | Path | None = None, **kwargs: Any) -> Iterator[dict]:
    """Parse a source, yielding Record-compatible dicts.

    Args:
        source: Source name (e.g. "twitter", "imessage").
        path: Path to export file/directory. Optional for sources with defaults.
        **kwargs: Source-specific options.

    Yields:
        Dicts with keys: id, text, source, date, title (optional), metadata (optional).
    """
    if source not in _SOURCES:
        raise ValueError(f"Unknown source: {source!r}. Available: {', '.join(list_sources())}")

    mod = importlib.import_module(_SOURCES[source])
    yield from mod.parse(path=Path(path) if path else None, **kwargs)
