"""Source registry — maps source names to parse functions."""

import importlib
import logging
from collections.abc import Iterator
from pathlib import Path
from typing import Any

log = logging.getLogger("parsers")

# name → module path under parsers.sources
_SOURCES: dict[str, str] = {
    "imessage": "parsers.sources.imessage",
    "chatgpt": "parsers.sources.chatgpt",
    "logseq": "parsers.sources.logseq",
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
