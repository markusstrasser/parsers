"""Films parser — reads movie directories or JSON lists.

Parses movie filenames for title/year, or reads pre-parsed JSON.
Supports Letterboxd CSV, IMDb CSV, or raw directory of movie files/folders.
"""

import json
import logging
import re
from collections.abc import Iterator
from pathlib import Path

log = logging.getLogger("parsers")


def _parse_filename(name: str) -> dict | None:
    """Extract movie title and year from a filename."""
    name = re.sub(r"\.(mkv|mp4|avi|m4v)$", "", name, flags=re.IGNORECASE)

    for pattern in [
        r"^(.+?)\s*\((\d{4})\)",
        r"^(.+?)\s*\[(\d{4})\]",
        r"^(.+?)\.(\d{4})\.",
    ]:
        m = re.match(pattern, name)
        if m:
            title = m.group(1).replace(".", " ").rstrip(" -_.")
            return {"title": re.sub(r"\s+", " ", title).strip(), "year": int(m.group(2))}

    m = re.match(
        r"^(.+?)\s+(\d{4})\s+(?:Criterion|Director|Remastered|1080p|720p|2160p|HDRip|BluRay|WEB|DVDRip)",
        name, re.IGNORECASE,
    )
    if m:
        return {"title": re.sub(r"\s+", " ", m.group(1)).strip(), "year": int(m.group(2))}

    m = re.search(r"(?:^|[\s._-])(\d{4})(?:[\s._-]|$)", name)
    if m:
        year = int(m.group(1))
        if 1920 <= year <= 2030:
            title = name[:m.start()].strip(" ._-").replace(".", " ")
            if title:
                return {"title": re.sub(r"\s+", " ", title).strip(), "year": year}
    return None


def parse(path: Path | None = None, *, status: str = "seen", **kwargs) -> Iterator[dict]:
    """Yield one record per film.

    Args:
        path: Directory of movie files/folders, or a JSON file with movie list.
        status: Default status for all movies ("seen" or "watchlist").
    """
    if path is None:
        raise ValueError("films parser requires path to movie directory or JSON file")
    if not path.exists():
        raise FileNotFoundError(f"Films path not found: {path}")

    if path.suffix == ".json":
        with open(path) as f:
            data = json.load(f)
        movies = data if isinstance(data, list) else data.get("seen", []) + data.get("watchlist", [])
        for m in movies:
            title = m.get("title", "Unknown")
            year = m.get("year", "")
            s = m.get("status", status)
            yield {
                "id": f"movie_{s}_{title.lower().replace(' ', '_')[:40]}",
                "source": "movies",
                "title": f"{title} ({year})",
                "date": f"{year}-01-01" if year else "",
                "text": f"{title} ({year}) — {s}",
                "metadata": {"status": s, "year": year, "channel": "curated"},
            }
    elif path.is_dir():
        seen = set()
        for entry in sorted(path.iterdir()):
            if entry.name.startswith(".") or entry.name == "_seen":
                continue
            parsed = _parse_filename(entry.name)
            if not parsed:
                continue
            key = (parsed["title"].lower(), parsed["year"])
            if key in seen:
                continue
            seen.add(key)
            yield {
                "id": f"movie_{status}_{parsed['title'].lower().replace(' ', '_')[:40]}",
                "source": "movies",
                "title": f"{parsed['title']} ({parsed['year']})",
                "date": f"{parsed['year']}-01-01",
                "text": f"{parsed['title']} ({parsed['year']}) — {status}",
                "metadata": {"status": status, "year": parsed["year"], "channel": "curated"},
            }
    else:
        raise ValueError(f"Expected directory or JSON file, got: {path}")
