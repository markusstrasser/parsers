"""Record schema for parsers output.

Compatible with emb.Entry — same field names, same semantics.
No emb dependency; parsers defines its own type.
"""

from typing import Any, TypedDict


class Record(TypedDict, total=False):
    """One parsed record. Required: id, text, source, date.

    Matches emb.Entry field names so `parsers <source> | emb embed -` works.
    """

    id: str  # required — unique within source (e.g. "twitter_12345")
    text: str  # required — main content
    source: str  # required — source name (e.g. "twitter", "imessage")
    title: str  # optional — display title
    date: str  # required — ISO 8601 date, day precision ("YYYY-MM-DD")
    metadata: dict[str, Any]  # optional — must include "channel" if present
