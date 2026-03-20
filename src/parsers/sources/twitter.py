"""Twitter/X parser — reads unified SQLite database.

Source: twitter_unified.sqlite (produced by selve's unify_twitter_exports.py
from archive zips, bookmarks JSON, and CSV exports).

Platform: any (SQLite).
"""

import logging
import sqlite3
from collections.abc import Iterator
from pathlib import Path

log = logging.getLogger("parsers")


def parse(path: Path | None = None, **kwargs) -> Iterator[dict]:
    """Yield one record per tweet/bookmark from unified Twitter SQLite DB.

    Args:
        path: Path to twitter_unified.sqlite.
    """
    if path is None:
        raise ValueError("twitter parser requires path to twitter_unified.sqlite")
    if not path.exists():
        raise FileNotFoundError(f"Twitter database not found: {path}")

    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row

    tables = {r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()}
    has_quotes = "quote_tweet" in tables
    has_engagement = "tweet_engagement" in tables

    extra_joins = []
    extra_cols = []
    if has_quotes:
        extra_joins.append("LEFT JOIN quote_tweet qt ON p.post_id = qt.post_id")
        extra_cols.append("qt.is_quote, qt.quoted_author, qt.quoted_text")
    else:
        extra_cols.append("NULL as is_quote, NULL as quoted_author, NULL as quoted_text")
    if has_engagement:
        extra_joins.append("LEFT JOIN tweet_engagement te ON p.post_id = te.post_id")
        extra_cols.append("te.favorite_count, te.reply_count, te.is_reply")
    else:
        extra_cols.append("NULL as favorite_count, NULL as reply_count, NULL as is_reply")

    rows = conn.execute(f"""
        SELECT p.post_id, p.tweet_url, p.author_screen_name, p.author_name,
               p.full_text, p.note_tweet_text, p.tweeted_at, p.bookmarked_at,
               COUNT(pm.media_uid) as media_count,
               GROUP_CONCAT(DISTINCT pm.media_type) as media_types,
               GROUP_CONCAT(pm.alt_text, ' | ') as alt_texts,
               {', '.join(extra_cols)}
        FROM post p
        LEFT JOIN post_media pm ON p.post_id = pm.post_id
        {' '.join(extra_joins)}
        WHERE p.full_text IS NOT NULL AND p.full_text != ''
        GROUP BY p.post_id
    """).fetchall()
    conn.close()

    count = 0
    for row in rows:
        try:
            text = row["note_tweet_text"] or row["full_text"]
            author = row["author_screen_name"] or "unknown"

            alt = row["alt_texts"]
            if alt:
                text = f"{text}\n[image: {alt}]"
            if row["is_quote"] and row["quoted_text"]:
                qt_author = row["quoted_author"] or "unknown"
                text = f"{text}\n[quoting @{qt_author}: {row['quoted_text']}]"

            date_raw = row["bookmarked_at"] or row["tweeted_at"] or ""
            date = date_raw[:10] if date_raw else ""

            meta: dict = {
                "screen_name": author,
                "tweet_url": row["tweet_url"],
                "media_count": row["media_count"],
                "channel": "curated",
            }
            fav = row["favorite_count"]
            if fav is not None:
                meta["favorite_count"] = fav

            yield {
                "id": f"twitter_{row['post_id']}",
                "source": "twitter",
                "title": f"Tweet by @{author}",
                "date": date,
                "text": text,
                "metadata": meta,
            }
            count += 1
        except Exception:
            log.exception(f"Failed to emit tweet {row['post_id']}")

    log.info(f"Twitter: emitted {count} posts")
