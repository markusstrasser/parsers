"""Instagram parser — reads Instagram data export directory.

Parses saved_posts.html from Instagram export. Posts may need enrichment
to have text content (pre-enrichment: URL-only, no text → skip).
"""

import logging
import re
from collections.abc import Iterator
from datetime import datetime
from html.parser import HTMLParser
from pathlib import Path

log = logging.getLogger("parsers")


class _SavedPostsParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.posts: list[dict] = []
        self.current_post: dict = {}
        self.in_username = False
        self.in_table = False
        self.in_td = False
        self.td_content: list[str] = []
        self.last_tag = ""

    def handle_starttag(self, tag, attrs):
        self.last_tag = tag
        attrs_dict = dict(attrs)
        if tag == "h2":
            self.in_username = True
        elif tag == "table":
            self.in_table = True
        elif tag == "td" and self.in_table:
            self.in_td = True
            self.td_content = []
        elif tag == "a" and "href" in attrs_dict and self.in_table:
            href = attrs_dict["href"] or ""
            if "/p/" in href or "/reel/" in href:
                self.current_post["url"] = href
                m = re.search(r"/(p|reel)/([A-Za-z0-9_-]+)/", str(href))
                if m:
                    self.current_post["post_id"] = m.group(2)
                    self.current_post["post_type"] = "reel" if m.group(1) == "reel" else "post"

    def handle_data(self, data):
        data = data.strip()
        if self.in_username and data and self.last_tag == "h2":
            if self.current_post and "username" in self.current_post:
                self.posts.append(self.current_post)
            self.current_post = {"username": data}
            self.in_username = False
        elif self.in_td and data:
            self.td_content.append(data)

    def handle_endtag(self, tag):
        if tag == "td" and self.in_td:
            self.in_td = False
            content = " ".join(self.td_content).strip()
            if content.startswith(("Jan", "Feb", "Mar", "Apr", "May", "Jun",
                                   "Jul", "Aug", "Sep", "Oct", "Nov", "Dec")):
                try:
                    dt = datetime.strptime(content, "%b %d, %Y %I:%M %p")
                    self.current_post["saved_date"] = dt.strftime("%Y-%m-%d")
                except ValueError:
                    pass
        elif tag == "table":
            self.in_table = False

    def close(self):
        if self.current_post and "username" in self.current_post:
            self.posts.append(self.current_post)
        HTMLParser.close(self)


def parse(path: Path | None = None, **kwargs) -> Iterator[dict]:
    """Yield one record per saved Instagram post.

    Args:
        path: Path to Instagram export directory.
    """
    if path is None:
        raise ValueError("instagram parser requires path to export directory")
    if not path.is_dir():
        raise FileNotFoundError(f"Instagram export not found: {path}")

    saved_file = path / "your_instagram_activity" / "saved" / "saved_posts.html"
    if not saved_file.exists():
        log.warning(f"saved_posts.html not found in {path}")
        return

    with open(saved_file, "r", encoding="utf-8") as f:
        content = f.read()

    parser = _SavedPostsParser()
    parser.feed(content)
    parser.close()

    count = 0
    for post in parser.posts:
        username = post.get("username", "")
        post_id = post.get("post_id", "")
        post_type = post.get("post_type", "post")
        saved_date = post.get("saved_date", "")
        url = post.get("url", "")

        text = f"Saved {post_type} from @{username}"
        if url:
            text += f" — {url}"

        yield {
            "id": f"instagram_{post_id}",
            "source": "instagram",
            "title": f"Saved {post_type} from @{username}",
            "date": saved_date,
            "text": text,
            "metadata": {
                "username": username,
                "post_type": post_type,
                "url": url,
                "channel": "curated",
            },
        }
        count += 1

    log.info(f"Instagram: emitted {count} saved posts")
