"""Git commits parser — indexes commits from local git repos.

Scans a directory for git repositories and extracts recent commits.
"""

import logging
import subprocess
from collections.abc import Iterator
from datetime import datetime
from pathlib import Path

log = logging.getLogger("parsers")


def parse(path: Path | None = None, *, max_per_repo: int = 100, **kwargs) -> Iterator[dict]:
    """Yield one record per git commit from repos under path.

    Args:
        path: Parent directory containing git repos (e.g. ~/Projects).
        max_per_repo: Max commits per repo (default 100).
    """
    if path is None:
        path = Path.home() / "Projects"
    if not path.is_dir():
        raise FileNotFoundError(f"Directory not found: {path}")

    repos = [d for d in path.iterdir() if d.is_dir() and (d / ".git").exists()]
    count = 0

    for repo in repos:
        try:
            result = subprocess.run(
                ["git", "-C", str(repo), "log", f"--max-count={max_per_repo}",
                 "--format=%H%x00%an%x00%at%x00%s%x00%b%x1e"],
                capture_output=True, text=True, timeout=30,
            )
            if result.returncode != 0:
                continue

            for entry in result.stdout.split("\x1e"):
                entry = entry.strip()
                if not entry:
                    continue
                parts = entry.split("\x00")
                if len(parts) < 4:
                    continue

                hash_, author, ts_str, subject = parts[0], parts[1], parts[2], parts[3]
                body = parts[4] if len(parts) > 4 else ""

                try:
                    date = datetime.fromtimestamp(int(ts_str)).strftime("%Y-%m-%d")
                except (ValueError, OSError):
                    date = ""

                # Get changed files
                files_result = subprocess.run(
                    ["git", "-C", str(repo), "show", "--name-only", "--format=", hash_],
                    capture_output=True, text=True, timeout=10,
                )
                files = [f for f in files_result.stdout.strip().split("\n") if f][:10]

                text_parts = [subject]
                if body.strip():
                    text_parts.append(body.strip())
                if files:
                    text_parts.append("Files: " + ", ".join(files))

                yield {
                    "id": f"git_{hash_[:8]}",
                    "source": "git",
                    "title": f"[{repo.name}] {subject[:100]}",
                    "date": date,
                    "text": "\n".join(text_parts),
                    "metadata": {
                        "hash": hash_,
                        "repo": repo.name,
                        "author": author,
                        "channel": "authored",
                    },
                }
                count += 1
        except Exception:
            log.exception(f"Failed to index {repo.name}")

    log.info(f"Git: emitted {count} commits from {len(repos)} repos")
