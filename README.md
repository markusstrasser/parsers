# parsers

Parse personal data exports into uniform JSONL records.

```bash
parsers imessage | head -1
# {"id": "imessage_alice", "source": "imessage", "title": "[dm] Alice", "date": "2024-03-15", "text": "...", "metadata": {"channel": "operational", ...}}

parsers chatgpt conversations.json -o chatgpt.jsonl
parsers list
```

Every record has the same shape:

```json
{"id": "...", "source": "...", "title": "...", "date": "YYYY-MM-DD", "text": "...", "metadata": {"channel": "...", ...}}
```

Compatible with [emb](https://github.com/markusstrasser/emb) — pipe directly into `emb embed`:

```bash
parsers twitter ./twitter_unified.sqlite | emb embed - -o twitter.index
emb search -i twitter.index "machine learning"
```

## Install

```bash
# From GitHub
pip install "parsers @ git+https://github.com/markusstrasser/parsers.git"

# Local editable
git clone https://github.com/markusstrasser/parsers.git
cd parsers && pip install -e .

# Or just run with uvx
uvx --from "parsers @ git+https://github.com/markusstrasser/parsers.git" parsers list
```

## Supported sources

### File-based (no auth needed)

| Source | Reads | Platform |
|--------|-------|----------|
| `twitter` | Unified SQLite DB (from archive zips, bookmarks, CSVs) | Any |
| `chatgpt` | `conversations.json` from ChatGPT data export | Any |
| `claude` | Claude data export (ZIP, directory, or conversations.json) | Any |
| `logseq` | Logseq graph directory (pages/ + journals/) | Any |
| `bear` | Bear notes markdown export directory | Any |
| `notes` | Apple Notes snapshot directory (.md files) | Any |
| `papers` | Directory of markdown papers with YAML frontmatter | Any |
| `mbox` | Standard mbox email files (Gmail Takeout, etc.) | Any |
| `films` | Movie directory (parses filenames) or JSON list | Any |
| `hinge` | Hinge matches JSON export | Any |
| `instagram` | Instagram data export (saved_posts.html) | Any |
| `pinterest` | gallery-dl JSON metadata files | Any |
| `raycast` | Decrypted Raycast export JSON | Any |
| `git` | Local git repos (scans ~/Projects by default) | Any |
| `healthkit` | Apple HealthKit per-metric JSON files | Any |
| `yfull` | YFull CSV/FASTA exports (Y-DNA, mtDNA) | Any |

### OS-specific (default paths, no auth)

| Source | Default path | Platform |
|--------|-------------|----------|
| `imessage` | `~/Library/Messages/chat.db` | macOS |
| `whatsapp` | `~/Library/Group Containers/.../ChatStorage.sqlite` | macOS |
| `signal` | Signal Desktop via `sigexport` | Any (needs Signal Desktop) |

## Python API

```python
from parsers import parse, list_sources

# List available parsers
print(list_sources())

# Parse a source
for record in parse("chatgpt", path="./conversations.json"):
    print(record["title"], record["date"])

# Sources with default paths
for record in parse("imessage"):
    print(record["title"])
```

## Output contract

- **stdout** = JSONL only (one JSON object per line)
- **stderr** = logging (progress, counts, warnings)
- **Exit 0** = success, **Exit 2** = missing input
- Each record has: `id` (unique within source), `source`, `text`, `date` (ISO 8601 YYYY-MM-DD)
- `metadata.channel` classifies content: `authored`, `curated`, `operational`, `exhaust`, `ai_conversation`
