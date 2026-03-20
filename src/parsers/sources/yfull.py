"""YFull haplogroup export parser.

Reads YFull CSV/JSON exports (SNP calls, STR profiles, matches, mtDNA).
Produces high-signal entries for semantic search.
"""

import logging
from collections.abc import Iterator
from datetime import datetime
from pathlib import Path

log = logging.getLogger("parsers")


def parse(path: Path | None = None, **kwargs) -> Iterator[dict]:
    """Yield records from YFull data exports.

    Args:
        path: Directory containing YFull CSV/JSON export files.
    """
    if path is None:
        raise ValueError("yfull parser requires path to yfull data directory")
    if not path.is_dir():
        raise FileNotFoundError(f"YFull directory not found: {path}")

    count = 0

    # Parse SNP calls
    snp_file = next(path.glob("SNP_for_*.csv"), None)
    if snp_file:
        with open(snp_file) as f:
            lines = f.readlines()

        haplogroup = ""
        if lines and lines[0].startswith("Haplogroup;"):
            haplogroup = lines[0].split(";", 1)[1].strip()

        positive = 0
        negative = 0
        for line in lines[2:]:
            parts = line.strip().rstrip(";").split(";")
            if len(parts) >= 2:
                status = parts[1].strip().lower()
                if "positive" in status:
                    positive += 1
                elif "negative" in status:
                    negative += 1

        if haplogroup:
            yield {
                "id": "yfull_y_haplogroup",
                "source": "yfull",
                "title": f"Y-DNA Haplogroup: {haplogroup}",
                "date": datetime.fromtimestamp(snp_file.stat().st_mtime).strftime("%Y-%m-%d"),
                "text": (
                    f"Y-DNA haplogroup {haplogroup}. "
                    f"SNPs tested: {positive + negative}. "
                    f"Positive: {positive}, negative: {negative}."
                ),
                "metadata": {"type": "y_haplogroup", "haplogroup": haplogroup, "channel": "authored"},
            }
            count += 1

    # Parse STR profile
    str_file = next(path.glob("STR_for_*.csv"), None)
    if str_file:
        markers = 0
        with open(str_file) as f:
            for line in f:
                parts = line.strip().rstrip(";").split(";")
                if len(parts) >= 2 and parts[0].strip():
                    markers += 1

        yield {
            "id": "yfull_str_summary",
            "source": "yfull",
            "title": f"Y-STR Profile — {markers} markers",
            "date": datetime.fromtimestamp(str_file.stat().st_mtime).strftime("%Y-%m-%d"),
            "text": f"Y-STR profile: {markers} markers tested.",
            "metadata": {"type": "str_summary", "total_markers": markers, "channel": "authored"},
        }
        count += 1

    # Parse SNP matches
    match_file = next(path.glob("SNP_matches_*.csv"), None)
    if match_file:
        with open(match_file) as f:
            lines = f.readlines()
        for line in lines[1:]:
            parts = line.strip().split(";")
            if len(parts) < 6:
                continue
            yfull_id = parts[4]
            country = parts[3]
            terminal_hg = parts[5]
            tmrca = parts[1]

            yield {
                "id": f"yfull_snp_match_{yfull_id}",
                "source": "yfull",
                "title": f"Y-DNA Match: {yfull_id} ({country})",
                "date": datetime.fromtimestamp(match_file.stat().st_mtime).strftime("%Y-%m-%d"),
                "text": (
                    f"Y-DNA SNP match with {yfull_id} from {country}. "
                    f"Terminal haplogroup: {terminal_hg}. TMRCA: {tmrca}."
                ),
                "metadata": {"type": "snp_match", "country": country, "channel": "authored"},
            }
            count += 1

    # Parse mtDNA FASTA
    fasta_files = list(path.glob("*.fasta"))
    for fasta_file in fasta_files:
        with open(fasta_file) as f:
            lines = f.readlines()
        seq = "".join(l.strip() for l in lines[1:] if not l.startswith(">"))
        yield {
            "id": f"yfull_mtdna_{fasta_file.stem}",
            "source": "yfull",
            "title": f"mtDNA Sequence ({fasta_file.stem})",
            "date": datetime.fromtimestamp(fasta_file.stat().st_mtime).strftime("%Y-%m-%d"),
            "text": f"Mitochondrial DNA sequence: {len(seq)} bp.",
            "metadata": {"type": "mtdna", "length": len(seq), "channel": "authored"},
        }
        count += 1

    log.info(f"YFull: emitted {count} entries")
