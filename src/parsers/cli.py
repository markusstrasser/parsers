"""CLI entry point — stdout is JSONL only, all logging to stderr."""

import json
import logging
import sys
from pathlib import Path

import typer

from parsers.registry import list_sources, parse

app = typer.Typer(add_completion=False, no_args_is_help=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s: %(message)s",
    stream=sys.stderr,
)
log = logging.getLogger("parsers")


@app.command("list")
def cmd_list():
    """Show available parsers."""
    for name in list_sources():
        typer.echo(name, err=True)


@app.command()
def run(
    source: str = typer.Argument(help="Source name (e.g. twitter, imessage, chatgpt)"),
    path: str | None = typer.Argument(None, help="Path to export file or directory"),
    output: Path | None = typer.Option(None, "-o", "--output", help="Output file (default: stdout)"),
):
    """Parse a data export into JSONL records."""
    if source == "list":
        cmd_list()
        return

    out = open(output, "w") if output else sys.stdout
    count = 0
    try:
        for record in parse(source, path=path):
            out.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")
            count += 1
    except ValueError as e:
        log.error(str(e))
        raise typer.Exit(2)
    except FileNotFoundError as e:
        log.error(f"Input not found: {e}")
        raise typer.Exit(2)
    finally:
        if output:
            out.close()

    log.info(f"Emitted {count} records from {source}")


if __name__ == "__main__":
    app()
