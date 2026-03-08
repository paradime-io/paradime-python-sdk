"""
paradime/cli/console.py
~~~~~~~~~~~~~~~~~~~~~~~
Unified console output for the Paradime CLI.

All CLI and core-script output should use the helpers in this module
rather than calling print(), click.echo(), or Rich directly.
"""

from __future__ import annotations

import json
import os
import sys
from contextlib import contextmanager
from typing import Generator, Iterable, Sequence

from rich import box
from rich.console import Console
from rich.status import Status
from rich.table import Table
from rich.theme import Theme

# ---------------------------------------------------------------------------
# Theme — Paradime brand colours, scoped so we never fight the host terminal
# ---------------------------------------------------------------------------

_THEME = Theme(
    {
        "info": "dim cyan",
        "success": "bold green",
        "warning": "bold yellow",
        "error": "bold red",
        "muted": "dim",
        "brand": "#827be6",
        "url": "underline #9696a0",
        "label": "bold",
    }
)

console = Console(theme=_THEME, highlight=False)
err_console = Console(theme=_THEME, stderr=True, highlight=False)

# ---------------------------------------------------------------------------
# Log level — controlled by PARADIME_LOG_LEVEL env var
# ---------------------------------------------------------------------------

_LOG_LEVEL = os.environ.get("PARADIME_LOG_LEVEL", "info").lower()

# ---------------------------------------------------------------------------
# Semantic log functions
# ---------------------------------------------------------------------------


def info(message: str) -> None:
    """Dim informational line — background process noise."""
    console.print(f"[muted]ℹ[/]  {message}", highlight=False)


def success(message: str) -> None:
    """Green success confirmation."""
    console.print(f"[success]✓[/]  {message}")


def warning(message: str) -> None:
    """Yellow warning — non-fatal, worth reading."""
    console.print(f"[warning]⚠[/]  {message}")


def error(message: str, *, exit_code: int | None = None) -> None:
    """Red error line on stderr. Optionally exit."""
    err_console.print(f"[error]✗[/]  {message}")
    if exit_code is not None:
        sys.exit(exit_code)


def debug(message: str) -> None:
    """Only shown when PARADIME_LOG_LEVEL=debug."""
    if _LOG_LEVEL == "debug":
        console.print(f"[muted]  {message}[/]")


def header(title: str, subtitle: str | None = None) -> None:
    """Command header — bold brand-coloured title, optional muted subtitle."""
    console.print()
    console.print(f"[brand bold]{title}[/]")
    if subtitle:
        console.print(f"[muted]  {subtitle}[/]")
    console.print()


def url(label: str, href: str) -> None:
    """Display a labelled URL."""
    console.print(f"  {label}: [url]{href}[/]")


def kv(label: str, value: str) -> None:
    """Key / value detail row, aligned."""
    console.print(f"  [label]{label}:[/] {value}")


# ---------------------------------------------------------------------------
# Progress / spinner
# ---------------------------------------------------------------------------


@contextmanager
def spinner(message: str) -> Generator[Status, None, None]:
    """
    Context manager wrapping a Rich Status spinner.

    Usage::

        with spinner("Triggering Airbyte sync…") as status:
            results = trigger_airbyte_jobs(...)
            status.update("Waiting for completion…")
        success("Sync complete.")
    """
    with console.status(f"[muted]{message}[/]", spinner="dots") as status:
        yield status


# ---------------------------------------------------------------------------
# Tables
# ---------------------------------------------------------------------------


def table(
    columns: Sequence[str],
    rows: Iterable[Sequence[str]],
    *,
    title: str | None = None,
    caption: str | None = None,
) -> None:
    """
    Print a clean table using Rich.

    Column headers are bold; rows are plain.  No visible box border —
    just spacing and a thin header rule.
    """
    t = Table(
        box=box.SIMPLE_HEAD,
        show_footer=False,
        title=title,
        caption=caption,
        header_style="bold",
        title_style="brand bold",
        caption_style="muted",
        expand=False,
    )
    for col in columns:
        t.add_column(col, no_wrap=False)
    for row in rows:
        t.add_row(*row)
    console.print(t)


# ---------------------------------------------------------------------------
# JSON output
# ---------------------------------------------------------------------------


def json_out(data: dict | list) -> None:
    """
    Emit machine-readable JSON to stdout.

    Call this instead of all Rich helpers when --json flag is set.
    """
    import click

    click.echo(json.dumps(data))
