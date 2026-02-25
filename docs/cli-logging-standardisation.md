# Paradime CLI — Standardised Console Logging

> **Branch:** `claude/standardize-cli-logging-sA6vL`
> **Status:** Proposal / Design

---

## 1. Executive Summary

The Paradime CLI currently mixes four distinct output mechanisms — `print()`, `click.echo()`, `console.print()` (Rich), and `logger.info()` — across 40+ files. This produces an inconsistent user experience: ASCII-art separators sit next to Rich panels, raw timestamps collide with unstyled `click.echo` lines, and error messages have no consistent visual language.

This document proposes a **single, thin `paradime.cli.console` module** that wraps [Rich](https://rich.readthedocs.io) and exposes a small, stable API that every layer of the CLI uses. The aesthetic target is the same restrained, typographic style used by Claude Code: monochrome-first, purposeful colour, live spinners for long operations, and clean aligned tables — no heavy ASCII art.

---

## 2. Current State Analysis

### 2.1 Output mechanisms in use

| Mechanism | Files | Typical usage |
|---|---|---|
| `print()` (bare Python) | All 15 `core/scripts/*.py` | ASCII progress tables, per-item status |
| `click.echo()` | All 9 `cli/*.py` & 15 `cli/integrations/*.py` | Command start/end messages, errors |
| `console.print()` (Rich) | `cli/rich_text_output.py` (8 functions) | Bolt run status, header panel, error table |
| `logging.basicConfig` / `logger.info` | All 15 `core/scripts/*.py` | Timestamped debug lines during polling |

### 2.2 Visual inconsistencies

```
# core/scripts/airbyte.py — heavy ASCII art
============================================================
🚀 TRIGGERING AIRBYTE JOBS
============================================================

[1/3] 🔌 conn-abc
----------------------------------------

# cli/integrations/airbyte.py — plain echo
Starting sync jobs for 3 Airbyte connection(s)...

# cli/rich_text_output.py — Rich panel with purple border
┌────────────────────── Paradime CLI v0.9.2 ──────────────────────┐
│  Use the Paradime CLI to login and trigger Bolt dbt schedules…   │
└──────────────────────────────────────────────────────────────────┘
```

All three can appear **in the same command invocation**.

### 2.3 Structural problems

- `rich_text_output.py` is only imported by `bolt.py` and `catalog.py`; the 15 integration CLI files never use it.
- Core scripts print directly to `stdout`; they have no awareness of `--json` mode or quiet mode.
- `logging.basicConfig` configures the root logger globally at import time, which can interfere with any host application that imports the SDK.
- There is no `--no-color` / `TERM=dumb` path; Rich handles this automatically, but bare `print()` calls emit ANSI escape codes on some platforms.

---

## 3. Design Principles

These mirror the Claude Code aesthetic:

1. **Monochrome-first.** Text communicates; colour reinforces. Never use colour as the only signal.
2. **Symbol prefix over emoji.** Use `✓ ✗ ⚠ ℹ` for semantic states. Emojis are decorative and optional.
3. **Spinners for latency, not ASCII separators.** Long-running operations get a single in-place spinner line, not a banner that repeats every poll cycle.
4. **Clean tables.** Rich `Table` with no visible box border (`box=None` or `box.SIMPLE_HEAD`) for data display. No `====` or `----` hand-drawn dividers.
5. **Structured JSON output.** Every command that emits data also supports `--json` for machine consumption. The human path and the JSON path are branched at the top of the call stack, not scattered throughout helper functions.
6. **Single `console` singleton.** All output goes through one `rich.console.Console` instance. This enables `TERM=dumb` and `NO_COLOR` propagation automatically.
7. **Opt-in verbosity.** The default log level shows meaningful progress. Verbose/debug detail is behind `--verbose` / `PARADIME_LOG_LEVEL=debug`.

---

## 4. Technical Design

### 4.1 New module: `paradime/cli/console.py`

This is the **only** file that imports Rich directly. Everything else calls into this module.

```python
"""
paradime/cli/console.py
~~~~~~~~~~~~~~~~~~~~~~~
Unified console output for the Paradime CLI.

All CLI and core-script output should use the helpers in this module
rather than calling print(), click.echo(), or Rich directly.
"""

from __future__ import annotations

import json
import sys
from contextlib import contextmanager
from typing import Generator, Iterable, Sequence

from rich.console import Console
from rich.live import Live
from rich.spinner import Spinner
from rich.status import Status
from rich.table import Table, box
from rich.text import Text
from rich.theme import Theme

# ---------------------------------------------------------------------------
# Theme — Paradime brand colours, scoped so we never fight the host terminal
# ---------------------------------------------------------------------------

_THEME = Theme(
    {
        "info":    "dim cyan",
        "success": "bold green",
        "warning": "bold yellow",
        "error":   "bold red",
        "muted":   "dim",
        "brand":   "#827be6",          # Paradime purple
        "url":     "underline #9696a0",
        "label":   "bold",
    }
)

console = Console(theme=_THEME, highlight=False)
err_console = Console(theme=_THEME, stderr=True, highlight=False)


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

        with console.spinner("Triggering Airbyte sync…") as status:
            results = trigger_airbyte_jobs(...)
            status.update("Waiting for completion…")
        console.success("Sync complete.")
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
    click_echo = _lazy_click_echo()
    click_echo(json.dumps(data))


def _lazy_click_echo():
    """Import click lazily to keep this module importable without click."""
    import click
    return click.echo
```

### 4.2 Updated `rich_text_output.py` (transition shim)

During the migration, keep `rich_text_output.py` as a thin wrapper that delegates to `console.py`, so existing callers in `bolt.py` keep working with zero changes:

```python
"""
paradime/cli/rich_text_output.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Backwards-compatible shim — delegates to paradime.cli.console.
New code should import from paradime.cli.console directly.
"""

from pathlib import Path
from paradime.cli import console as _c


def print_cli_header(version: str) -> None:
    _c.header(
        f"Paradime CLI v{version}",
        subtitle="Trigger Bolt dbt schedules from the terminal.",
    )
    _c.url(
        "Examples",
        "https://github.com/paradime-io/paradime-python-sdk/tree/main/examples",
    )


def print_error_table(error: str, is_json: bool) -> None:
    if is_json:
        _c.json_out({"error": error})
        return
    _c.error(error)


def print_run_started(run_id: int, is_json: bool) -> None:
    if is_json:
        _c.json_out({"run_id": run_id, "url": f"https://app.paradime.io/bolt/run_id/{run_id}"})
        return
    _c.success("Bolt run has started.")
    _c.url("Run details", f"https://app.paradime.io/bolt/run_id/{run_id}")


def print_success(message: str, is_json: bool) -> None:
    if is_json:
        _c.json_out({"message": message})
        return
    _c.success(message)


def print_run_status(status: str, json: bool) -> None:
    if not json:
        _c.info(f"Current run status: {status}")


def print_artifact_downloading(*, schedule_name: str, artifact_path: str) -> None:
    _c.info(f"Downloading {artifact_path!r} from schedule {schedule_name!r}…")


def print_artifact_downloaded(artifact_path: Path) -> None:
    _c.success(f"Artifact saved to {artifact_path.absolute().as_posix()!r}.")
```

### 4.3 Integration CLI pattern (before → after)

**Before** (`cli/integrations/airbyte.py:76`):

```python
click.echo(f"Starting {job_type} jobs for {len(connection_id)} Airbyte connection(s)...")
try:
    results = trigger_airbyte_jobs(...)
    ...
except Exception as e:
    click.echo(f"❌ Airbyte {job_type} failed: {str(e)}")
    raise click.Abort()
```

**After**:

```python
from paradime.cli import console

console.header(f"Airbyte — {job_type.capitalize()} Jobs")
try:
    with console.spinner(f"Triggering {job_type} for {len(connection_id)} connection(s)…") as status:
        results = trigger_airbyte_jobs(...)
        status.update("Waiting for completion…")
except Exception as e:
    console.error(f"Airbyte {job_type} failed: {e}", exit_code=1)

failed = [r for r in results if any(w in r for w in ("FAILED", "CANCELLED", "INCOMPLETE"))]
if failed:
    sys.exit(1)
```

### 4.4 Core scripts pattern (before → after)

**Before** (`core/scripts/airbyte.py:57`):

```python
print(f"\n{'='*60}")
print("🚀 TRIGGERING AIRBYTE JOBS")
print(f"{'='*60}")
...
print(f"{'CONNECTION':<25} {'STATUS':<10} {'JOB TYPE':<10}")
print(f"{'-'*25} {'-'*10} {'-'*10}")
for connection_id, response_txt in connection_results:
    print(f"{connection_id:<25} {status:<10} {job_type.upper()}")
```

**After**:

```python
from paradime.cli import console

console.header("Triggering Airbyte Jobs")

# ... (job logic unchanged) ...

console.table(
    columns=["Connection", "Status", "Job Type"],
    rows=[
        (conn_id, _status_text(resp), job_type.upper())
        for conn_id, resp in connection_results
    ],
    title="Job Results",
)
```

### 4.5 Visual output comparison

**Before** (current):

```
============================================================
🚀 TRIGGERING AIRBYTE JOBS
============================================================

[1/3] 🔌 conn-abc
----------------------------------------
12:30:01 🔍 [conn-abc] Checking connection status...
12:30:01 📊 [conn-abc] Status: active
12:30:02 🚀 [conn-abc] Triggering sync job...
12:30:02 ✅ [conn-abc] Job triggered (ID: 42)

================================================================================
📊 JOB RESULTS
================================================================================
CONNECTION               STATUS     JOB TYPE
------------------------- ---------- ----------
conn-abc                 ✅ SUCCESS  SYNC
conn-def                 ❌ FAILED   SYNC
================================================================================
```

**After** (proposed):

```
  Airbyte — Sync Jobs

  ⠸ Triggering sync for 3 connection(s)…

  Job Results

   Connection   Status      Job Type
  ─────────────────────────────────────
   conn-abc     ✓ Success   SYNC
   conn-def     ✗ Failed    SYNC
   conn-ghi     ✓ Success   SYNC

✓  2 of 3 jobs succeeded.
✗  1 job failed — check the Airbyte dashboard.
```

### 4.6 Logging (replacing `logging.basicConfig`)

The global `logging.basicConfig` in all `core/scripts/*.py` files is removed. Structured debug output is routed through the same `console` module using a `PARADIME_LOG_LEVEL` environment variable check:

```python
# console.py addition
import os as _os

_LOG_LEVEL = _os.environ.get("PARADIME_LOG_LEVEL", "info").lower()

def debug(message: str) -> None:
    """Only shown when PARADIME_LOG_LEVEL=debug."""
    if _LOG_LEVEL == "debug":
        console.print(f"[muted]  {message}[/]")
```

This replaces `logger.info(...)` calls with `console.debug(...)` — invisible by default, shown when the user sets `PARADIME_LOG_LEVEL=debug`.

---

## 5. Colour Palette

| Token | Hex / Named | Used for |
|---|---|---|
| `brand` | `#827be6` | Headers, titles, the Paradime "feel" |
| `success` | `bold green` | ✓ confirmations |
| `error` | `bold red` | ✗ failures (on stderr) |
| `warning` | `bold yellow` | ⚠ non-fatal issues |
| `info` / `muted` | `dim` | Secondary text, spinner labels, timestamps |
| `url` | `underline #9696a0` | Clickable links |
| `label` | `bold` | Key/value labels |

All tokens are declared once in `_THEME` and referenced by name. If the terminal signals `NO_COLOR` or `TERM=dumb`, Rich automatically strips all styling — no code change needed.

---

## 6. Implementation Plan

### Phase 1 — Foundation (1–2 days)

- [ ] Create `paradime/cli/console.py` with the API described in §4.1.
- [ ] Rewrite `paradime/cli/rich_text_output.py` as a shim (§4.2) — all existing `bolt.py` / `catalog.py` tests pass with zero changes.
- [ ] Add `PARADIME_LOG_LEVEL` env var support.
- [ ] Update `pyproject.toml` if any new dependency is needed (none expected; Rich is already listed).

### Phase 2 — CLI integration layer (1 day)

- [ ] Update `paradime/cli/login.py` — replace `click.echo` with `console.*`.
- [ ] Update `paradime/cli/version.py` — use `console.header`.
- [ ] Update all 15 `paradime/cli/integrations/*.py` — replace `click.echo` with `console.*` and wrap core calls in `console.spinner`.

### Phase 3 — Core scripts (2 days)

- [ ] Update all 15 `paradime/core/scripts/*.py`:
  - Remove `logging.basicConfig` / `logger` setup.
  - Replace `print()` ASCII-art headers with `console.header()`.
  - Replace hand-drawn tables with `console.table()`.
  - Replace inline timestamp `print()` status lines with `console.debug()`.
- [ ] Ensure core scripts no longer import `logging` (or scope it to a single shared util if truly needed).

### Phase 4 — Validation

- [ ] Run the full test suite — confirm no regressions.
- [ ] Manual smoke test: `paradime bolt run`, `paradime run airbyte-sync`, `paradime login`.
- [ ] Test `--json` flag still produces clean JSON on stdout with no Rich markup.
- [ ] Test `NO_COLOR=1 paradime bolt run` — confirms no ANSI codes leak.
- [ ] Test `PARADIME_LOG_LEVEL=debug paradime run airbyte-sync` — debug lines appear.

---

## 7. File Change Summary

| File | Action | Scope |
|---|---|---|
| `paradime/cli/console.py` | **Create** | ~120 lines, new canonical output module |
| `paradime/cli/rich_text_output.py` | **Rewrite** as shim | ~30 lines (down from 82) |
| `paradime/cli/login.py` | **Update** | 2 `click.echo` → `console.*` |
| `paradime/cli/version.py` | **Update** | 1–2 lines |
| `paradime/cli/bolt.py` | **No change** | Calls `rich_text_output.py` shim |
| `paradime/cli/catalog.py` | **No change** | Calls `rich_text_output.py` shim |
| `paradime/cli/integrations/*.py` (×15) | **Update** | `click.echo` → `console.*`, add spinners |
| `paradime/core/scripts/*.py` (×15) | **Update** | Remove `print()`/`logging`, use `console.*` |

**No changes** to: APIs, client code, argument parsing, `pyproject.toml`, or tests.

---

## 8. Non-Goals

- Switching from Click to Typer. Click is working well; Typer adds no value here and the migration cost is high.
- Adding a progress bar / `tqdm`. Rich spinners are sufficient for the current use cases.
- Changing command structure or argument names.
- Breaking the `--json` contract for downstream CI consumers.

---

## 9. Risks & Mitigations

| Risk | Mitigation |
|---|---|
| Rich `Status` spinner interferes with stdout in CI environments | `Console(stderr=True)` for the spinner keeps stdout clean; CI typically sets `NO_COLOR` or is non-TTY, which auto-disables animation |
| Core scripts used as SDK library, not just CLI | `console.py` must be importable without side effects; `logging.basicConfig` removal already fixes the main issue |
| Windows terminal ANSI support | Rich handles this natively via its Windows console detection |
| `--json` regression — Rich markup leaks into JSON output | `console.py` JSON path uses `click.echo` directly (no Rich); covered by Phase 4 testing |
