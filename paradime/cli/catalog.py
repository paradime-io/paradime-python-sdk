import re
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable, Dict, Final, List, Optional

import click
import requests

from paradime.apis.catalog.types import (
    CatalogRefresh,
    CatalogRefreshProgress,
    CatalogRefreshStatus,
    CatalogRefreshStep,
    CatalogRefreshStepStatus,
)
from paradime.client.api_exception import ParadimeAPIException
from paradime.client.paradime_cli_client import get_cli_client_or_exit

POLL_INTERVAL: Final = 5.0
"""Seconds between two reads of the refresh state."""
HEARTBEAT_INTERVAL: Final = 60.0
"""Seconds without any change before a running step logs that it is still running."""
MAX_POLL_BACKOFF: Final = 60.0
"""The longest wait between two reads after a read failed."""
POLL_ERROR_BUDGET: Final = 600.0
"""Seconds of failed reads in a row after which the command gives up."""
DEFAULT_TIMEOUT: Final = "2h"
DETAIL_TAIL_LINES: Final = 40
"""How many lines of a failed step's output (e.g. dbt's) to print."""

REFRESH_LABEL: Final = "catalog refresh"

_POLL_ERRORS = (ParadimeAPIException, requests.RequestException, ValueError, KeyError)

_LABEL_WIDTH: Final = 20


def log_line(label: str, message: str, echo: Callable[[str], None] = click.echo) -> None:
    """One plain, aligned log line, e.g. `[Catalog build]     done in 1m03s`."""
    echo(f"{('[' + label + ']').ljust(_LABEL_WIDTH)} {message}")


def step_label(step: CatalogRefreshStep) -> str:
    return step.label or step.kind


def format_duration(seconds: float) -> str:
    seconds = max(0, int(round(seconds)))
    hours, rest = divmod(seconds, 3600)
    minutes, secs = divmod(rest, 60)
    if hours:
        return f"{hours}h{minutes:02d}m"
    if minutes:
        return f"{minutes}m{secs:02d}s"
    return f"{secs}s"


def format_progress(progress: CatalogRefreshProgress) -> str:
    """E.g. `Building assets 18,400 / 41,200 assets`; a total of 0 means unmeasured."""
    if progress.total > 0:
        return f"{progress.stage_label} {progress.done:,} / {progress.total:,} {progress.unit}"
    if progress.done > 0:
        return f"{progress.stage_label} {progress.done:,} {progress.unit}"
    return progress.stage_label


_DURATION_PART = re.compile(r"(\d+)([hms])")


def parse_timeout(value: str) -> float:
    """Parse `90`, `90s`, `45m`, `2h` or `1h30m` into seconds."""

    text = value.strip().lower()
    if text.isdigit():
        seconds = float(text)
    else:
        parts = _DURATION_PART.findall(text)
        if not parts or "".join(number + unit for number, unit in parts) != text:
            raise ValueError(f"invalid duration {value!r}, use e.g. 90s, 45m or 2h")
        scale = {"h": 3600, "m": 60, "s": 1}
        seconds = float(sum(int(number) * scale[unit] for number, unit in parts))
    if seconds <= 0:
        raise ValueError("the timeout must be more than 0")
    return seconds


def _parse_time(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


@dataclass
class _StepState:
    status: CatalogRefreshStepStatus = CatalogRefreshStepStatus.PENDING
    progress: Optional[CatalogRefreshProgress] = None
    first_seen_running: Optional[float] = None
    last_output: float = 0.0


@dataclass
class RefreshFollower:
    """Follows a catalog refresh until it ends, logging one line per change.

    `run` returns the exit code: 0 only if every step succeeded (or, with
    `allow_integration_failures`, only non-blocking steps failed).
    """

    get_refresh: Callable[[str], Optional[CatalogRefresh]]
    token: str
    timeout: float
    allow_integration_failures: bool = False
    echo: Callable[[str], None] = click.echo
    sleep: Callable[[float], None] = time.sleep
    monotonic: Callable[[], float] = time.monotonic
    now: Callable[[], datetime] = field(default=lambda: datetime.now(timezone.utc))

    def __post_init__(self) -> None:
        self._steps: Dict[str, _StepState] = {}

    def _log(self, label: str, message: str) -> None:
        log_line(label, message, self.echo)

    def run(self) -> int:
        started = self.monotonic()
        deadline = started + self.timeout
        last: Optional[CatalogRefresh] = None
        failing_since: Optional[float] = None
        failures = 0

        while True:
            try:
                refresh = self.get_refresh(self.token)
                if refresh is None:
                    raise ValueError(f"no catalog refresh with id {self.token}")
            except _POLL_ERRORS as error:
                now = self.monotonic()
                failing_since = failing_since if failing_since is not None else now
                failures += 1
                if now - failing_since >= POLL_ERROR_BUDGET:
                    self._log(
                        REFRESH_LABEL,
                        f"FAILED: could not read the refresh state for "
                        f"{format_duration(now - failing_since)}: {error}",
                    )
                    return 1
                wait = min(POLL_INTERVAL * 2 ** (failures - 1), MAX_POLL_BACKOFF)
                self._log(
                    REFRESH_LABEL,
                    f"could not read the refresh state ({error}), retrying in "
                    f"{format_duration(wait)}",
                )
                if now + wait >= deadline:
                    return self._timed_out(last, now - started)
                self.sleep(wait)
                continue

            failing_since = None
            failures = 0
            last = refresh
            self._report(refresh)

            if refresh.finished:
                return self._summary(refresh, self.monotonic() - started)

            now = self.monotonic()
            if now >= deadline:
                return self._timed_out(refresh, now - started)
            self.sleep(min(POLL_INTERVAL, max(deadline - now, 0.0)))

    def _report(self, refresh: CatalogRefresh) -> None:
        now = self.monotonic()
        for step in refresh.steps:
            state = self._steps.setdefault(step.kind, _StepState())
            label = step_label(step)

            if step.status is CatalogRefreshStepStatus.RUNNING:
                if state.status is not CatalogRefreshStepStatus.RUNNING:
                    state.first_seen_running = now
                    message = "started"
                    if step.progress:
                        message += f", {format_progress(step.progress)}"
                    self._log(label, message)
                    state.last_output = now
                elif step.progress and step.progress != state.progress:
                    self._log(label, f"running: {format_progress(step.progress)}")
                    state.last_output = now
                elif now - state.last_output >= HEARTBEAT_INTERVAL:
                    elapsed = self._elapsed(step, state, now)
                    self._log(label, f"still running ({format_duration(elapsed)})")
                    state.last_output = now
            elif step.is_done and step.status is not state.status:
                self._log_done(step, state, now)

            state.status = step.status
            state.progress = step.progress

    def _elapsed(self, step: CatalogRefreshStep, state: _StepState, now: float) -> float:
        started_at = _parse_time(step.started_at)
        if started_at is not None:
            return (self.now() - started_at).total_seconds()
        return now - (state.first_seen_running or now)

    def _duration(self, step: CatalogRefreshStep, state: _StepState, now: float) -> float:
        started_at = _parse_time(step.started_at)
        finished_at = _parse_time(step.finished_at)
        if started_at is not None and finished_at is not None:
            return (finished_at - started_at).total_seconds()
        return now - (state.first_seen_running or now)

    def _log_done(self, step: CatalogRefreshStep, state: _StepState, now: float) -> None:
        label = step_label(step)
        if step.status is CatalogRefreshStepStatus.SUCCEEDED:
            self._log(label, f"done in {format_duration(self._duration(step, state, now))}")
        elif step.status is CatalogRefreshStepStatus.SKIPPED:
            reason = f": {step.error.message}" if step.error else ""
            self._log(label, f"skipped{reason}")
        else:
            if step.error:
                reason = step.error.message
            elif step.status is CatalogRefreshStepStatus.LOST:
                reason = "the step stopped reporting progress"
            else:
                reason = "no error message"
            prefix = "FAILED" if self._fails(step) else "FAILED (allowed)"
            self._log(label, f"{prefix}: {reason}")
            if step.error and step.error.detail:
                lines = step.error.detail.rstrip().splitlines()[-DETAIL_TAIL_LINES:]
                for line in lines:
                    self.echo(f"    {line}")

    def _fails(self, step: CatalogRefreshStep) -> bool:
        """Whether a failed step fails the command."""
        return step.blocking or not self.allow_integration_failures

    def _summary(self, refresh: CatalogRefresh, elapsed: float) -> int:
        total = len(refresh.steps)
        failed = [step for step in refresh.steps if step.is_failed]
        fatal = [step for step in failed if self._fails(step)]
        allowed = [step for step in failed if not self._fails(step)]

        def names(steps: List[CatalogRefreshStep]) -> str:
            return ", ".join(step_label(step) for step in steps)

        if fatal:
            self._log(
                REFRESH_LABEL,
                f"FAILED: {len(failed)} of {total} steps failed ({names(failed)})",
            )
            return 1

        if refresh.status in (CatalogRefreshStatus.FAILED, CatalogRefreshStatus.LOST):
            if refresh.error:
                reason = refresh.error.message
            elif refresh.status is CatalogRefreshStatus.LOST:
                reason = "the refresh stopped reporting progress"
            else:
                reason = "no error message"
            self._log(REFRESH_LABEL, f"FAILED: {reason}")
            if refresh.error and refresh.error.detail:
                for line in refresh.error.detail.rstrip().splitlines()[-DETAIL_TAIL_LINES:]:
                    self.echo(f"    {line}")
            return 1

        if refresh.status is CatalogRefreshStatus.SKIPPED:
            reason = f": {refresh.error.message}" if refresh.error else ""
            self._log(
                REFRESH_LABEL, f"FAILED: the refresh was skipped, nothing was refreshed{reason}"
            )
            return 1

        message = f"done in {format_duration(elapsed)}"
        succeeded = sum(
            1 for step in refresh.steps if step.status is CatalogRefreshStepStatus.SUCCEEDED
        )
        message += f", {succeeded} of {total} steps succeeded"
        if allowed:
            message += (
                f"; {len(allowed)} integration step(s) failed and were allowed "
                f"({names(allowed)})"
            )
        self._log(REFRESH_LABEL, message)
        return 0

    def _timed_out(self, refresh: Optional[CatalogRefresh], elapsed: float) -> int:
        open_steps = [step for step in (refresh.steps if refresh else []) if not step.is_done]
        still = (
            "; not finished: " + ", ".join(step_label(step) for step in open_steps)
            if open_steps
            else ""
        )
        self._log(
            REFRESH_LABEL,
            f"FAILED: timed out after {format_duration(elapsed)}{still}. "
            f"The refresh continues in the background.",
        )
        return 1


class _Timeout(click.ParamType):
    name = "duration"

    def convert(
        self, value: object, param: Optional[click.Parameter], ctx: Optional[click.Context]
    ) -> float:
        if isinstance(value, (int, float)):
            return float(value)
        try:
            return parse_timeout(str(value))
        except ValueError as error:
            self.fail(str(error), param, ctx)


@click.group()
def catalog() -> None:
    """
    Work with Paradime Catalog from the CLI.
    """
    pass


@click.command()
@click.option(
    "--no-wait",
    is_flag=True,
    help="Request the refresh and exit without waiting for it to finish.",
)
@click.option(
    "--timeout",
    type=_Timeout(),
    default=DEFAULT_TIMEOUT,
    show_default=True,
    help=(
        "How long to wait for the refresh, e.g. 90s, 45m or 2h. When it expires the "
        "command fails; the refresh itself continues."
    ),
)
@click.option(
    "--allow-integration-failures",
    is_flag=True,
    help=(
        "Do not fail the command when only an integration step (e.g. DataHub, Altimate) "
        "fails. The failure is still logged. A failed docs generate or catalog build "
        "always fails the command."
    ),
)
def refresh(no_wait: bool, timeout: float, allow_integration_failures: bool) -> None:
    """
    Refresh the catalog and wait until every refresh step ends.

    Logs each step as it starts, progresses and ends, and exits non-zero when a step
    fails. Use --no-wait to only request the refresh.
    """

    client = get_cli_client_or_exit()

    try:
        request = client.catalog.refresh()
    except ParadimeAPIException as error:
        log_line(REFRESH_LABEL, f"FAILED: the refresh could not be requested: {error}")
        sys.exit(1)

    if not request.token:
        log_line(REFRESH_LABEL, "requested; this workspace does not report refresh progress")
        return

    if request.joined:
        log_line(REFRESH_LABEL, f"already running, following refresh id {request.token}")
    else:
        log_line(REFRESH_LABEL, f"started, refresh id {request.token}")

    if no_wait:
        return

    exit_code = RefreshFollower(
        get_refresh=client.catalog.get_refresh,
        token=request.token,
        timeout=timeout,
        allow_integration_failures=allow_integration_failures,
    ).run()
    sys.exit(exit_code)


catalog.add_command(refresh)
