import re
import sys
import time
from pathlib import Path
from typing import List, Optional, Set

import click
from prompt_toolkit import PromptSession
from prompt_toolkit.history import FileHistory
from rich.markdown import Markdown
from rich.markup import escape
from rich.panel import Panel
from rich.text import Text

from paradime.apis.dinoai_agents.types import DinoaiAgentRun, DinoaiAgentRunStatus, DinoaiAgentStep
from paradime.cli import console
from paradime.client.api_exception import ParadimeAPIException
from paradime.client.paradime_cli_client import get_cli_client_or_exit
from paradime.client.paradime_client import Paradime

_POLL_INTERVAL = 2  # seconds

# Tool steps are printed as one line each. Their input arrives cut at this
# length, which is plenty for the line and keeps each poll small.
_STEP_MAX_CHARS = 300
_TOOL_LABELS = {
    "run_sql_query": "Ran a SQL query",
    "search_catalog": "Searched the data catalog",
    "get_all_models": "Listed the dbt models",
    "get_mart_models": "Listed the mart models",
    "get_node_details": "Read a model's details",
    "get_lineage": "Traced the lineage",
    "get_column_level_lineage": "Traced a column's lineage",
    "get_model_health": "Checked the model's health",
    "get_model_performance": "Checked the model's run times",
    "read_file": "Read a file",
    "edit_file": "Edited a file",
    "write_file": "Wrote a file",
    "ripgrep_search": "Searched the project",
    "search_files_and_directories": "Browsed the project",
    "run_terminal_command": "Ran a command",
    "todo_write": "Planned the steps",
    "load_skill_instructions": "Loaded a skill",
    "run_subagent": "Ran a sub-task",
    "invoke_agent": "Asked another agent",
}
# Delivery and bookkeeping calls: their result already shows as a message, or
# is not work the reader cares about.
_HIDDEN_TOOLS = frozenset({"send_api_message", "post_slack_message", "todo_read"})
# The argument that says what a call was about, in order of preference.
_DETAIL_KEYS = (
    "query",
    "sql",
    "command",
    "search_query",
    "pattern",
    "path",
    "file_path",
    "unique_id",
    "model_name",
    "task",
)
# The input can be cut mid-string, so read the value with a regex, not json.loads.
_DETAIL = re.compile(r'"(%s)"\s*:\s*"((?:[^"\\]|\\.)*)' % "|".join(_DETAIL_KEYS))
_MAX_DETAIL = 90


@click.command()
@click.option("--agent", "-a", default=None, help="Named agent (.dinoai/agents/<name>.yml).")
@click.option("--message", "-m", default=None, help="Opening message.")
@click.option("--session", "-s", default=None, help="Resume an existing session by ID.")
@click.option(
    "--model-family",
    default=None,
    help="Model family (model preset) slug for a new session. An unknown slug runs on the "
    "workspace's default family, with a warning.",
)
def dinoai(
    agent: Optional[str],
    message: Optional[str],
    session: Optional[str],
    model_family: Optional[str],
) -> None:
    """
    Talk to your data with a DinoAI programmable agent.

    Passing --message runs a single turn and exits (no interactive loop).
    Omit --message to drop into the interactive prompt.

    \b
    Examples:
      paradime dinoai
      paradime dinoai --agent data-quality-checker
      paradime dinoai --message "What dbt tests are failing?"
      paradime dinoai --session xwzdneft6emspe0f
      paradime dinoai --agent analyst --model-family fast --message "Revenue by month?"
    """
    if model_family and session:
        raise click.UsageError(
            "--model-family applies when a new session starts; it cannot change the model "
            "of an existing session."
        )
    client = get_cli_client_or_exit()
    session_id: Optional[str] = session
    # Track rendered messages by ts to dedup across poll iterations and turns,
    # even if the backend re-orders or re-emits the run.messages list.
    rendered: Set[str] = set()
    steps = _StepFeed()

    # Resume: replay existing history so the user has context
    if session_id:
        with console.spinner("Loading session…"):
            run = steps.read(client, session_id)
        # Earlier turns' tool calls are history: only show the ones that follow.
        steps.new_lines(run)
        console.console.print(_session_panel(agent, session_id))
        last_content: Optional[str] = None
        for msg in run.messages:
            if msg.content == last_content:
                continue
            _render_message(msg.role, msg.content)
            rendered.add(msg.ts)
            last_content = msg.content

    # Piped stdin — read message from stdin if not provided
    if not sys.stdin.isatty() and not message:
        message = sys.stdin.read().strip()

    # Run-once mode: --message provided (or piped via stdin) → fire one turn and
    # exit, non-zero on failure so scripts and Bolt schedules can detect it
    if message:
        _, final_status = _send(
            client,
            agent=agent,
            message=message,
            session_id=session_id,
            rendered=rendered,
            steps=steps,
            model_family=model_family,
        )
        if final_status in (
            DinoaiAgentRunStatus.FAILED,
            DinoaiAgentRunStatus.EXPIRED,
            DinoaiAgentRunStatus.STOPPED,
        ):
            sys.exit(1)
        return

    # No message and no TTY — nothing to do
    if not sys.stdin.isatty():
        return

    # Interactive loop
    history_path = Path.home() / ".paradime" / "dinoai_history"
    history_path.parent.mkdir(parents=True, exist_ok=True)
    prompt_session: PromptSession = PromptSession(history=FileHistory(str(history_path)))

    while True:
        try:
            user_input = prompt_session.prompt("> ")
        except (KeyboardInterrupt, EOFError):
            break

        if not user_input.strip():
            break

        try:
            new_session_id, _ = _send(
                client,
                agent=agent,
                message=user_input,
                session_id=session_id,
                rendered=rendered,
                steps=steps,
                model_family=model_family,
            )
            # Show session panel once, when the session is first established
            if session_id is None:
                console.console.print(_session_panel(agent, new_session_id))
            session_id = new_session_id
        except ParadimeAPIException as exc:
            console.error(str(exc))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _send(
    client: Paradime,
    *,
    agent: Optional[str],
    message: str,
    session_id: Optional[str],
    rendered: Set[str],
    steps: "_StepFeed",
    model_family: Optional[str] = None,
) -> tuple[str, Optional[DinoaiAgentRunStatus]]:
    new_session = session_id is None
    if session_id is None:
        result = client.dinoai_agents.trigger_run(
            agent=agent,
            message=message,
            model_family=model_family,
        )
        session_id = result.agent_session_id
        if result.warning:
            console.warning(result.warning)
    else:
        client.dinoai_agents.send_message(agent_session_id=session_id, message=message)

    # Print the session ID up front so the user can resume if they Ctrl-C
    if new_session:
        console.console.print(f"[dim]Session {session_id}[/]")

    final_status = _poll(client, session_id=session_id, rendered=rendered, steps=steps)
    return session_id, final_status


def _poll(
    client: Paradime, *, session_id: str, rendered: Set[str], steps: "_StepFeed"
) -> Optional[DinoaiAgentRunStatus]:
    """Poll until a terminal status, streaming new messages to the console.

    Returns the terminal status, or None if the user aborted with Ctrl-C.

    Dedup is done by ts (message timestamp), tracked across all turns — this is
    resilient to backend re-ordering or re-emission of the messages list.

    For COMPLETED/FAILED the break condition requires an actual agent message to
    have streamed in — we don't trust a stale terminal status carrying over from
    the previous turn. STOPPED only needs the turn to have started, since a stopped
    run may never emit a message. EXPIRED breaks immediately: the pod never started,
    so no message will ever arrive.

    An unknown status (a newer backend state this SDK doesn't model) is treated as
    non-terminal, so polling continues rather than failing.

    Ctrl-C aborts the current turn without killing the chat — the run continues
    server-side and can be rejoined with `--session <id>`.

    On a backend that reports the run's steps, each finished tool call prints as
    one line, and while the agent pod boots the spinner shows its start-up step.
    """
    start = time.monotonic()
    display_status = "QUEUED"
    run_status = "QUEUED"
    new_agent_messages = 0
    turn_started = False
    last_content: Optional[str] = None
    try:
        with console.spinner(_spinner_label(display_status, start)) as status:
            while True:
                run = steps.read(client, session_id)
                run_status = run.status.value

                for line in steps.new_lines(run):
                    console.console.print(f"[muted]  ↳ {escape(line)}[/]")

                for msg in run.messages:
                    if msg.ts in rendered:
                        last_content = msg.content
                        continue
                    if msg.role.lower() == "user":
                        rendered.add(msg.ts)
                        last_content = msg.content
                        continue
                    # Agents sometimes emit the same content twice (e.g. once via a
                    # send_api_message tool call, then again as a final assistant
                    # turn). Skip if content matches the previous message.
                    if msg.content == last_content:
                        rendered.add(msg.ts)
                        continue
                    _render_message(msg.role, msg.content)
                    rendered.add(msg.ts)
                    last_content = msg.content
                    new_agent_messages += 1

                if run.status == DinoaiAgentRunStatus.EXPIRED:
                    console.error(
                        "The agent never started (expired). Retry the run — if this "
                        "persists, check the workspace's agent configuration."
                    )
                    return run.status

                is_terminal = run.status in (
                    DinoaiAgentRunStatus.COMPLETED,
                    DinoaiAgentRunStatus.FAILED,
                    DinoaiAgentRunStatus.STOPPED,
                )
                # Evidence that *this* turn started: either the run is no longer in a
                # terminal state, or an agent message has streamed in.
                turn_started = turn_started or not is_terminal or new_agent_messages > 0

                # A stopped run never emits another message, so seeing the turn start is
                # enough — waiting for a message would hang if it was stopped while idle.
                if run.status == DinoaiAgentRunStatus.STOPPED and turn_started:
                    console.error(
                        "The run was stopped before it finished. Send another message to "
                        f"continue, or rejoin with: paradime dinoai --session {session_id}"
                    )
                    return run.status

                if is_terminal and new_agent_messages > 0:
                    if run.status == DinoaiAgentRunStatus.FAILED:
                        last = run.messages[-1].content if run.messages else "no details"
                        console.error(f"Run failed: {last}")
                    return run.status

                # If the status is still terminal but no agent message has streamed
                # yet, we're seeing a stale carry-over from the previous turn —
                # show "WAITING" instead of "COMPLETED" so it doesn't look frozen.
                if is_terminal:
                    display_status = "WAITING"
                else:
                    display_status = run.status.value
                    booting = [s.label for s in run.startup_steps or [] if not s.done]
                    if run.status == DinoaiAgentRunStatus.QUEUED and booting:
                        display_status = booting[0]

                status.update(_spinner_label(display_status, start))
                time.sleep(_POLL_INTERVAL)
    except KeyboardInterrupt:
        console.console.print(
            f"[muted]↩ aborted local view — run still {run_status} server-side. "
            f"Rejoin with: paradime dinoai --session {session_id}[/]"
        )
        return None


class _StepFeed:
    """The tool calls of a run, on backends that report ``dinoaiAgentRun.steps``.

    Older backends reject the steps query. The first refusal switches this
    session to the plain run, so the chat keeps working without the step lines.
    """

    def __init__(self) -> None:
        self.supported: Optional[bool] = None
        self.seen: Set[int] = set()

    def read(self, client: Paradime, session_id: str) -> DinoaiAgentRun:
        if self.supported is not False:
            try:
                run = client.dinoai_agents.get_run(
                    agent_session_id=session_id,
                    include_steps=True,
                    include_tool_io=True,
                    steps_after=max(self.seen) if self.seen else None,
                    max_chars=_STEP_MAX_CHARS,
                )
            except ParadimeAPIException as exc:
                if "Cannot query field" not in str(exc):
                    raise
                self.supported = False
            else:
                self.supported = True
                return run
        return client.dinoai_agents.get_run(agent_session_id=session_id)

    def new_lines(self, run: DinoaiAgentRun) -> List[str]:
        """One line for each tool call not shown yet."""
        lines = []
        for step in run.steps or []:
            if step.index in self.seen:
                continue
            self.seen.add(step.index)
            line = _describe_step(step)
            if line:
                lines.append(line)
        return lines


def _describe_step(step: DinoaiAgentStep) -> Optional[str]:
    """A line for a tool call, or None. The agent's text already streams as messages."""
    if step.role.upper() != "TOOL" or step.tool_name in _HIDDEN_TOOLS:
        return None
    name = step.tool_name or ""
    label = _TOOL_LABELS.get(name) or name.replace("_", " ").capitalize() or "Used a tool"
    detail = _step_detail(step.tool_input)
    return f"{label}: {detail}" if detail else label


def _step_detail(tool_input: Optional[str]) -> Optional[str]:
    if not tool_input:
        return None
    values = dict(_DETAIL.findall(tool_input))
    for key in _DETAIL_KEYS:
        if values.get(key):
            text = " ".join(values[key].replace("\\n", " ").replace('\\"', '"').split())
            return text if len(text) <= _MAX_DETAIL else text[:_MAX_DETAIL].rstrip() + " …"
    return None


def _spinner_label(status_text: str, start: float) -> str:
    elapsed = int(time.monotonic() - start)
    mins, secs = divmod(elapsed, 60)
    return f"DinoAI is thinking… ({status_text}, {mins}:{secs:02d})"


def _session_panel(agent: Optional[str], session_id: Optional[str]) -> Panel:
    content = Text()
    content.append("dinoai  ", style="bold white")
    content.append(agent or "ad-hoc", style="bold #827be6")
    content.append("\n")
    if session_id:
        content.append(f"Session {session_id}  ·  ", style="dim")
    content.append("blank line or Ctrl-C to exit", style="dim")
    return Panel(content, border_style="#827be6", padding=(0, 1))


def _render_message(role: str, content: str) -> None:
    """Render a message — user turns get a subtle prefix; agent turns render as markdown."""
    console.console.print()
    if role.lower() == "user":
        console.console.print(f"[muted]> {content}[/]")
    else:
        try:
            console.console.print(Markdown(content))
        except Exception:
            console.console.print(content)
    console.console.print()
