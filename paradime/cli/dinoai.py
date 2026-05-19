import sys
import time
from pathlib import Path
from typing import Optional, Set

import click
from prompt_toolkit import PromptSession
from prompt_toolkit.history import FileHistory
from rich.markdown import Markdown
from rich.panel import Panel
from rich.text import Text

from paradime.apis.dinoai_agents.types import DinoaiAgentRunStatus
from paradime.cli import console
from paradime.client.api_exception import ParadimeAPIException
from paradime.client.paradime_cli_client import get_cli_client_or_exit
from paradime.client.paradime_client import Paradime

_POLL_INTERVAL = 2  # seconds


@click.command()
@click.option("--agent", "-a", default=None, help="Named agent (.dinoai/agents/<name>.yml).")
@click.option("--message", "-m", default=None, help="Opening message.")
@click.option("--session", "-s", default=None, help="Resume an existing session by ID.")
def dinoai(
    agent: Optional[str],
    message: Optional[str],
    session: Optional[str],
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
    """
    client = get_cli_client_or_exit()
    session_id: Optional[str] = session
    # Track rendered messages by ts to dedup across poll iterations and turns,
    # even if the backend re-orders or re-emits the run.messages list.
    rendered: Set[str] = set()

    # Resume: replay existing history so the user has context
    if session_id:
        with console.spinner("Loading session…"):
            run = client.dinoai_agents.get_run(agent_session_id=session_id)
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

    # Run-once mode: --message provided (or piped via stdin) → fire one turn and exit
    if message:
        _send(
            client,
            agent=agent,
            message=message,
            session_id=session_id,
            rendered=rendered,
        )
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
            new_session_id = _send(
                client,
                agent=agent,
                message=user_input,
                session_id=session_id,
                rendered=rendered,
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
) -> str:
    new_session = session_id is None
    if session_id is None:
        result = client.dinoai_agents.trigger_run(
            agent=agent,
            message=message,
        )
        session_id = result.agent_session_id
    else:
        client.dinoai_agents.send_message(agent_session_id=session_id, message=message)

    # Print the session ID up front so the user can resume if they Ctrl-C
    if new_session:
        console.console.print(f"[dim]Session {session_id}[/]")

    _poll(client, session_id=session_id, rendered=rendered)
    return session_id


def _poll(client: Paradime, *, session_id: str, rendered: Set[str]) -> None:
    """Poll until COMPLETED or FAILED, streaming new messages to the console.

    Dedup is done by ts (message timestamp), tracked across all turns — this is
    resilient to backend re-ordering or re-emission of the messages list.

    The break condition requires an actual agent message to have streamed in —
    we don't trust a stale COMPLETED status carrying over from the previous
    turn. The user can Ctrl-C if a run hangs server-side; the chat continues.

    Ctrl-C aborts the current turn without killing the chat — the run continues
    server-side and can be rejoined with `--session <id>`.
    """
    start = time.monotonic()
    display_status = "QUEUED"
    new_agent_messages = 0
    last_content: Optional[str] = None
    try:
        with console.spinner(_spinner_label(display_status, start)) as status:
            while True:
                run = client.dinoai_agents.get_run(agent_session_id=session_id)

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

                is_terminal = run.status in (
                    DinoaiAgentRunStatus.COMPLETED,
                    DinoaiAgentRunStatus.FAILED,
                )

                if is_terminal and new_agent_messages > 0:
                    if run.status == DinoaiAgentRunStatus.FAILED:
                        last = run.messages[-1].content if run.messages else "no details"
                        console.error(f"Run failed: {last}")
                    break

                # If the status is still terminal but no agent message has streamed
                # yet, we're seeing a stale carry-over from the previous turn —
                # show "WAITING" instead of "COMPLETED" so it doesn't look frozen.
                if is_terminal:
                    display_status = "WAITING"
                else:
                    display_status = run.status.value

                status.update(_spinner_label(display_status, start))
                time.sleep(_POLL_INTERVAL)
    except KeyboardInterrupt:
        console.console.print(
            f"[muted]↩ aborted local view — run still {display_status} server-side. "
            f"Rejoin with: paradime dinoai --session {session_id}[/]"
        )


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
