from __future__ import annotations

import sys
import time
from pathlib import Path
from typing import TYPE_CHECKING, Final, List, Optional

if TYPE_CHECKING:
    from paradime.client.paradime_client import Paradime

import click
import requests

from paradime.apis.bolt.types import BoltRunState
from paradime.cli.rich_text_output import (
    print_artifact_downloaded,
    print_artifact_downloading,
    print_error_table,
    print_run_started,
    print_run_status,
    print_success,
)
from paradime.cli.version import print_version
from paradime.client.api_exception import ParadimeAPIException, ParadimeException
from paradime.client.paradime_cli_client import get_cli_client, get_cli_client_or_exit
from paradime.core.bolt.schedule import (
    SCHEDULE_FILE_NAME,
    _get_schedules,
    get_slug_format_warnings,
    is_valid_schedule_at_path,
)
from paradime.core.bolt.yaml_rewriter import mint_slugs_in_yaml_files

WAIT_SLEEP: Final = 10
WAIT_SLEEP_STREAMING: Final = 2


def _wait_with_logs(client: "Paradime", run_id: int, is_json: bool) -> None:
    """Wait for a run to finish while streaming live logs for each command."""
    from rich.panel import Panel

    from paradime.cli.console import console as rich_console

    seen_command_ids: set = set()
    while True:
        status = client.bolt.get_run_status(run_id)
        if not status:
            print_error_table("Unable to fetch status from bolt.", is_json=is_json)
            sys.exit(1)

        # Stream logs for any new commands that have appeared
        try:
            commands = client.bolt.list_run_commands(run_id)
        except ParadimeAPIException:
            commands = []

        for cmd in commands:
            if cmd.id in seen_command_ids:
                continue
            seen_command_ids.add(cmd.id)
            if not is_json:
                rich_console.print()
                rich_console.print(
                    Panel(
                        f"[bold]{cmd.command}[/]",
                        title=f"[muted]Command {cmd.id}[/]",
                        border_style="brand",
                        padding=(0, 1),
                    )
                )
            for log_line in client.bolt.stream_command_logs(cmd.id):
                if not is_json:
                    click.echo(f"{log_line.line}", nl=False)

        if status is not BoltRunState.RUNNING:
            print_run_status(status.value, is_json)
            break

        if not is_json:
            with rich_console.status(
                f"[muted]Current run status: {status.value}[/]", spinner="dots"
            ):
                time.sleep(WAIT_SLEEP_STREAMING)
        else:
            print_run_status(status.value, is_json)
            time.sleep(WAIT_SLEEP_STREAMING)

    if status is not BoltRunState.SUCCESS:
        sys.exit(1)


@click.command()
@click.argument("slug")
def unsuspend(slug: str) -> None:
    """
    Enable a suspended Paradime Bolt schedule.

    SLUG is the schedule's slug (the identifier returned by createBoltSchedule
    and shown in the Bolt UI).
    """
    client = get_cli_client_or_exit()
    client.bolt.suspend_schedule(
        slug=slug,
        suspend=False,
    )

    print_success("Successfully enabled schedule.", is_json=False)


@click.command()
@click.argument("slug")
def suspend(slug: str) -> None:
    """
    Suspend a Paradime Bolt schedule.

    SLUG is the schedule's slug (the identifier returned by createBoltSchedule
    and shown in the Bolt UI).
    """
    client = get_cli_client_or_exit()
    client.bolt.suspend_schedule(
        slug=slug,
        suspend=True,
    )

    print_success("Successfully suspended schedule.", is_json=False)


@click.group()
def schedule() -> None:
    """
    Work with Paradime Bolt from the CLI.
    """
    pass


@click.command(name="retry")
@click.option("--wait", help="Wait for the retry run to finish", is_flag=True)
@click.option("--json", help="JSON formatted response", is_flag=True)
@click.argument("slug")
def schedule_retry(wait: bool, json: bool, slug: str) -> None:
    """
    Retry the latest failed run of a Paradime Bolt schedule.

    SLUG is the schedule's slug.
    """
    if not json:
        print_version()

    client = get_cli_client_or_exit()
    try:
        new_run_id = client.bolt.retry_schedule_from_failure(slug=slug)
    except ParadimeAPIException as e:
        print_error_table(f"Failed to retry schedule: {e}", is_json=json)
        sys.exit(1)

    print_run_started(new_run_id, json)

    if wait:
        _wait_with_logs(client, new_run_id, is_json=json)


schedule.add_command(unsuspend)
schedule.add_command(suspend)
schedule.add_command(schedule_retry)


@click.command()
@click.option("--branch", default=None, help="Git branch name or commit hash to checkout.")
@click.option(
    "--command",
    multiple=True,
    default=[],
    help="Command(s) to override the default commands.",
)
@click.option(
    "--pr-number", default=None, type=int, help="Pull request number to associate with the run."
)
@click.option("--wait", help="Wait for the run to finish", is_flag=True)
@click.option("--json", help="JSON formatted response", is_flag=True)
@click.argument("slug")
def run(
    branch: str,
    command: List[str],
    pr_number: Optional[int],
    wait: bool,
    json: bool,
    slug: str,
) -> None:
    """
    Trigger a Paradime Bolt run.

    SLUG is the schedule's slug.
    """
    if not json:
        print_version()

    # trigger run
    client = get_cli_client_or_exit()
    try:
        run_id = client.bolt.trigger_run(
            slug=slug,
            branch=branch,
            commands=list(command) if command else None,
            pr_number=pr_number,
        )
    except ParadimeAPIException as e:
        print_error_table(f"Failed to trigger run: {e}", is_json=json)
        sys.exit(1)

    print_run_started(run_id, json)

    if wait:
        _wait_with_logs(client, run_id, is_json=json)


@click.command()
@click.option(
    "--all",
    "retry_all",
    is_flag=True,
    help="Retry ALL commands from the original run (default: failed commands only).",
)
@click.option("--wait", help="Wait for the retry run to finish", is_flag=True)
@click.option("--json", help="JSON formatted response", is_flag=True)
@click.argument("run_id", type=int)
def retry(retry_all: bool, wait: bool, json: bool, run_id: int) -> None:
    """
    Retry a failed Paradime Bolt run.
    """
    if not json:
        print_version()

    client = get_cli_client_or_exit()
    try:
        new_run_id = (
            client.bolt.retry_run_all(run_id) if retry_all else client.bolt.retry_run(run_id)
        )
    except ParadimeAPIException as e:
        print_error_table(f"Failed to retry run: {e}", is_json=json)
        sys.exit(1)

    print_run_started(new_run_id, json)

    if wait:
        _wait_with_logs(client, new_run_id, is_json=json)


@click.command()
@click.option(
    "--path",
    help="Path to paradime_schedules.yml file or project root containing .bolt/ directory.",
    show_default=True,
    default=".",
)
def verify(path: str) -> None:
    """
    Verify schedule YAML files and mint slugs for new schedules.

    Validates the schedule configuration, then checks for schedules whose
    ``name`` is not a valid slug. For those, calls the Paradime backend to
    mint slugs and rewrites the YAML in place — moving the current ``name``
    to ``display_name`` and inserting the minted slug as ``name``.

    Also resolves cross-references in ``deferred_schedule``, ``turbo_ci``,
    and ``schedule_trigger`` sections to use the minted slugs.

    Supports both the flat ``paradime_schedules.yml`` layout and the modular
    ``.bolt/`` folder layout.
    """
    print_version()
    schedule_path = Path(path)

    # Fetch existing schedule names from the backend early so we can use
    # them for both validation and slug minting.
    #
    # ``existing_names`` are the current workspace's deployed schedule names
    # (used for grandfathering, unregistered detection and minting).
    # ``all_schedules_ref`` are (workspace_name, schedule_name) pairs across all
    # workspaces (used only to validate cross-workspace schedule_trigger refs).
    existing_names: set[str] = set()
    all_schedules_ref: set[tuple[str, str]] = set()
    try:
        client = get_cli_client_or_exit()
        try:
            all_schedules = client.bolt.list_schedules(offset=0, limit=10000)
            existing_names = {s.name for s in all_schedules.schedules}
        except Exception:
            pass
        try:
            all_schedules_ref = set(client.bolt.list_all_schedule_names())
        except Exception:
            pass
    except (ParadimeAPIException, ParadimeException):
        client = None

    error_string = is_valid_schedule_at_path(
        schedule_path,
        existing_names=existing_names,
        schedule_trigger_refs=all_schedules_ref or None,
    )
    if error_string:
        print_error_table(error_string, is_json=False)
        sys.exit(1)

    # Check for names not yet registered in the backend and mint slugs.
    try:
        schedules = _get_schedules(schedule_path)
    except Exception:
        schedules = None

    if not schedules:
        click.secho("No schedules found.", fg="yellow")
        return

    try:
        if not client:
            client = get_cli_client_or_exit()
        root = schedule_path.parent if schedule_path.is_file() else schedule_path

        unregistered = [s.name for s in schedules.schedules if s.name not in existing_names]

        if unregistered:
            changed = mint_slugs_in_yaml_files(
                mint_fn=client.bolt.create_schedule_slugs,
                root=root,
                existing_names=existing_names,
            )
            if changed:
                click.secho(f"Minted slugs in {changed} file(s).", fg="green")
            else:
                click.secho("All schedules verified.", fg="green")
        else:
            click.secho("All schedules verified.", fg="green")
    except (ParadimeAPIException, ParadimeException) as e:
        click.secho(
            f"Could not mint slugs (API unavailable): {e}\n"
            f"Non-slug schedule names will be grandfathered by the backend on deploy.",
            fg="yellow",
        )
    except Exception:
        # Fall back to warnings if minting fails for any reason
        if schedules:
            for warning in get_slug_format_warnings(schedules):
                click.secho(f"warning: {warning}", fg="yellow")


@click.command()
@click.option(
    "--slug",
    "--schedule-name",
    "slug",
    help="The schedule's slug (the identifier returned by createBoltSchedule and shown in the Bolt UI). `--schedule-name` is accepted as a deprecated alias.",
    required=True,
)
@click.option(
    "--artifact-path",
    help="The path to the artifact in the Bolt run.",
    default="target/manifest.json",
    show_default=True,
)
@click.option(
    "--command-index",
    help="The index of the command in the schedule. Defaults to searching through all commands from the last command to the first.",
    default=None,
    type=int,
)
@click.option(
    "--output-path",
    help="The path to save the artifact. Defaults to the current directory.",
    default=None,
)
def artifact(
    slug: str,
    artifact_path: str,
    command_index: Optional[int] = None,
    output_path: Optional[str] = None,
) -> None:
    """
    Download the latest artifact from a Paradime Bolt schedule.
    """
    print_version()
    client = get_cli_client_or_exit()
    try:
        print_artifact_downloading(schedule_name=slug, artifact_path=artifact_path)

        artifact_url = client.bolt.get_latest_artifact_url(
            slug=slug,
            artifact_path=artifact_path,
            command_index=command_index,
        )

        file_name = Path(artifact_path).name
        if output_path is None:  # save to current directory
            output_file_path = Path.cwd() / file_name
        elif Path(output_path).is_dir():
            output_file_path = Path(output_path) / file_name
        else:
            output_file_path = Path(output_path)

        output_file_path.write_text(requests.get(artifact_url).text)

        print_artifact_downloaded(output_file_path)
    except ParadimeException as e:
        print_error_table(f"Failed to get artifact: {e}", is_json=False)
        sys.exit(1)


@click.group()
def bolt() -> None:
    """
    Work with Paradime Bolt from the CLI.
    """
    pass


# bolt
bolt.add_command(run)
bolt.add_command(retry)
bolt.add_command(schedule)
bolt.add_command(verify)
bolt.add_command(artifact)
