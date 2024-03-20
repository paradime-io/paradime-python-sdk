import sys
import time
from pathlib import Path
from typing import Final, List

import click
from tabulate import tabulate

from paradime.apis.bolt.types import BoltRunState
from paradime.client.api_exception import ParadimeAPIException
from paradime.client.paradime_cli_client import get_cli_client_or_exit
from paradime.core.bolt.schedule import (
    SCHEDULE_FILE_NAME,
    is_allowed_command,
    is_valid_schedule_at_path,
    parse_command,
)

WAIT_SLEEP: Final = 10


@click.command()
@click.option("--branch", default=None, help="Git branch name or commit hash to checkout.")
@click.option(
    "--command",
    multiple=True,
    default=[],
    help="Command(s) to override the default commands.",
)
@click.option("--wait", help="Wait for the run to finish", is_flag=True)
@click.option("--json", help="JSON formatted response", is_flag=True)
@click.argument("schedule_name")
def run(
    branch: str,
    command: List[str],
    wait: bool,
    json: bool,
    schedule_name: str,
) -> None:
    """
    Trigger a Paradime Bolt run.
    """
    # verify
    if command:
        for _command in command:
            if not is_allowed_command(parse_command(_command)):
                click.echo(
                    f"Command {_command!r} is not allowed."
                    if not json
                    else {"error": f"Command {_command!r} is not allowed."}
                )
                sys.exit(1)

    # trigger run
    client = get_cli_client_or_exit()
    try:
        run_id = client.bolt.trigger_run(
            schedule_name,
            branch=branch,
            commands=list(command) if command else None,
        )
    except ParadimeAPIException as e:
        click.echo(
            f"Failed to trigger run: {e}" if not json else {"error": f"Failed to trigger run: {e}"}
        )
        sys.exit(1)

    click.echo(
        {
            "run_id": run_id,
            "url": f"https://app.paradime.io/bolt/run_id/{run_id}",
        }
        if json
        else run_id
    )

    if wait:
        while True:
            status = client.bolt.get_run_status(run_id)
            if not status:
                click.echo(
                    "Unable to fetch status from bolt."
                    if not json
                    else {"error": "Unable to fetch status from bolt."}
                )
                sys.exit(1)

            click.echo({"status": status.value} if json else status)
            if status is not BoltRunState.RUNNING:
                break
            time.sleep(WAIT_SLEEP)

        if status is not BoltRunState.SUCCESS:
            sys.exit(1)


@click.command()
@click.option(
    "--path",
    help="Path to paradime_schedules.yml file.",
    show_default=True,
    default=SCHEDULE_FILE_NAME,
)
def verify(path: str) -> None:
    """
    Verify the paradime_schedules.yml file.
    """
    error_string = is_valid_schedule_at_path(Path(path))
    if error_string:
        click.echo(error_string)
        sys.exit(1)


@click.group()
def list() -> None:
    """
    List bolt resources.
    """
    pass


@click.command()
@click.option("--offset", help="Offset", default=0)
@click.option("--limit", help="Limit the number of schedules to be returned", default=10)
@click.option("--inactive", help="Show inactive schedules", is_flag=True)
@click.option("--json", help="JSON formatted response", is_flag=True)
def schedules(offset: int, limit: int, inactive: bool, json: bool) -> None:
    """
    List bolt schedule names.
    """
    client = get_cli_client_or_exit()
    try:
        schedules_response = client.bolt.list_schedules(
            offset=offset,
            limit=limit,
            show_inactive=inactive,
        )
    except ParadimeAPIException as e:
        click.echo(
            f"Failed to trigger run: {e}" if not json else {"error": f"Failed to trigger run: {e}"}
        )
        sys.exit(1)

    if json:
        click.echo(schedules_response.json())
    else:
        click.echo(
            tabulate(
                [
                    [
                        schedule.name,
                        schedule.schedule,
                        (
                            "\n".join([_crop(command) for command in schedule.commands])
                            if schedule.commands
                            else ""
                        ),
                        schedule.owner,
                        schedule.source == "ui",
                        schedule.uuid,
                    ]
                    for schedule in schedules_response.schedules
                ],
                headers=("Name", "Cron", "Commands", "owner", "UI", "UUID"),
                tablefmt="grid",
            ),
            color=True,
        )


def _crop(command: str, size: int = 20) -> str:
    return command[:size] + "..." if len(command) > size else command


@click.group()
def bolt() -> None:
    """
    Work with Paradime Bolt from the CLI.
    """
    pass


# bolt
list.add_command(schedules)
bolt.add_command(run)
bolt.add_command(list)
bolt.add_command(verify)
