import sys
import time
from pathlib import Path
from typing import Final, List

import click
from rich_text_output import print_error_table, print_run_started, print_run_status

from paradime.apis.bolt.types import BoltRunState
from paradime.client.api_exception import ParadimeAPIException
from paradime.client.paradime_cli_client import get_cli_client_or_exit
from paradime.core.bolt.schedule import (
    SCHEDULE_FILE_NAME,
    is_allowed_command,
    is_valid_schedule_at_path,
    parse_command,
)

from version import version

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
    version()
    # verify
    if command:
        for _command in command:
            if not is_allowed_command(parse_command(_command)):
                if json:
                    print_error_table({"error": f"Command {_command!r} is not allowed."}, True)
                else:
                    print_error_table(f"Command {_command!r} is not allowed.", False)
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
        if json:
            print_error_table({"error": f"Failed to trigger run: {e}"}, True)
        else:
            print_error_table(f"Failed to trigger run: {e}", False)
        sys.exit(1)

    print_run_started(run_id)

    if wait:
        while True:
            status = client.bolt.get_run_status(run_id)
            if not status:
                if json:
                    print_error_table({"error": "Unable to fetch status from bolt."}, True)
                else:
                    print_error_table( "Unable to fetch status from bolt.", False)
                sys.exit(1)

            print_run_status(status.value, run_id)
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
    version()
    error_string = is_valid_schedule_at_path(Path(path))
    if error_string:
        print_error_table( error_string, False)
        sys.exit(1)


@click.group()
def bolt() -> None:
    """
    Work with Paradime Bolt from the CLI.
    """
    pass


# bolt
bolt.add_command(run)
bolt.add_command(verify)
