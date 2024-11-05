import sys
import time
from pathlib import Path
from typing import Final, List, Optional

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
from paradime.client.paradime_cli_client import get_cli_client_or_exit
from paradime.core.bolt.schedule import SCHEDULE_FILE_NAME, is_valid_schedule_at_path

WAIT_SLEEP: Final = 10


@click.command()
@click.argument("schedule_name")
def unsuspend(schedule_name: str) -> None:
    """
    Enable a suspended Paradime Bolt schedule.
    """
    client = get_cli_client_or_exit()
    client.bolt.suspend_schedule(
        schedule_name=schedule_name,
        suspend=False,
    )

    print_success("Successfully enabled schedule.", is_json=False)


@click.command()
@click.argument("schedule_name")
def suspend(schedule_name: str) -> None:
    """
    Suspend a Paradime Bolt schedule.
    """
    client = get_cli_client_or_exit()
    client.bolt.suspend_schedule(
        schedule_name=schedule_name,
        suspend=True,
    )

    print_success("Successfully suspended schedule.", is_json=False)


@click.group()
def schedule() -> None:
    """
    Work with Paradime Bolt from the CLI.
    """
    pass


schedule.add_command(unsuspend)
schedule.add_command(suspend)


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
    if not json:
        print_version()

    # trigger run
    client = get_cli_client_or_exit()
    try:
        run_id = client.bolt.trigger_run(
            schedule_name,
            branch=branch,
            commands=list(command) if command else None,
        )
    except ParadimeAPIException as e:
        print_error_table(f"Failed to trigger run: {e}", is_json=json)
        sys.exit(1)

    print_run_started(run_id, json)

    if wait:
        while True:
            status = client.bolt.get_run_status(run_id)
            if not status:
                print_error_table("Unable to fetch status from bolt.", is_json=json)
                sys.exit(1)

            print_run_status(status.value, json)
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
    print_version()
    error_string = is_valid_schedule_at_path(Path(path))
    if error_string:
        print_error_table(error_string, is_json=False)
        sys.exit(1)


@click.command()
@click.option(
    "--schedule-name",
    help="The name of the Bolt schedule.",
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
    schedule_name: str,
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
        print_artifact_downloading(schedule_name=schedule_name, artifact_path=artifact_path)

        artifact_url = client.bolt.get_latest_artifact_url(
            schedule_name=schedule_name,
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
bolt.add_command(schedule)
bolt.add_command(verify)
bolt.add_command(artifact)
