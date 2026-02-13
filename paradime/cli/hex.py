import os
import sys
import time
from typing import Dict, Final, Optional

import click
import requests

WAIT_SLEEP: Final = 10


def get_hex_token() -> str:
    """Get Hex API token from environment."""
    token = os.getenv("HEX_API_TOKEN")
    if not token:
        click.echo("Error: HEX_API_TOKEN environment variable is not set")
        sys.exit(1)
    return token


def get_hex_base_url() -> str:
    """Get Hex base URL from environment."""
    return os.getenv("HEX_BASE_URL", "https://app.hex.tech")


def get_hex_headers() -> Dict[str, str]:
    """Get headers for Hex API requests."""
    return {
        "Authorization": f"Bearer {get_hex_token()}",
        "Content-Type": "application/json",
    }


@click.command()
@click.argument("project_id")
@click.option(
    "--input-param",
    multiple=True,
    help="Input parameters in key=value format (can be used multiple times)",
)
@click.option(
    "--update-published/--no-update-published",
    default=True,
    help="Update cached app state with run results",
)
@click.option(
    "--wait",
    is_flag=True,
    help="Wait for the run to complete",
)
@click.option(
    "--timeout-minutes",
    type=int,
    default=60,
    help="Maximum wait time in minutes (default: 60)",
)
@click.option("--json", "json_output", is_flag=True, help="JSON formatted response")
def trigger(
    project_id: str,
    input_param: tuple,
    update_published: bool,
    wait: bool,
    timeout_minutes: int,
    json_output: bool,
) -> None:
    """
    Trigger a Hex project run.

    PROJECT_ID is the UUID of the Hex project to trigger.

    Example:
        paradime hex trigger abc-123-def --input-param date=2024-01-01 --wait
    """
    base_url = get_hex_base_url()
    api_url = f"{base_url}/api/v1/projects/{project_id}/runs"

    # Parse input parameters
    input_params = {}
    if input_param:
        for param in input_param:
            if "=" not in param:
                click.echo(
                    f"Invalid input parameter format: {param}. Expected key=value"
                    if not json_output
                    else {"error": f"Invalid input parameter format: {param}"}
                )
                sys.exit(1)
            key, value = param.split("=", 1)
            input_params[key] = value

    # Prepare request payload
    payload = {
        "updatePublishedResults": update_published,
        "useCachedSqlResults": True,
    }
    if input_params:
        payload["inputParams"] = input_params

    # Trigger the run
    try:
        response = requests.post(
            api_url,
            headers=get_hex_headers(),
            json=payload,
            timeout=30,
        )
        response.raise_for_status()
        result = response.json()
    except requests.RequestException as e:
        click.echo(
            f"Failed to trigger Hex project: {e}"
            if not json_output
            else {"error": f"Failed to trigger Hex project: {e}"}
        )
        sys.exit(1)

    run_id = result.get("runId")
    run_url = result.get("runUrl")
    run_status = result.get("status")

    click.echo(
        {"run_id": run_id, "status": run_status, "url": run_url}
        if json_output
        else f"{run_id}\nStatus: {run_status}\nURL: {run_url}"
    )

    if wait:
        final_status = _wait_for_completion(
            project_id=project_id,
            run_id=run_id,
            timeout_minutes=timeout_minutes,
            json_output=json_output,
        )

        if final_status not in ["COMPLETED"]:
            sys.exit(1)


def _wait_for_completion(
    project_id: str,
    run_id: str,
    timeout_minutes: int,
    json_output: bool,
) -> Optional[str]:
    """Wait for a Hex project run to complete."""
    base_url = get_hex_base_url()
    api_url = f"{base_url}/api/v1/projects/{project_id}/runs/{run_id}"

    start_time = time.time()
    timeout_seconds = timeout_minutes * 60
    counter = 0

    while True:
        elapsed_time = time.time() - start_time

        # Check timeout
        if elapsed_time >= timeout_seconds:
            click.echo(
                f"Timeout: Run did not complete within {timeout_minutes} minutes"
                if not json_output
                else {"error": f"Timeout after {timeout_minutes} minutes"}
            )
            return None

        try:
            response = requests.get(
                api_url,
                headers=get_hex_headers(),
                timeout=30,
            )
            response.raise_for_status()
            run_info = response.json()
            run_status = run_info.get("status")

            # Log progress every 60 seconds (every 6 polls)
            if counter % 6 == 0 and not json_output:
                elapsed_minutes = int(elapsed_time / 60)
                click.echo(f"  {elapsed_minutes}m elapsed - Status: {run_status}")

            # Check if run is complete
            if run_status in ["COMPLETED", "ERRORED", "KILLED", "UNABLE_TO_ALLOCATE_KERNEL"]:
                click.echo(
                    {"status": run_status}
                    if json_output
                    else f"Run completed with status: {run_status}"
                )
                return run_status

            counter += 1
            time.sleep(WAIT_SLEEP)

        except requests.RequestException as e:
            if not json_output:
                click.echo(f"Network error checking run status: {e}. Retrying...")
            time.sleep(WAIT_SLEEP)
            counter += 1


@click.command()
@click.option(
    "--limit",
    type=int,
    default=100,
    help="Number of projects to fetch (default: 100)",
)
@click.option(
    "--include-archived",
    is_flag=True,
    help="Include archived projects",
)
@click.option(
    "--include-trashed",
    is_flag=True,
    help="Include trashed projects",
)
@click.option("--json", "json_output", is_flag=True, help="JSON formatted response")
def list_projects(
    limit: int,
    include_archived: bool,
    include_trashed: bool,
    json_output: bool,
) -> None:
    """
    List all Hex projects in the workspace.

    Example:
        paradime hex list-projects
        paradime hex list-projects --include-archived
    """
    base_url = get_hex_base_url()
    api_url = f"{base_url}/api/v1/projects"

    params = {
        "limit": limit,
        "includeArchived": str(include_archived).lower(),
        "includeTrashed": str(include_trashed).lower(),
    }

    try:
        response = requests.get(
            api_url,
            headers=get_hex_headers(),
            params=params,
            timeout=30,
        )
        response.raise_for_status()
        projects = response.json()
    except requests.RequestException as e:
        click.echo(
            f"Failed to fetch Hex projects: {e}"
            if not json_output
            else {"error": f"Failed to fetch Hex projects: {e}"}
        )
        sys.exit(1)

    if json_output:
        click.echo(projects)
    else:
        click.echo(f"\nFound {len(projects)} Hex project(s):\n")
        for project in projects:
            project_id = project.get("projectId", "N/A")
            name = project.get("name", "Unnamed")
            owner = project.get("ownerEmail", "N/A")
            click.echo(f"  • {name}")
            click.echo(f"    ID: {project_id}")
            click.echo(f"    Owner: {owner}")
            click.echo()


@click.group()
def hex() -> None:
    """
    Work with Hex projects from the CLI.

    Requires HEX_API_TOKEN environment variable to be set.
    """
    pass


# Add commands to hex group
hex.add_command(trigger)
hex.add_command(list_projects)
