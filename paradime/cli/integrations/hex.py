import sys
from typing import List

import click

from paradime.cli.utils import env_click_option
from paradime.core.scripts.hex import list_hex_projects, trigger_hex_runs


@click.command(context_settings=dict(max_content_width=160))
@env_click_option(
    "api-token",
    "HEX_API_TOKEN",
    help="Your Hex API token. You can find this in your Hex workspace settings.",
)
@env_click_option(
    "base-url",
    "HEX_BASE_URL",
    help="Hex base URL. Default: https://app.hex.tech",
    default="https://app.hex.tech",
)
@click.option(
    "--project-id",
    multiple=True,
    help="The ID(s) of the project(s) you want to trigger",
    required=True,
)
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
    "--wait-for-completion/--no-wait-for-completion",
    default=True,
    help="Wait for runs to complete before returning (default: True)",
)
@click.option(
    "--timeout-minutes",
    type=int,
    help="Maximum time to wait for run completion (in minutes). Only used with --wait-for-completion.",
    default=1440,
)
def hex_trigger(
    api_token: str,
    base_url: str,
    project_id: List[str],
    input_param: tuple,
    update_published: bool,
    wait_for_completion: bool,
    timeout_minutes: int,
) -> None:
    """
    Trigger runs for Hex projects.
    """
    click.echo(f"Starting runs for {len(project_id)} Hex project(s)...")

    # Parse input parameters
    input_params = {}
    if input_param:
        for param in input_param:
            if "=" not in param:
                click.echo(f"❌ Invalid input parameter format: {param}. Expected key=value")
                sys.exit(1)
            key, value = param.split("=", 1)
            input_params[key] = value

    try:
        results = trigger_hex_runs(
            api_token=api_token,
            base_url=base_url,
            project_ids=list(project_id),
            input_params=input_params if input_params else None,
            update_published=update_published,
            wait_for_completion=wait_for_completion,
            timeout_minutes=timeout_minutes,
        )

        # Check if any runs failed or errored
        failed_runs = [
            result
            for result in results
            if "ERRORED" in result or "KILLED" in result or "UNABLE_TO_ALLOCATE_KERNEL" in result
        ]
        if failed_runs:
            sys.exit(1)

    except Exception as e:
        click.echo(f"❌ Hex project run failed: {str(e)}")
        raise click.Abort()


@click.command(context_settings=dict(max_content_width=160))
@env_click_option(
    "api-token",
    "HEX_API_TOKEN",
    help="Your Hex API token. You can find this in your Hex workspace settings.",
)
@env_click_option(
    "base-url",
    "HEX_BASE_URL",
    help="Hex base URL. Default: https://app.hex.tech",
    default="https://app.hex.tech",
)
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
def hex_list_projects(
    api_token: str,
    base_url: str,
    limit: int,
    include_archived: bool,
    include_trashed: bool,
) -> None:
    """
    List all available Hex projects in the workspace.
    """
    click.echo("Listing Hex projects...")

    list_hex_projects(
        api_token=api_token,
        base_url=base_url,
        limit=limit,
        include_archived=include_archived,
        include_trashed=include_trashed,
    )
