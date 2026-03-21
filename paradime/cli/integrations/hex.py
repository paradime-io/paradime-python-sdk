from __future__ import annotations

import sys
from typing import List

import click

from paradime.cli import console
from paradime.cli.utils import COMMA_LIST, env_click_option
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
    "--project-ids",
    type=COMMA_LIST,
    help="Comma-separated project ID(s) to trigger",
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
    "--wait/--no-wait",
    default=True,
    help="Wait for runs to complete before returning (default: True)",
)
@click.option(
    "--timeout",
    type=int,
    help="Maximum time to wait in minutes.",
    default=1440,
)
@click.option("--json", "json_output", is_flag=True, help="Output results as JSON.", default=False)
def hex_trigger(
    api_token: str,
    base_url: str,
    project_ids: List[str],
    input_param: tuple,
    update_published: bool,
    wait: bool,
    timeout: int,
    json_output: bool,
) -> None:
    """
    Trigger runs for Hex projects.
    """
    if not json_output:
        console.header("Hex — Trigger Project Runs")

    # Parse input parameters
    input_params = {}
    if input_param:
        for param in input_param:
            if "=" not in param:
                console.error(
                    f"Invalid input parameter format: {param}. Expected key=value", exit_code=1
                )
            key, value = param.split("=", 1)
            input_params[key] = value

    try:
        results = trigger_hex_runs(
            api_token=api_token,
            base_url=base_url,
            project_ids=project_ids,
            input_params=input_params if input_params else None,
            update_published=update_published,
            wait_for_completion=wait,
            timeout_minutes=timeout,
        )

        if json_output:
            failed = [
                r
                for r in results
                if "ERRORED" in r or "KILLED" in r or "UNABLE_TO_ALLOCATE_KERNEL" in r
            ]
            console.json_out(
                {"results": results, "failed_count": len(failed), "success": len(failed) == 0}
            )
            if failed:
                sys.exit(1)
            return

        # Check if any runs failed or errored
        failed_runs = [
            result
            for result in results
            if "ERRORED" in result or "KILLED" in result or "UNABLE_TO_ALLOCATE_KERNEL" in result
        ]
        if failed_runs:
            console.error(f"{len(failed_runs)} project run(s) failed.")
            sys.exit(1)

    except Exception as e:
        if json_output:
            console.json_out({"error": str(e), "success": False})
            sys.exit(1)
        console.error(f"Hex project run failed: {e}", exit_code=1)


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
@click.option("--json", "json_output", is_flag=True, help="Output results as JSON.", default=False)
def hex_list_projects(
    api_token: str,
    base_url: str,
    limit: int,
    include_archived: bool,
    include_trashed: bool,
    json_output: bool,
) -> None:
    """
    List all available Hex projects in the workspace.
    """
    if not json_output:
        console.info("Listing Hex projects…")

    result = list_hex_projects(
        api_token=api_token,
        base_url=base_url,
        limit=limit,
        include_archived=include_archived,
        include_trashed=include_trashed,
        json_output=json_output,
    )
    if json_output and result is not None:
        console.json_out(result)
