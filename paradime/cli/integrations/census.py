from __future__ import annotations

import sys
from typing import List

import click

from paradime.cli import console
from paradime.cli.utils import env_click_option
from paradime.core.scripts.census import list_census_syncs, trigger_census_syncs


@click.command(context_settings=dict(max_content_width=160))
@env_click_option(
    "api-token",
    "CENSUS_API_TOKEN",
    help="Your Census API token. You can find this in your Census account settings.",
)
@click.option(
    "--sync-ids",
    multiple=True,
    help="The ID(s) of the sync(s) you want to trigger",
    required=True,
)
@click.option(
    "--force-full-sync",
    is_flag=True,
    help="Force a full sync instead of incremental",
    default=False,
)
@click.option(
    "--wait/--no-wait",
    default=True,
    help="Wait for syncs to complete before returning (default: True)",
)
@click.option(
    "--timeout",
    type=int,
    help="Maximum time to wait in minutes.",
    default=1440,
)
@click.option("--json", "json_output", is_flag=True, help="Output results as JSON.", default=False)
def census_sync(
    api_token: str,
    sync_ids: List[str],
    force_full_sync: bool,
    wait: bool,
    timeout: int,
    json_output: bool,
) -> None:
    """
    Trigger syncs for Census.
    """
    if not json_output:
        console.header("Census — Trigger Syncs")

    try:
        results = trigger_census_syncs(
            api_token=api_token,
            sync_ids=list(sync_ids),
            force_full_sync=force_full_sync,
            wait_for_completion=wait,
            timeout_minutes=timeout,
        )

        if json_output:
            failed = [r for r in results if "FAILED" in r or "CANCELLED" in r]
            console.json_out(
                {"results": results, "failed_count": len(failed), "success": len(failed) == 0}
            )
            if failed:
                sys.exit(1)
            return

        # Check if any syncs failed or were cancelled
        failed_syncs = [result for result in results if "FAILED" in result or "CANCELLED" in result]
        if failed_syncs:
            console.error(f"{len(failed_syncs)} sync(s) failed.")
            sys.exit(1)

    except Exception as e:
        if json_output:
            console.json_out({"error": str(e), "success": False})
            sys.exit(1)
        console.error(f"Census sync failed: {e}", exit_code=1)


@click.command(context_settings=dict(max_content_width=160))
@env_click_option(
    "api-token",
    "CENSUS_API_TOKEN",
    help="Your Census API token. You can find this in your Census account settings.",
)
@click.option("--json", "json_output", is_flag=True, help="Output results as JSON.", default=False)
def census_list_syncs(
    api_token: str,
    json_output: bool,
) -> None:
    """
    List all available Census syncs with their status.
    """
    if not json_output:
        console.info("Listing all Census syncs…")

    result = list_census_syncs(
        api_token=api_token,
        json_output=json_output,
    )
    if json_output and result is not None:
        console.json_out(result)
