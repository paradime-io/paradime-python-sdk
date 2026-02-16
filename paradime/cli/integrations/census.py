import sys
from typing import List

import click

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
    "--wait-for-completion/--no-wait-for-completion",
    default=True,
    help="Wait for syncs to complete before returning (default: True)",
)
@click.option(
    "--timeout-minutes",
    type=int,
    help="Maximum time to wait for sync completion (in minutes). Only used with --wait-for-completion.",
    default=1440,
)
def census_sync(
    api_token: str,
    sync_ids: List[str],
    force_full_sync: bool,
    wait_for_completion: bool,
    timeout_minutes: int,
) -> None:
    """
    Trigger syncs for Census.
    """
    click.echo(f"Starting syncs for {len(sync_ids)} Census sync(s)...")

    try:
        results = trigger_census_syncs(
            api_token=api_token,
            sync_ids=list(sync_ids),
            force_full_sync=force_full_sync,
            wait_for_completion=wait_for_completion,
            timeout_minutes=timeout_minutes,
        )

        # Check if any syncs failed or were cancelled
        failed_syncs = [result for result in results if "FAILED" in result or "CANCELLED" in result]
        if failed_syncs:
            sys.exit(1)

    except Exception as e:
        click.echo(f"❌ Census sync failed: {str(e)}")
        raise click.Abort()


@click.command(context_settings=dict(max_content_width=160))
@env_click_option(
    "api-token",
    "CENSUS_API_TOKEN",
    help="Your Census API token. You can find this in your Census account settings.",
)
def census_list_syncs(
    api_token: str,
) -> None:
    """
    List all available Census syncs with their status.
    """
    click.echo("Listing all Census syncs...")

    list_census_syncs(
        api_token=api_token,
    )
