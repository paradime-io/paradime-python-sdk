import sys
from typing import List, Optional

import click

from paradime.cli.utils import env_click_option
from paradime.core.scripts.fivetran import list_fivetran_connectors, trigger_fivetran_sync


@click.command(context_settings=dict(max_content_width=160))
@env_click_option(
    "api-key",
    "FIVETRAN_API_KEY",
    help="Your Fivetran API key. You can find this in your Fivetran account settings.",
)
@env_click_option(
    "api-secret",
    "FIVETRAN_API_SECRET",
    help="Your Fivetran API secret. You can find this in your Fivetran account settings.",
)
@click.option(
    "--connector-id",
    multiple=True,
    help="The ID(s) of the connector(s) you want to sync",
    required=True,
)
@click.option(
    "--force",
    is_flag=True,
    help="Force restart any ongoing syncs",
    default=False,
)
@click.option(
    "--wait-for-completion",
    is_flag=True,
    help="Wait for syncs to complete before returning",
    default=True,
)
@click.option(
    "--timeout-minutes",
    type=int,
    help="Maximum time to wait for sync completion (in minutes). Only used with --wait-for-completion.",
    default=1440,
)
def fivetran_sync(
    api_key: str,
    api_secret: str,
    connector_id: List[str],
    force: bool,
    wait_for_completion: bool,
    timeout_minutes: int,
) -> None:
    """
    Trigger sync for Fivetran connectors.
    """
    click.echo(f"Starting sync for {len(connector_id)} Fivetran connector(s)...")

    try:
        results = trigger_fivetran_sync(
            api_key=api_key,
            api_secret=api_secret,
            connector_ids=list(connector_id),
            force=force,
            wait_for_completion=wait_for_completion,
            timeout_minutes=timeout_minutes,
        )

        # Check if any syncs failed, were paused, or rescheduled
        failed_syncs = [
            result
            for result in results
            if "FAILED" in result or "PAUSED" in result or "RESCHEDULED" in result
        ]
        if failed_syncs:
            sys.exit(1)

    except Exception as e:
        click.echo(f"❌ Fivetran sync failed: {str(e)}")
        raise click.Abort()


@click.command(context_settings=dict(max_content_width=160))
@env_click_option(
    "api-key",
    "FIVETRAN_API_KEY",
    help="Your Fivetran API key. You can find this in your Fivetran account settings.",
)
@env_click_option(
    "api-secret",
    "FIVETRAN_API_SECRET",
    help="Your Fivetran API secret. You can find this in your Fivetran account settings.",
)
@click.option(
    "--group-id",
    help="Optional group ID to filter connectors by group",
    required=False,
)
def fivetran_list_connectors(
    api_key: str,
    api_secret: str,
    group_id: Optional[str],
) -> None:
    """
    List all available Fivetran connectors with their status.
    """
    if group_id:
        click.echo(f"Listing Fivetran connectors for group {group_id}...")
    else:
        click.echo("Listing all Fivetran connectors...")

    list_fivetran_connectors(
        api_key=api_key,
        api_secret=api_secret,
        group_id=group_id,
    )
