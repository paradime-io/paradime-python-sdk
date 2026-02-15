import sys
from typing import List

import click

from paradime.core.scripts.hightouch import (
    list_hightouch_sync_sequences,
    list_hightouch_syncs,
    trigger_hightouch_sync_sequences,
    trigger_hightouch_syncs,
)


@click.command(context_settings=dict(max_content_width=160))
@click.option(
    "--sync-id",
    multiple=True,
    help="The ID(s) of the sync(s) you want to trigger",
    required=True,
)
@click.option(
    "--full-resync",
    is_flag=True,
    help="Resync all rows in the query, ignoring previously synced rows",
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
def hightouch_sync(
    sync_id: List[str],
    full_resync: bool,
    wait_for_completion: bool,
    timeout_minutes: int,
) -> None:
    """
    Trigger syncs for Hightouch.

    Requires the HIGHTOUCH_API_TOKEN environment variable to be set.
    """
    click.echo(f"Starting syncs for {len(sync_id)} Hightouch sync(s)...")

    try:
        results = trigger_hightouch_syncs(
            sync_ids=list(sync_id),
            full_resync=full_resync,
            wait_for_completion=wait_for_completion,
            timeout_minutes=timeout_minutes,
        )

        # Check if any syncs failed or were cancelled
        failed_syncs = [
            result
            for result in results
            if "FAILED" in result
            or "CANCELLED" in result
            or "INTERRUPTED" in result
            or "ABORTED" in result
        ]
        if failed_syncs:
            sys.exit(1)

    except Exception as e:
        click.echo(f"❌ Hightouch sync failed: {str(e)}")
        raise click.Abort()


@click.command(context_settings=dict(max_content_width=160))
@click.option(
    "--sync-sequence-id",
    multiple=True,
    help="The ID(s) of the sync sequence(s) you want to trigger",
    required=True,
)
@click.option(
    "--wait-for-completion/--no-wait-for-completion",
    default=True,
    help="Wait for sync sequences to complete before returning (default: True)",
)
@click.option(
    "--timeout-minutes",
    type=int,
    help="Maximum time to wait for sequence completion (in minutes). Only used with --wait-for-completion.",
    default=1440,
)
def hightouch_sync_sequence(
    sync_sequence_id: List[str],
    wait_for_completion: bool,
    timeout_minutes: int,
) -> None:
    """
    Trigger sync sequences for Hightouch.

    Requires the HIGHTOUCH_API_TOKEN environment variable to be set.
    """
    click.echo(f"Starting {len(sync_sequence_id)} Hightouch sync sequence(s)...")

    try:
        results = trigger_hightouch_sync_sequences(
            sync_sequence_ids=list(sync_sequence_id),
            wait_for_completion=wait_for_completion,
            timeout_minutes=timeout_minutes,
        )

        # Check if any sequences failed or were cancelled
        failed_sequences = [
            result
            for result in results
            if "FAILED" in result
            or "CANCELLED" in result
            or "INTERRUPTED" in result
            or "ABORTED" in result
        ]
        if failed_sequences:
            sys.exit(1)

    except Exception as e:
        click.echo(f"❌ Hightouch sync sequence failed: {str(e)}")
        raise click.Abort()


@click.command(context_settings=dict(max_content_width=160))
def hightouch_list_syncs() -> None:
    """
    List all available Hightouch syncs with their status.

    Requires the HIGHTOUCH_API_TOKEN environment variable to be set.
    """
    click.echo("Listing all Hightouch syncs...")

    list_hightouch_syncs()


@click.command(context_settings=dict(max_content_width=160))
def hightouch_list_sync_sequences() -> None:
    """
    List all available Hightouch sync sequences with their status.

    Requires the HIGHTOUCH_API_TOKEN environment variable to be set.
    """
    click.echo("Listing all Hightouch sync sequences...")

    list_hightouch_sync_sequences()
