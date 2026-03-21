from __future__ import annotations

import sys
from typing import List

import click

from paradime.cli import console
from paradime.cli.utils import COMMA_LIST, env_click_option
from paradime.core.scripts.hightouch import (
    list_hightouch_sync_sequences,
    list_hightouch_syncs,
    trigger_hightouch_sync_sequences,
    trigger_hightouch_syncs,
)


@click.command(context_settings=dict(max_content_width=160))
@env_click_option(
    "api-token",
    "HIGHTOUCH_API_TOKEN",
    help="Your Hightouch API token. You can create this in your Hightouch workspace settings.",
)
@click.option(
    "--sync-ids",
    type=COMMA_LIST,
    help="Comma-separated sync ID(s) to trigger",
    required=True,
)
@click.option(
    "--full-resync",
    is_flag=True,
    help="Resync all rows in the query, ignoring previously synced rows",
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
def hightouch_sync(
    api_token: str,
    sync_ids: List[str],
    full_resync: bool,
    wait: bool,
    timeout: int,
    json_output: bool,
) -> None:
    """
    Trigger syncs for Hightouch.
    """
    if not json_output:
        console.header("Hightouch — Trigger Syncs")

    try:
        results = trigger_hightouch_syncs(
            api_token=api_token,
            sync_ids=sync_ids,
            full_resync=full_resync,
            wait_for_completion=wait,
            timeout_minutes=timeout,
        )

        if json_output:
            failed = [
                r
                for r in results
                if "FAILED" in r or "CANCELLED" in r or "INTERRUPTED" in r or "ABORTED" in r
            ]
            console.json_out(
                {"results": results, "failed_count": len(failed), "success": len(failed) == 0}
            )
            if failed:
                sys.exit(1)
            return

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
            console.error(f"{len(failed_syncs)} sync(s) failed.")
            sys.exit(1)

    except Exception as e:
        if json_output:
            console.json_out({"error": str(e), "success": False})
            sys.exit(1)
        console.error(f"Hightouch sync failed: {e}", exit_code=1)


@click.command(context_settings=dict(max_content_width=160))
@env_click_option(
    "api-token",
    "HIGHTOUCH_API_TOKEN",
    help="Your Hightouch API token. You can create this in your Hightouch workspace settings.",
)
@click.option(
    "--sync-sequence-ids",
    type=COMMA_LIST,
    help="Comma-separated sync sequence ID(s) to trigger",
    required=True,
)
@click.option(
    "--wait/--no-wait",
    default=True,
    help="Wait for sync sequences to complete before returning (default: True)",
)
@click.option(
    "--timeout",
    type=int,
    help="Maximum time to wait in minutes.",
    default=1440,
)
@click.option("--json", "json_output", is_flag=True, help="Output results as JSON.", default=False)
def hightouch_sync_sequence(
    api_token: str,
    sync_sequence_ids: List[str],
    wait: bool,
    timeout: int,
    json_output: bool,
) -> None:
    """
    Trigger sync sequences for Hightouch.
    """
    if not json_output:
        console.header("Hightouch — Trigger Sync Sequences")

    try:
        results = trigger_hightouch_sync_sequences(
            api_token=api_token,
            sync_sequence_ids=sync_sequence_ids,
            wait_for_completion=wait,
            timeout_minutes=timeout,
        )

        if json_output:
            failed = [
                r
                for r in results
                if "FAILED" in r or "CANCELLED" in r or "INTERRUPTED" in r or "ABORTED" in r
            ]
            console.json_out(
                {"results": results, "failed_count": len(failed), "success": len(failed) == 0}
            )
            if failed:
                sys.exit(1)
            return

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
            console.error(f"{len(failed_sequences)} sync sequence(s) failed.")
            sys.exit(1)

    except Exception as e:
        if json_output:
            console.json_out({"error": str(e), "success": False})
            sys.exit(1)
        console.error(f"Hightouch sync sequence failed: {e}", exit_code=1)


@click.command(context_settings=dict(max_content_width=160))
@env_click_option(
    "api-token",
    "HIGHTOUCH_API_TOKEN",
    help="Your Hightouch API token. You can create this in your Hightouch workspace settings.",
)
@click.option("--json", "json_output", is_flag=True, help="Output results as JSON.", default=False)
def hightouch_list_syncs(api_token: str, json_output: bool) -> None:
    """
    List all available Hightouch syncs with their status.
    """
    if not json_output:
        console.info("Listing all Hightouch syncs…")

    result = list_hightouch_syncs(api_token=api_token, json_output=json_output)
    if json_output and result is not None:
        console.json_out(result)


@click.command(context_settings=dict(max_content_width=160))
@env_click_option(
    "api-token",
    "HIGHTOUCH_API_TOKEN",
    help="Your Hightouch API token. You can create this in your Hightouch workspace settings.",
)
@click.option("--json", "json_output", is_flag=True, help="Output results as JSON.", default=False)
def hightouch_list_sync_sequences(api_token: str, json_output: bool) -> None:
    """
    List all available Hightouch sync sequences with their status.
    """
    if not json_output:
        console.info("Listing all Hightouch sync sequences…")

    result = list_hightouch_sync_sequences(api_token=api_token, json_output=json_output)
    if json_output and result is not None:
        console.json_out(result)
