from __future__ import annotations

import sys
from typing import List, Optional

import click

from paradime.cli import console
from paradime.cli.utils import COMMA_LIST, deprecated_alias_option, env_click_option, resolve_deprecated_option
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
    "--connector-ids",
    type=COMMA_LIST,
    help="Comma-separated connector ID(s) to sync",
    required=False,
)
@deprecated_alias_option("connector-id", "connector-ids", type=COMMA_LIST, default=None)
@click.option(
    "--force",
    is_flag=True,
    help="Force restart any ongoing syncs",
    default=False,
)
@click.option(
    "--wait/--no-wait",
    default=True,
    help="Wait for syncs to complete before returning",
)
@click.option(
    "--timeout",
    type=int,
    help="Maximum time to wait in minutes.",
    default=1440,
)
@click.option("--json", "json_output", is_flag=True, help="Output results as JSON.", default=False)
def fivetran_sync(
    api_key: str,
    api_secret: str,
    connector_ids: Optional[List[str]],
    connector_id: Optional[List[str]],
    force: bool,
    wait: bool,
    timeout: int,
    json_output: bool,
) -> None:
    """
    Trigger sync for Fivetran connectors.
    """
    connector_ids = resolve_deprecated_option(connector_ids, connector_id, "connector-ids", "connector-id")
    if not connector_ids:
        raise click.UsageError("Must specify --connector-ids")

    if not json_output:
        console.header("Fivetran — Trigger Connector Syncs")

    try:
        results = trigger_fivetran_sync(
            api_key=api_key,
            api_secret=api_secret,
            connector_ids=connector_ids,
            force=force,
            wait_for_completion=wait,
            timeout_minutes=timeout,
        )

        if json_output:
            failed = [r for r in results if "FAILED" in r or "PAUSED" in r or "RESCHEDULED" in r]
            console.json_out(
                {"results": results, "failed_count": len(failed), "success": len(failed) == 0}
            )
            if failed:
                sys.exit(1)
            return

        # Check if any syncs failed, were paused, or rescheduled
        failed_syncs = [
            result
            for result in results
            if "FAILED" in result or "PAUSED" in result or "RESCHEDULED" in result
        ]
        if failed_syncs:
            console.error(f"{len(failed_syncs)} connector sync(s) failed.")
            sys.exit(1)

    except Exception as e:
        if json_output:
            console.json_out({"error": str(e), "success": False})
            sys.exit(1)
        console.error(f"Fivetran sync failed: {e}", exit_code=1)


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
@click.option("--json", "json_output", is_flag=True, help="Output results as JSON.", default=False)
def fivetran_list_connectors(
    api_key: str,
    api_secret: str,
    group_id: Optional[str],
    json_output: bool,
) -> None:
    """
    List all available Fivetran connectors with their status.
    """
    if not json_output:
        if group_id:
            console.info(f"Listing Fivetran connectors for group {group_id}…")
        else:
            console.info("Listing all Fivetran connectors…")

    result = list_fivetran_connectors(
        api_key=api_key,
        api_secret=api_secret,
        group_id=group_id,
        json_output=json_output,
    )
    if json_output and result is not None:
        console.json_out(result)
