from __future__ import annotations

import sys
from typing import List, Optional

import click

from paradime.cli import console
from paradime.cli.utils import COMMA_LIST, env_click_option
from paradime.core.scripts.metaplane import list_metaplane_monitors, trigger_metaplane_monitors


@click.command(context_settings=dict(max_content_width=160))
@env_click_option(
    "api-key",
    "METAPLANE_API_KEY",
    help="Your Metaplane API key.",
)
@click.option(
    "--monitor-ids",
    type=COMMA_LIST,
    help="Comma-separated monitor ID(s) to trigger",
    required=True,
)
@click.option(
    "--wait/--no-wait",
    default=True,
    help="Wait for monitor runs to complete before returning",
)
@click.option(
    "--timeout",
    type=int,
    help="Maximum time to wait in minutes.",
    default=1440,
)
@click.option("--json", "json_output", is_flag=True, help="Output results as JSON.", default=False)
def metaplane_trigger(
    api_key: str,
    monitor_ids: List[str],
    wait: bool,
    timeout: int,
    json_output: bool,
) -> None:
    """
    Trigger runs for Metaplane monitors.
    """
    if not json_output:
        console.header("Metaplane — Trigger Monitor Runs")

    try:
        results = trigger_metaplane_monitors(
            api_key=api_key,
            monitor_ids=monitor_ids,
            wait=wait,
            timeout_minutes=timeout,
        )

        if json_output:
            failed = [r for r in results if "FAILED" in r]
            console.json_out(
                {"results": results, "failed_count": len(failed), "success": len(failed) == 0}
            )
            if failed:
                sys.exit(1)
            return

        # Check if any monitor runs failed
        failed_runs = [result for result in results if "FAILED" in result]
        if failed_runs:
            console.error(f"{len(failed_runs)} monitor run(s) failed.")
            sys.exit(1)

    except Exception as e:
        if json_output:
            console.json_out({"error": str(e), "success": False})
            sys.exit(1)
        console.error(f"Metaplane trigger failed: {e}", exit_code=1)


@click.command(context_settings=dict(max_content_width=160))
@env_click_option(
    "api-key",
    "METAPLANE_API_KEY",
    help="Your Metaplane API key.",
)
@click.option("--json", "json_output", is_flag=True, help="Output results as JSON.", default=False)
def metaplane_list_monitors(
    api_key: str,
    json_output: bool,
) -> None:
    """
    List all available Metaplane monitors with their status.
    """
    if not json_output:
        console.info("Listing all Metaplane monitors...")

    result = list_metaplane_monitors(
        api_key=api_key,
        json_output=json_output,
    )
    if json_output and result is not None:
        console.json_out(result)
