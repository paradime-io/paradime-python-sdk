from __future__ import annotations

import sys
from typing import List, Optional

import click

from paradime.cli import console
from paradime.cli.utils import COMMA_LIST
from paradime.core.scripts.zapier import trigger_zapier_webhooks


@click.command(context_settings=dict(max_content_width=160))
@click.option(
    "--webhook-urls",
    type=COMMA_LIST,
    help="Comma-separated Zapier webhook URL(s) to trigger",
    required=True,
)
@click.option(
    "--payload",
    type=str,
    help="JSON payload to send to the webhooks (optional)",
    required=False,
)
@click.option("--json", "json_output", is_flag=True, help="Output results as JSON.", default=False)
def zapier_trigger(
    webhook_urls: List[str],
    payload: Optional[str],
    json_output: bool,
) -> None:
    """
    Trigger Zapier webhooks.
    """
    if not json_output:
        console.header("Zapier — Trigger Webhooks")

    try:
        results = trigger_zapier_webhooks(
            webhook_urls=webhook_urls,
            payload=payload,
        )

        if json_output:
            failed = [r for r in results if "FAILED" in r]
            console.json_out(
                {"results": results, "failed_count": len(failed), "success": len(failed) == 0}
            )
            if failed:
                sys.exit(1)
            return

        failed_triggers = [r for r in results if "FAILED" in r]
        if failed_triggers:
            console.error(f"{len(failed_triggers)} webhook(s) failed.")
            sys.exit(1)

    except Exception as e:
        if json_output:
            console.json_out({"error": str(e), "success": False})
            sys.exit(1)
        console.error(f"Zapier webhook trigger failed: {e}", exit_code=1)
