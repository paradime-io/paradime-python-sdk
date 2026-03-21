from __future__ import annotations

import sys
from typing import List

import click

from paradime.cli import console
from paradime.cli.utils import COMMA_LIST, env_click_option
from paradime.core.scripts.gcp_looker import (
    list_looker_scheduled_plans,
    trigger_looker_scheduled_plans,
)


@click.command(context_settings=dict(max_content_width=160))
@env_click_option(
    "base-url",
    "LOOKER_BASE_URL",
    help="Looker instance base URL (e.g. https://mycompany.cloud.looker.com).",
)
@env_click_option(
    "client-id",
    "LOOKER_CLIENT_ID",
    help="Looker API client ID.",
)
@env_click_option(
    "client-secret",
    "LOOKER_CLIENT_SECRET",
    help="Looker API client secret.",
)
@click.option(
    "--plan-ids",
    type=COMMA_LIST,
    help="Comma-separated scheduled plan ID(s) to trigger",
    required=True,
)
@click.option("--json", "json_output", is_flag=True, help="Output results as JSON.", default=False)
def gcp_looker_trigger(
    base_url: str,
    client_id: str,
    client_secret: str,
    plan_ids: List[str],
    json_output: bool,
) -> None:
    """
    Trigger Looker scheduled plans by ID.
    """
    if not json_output:
        console.header("Looker — Trigger Scheduled Plans")

    try:
        results = trigger_looker_scheduled_plans(
            base_url=base_url,
            client_id=client_id,
            client_secret=client_secret,
            plan_ids=plan_ids,
        )

        if json_output:
            failed = [r for r in results if "FAILED" in r]
            console.json_out(
                {"results": results, "failed_count": len(failed), "success": len(failed) == 0}
            )
            if failed:
                sys.exit(1)
            return

        failed = [r for r in results if "FAILED" in r]
        if failed:
            console.error(f"{len(failed)} scheduled plan trigger(s) failed.")
            sys.exit(1)

    except Exception as e:
        if json_output:
            console.json_out({"error": str(e), "success": False})
            sys.exit(1)
        console.error(f"Looker scheduled plan trigger failed: {e}", exit_code=1)


@click.command(context_settings=dict(max_content_width=160))
@env_click_option(
    "base-url",
    "LOOKER_BASE_URL",
    help="Looker instance base URL (e.g. https://mycompany.cloud.looker.com).",
)
@env_click_option(
    "client-id",
    "LOOKER_CLIENT_ID",
    help="Looker API client ID.",
)
@env_click_option(
    "client-secret",
    "LOOKER_CLIENT_SECRET",
    help="Looker API client secret.",
)
@click.option("--json", "json_output", is_flag=True, help="Output results as JSON.", default=False)
def gcp_looker_list_plans(
    base_url: str,
    client_id: str,
    client_secret: str,
    json_output: bool,
) -> None:
    """
    List all Looker scheduled plans.
    """
    if not json_output:
        console.info("Listing Looker scheduled plans...")

    result = list_looker_scheduled_plans(
        base_url=base_url,
        client_id=client_id,
        client_secret=client_secret,
        json_output=json_output,
    )
    if json_output and result is not None:
        console.json_out(result)
