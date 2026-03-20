from __future__ import annotations

import sys
from typing import List, Optional

import click

from paradime.cli import console
from paradime.cli.utils import env_click_option
from paradime.core.scripts.airbyte import list_airbyte_connections, trigger_airbyte_jobs


@click.command(context_settings=dict(max_content_width=160))
@env_click_option(
    "client-id",
    "AIRBYTE_CLIENT_ID",
    help="Your Airbyte client ID.",
)
@env_click_option(
    "client-secret",
    "AIRBYTE_CLIENT_SECRET",
    help="Your Airbyte client secret.",
)
@env_click_option(
    "base-url",
    "AIRBYTE_BASE_URL",
    help="Airbyte API base URL. Default: https://api.airbyte.com/v1 (Cloud)",
    default="https://api.airbyte.com/v1",
)
@click.option(
    "--use-server-auth",
    is_flag=True,
    help="Use basic authentication for self-hosted Airbyte Server (instead of OAuth for Cloud)",
    default=False,
)
@click.option(
    "--connection-ids",
    multiple=True,
    help="The ID(s) of the connection(s) you want to run jobs for",
    required=True,
)
@click.option(
    "--job-type",
    type=click.Choice(["sync", "reset"]),
    help="Type of job to run (sync or reset)",
    required=True,
)
@click.option(
    "--workspace-id",
    help="Optional workspace ID",
    required=False,
)
@click.option(
    "--wait/--no-wait",
    default=True,
    help="Wait for jobs to complete before returning",
)
@click.option(
    "--timeout",
    type=int,
    help="Maximum time to wait in minutes.",
    default=1440,
)
@click.option("--json", "json_output", is_flag=True, help="Output results as JSON.", default=False)
def airbyte_sync(
    client_id: str,
    client_secret: str,
    base_url: str,
    use_server_auth: bool,
    connection_ids: List[str],
    job_type: str,
    workspace_id: Optional[str],
    wait: bool,
    timeout: int,
    json_output: bool,
) -> None:
    """
    Trigger sync or reset jobs for Airbyte connections.
    """
    if not json_output:
        console.header(f"Airbyte — {job_type.capitalize()} Jobs")

    try:
        results = trigger_airbyte_jobs(
            client_id=client_id,
            client_secret=client_secret,
            connection_ids=list(connection_ids),
            job_type=job_type,
            workspace_id=workspace_id,
            wait_for_completion=wait,
            timeout_minutes=timeout,
            base_url=base_url,
            use_cloud_auth=not use_server_auth,
        )

        if json_output:
            failed = [r for r in results if "FAILED" in r or "CANCELLED" in r or "INCOMPLETE" in r]
            console.json_out(
                {"results": results, "failed_count": len(failed), "success": len(failed) == 0}
            )
            if failed:
                sys.exit(1)
            return

        # Check if any jobs failed, were cancelled, or incomplete
        failed_jobs = [
            result
            for result in results
            if "FAILED" in result or "CANCELLED" in result or "INCOMPLETE" in result
        ]
        if failed_jobs:
            console.error(f"{len(failed_jobs)} job(s) failed.")
            sys.exit(1)

    except Exception as e:
        if json_output:
            console.json_out({"error": str(e), "success": False})
            sys.exit(1)
        console.error(f"Airbyte {job_type} failed: {e}", exit_code=1)


@click.command(context_settings=dict(max_content_width=160))
@env_click_option(
    "client-id",
    "AIRBYTE_CLIENT_ID",
    help="Your Airbyte client ID (Cloud) or API key (Server).",
)
@env_click_option(
    "client-secret",
    "AIRBYTE_CLIENT_SECRET",
    help="Your Airbyte client secret (Cloud) or API secret (Server).",
)
@env_click_option(
    "base-url",
    "AIRBYTE_BASE_URL",
    help="Airbyte API base URL. Default: https://api.airbyte.com/v1 (Cloud)",
    default="https://api.airbyte.com/v1",
)
@click.option(
    "--use-server-auth",
    is_flag=True,
    help="Use basic authentication for self-hosted Airbyte Server (instead of OAuth for Cloud)",
    default=False,
)
@click.option(
    "--workspace-id",
    help="Optional workspace ID to filter connections by workspace",
    required=False,
)
@click.option("--json", "json_output", is_flag=True, help="Output results as JSON.", default=False)
def airbyte_list_connections(
    client_id: str,
    client_secret: str,
    base_url: str,
    use_server_auth: bool,
    workspace_id: Optional[str],
    json_output: bool,
) -> None:
    """
    List all available Airbyte connections with their status.
    """
    if not json_output:
        if workspace_id:
            console.info(f"Listing Airbyte connections for workspace {workspace_id}…")
        else:
            console.info("Listing all Airbyte connections…")

    result = list_airbyte_connections(
        client_id=client_id,
        client_secret=client_secret,
        workspace_id=workspace_id,
        base_url=base_url,
        use_cloud_auth=not use_server_auth,
        json_output=json_output,
    )
    if json_output and result is not None:
        console.json_out(result)
