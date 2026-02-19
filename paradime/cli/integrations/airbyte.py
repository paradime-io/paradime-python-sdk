import sys
from typing import List, Optional

import click

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
    "--connection-id",
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
    "--wait-for-completion",
    is_flag=True,
    help="Wait for jobs to complete before returning",
    default=True,
)
@click.option(
    "--timeout-minutes",
    type=int,
    help="Maximum time to wait for job completion (in minutes). Only used with --wait-for-completion.",
    default=1440,
)
def airbyte_sync(
    client_id: str,
    client_secret: str,
    base_url: str,
    use_server_auth: bool,
    connection_id: List[str],
    job_type: str,
    workspace_id: Optional[str],
    wait_for_completion: bool,
    timeout_minutes: int,
) -> None:
    """
    Trigger sync or reset jobs for Airbyte connections.
    """
    click.echo(f"Starting {job_type} jobs for {len(connection_id)} Airbyte connection(s)...")

    try:
        results = trigger_airbyte_jobs(
            client_id=client_id,
            client_secret=client_secret,
            connection_ids=list(connection_id),
            job_type=job_type,
            workspace_id=workspace_id,
            wait_for_completion=wait_for_completion,
            timeout_minutes=timeout_minutes,
            base_url=base_url,
            use_cloud_auth=not use_server_auth,
        )

        # Check if any jobs failed, were cancelled, or incomplete
        failed_jobs = [
            result
            for result in results
            if "FAILED" in result or "CANCELLED" in result or "INCOMPLETE" in result
        ]
        if failed_jobs:
            sys.exit(1)

    except Exception as e:
        click.echo(f"❌ Airbyte {job_type} failed: {str(e)}")
        raise click.Abort()


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
def airbyte_list_connections(
    client_id: str,
    client_secret: str,
    base_url: str,
    use_server_auth: bool,
    workspace_id: Optional[str],
) -> None:
    """
    List all available Airbyte connections with their status.
    """
    if workspace_id:
        click.echo(f"Listing Airbyte connections for workspace {workspace_id}...")
    else:
        click.echo("Listing all Airbyte connections...")

    list_airbyte_connections(
        client_id=client_id,
        client_secret=client_secret,
        workspace_id=workspace_id,
        base_url=base_url,
        use_cloud_auth=not use_server_auth,
    )
