import sys
from typing import List, Optional

import click

from paradime.cli.utils import env_click_option
from paradime.core.scripts.gcp_cloud_function import (
    list_cloud_functions,
    trigger_cloud_functions,
)


@click.command(context_settings=dict(max_content_width=160))
@env_click_option(
    "service-account-key-file",
    "GCP_SERVICE_ACCOUNT_KEY_FILE",
    help="Path to your GCP service account JSON key file.",
)
@env_click_option(
    "project",
    "GCP_PROJECT_ID",
    help="Your GCP project ID.",
)
@env_click_option(
    "location",
    "GCP_LOCATION",
    help="GCP region (e.g. 'us-central1').",
)
@click.option(
    "--function-name",
    multiple=True,
    help="The name(s) of the Cloud Function(s) to invoke.",
    required=True,
)
@click.option(
    "--payload",
    help="JSON payload to send to the function (optional).",
    required=False,
)
@click.option(
    "--wait-for-completion/--no-wait-for-completion",
    help="Wait for the function invocation to complete before returning.",
    default=True,
)
@click.option(
    "--timeout-minutes",
    type=int,
    help="Maximum time to wait for completion (in minutes). Only used with --wait-for-completion.",
    default=30,
)
def gcp_cloud_function_trigger(
    service_account_key_file: str,
    project: str,
    location: str,
    function_name: List[str],
    payload: Optional[str],
    wait_for_completion: bool,
    timeout_minutes: int,
) -> None:
    """
    Invoke Google Cloud Functions by name.
    """
    click.echo(f"Invoking {len(function_name)} Cloud Function(s)...")

    try:
        results = trigger_cloud_functions(
            service_account_key_file=service_account_key_file,
            project=project,
            location=location,
            function_names=list(function_name),
            payload=payload,
            wait_for_completion=wait_for_completion,
            timeout_minutes=timeout_minutes,
        )

        failed = [r for r in results if "FAILED" in r]
        if failed:
            sys.exit(1)

    except Exception as e:
        click.echo(f"❌ Cloud Function invocation failed: {str(e)}")
        raise click.Abort()


@click.command(context_settings=dict(max_content_width=160))
@env_click_option(
    "service-account-key-file",
    "GCP_SERVICE_ACCOUNT_KEY_FILE",
    help="Path to your GCP service account JSON key file.",
)
@env_click_option(
    "project",
    "GCP_PROJECT_ID",
    help="Your GCP project ID.",
)
@env_click_option(
    "location",
    "GCP_LOCATION",
    help="GCP region (e.g. 'us-central1').",
)
def gcp_cloud_function_list(
    service_account_key_file: str,
    project: str,
    location: str,
) -> None:
    """
    List all Cloud Functions in the specified project and region.
    """
    click.echo("Listing Cloud Functions...")

    list_cloud_functions(
        service_account_key_file=service_account_key_file,
        project=project,
        location=location,
    )
