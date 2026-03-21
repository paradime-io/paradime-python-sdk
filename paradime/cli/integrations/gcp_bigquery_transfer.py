import sys
from typing import List

import click

from paradime.cli.utils import COMMA_LIST, env_click_option
from paradime.core.scripts.gcp_bigquery_transfer import (
    list_bigquery_transfers,
    trigger_bigquery_transfer,
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
    help="BigQuery Data Transfer location (e.g. 'us', 'eu', 'us-central1').",
    default="us",
    required=False,
)
@click.option(
    "--scheduled-query-names",
    type=COMMA_LIST,
    help="Comma-separated display name(s) of the scheduled query/queries to trigger.",
    required=True,
)
@click.option(
    "--wait/--no-wait",
    help="Wait for the scheduled query run to complete before returning.",
    default=True,
)
@click.option(
    "--timeout-minutes",
    type=int,
    help="Maximum time to wait for completion (in minutes). Only used with --wait.",
    default=1440,
)
def gcp_bigquery_transfer_trigger(
    service_account_key_file: str,
    project: str,
    location: str,
    scheduled_query_names: List[str],
    wait: bool,
    timeout_minutes: int,
) -> None:
    """
    Trigger BigQuery scheduled queries by display name.
    """
    click.echo(f"Starting {len(scheduled_query_names)} BigQuery scheduled query/queries...")

    try:
        results = trigger_bigquery_transfer(
            service_account_key_file=service_account_key_file,
            project=project,
            location=location,
            scheduled_query_names=scheduled_query_names,
            wait_for_completion=wait,
            timeout_minutes=timeout_minutes,
        )

        failed = [r for r in results if "FAILED" in r or "CANCELLED" in r]
        if failed:
            sys.exit(1)

    except Exception as e:
        click.echo(f"❌ BigQuery scheduled query trigger failed: {str(e)}")
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
    help="BigQuery Data Transfer location (e.g. 'us', 'eu', 'us-central1').",
    default="us",
    required=False,
)
def gcp_bigquery_transfer_list(
    service_account_key_file: str,
    project: str,
    location: str,
) -> None:
    """
    List all BigQuery scheduled queries.
    """
    click.echo("Listing BigQuery scheduled queries...")

    list_bigquery_transfers(
        service_account_key_file=service_account_key_file,
        project=project,
        location=location,
    )
