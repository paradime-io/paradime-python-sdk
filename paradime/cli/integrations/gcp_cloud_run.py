import sys
from typing import List

import click

from paradime.cli.utils import env_click_option
from paradime.core.scripts.gcp_cloud_run import list_cloud_run_jobs, trigger_cloud_run_jobs


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
    "--job-name",
    multiple=True,
    help="The name(s) of the Cloud Run Job(s) to trigger.",
    required=True,
)
@click.option(
    "--wait-for-completion/--no-wait-for-completion",
    help="Wait for the job execution to complete before returning.",
    default=True,
)
@click.option(
    "--timeout-minutes",
    type=int,
    help="Maximum time to wait for completion (in minutes). Only used with --wait-for-completion.",
    default=1440,
)
def gcp_cloud_run_trigger(
    service_account_key_file: str,
    project: str,
    location: str,
    job_name: List[str],
    wait_for_completion: bool,
    timeout_minutes: int,
) -> None:
    """
    Trigger Cloud Run Jobs by name.
    """
    click.echo(f"Triggering {len(job_name)} Cloud Run Job(s)...")

    try:
        results = trigger_cloud_run_jobs(
            service_account_key_file=service_account_key_file,
            project=project,
            location=location,
            job_names=list(job_name),
            wait_for_completion=wait_for_completion,
            timeout_minutes=timeout_minutes,
        )

        failed = [r for r in results if "FAILED" in r]
        if failed:
            sys.exit(1)

    except Exception as e:
        click.echo(f"❌ Cloud Run Job trigger failed: {str(e)}")
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
def gcp_cloud_run_list(
    service_account_key_file: str,
    project: str,
    location: str,
) -> None:
    """
    List all Cloud Run Jobs in the specified project and region.
    """
    click.echo("Listing Cloud Run Jobs...")

    list_cloud_run_jobs(
        service_account_key_file=service_account_key_file,
        project=project,
        location=location,
    )
