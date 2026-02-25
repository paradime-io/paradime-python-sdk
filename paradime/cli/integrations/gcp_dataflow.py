import sys
from typing import Optional

import click

from paradime.cli.utils import env_click_option
from paradime.core.scripts.gcp_dataflow import list_dataflow_jobs, trigger_dataflow_job


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
    "--template-path",
    help="GCS path to the Dataflow template (e.g. gs://bucket/templates/my-template).",
    required=True,
)
@click.option(
    "--job-name",
    help="Name for the launched Dataflow job.",
    required=True,
)
@click.option(
    "--template-type",
    type=click.Choice(["classic", "flex"]),
    help="Type of Dataflow template (classic or flex).",
    default="classic",
)
@click.option(
    "--parameters",
    help='JSON string of template parameters (e.g. \'{"inputFile": "gs://..."}\').',
    required=False,
)
@click.option(
    "--wait-for-completion/--no-wait-for-completion",
    help="Wait for the Dataflow job to complete before returning.",
    default=True,
)
@click.option(
    "--timeout-minutes",
    type=int,
    help="Maximum time to wait for completion (in minutes). Only used with --wait-for-completion.",
    default=1440,
)
def gcp_dataflow_trigger(
    service_account_key_file: str,
    project: str,
    location: str,
    template_path: str,
    job_name: str,
    template_type: str,
    parameters: Optional[str],
    wait_for_completion: bool,
    timeout_minutes: int,
) -> None:
    """
    Launch a Dataflow job from a template (classic or flex).
    """
    click.echo(f"Launching Dataflow {template_type} template job '{job_name}'...")

    try:
        results = trigger_dataflow_job(
            service_account_key_file=service_account_key_file,
            project=project,
            location=location,
            template_path=template_path,
            job_name=job_name,
            template_type=template_type,
            parameters=parameters,
            wait_for_completion=wait_for_completion,
            timeout_minutes=timeout_minutes,
        )

        failed = [r for r in results if "FAILED" in r]
        if failed:
            sys.exit(1)

    except Exception as e:
        click.echo(f"❌ Dataflow job launch failed: {str(e)}")
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
def gcp_dataflow_list(
    service_account_key_file: str,
    project: str,
    location: str,
) -> None:
    """
    List Dataflow jobs in the specified project and region.
    """
    click.echo("Listing Dataflow jobs...")

    list_dataflow_jobs(
        service_account_key_file=service_account_key_file,
        project=project,
        location=location,
    )
