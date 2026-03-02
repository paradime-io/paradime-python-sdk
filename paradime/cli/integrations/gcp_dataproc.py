import sys
from typing import List, Optional

import click

from paradime.cli.utils import env_click_option
from paradime.core.scripts.gcp_dataproc import list_dataproc_clusters, trigger_dataproc_jobs


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
    "--cluster-name",
    help="The name of the Dataproc cluster to submit the job to.",
    required=True,
)
@click.option(
    "--job-type",
    type=click.Choice(["pyspark", "spark", "hive", "spark-sql", "pig", "presto"]),
    help="Type of Dataproc job to submit.",
    required=True,
)
@click.option(
    "--main-file",
    help="GCS path to the main file (required for pyspark/spark jobs).",
    required=False,
)
@click.option(
    "--main-class",
    help="Main class name (for spark jobs with JAR).",
    required=False,
)
@click.option(
    "--args",
    multiple=True,
    help="Arguments to pass to the job.",
    required=False,
)
@click.option(
    "--job-file",
    help="GCS path to the query file (required for hive/spark-sql/pig/presto jobs).",
    required=False,
)
@click.option(
    "--wait-for-completion/--no-wait-for-completion",
    help="Wait for the Dataproc job to complete before returning.",
    default=True,
)
@click.option(
    "--timeout-minutes",
    type=int,
    help="Maximum time to wait for completion (in minutes). Only used with --wait-for-completion.",
    default=1440,
)
def gcp_dataproc_trigger(
    service_account_key_file: str,
    project: str,
    location: str,
    cluster_name: str,
    job_type: str,
    main_file: Optional[str],
    main_class: Optional[str],
    args: List[str],
    job_file: Optional[str],
    wait_for_completion: bool,
    timeout_minutes: int,
) -> None:
    """
    Submit a job to a Dataproc cluster.
    """
    click.echo(f"Submitting {job_type} job to Dataproc cluster '{cluster_name}'...")

    try:
        results = trigger_dataproc_jobs(
            service_account_key_file=service_account_key_file,
            project=project,
            location=location,
            cluster_name=cluster_name,
            job_type=job_type,
            main_file=main_file,
            main_class=main_class,
            args=list(args) if args else None,
            job_file=job_file,
            wait_for_completion=wait_for_completion,
            timeout_minutes=timeout_minutes,
        )

        failed = [r for r in results if "FAILED" in r or "ERROR" in r]
        if failed:
            sys.exit(1)

    except Exception as e:
        click.echo(f"❌ Dataproc job submission failed: {str(e)}")
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
def gcp_dataproc_list_clusters(
    service_account_key_file: str,
    project: str,
    location: str,
) -> None:
    """
    List all Dataproc clusters in the specified project and region.
    """
    click.echo("Listing Dataproc clusters...")

    list_dataproc_clusters(
        service_account_key_file=service_account_key_file,
        project=project,
        location=location,
    )
