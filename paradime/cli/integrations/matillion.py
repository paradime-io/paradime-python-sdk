import sys
from typing import List, Optional

import click

from paradime.cli.utils import env_click_option
from paradime.core.scripts.matillion import list_matillion_pipelines, trigger_matillion_pipeline


@click.command(context_settings=dict(max_content_width=160))
@env_click_option(
    "base-url",
    "MATILLION_BASE_URL",
    help="Your Matillion instance base URL (e.g., https://your-instance.matillion.com)",
)
@env_click_option(
    "api-token",
    "MATILLION_API_TOKEN",
    help="Your Matillion API token. You can generate this in your Matillion account settings.",
)
@click.option(
    "--pipeline-id",
    multiple=True,
    help="The ID(s) of the pipeline(s) you want to execute",
    required=True,
)
@click.option(
    "--environment",
    help="The Matillion environment name to execute the pipeline in",
    required=True,
)
@click.option(
    "--wait-for-completion",
    is_flag=True,
    help="Wait for pipeline executions to complete before returning",
    default=True,
)
@click.option(
    "--timeout-minutes",
    type=int,
    help="Maximum time to wait for execution completion (in minutes). Only used with --wait-for-completion.",
    default=1440,
)
def matillion_pipeline(
    base_url: str,
    api_token: str,
    pipeline_id: List[str],
    environment: str,
    wait_for_completion: bool,
    timeout_minutes: int,
) -> None:
    """
    Trigger execution for Matillion pipelines.
    """
    click.echo(f"Starting execution for {len(pipeline_id)} Matillion pipeline(s)...")

    try:
        results = trigger_matillion_pipeline(
            base_url=base_url,
            api_token=api_token,
            pipeline_ids=list(pipeline_id),
            environment=environment,
            wait_for_completion=wait_for_completion,
            timeout_minutes=timeout_minutes,
        )

        # Check if any executions failed or were cancelled
        failed_executions = [
            result
            for result in results
            if "FAILED" in result or "CANCELLED" in result
        ]
        if failed_executions:
            sys.exit(1)

    except Exception as e:
        click.echo(f"❌ Matillion pipeline execution failed: {str(e)}")
        raise click.Abort()


@click.command(context_settings=dict(max_content_width=160))
@env_click_option(
    "base-url",
    "MATILLION_BASE_URL",
    help="Your Matillion instance base URL (e.g., https://your-instance.matillion.com)",
)
@env_click_option(
    "api-token",
    "MATILLION_API_TOKEN",
    help="Your Matillion API token. You can generate this in your Matillion account settings.",
)
@click.option(
    "--environment",
    help="Optional environment name to filter pipelines by environment",
    required=False,
)
def matillion_list_pipelines(
    base_url: str,
    api_token: str,
    environment: Optional[str],
) -> None:
    """
    List all available Matillion pipelines with their status.
    """
    if environment:
        click.echo(f"Listing Matillion pipelines for environment {environment}...")
    else:
        click.echo("Listing all Matillion pipelines...")

    list_matillion_pipelines(
        base_url=base_url,
        api_token=api_token,
        environment=environment,
    )
