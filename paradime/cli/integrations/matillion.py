import sys
from typing import List, Optional

import click

from paradime.cli.utils import env_click_option
from paradime.core.scripts.matillion import (
    list_matillion_pipelines,
    list_matillion_projects,
    trigger_matillion_pipeline,
)


@click.command(context_settings=dict(max_content_width=160))
@env_click_option(
    "base-url",
    "MATILLION_BASE_URL",
    help="Your Matillion DPC API base URL (e.g., https://us1.api.matillion.com or https://eu1.api.matillion.com)",
)
@env_click_option(
    "client-id",
    "MATILLION_CLIENT_ID",
    help="Your Matillion OAuth client ID. Generate this in your Matillion account settings.",
)
@env_click_option(
    "client-secret",
    "MATILLION_CLIENT_SECRET",
    help="Your Matillion OAuth client secret. Generate this in your Matillion account settings.",
)
@click.option(
    "--project-id",
    help="The Matillion project ID",
    required=True,
)
@click.option(
    "--pipeline-name",
    multiple=True,
    help="The name(s) of the pipeline(s) you want to execute",
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
    client_id: str,
    client_secret: str,
    project_id: str,
    pipeline_name: List[str],
    environment: str,
    wait_for_completion: bool,
    timeout_minutes: int,
) -> None:
    """
    Trigger execution for Matillion Data Productivity Cloud pipelines.
    """
    click.echo(f"Starting execution for {len(pipeline_name)} Matillion pipeline(s)...")

    try:
        results = trigger_matillion_pipeline(
            base_url=base_url,
            client_id=client_id,
            client_secret=client_secret,
            project_id=project_id,
            pipeline_names=list(pipeline_name),
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
    help="Your Matillion DPC API base URL (e.g., https://us1.api.matillion.com or https://eu1.api.matillion.com)",
)
@env_click_option(
    "client-id",
    "MATILLION_CLIENT_ID",
    help="Your Matillion OAuth client ID. Generate this in your Matillion account settings.",
)
@env_click_option(
    "client-secret",
    "MATILLION_CLIENT_SECRET",
    help="Your Matillion OAuth client secret. Generate this in your Matillion account settings.",
)
@click.option(
    "--project-id",
    help="The Matillion project ID",
    required=True,
)
@click.option(
    "--environment",
    help="Optional environment name to filter pipelines by environment",
    required=False,
)
def matillion_list_pipelines(
    base_url: str,
    client_id: str,
    client_secret: str,
    project_id: str,
    environment: Optional[str],
) -> None:
    """
    List all available Matillion Data Productivity Cloud published pipelines.
    """
    if environment:
        click.echo(f"Listing Matillion pipelines for environment {environment}...")
    else:
        click.echo("Listing all Matillion pipelines...")

    list_matillion_pipelines(
        base_url=base_url,
        client_id=client_id,
        client_secret=client_secret,
        project_id=project_id,
        environment=environment,
    )


@click.command(context_settings=dict(max_content_width=160))
@env_click_option(
    "base-url",
    "MATILLION_BASE_URL",
    help="Your Matillion DPC API base URL (e.g., https://us1.api.matillion.com or https://eu1.api.matillion.com)",
)
@env_click_option(
    "client-id",
    "MATILLION_CLIENT_ID",
    help="Your Matillion OAuth client ID. Generate this in your Matillion account settings.",
)
@env_click_option(
    "client-secret",
    "MATILLION_CLIENT_SECRET",
    help="Your Matillion OAuth client secret. Generate this in your Matillion account settings.",
)
def matillion_list_projects(
    base_url: str,
    client_id: str,
    client_secret: str,
) -> None:
    """
    List all available Matillion Data Productivity Cloud projects.
    """
    click.echo("Listing all Matillion projects...")

    list_matillion_projects(
        base_url=base_url,
        client_id=client_id,
        client_secret=client_secret,
    )
