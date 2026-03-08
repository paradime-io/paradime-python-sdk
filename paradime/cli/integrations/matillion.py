from __future__ import annotations

import sys
from typing import List

import click

from paradime.cli import console
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
    "--project-names",
    help="The Matillion project name",
    required=True,
)
@click.option(
    "--pipeline-names",
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
    "--wait/--no-wait",
    help="Wait for pipeline executions to complete before returning",
    default=True,
)
@click.option(
    "--timeout",
    type=int,
    help="Maximum time to wait in minutes.",
    default=1440,
)
@click.option("--json", "json_output", is_flag=True, help="Output results as JSON.", default=False)
def matillion_pipeline(
    base_url: str,
    client_id: str,
    client_secret: str,
    project_names: str,
    pipeline_names: List[str],
    environment: str,
    wait: bool,
    timeout: int,
    json_output: bool,
) -> None:
    """
    Trigger execution for Matillion Data Productivity Cloud pipelines.
    """
    if not json_output:
        console.header("Matillion — Trigger Pipeline Executions")

    try:
        results = trigger_matillion_pipeline(
            base_url=base_url,
            client_id=client_id,
            client_secret=client_secret,
            project_name=project_names,
            pipeline_names=list(pipeline_names),
            environment=environment,
            wait_for_completion=wait,
            timeout_minutes=timeout,
        )

        if json_output:
            failed = [r for r in results if "FAILED" in r or "CANCELLED" in r]
            console.json_out(
                {"results": results, "failed_count": len(failed), "success": len(failed) == 0}
            )
            if failed:
                sys.exit(1)
            return

        # Check if any executions failed or were cancelled
        failed_executions = [
            result for result in results if "FAILED" in result or "CANCELLED" in result
        ]
        if failed_executions:
            console.error(f"{len(failed_executions)} pipeline execution(s) failed.")
            sys.exit(1)

    except Exception as e:
        if json_output:
            console.json_out({"error": str(e), "success": False})
            sys.exit(1)
        console.error(f"Matillion pipeline execution failed: {e}", exit_code=1)


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
    "--project-name",
    help="The Matillion project name",
    required=True,
)
@click.option(
    "--environment",
    help="The Matillion environment name to filter pipelines by",
    required=True,
)
@click.option("--json", "json_output", is_flag=True, help="Output results as JSON.", default=False)
def matillion_list_pipelines(
    base_url: str,
    client_id: str,
    client_secret: str,
    project_name: str,
    environment: str,
    json_output: bool,
) -> None:
    """
    List all available Matillion Data Productivity Cloud published pipelines.
    """
    if not json_output:
        console.info(f"Listing Matillion pipelines for environment {environment}…")

    result = list_matillion_pipelines(
        base_url=base_url,
        client_id=client_id,
        client_secret=client_secret,
        project_name=project_name,
        environment=environment,
        json_output=json_output,
    )
    if json_output and result is not None:
        console.json_out(result)


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
@click.option("--json", "json_output", is_flag=True, help="Output results as JSON.", default=False)
def matillion_list_projects(
    base_url: str,
    client_id: str,
    client_secret: str,
    json_output: bool,
) -> None:
    """
    List all available Matillion Data Productivity Cloud projects.
    """
    if not json_output:
        console.info("Listing all Matillion projects…")

    result = list_matillion_projects(
        base_url=base_url,
        client_id=client_id,
        client_secret=client_secret,
        json_output=json_output,
    )
    if json_output and result is not None:
        console.json_out(result)
