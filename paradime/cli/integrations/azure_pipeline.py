from __future__ import annotations

import sys
from typing import List, Optional

import click

from paradime.cli import console
from paradime.cli.utils import COMMA_LIST, env_click_option
from paradime.core.scripts.azure_pipeline import (
    list_azure_pipelines,
    trigger_azure_pipelines,
)


@click.command(context_settings=dict(max_content_width=160))
@env_click_option(
    "organization",
    "AZURE_DEVOPS_ORGANIZATION",
    help="Your Azure DevOps organization name.",
)
@env_click_option(
    "project",
    "AZURE_DEVOPS_PROJECT",
    help="Your Azure DevOps project name.",
)
@env_click_option(
    "pat",
    "AZURE_DEVOPS_PAT",
    help="Your Azure DevOps Personal Access Token (PAT).",
)
@click.option(
    "--pipeline-ids",
    type=COMMA_LIST,
    help="Comma-separated pipeline ID(s) to trigger",
    required=True,
)
@click.option(
    "--branch",
    type=str,
    help="Optional branch name to run the pipeline on (e.g., main, develop)",
    required=False,
)
@click.option(
    "--wait/--no-wait",
    default=True,
    help="Wait for pipeline runs to complete before returning.",
)
@click.option(
    "--timeout",
    type=int,
    help="Maximum time to wait in minutes.",
    default=1440,
)
@click.option("--json", "json_output", is_flag=True, help="Output results as JSON.", default=False)
def azure_pipeline_trigger(
    organization: str,
    project: str,
    pat: str,
    pipeline_ids: List[str],
    branch: Optional[str],
    wait: bool,
    timeout: int,
    json_output: bool,
) -> None:
    """
    Trigger runs for Azure DevOps Pipelines.
    """
    if not json_output:
        console.header("Azure Pipelines — Trigger Pipeline Runs")

    try:
        results = trigger_azure_pipelines(
            organization=organization,
            project=project,
            pat=pat,
            pipeline_ids=pipeline_ids,
            branch=branch,
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

        failed_runs = [
            result
            for result in results
            if "FAILED" in result or "CANCELLED" in result
        ]
        if failed_runs:
            console.error(f"{len(failed_runs)} pipeline run(s) failed.")
            sys.exit(1)

    except Exception as e:
        if json_output:
            console.json_out({"error": str(e), "success": False})
            sys.exit(1)
        console.error(f"Azure DevOps Pipeline run failed: {e}", exit_code=1)


@click.command(context_settings=dict(max_content_width=160))
@env_click_option(
    "organization",
    "AZURE_DEVOPS_ORGANIZATION",
    help="Your Azure DevOps organization name.",
)
@env_click_option(
    "project",
    "AZURE_DEVOPS_PROJECT",
    help="Your Azure DevOps project name.",
)
@env_click_option(
    "pat",
    "AZURE_DEVOPS_PAT",
    help="Your Azure DevOps Personal Access Token (PAT).",
)
@click.option("--json", "json_output", is_flag=True, help="Output results as JSON.", default=False)
def azure_pipeline_list(
    organization: str,
    project: str,
    pat: str,
    json_output: bool,
) -> None:
    """
    List all available pipelines in an Azure DevOps project.
    """
    if not json_output:
        console.info(f"Listing pipelines in Azure DevOps project {organization}/{project}...")

    result = list_azure_pipelines(
        organization=organization,
        project=project,
        pat=pat,
        json_output=json_output,
    )
    if json_output and result is not None:
        console.json_out(result)
