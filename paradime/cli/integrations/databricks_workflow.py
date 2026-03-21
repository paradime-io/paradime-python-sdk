from __future__ import annotations

import sys
from typing import List, Optional

import click

from paradime.cli import console
from paradime.cli.utils import COMMA_LIST, env_click_option
from paradime.core.scripts.databricks_workflow import (
    list_databricks_jobs,
    trigger_databricks_workflows,
)


@click.command(context_settings=dict(max_content_width=160))
@env_click_option(
    "host",
    "DATABRICKS_HOST",
    help="Databricks workspace URL (e.g. https://adb-xxxx.azuredatabricks.net).",
)
@env_click_option(
    "token",
    "DATABRICKS_TOKEN",
    help="Databricks personal access token.",
)
@click.option(
    "--job-ids",
    type=COMMA_LIST,
    help="Comma-separated Databricks job ID(s) to trigger",
    required=True,
)
@click.option(
    "--wait/--no-wait",
    default=True,
    help="Wait for workflow runs to complete before returning",
)
@click.option(
    "--timeout",
    type=int,
    help="Maximum time to wait in minutes.",
    default=1440,
)
@click.option("--json", "json_output", is_flag=True, help="Output results as JSON.", default=False)
def databricks_workflow_trigger(
    host: str,
    token: str,
    job_ids: List[str],
    wait: bool,
    timeout: int,
    json_output: bool,
) -> None:
    """
    Trigger one or more Databricks workflow (job) runs.
    """
    if not json_output:
        console.header("Databricks — Trigger Workflow Runs")

    try:
        results = trigger_databricks_workflows(
            host=host,
            token=token,
            job_ids=job_ids,
            wait_for_completion=wait,
            timeout_minutes=timeout,
        )

        if json_output:
            failed = [r for r in results if "FAILED" in r]
            console.json_out(
                {"results": results, "failed_count": len(failed), "success": len(failed) == 0}
            )
            if failed:
                sys.exit(1)
            return

        # Check if any runs failed
        failed_runs = [
            result
            for result in results
            if "FAILED" in result or "CANCELLED" in result or "TIMEDOUT" in result
        ]
        if failed_runs:
            console.error(f"{len(failed_runs)} workflow run(s) failed.")
            sys.exit(1)

    except Exception as e:
        if json_output:
            console.json_out({"error": str(e), "success": False})
            sys.exit(1)
        console.error(f"Databricks workflow trigger failed: {e}", exit_code=1)


@click.command(context_settings=dict(max_content_width=160))
@env_click_option(
    "host",
    "DATABRICKS_HOST",
    help="Databricks workspace URL (e.g. https://adb-xxxx.azuredatabricks.net).",
)
@env_click_option(
    "token",
    "DATABRICKS_TOKEN",
    help="Databricks personal access token.",
)
@click.option("--json", "json_output", is_flag=True, help="Output results as JSON.", default=False)
def databricks_workflow_list(
    host: str,
    token: str,
    json_output: bool,
) -> None:
    """
    List all available Databricks jobs (workflows).
    """
    if not json_output:
        console.header("Databricks — List Jobs")

    try:
        result = list_databricks_jobs(
            host=host,
            token=token,
            json_output=json_output,
        )
        if json_output and result is not None:
            console.json_out(result)
    except Exception as e:
        if json_output:
            console.json_out({"error": str(e), "success": False})
            sys.exit(1)
        console.error(f"Failed to list Databricks jobs: {e}", exit_code=1)
