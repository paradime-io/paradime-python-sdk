from __future__ import annotations

import sys
from typing import List, Optional

import click

from paradime.cli import console
from paradime.cli.utils import COMMA_LIST, env_click_option
from paradime.core.scripts.databricks_notebook import trigger_databricks_notebooks


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
    "--notebook-paths",
    type=COMMA_LIST,
    help="Comma-separated notebook path(s) to run (e.g. /Users/me/notebook1).",
    required=True,
)
@click.option(
    "--cluster-id",
    help="Existing cluster ID to run notebooks on.",
    required=True,
)
@click.option(
    "--parameters",
    type=str,
    help="JSON string of base parameters for the notebooks (optional).",
    required=False,
)
@click.option(
    "--wait/--no-wait",
    default=True,
    help="Wait for notebook runs to complete before returning.",
)
@click.option(
    "--timeout",
    type=int,
    help="Maximum time to wait in minutes.",
    default=1440,
)
@click.option("--json", "json_output", is_flag=True, help="Output results as JSON.", default=False)
def databricks_notebook_trigger(
    host: str,
    token: str,
    notebook_paths: List[str],
    cluster_id: str,
    parameters: Optional[str],
    wait: bool,
    timeout: int,
    json_output: bool,
) -> None:
    """
    Trigger Databricks notebook runs via the runs/submit API.
    """
    if not json_output:
        console.header("Databricks — Run Notebooks")

    try:
        results = trigger_databricks_notebooks(
            host=host,
            token=token,
            notebook_paths=notebook_paths,
            cluster_id=cluster_id,
            parameters=parameters,
            wait=wait,
            timeout_minutes=timeout,
        )

        if json_output:
            failed = [r for r in results if r in ("FAILED", "CANCELLED", "TIMEDOUT")]
            console.json_out(
                {"results": results, "failed_count": len(failed), "success": len(failed) == 0}
            )
            if failed:
                sys.exit(1)
            return

        failed_runs = [r for r in results if r in ("FAILED", "CANCELLED", "TIMEDOUT")]
        if failed_runs:
            console.error(f"{len(failed_runs)} notebook run(s) failed.")
            sys.exit(1)

    except Exception as e:
        if json_output:
            console.json_out({"error": str(e), "success": False})
            sys.exit(1)
        console.error(f"Databricks notebook run failed: {e}", exit_code=1)
