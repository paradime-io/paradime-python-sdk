from __future__ import annotations

import sys
from typing import List, Optional

import click

from paradime.cli import console
from paradime.cli.utils import COMMA_LIST, env_click_option
from paradime.core.scripts.github_actions import (
    list_github_workflows,
    trigger_github_workflows,
)


@click.command(context_settings=dict(max_content_width=160))
@env_click_option(
    "token",
    "GITHUB_TOKEN",
    help="GitHub personal access token or fine-grained token.",
)
@env_click_option(
    "repo",
    "GITHUB_REPOSITORY",
    help="GitHub repository in 'owner/repo' format.",
)
@click.option(
    "--workflow-ids",
    type=COMMA_LIST,
    help="Comma-separated workflow ID(s) or filenames to trigger.",
    required=True,
)
@click.option(
    "--ref",
    help="Git reference (branch/tag) to trigger the workflow on.",
    default="main",
)
@click.option(
    "--inputs",
    type=str,
    help="JSON string of workflow inputs (optional).",
    required=False,
)
@click.option(
    "--wait/--no-wait",
    default=True,
    help="Wait for workflow runs to complete before returning.",
)
@click.option(
    "--timeout",
    type=int,
    help="Maximum time to wait in minutes.",
    default=1440,
)
@click.option("--json", "json_output", is_flag=True, help="Output results as JSON.", default=False)
def github_actions_trigger(
    token: str,
    repo: str,
    workflow_ids: List[str],
    ref: str,
    inputs: Optional[str],
    wait: bool,
    timeout: int,
    json_output: bool,
) -> None:
    """
    Trigger GitHub Actions workflow dispatches.
    """
    import json

    if not json_output:
        console.header("GitHub Actions — Trigger Workflow Dispatches")

    parsed_inputs = None
    if inputs:
        try:
            parsed_inputs = json.loads(inputs)
        except json.JSONDecodeError as e:
            console.error(f"Invalid JSON in --inputs: {e}", exit_code=1)

    try:
        results = trigger_github_workflows(
            token=token,
            repo=repo,
            workflow_ids=workflow_ids,
            ref=ref,
            inputs=parsed_inputs,
            wait=wait,
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

        failed_runs = [r for r in results if "FAILED" in r or "CANCELLED" in r]
        if failed_runs:
            console.error(f"{len(failed_runs)} workflow run(s) failed.")
            sys.exit(1)

    except Exception as e:
        if json_output:
            console.json_out({"error": str(e), "success": False})
            sys.exit(1)
        console.error(f"GitHub Actions trigger failed: {e}", exit_code=1)


@click.command(context_settings=dict(max_content_width=160))
@env_click_option(
    "token",
    "GITHUB_TOKEN",
    help="GitHub personal access token or fine-grained token.",
)
@env_click_option(
    "repo",
    "GITHUB_REPOSITORY",
    help="GitHub repository in 'owner/repo' format.",
)
@click.option("--json", "json_output", is_flag=True, help="Output results as JSON.", default=False)
def github_actions_list(
    token: str,
    repo: str,
    json_output: bool,
) -> None:
    """
    List all GitHub Actions workflows in a repository.
    """
    if not json_output:
        console.info(f"Listing GitHub Actions workflows for {repo}...")

    result = list_github_workflows(
        token=token,
        repo=repo,
        json_output=json_output,
    )
    if json_output and result is not None:
        console.json_out(result)
