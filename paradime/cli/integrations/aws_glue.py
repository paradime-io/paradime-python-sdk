from __future__ import annotations

import sys
from typing import Optional

import click

from paradime.cli import console
from paradime.cli.utils import env_click_option
from paradime.core.scripts.aws_glue import (
    list_glue_jobs,
    list_glue_workflows,
    trigger_glue_jobs,
    trigger_glue_workflows,
)


@click.command(context_settings=dict(max_content_width=160))
@env_click_option(
    "aws-access-key-id",
    "AWS_ACCESS_KEY_ID",
    help="AWS Access Key ID for authentication. Can also use AWS credential chain.",
    required=False,
)
@env_click_option(
    "aws-secret-access-key",
    "AWS_SECRET_ACCESS_KEY",
    help="AWS Secret Access Key for authentication. Can also use AWS credential chain.",
    required=False,
)
@env_click_option(
    "aws-session-token",
    "AWS_SESSION_TOKEN",
    help="Optional AWS Session Token for temporary credentials.",
    required=False,
)
@env_click_option(
    "aws-region",
    "AWS_REGION",
    help="AWS region name (e.g., us-east-1, us-west-2). Defaults to default region from AWS config.",
    required=False,
)
@click.option(
    "--workflow-names",
    multiple=True,
    help="The name(s) of the AWS Glue workflow(s) to trigger",
    required=True,
)
@click.option(
    "--wait/--no-wait",
    help="Wait for workflow runs to complete before returning",
    default=True,
)
@click.option(
    "--timeout",
    type=int,
    help="Maximum time to wait in minutes.",
    default=1440,
)
@click.option("--json", "json_output", is_flag=True, help="Output results as JSON.", default=False)
def aws_glue_trigger_workflows(
    aws_access_key_id: Optional[str],  # noqa: ARG001
    aws_secret_access_key: Optional[str],  # noqa: ARG001
    aws_session_token: Optional[str],  # noqa: ARG001
    aws_region: Optional[str],
    workflow_names: tuple,
    wait: bool,
    timeout: int,
    json_output: bool,
) -> None:
    """
    Trigger one or more AWS Glue workflows.

    AWS credentials are read from environment variables or AWS credential chain:
    - AWS_ACCESS_KEY_ID
    - AWS_SECRET_ACCESS_KEY
    - AWS_SESSION_TOKEN (optional)
    - AWS_REGION

    Example:
        paradime run aws-glue-trigger --workflow-names my-workflow-1 --workflow-names my-workflow-2
    """
    # Note: AWS credentials are set via environment variables by env_click_option decorator
    # boto3 will pick them up automatically from the environment
    # We accept the parameters here to satisfy Click's parameter passing, but don't use them directly

    if not json_output:
        console.header("AWS Glue — Trigger Workflows")

    try:
        results = trigger_glue_workflows(
            workflow_names=list(workflow_names),
            wait_for_completion=wait,
            timeout_minutes=timeout,
            region_name=aws_region,
        )

        if json_output:
            failed = [r for r in results if "FAILED" in r or "ERROR" in r]
            console.json_out(
                {"results": results, "failed_count": len(failed), "success": len(failed) == 0}
            )
            if failed:
                sys.exit(1)
            return

        # Check if any workflow runs failed
        failed_workflows = [result for result in results if "FAILED" in result or "ERROR" in result]
        if failed_workflows:
            console.error(f"{len(failed_workflows)} workflow(s) failed.")
            sys.exit(1)

    except Exception as e:
        if json_output:
            console.json_out({"error": str(e), "success": False})
            sys.exit(1)
        console.error(f"AWS Glue workflow trigger failed: {e}", exit_code=1)


@click.command(context_settings=dict(max_content_width=160))
@env_click_option(
    "aws-access-key-id",
    "AWS_ACCESS_KEY_ID",
    help="AWS Access Key ID for authentication. Can also use AWS credential chain.",
    required=False,
)
@env_click_option(
    "aws-secret-access-key",
    "AWS_SECRET_ACCESS_KEY",
    help="AWS Secret Access Key for authentication. Can also use AWS credential chain.",
    required=False,
)
@env_click_option(
    "aws-session-token",
    "AWS_SESSION_TOKEN",
    help="Optional AWS Session Token for temporary credentials.",
    required=False,
)
@env_click_option(
    "aws-region",
    "AWS_REGION",
    help="AWS region name (e.g., us-east-1, us-west-2). Defaults to default region from AWS config.",
    required=False,
)
@click.option("--json", "json_output", is_flag=True, help="Output results as JSON.", default=False)
def aws_glue_list_workflows(
    aws_access_key_id: Optional[str],  # noqa: ARG001
    aws_secret_access_key: Optional[str],  # noqa: ARG001
    aws_session_token: Optional[str],  # noqa: ARG001
    aws_region: Optional[str],
    json_output: bool,
) -> None:
    """
    List all AWS Glue workflows with their status.

    AWS credentials are read from environment variables or AWS credential chain:
    - AWS_ACCESS_KEY_ID
    - AWS_SECRET_ACCESS_KEY
    - AWS_SESSION_TOKEN (optional)
    - AWS_REGION

    Example:
        paradime run aws-glue-list-workflows
    """
    # Note: AWS credentials are set via environment variables by env_click_option decorator
    # boto3 will pick them up automatically from the environment
    # We accept the parameters here to satisfy Click's parameter passing, but don't use them directly

    if not json_output:
        console.info("Listing AWS Glue workflows…")

    try:
        result = list_glue_workflows(region_name=aws_region, json_output=json_output)
        if json_output and result is not None:
            console.json_out(result)
    except Exception as e:
        console.error(f"Failed to list AWS Glue workflows: {e}", exit_code=1)


@click.command(context_settings=dict(max_content_width=160))
@env_click_option(
    "aws-access-key-id",
    "AWS_ACCESS_KEY_ID",
    help="AWS Access Key ID for authentication. Can also use AWS credential chain.",
    required=False,
)
@env_click_option(
    "aws-secret-access-key",
    "AWS_SECRET_ACCESS_KEY",
    help="AWS Secret Access Key for authentication. Can also use AWS credential chain.",
    required=False,
)
@env_click_option(
    "aws-session-token",
    "AWS_SESSION_TOKEN",
    help="Optional AWS Session Token for temporary credentials.",
    required=False,
)
@env_click_option(
    "aws-region",
    "AWS_REGION",
    help="AWS region name (e.g., us-east-1, us-west-2). Defaults to default region from AWS config.",
    required=False,
)
@click.option(
    "--job-names",
    multiple=True,
    help="The name(s) of the AWS Glue job(s) to trigger",
    required=True,
)
@click.option(
    "--wait/--no-wait",
    help="Wait for job runs to complete before returning",
    default=True,
)
@click.option(
    "--timeout",
    type=int,
    help="Maximum time to wait in minutes.",
    default=1440,
)
@click.option("--json", "json_output", is_flag=True, help="Output results as JSON.", default=False)
def aws_glue_trigger_jobs(
    aws_access_key_id: Optional[str],  # noqa: ARG001
    aws_secret_access_key: Optional[str],  # noqa: ARG001
    aws_session_token: Optional[str],  # noqa: ARG001
    aws_region: Optional[str],
    job_names: tuple,
    wait: bool,
    timeout: int,
    json_output: bool,
) -> None:
    """
    Trigger one or more AWS Glue jobs (ETL jobs).

    AWS credentials are read from environment variables or AWS credential chain:
    - AWS_ACCESS_KEY_ID
    - AWS_SECRET_ACCESS_KEY
    - AWS_SESSION_TOKEN (optional)
    - AWS_REGION

    Example:
        paradime run aws-glue-trigger-jobs --job-names demo-glue-job --job-names another-job
    """
    # Note: AWS credentials are set via environment variables by env_click_option decorator
    # boto3 will pick them up automatically from the environment
    # We accept the parameters here to satisfy Click's parameter passing, but don't use them directly

    if not json_output:
        console.header("AWS Glue — Trigger Jobs")

    try:
        results = trigger_glue_jobs(
            job_names=list(job_names),
            wait_for_completion=wait,
            timeout_minutes=timeout,
            region_name=aws_region,
        )

        if json_output:
            failed = [r for r in results if "FAILED" in r or "ERROR" in r]
            console.json_out(
                {"results": results, "failed_count": len(failed), "success": len(failed) == 0}
            )
            if failed:
                sys.exit(1)
            return

        # Check if any job runs failed
        failed_jobs = [result for result in results if "FAILED" in result or "ERROR" in result]
        if failed_jobs:
            console.error(f"{len(failed_jobs)} job(s) failed.")
            sys.exit(1)

    except Exception as e:
        if json_output:
            console.json_out({"error": str(e), "success": False})
            sys.exit(1)
        console.error(f"AWS Glue job trigger failed: {e}", exit_code=1)


@click.command(context_settings=dict(max_content_width=160))
@env_click_option(
    "aws-access-key-id",
    "AWS_ACCESS_KEY_ID",
    help="AWS Access Key ID for authentication. Can also use AWS credential chain.",
    required=False,
)
@env_click_option(
    "aws-secret-access-key",
    "AWS_SECRET_ACCESS_KEY",
    help="AWS Secret Access Key for authentication. Can also use AWS credential chain.",
    required=False,
)
@env_click_option(
    "aws-session-token",
    "AWS_SESSION_TOKEN",
    help="Optional AWS Session Token for temporary credentials.",
    required=False,
)
@env_click_option(
    "aws-region",
    "AWS_REGION",
    help="AWS region name (e.g., us-east-1, us-west-2). Defaults to default region from AWS config.",
    required=False,
)
@click.option("--json", "json_output", is_flag=True, help="Output results as JSON.", default=False)
def aws_glue_list_jobs(
    aws_access_key_id: Optional[str],  # noqa: ARG001
    aws_secret_access_key: Optional[str],  # noqa: ARG001
    aws_session_token: Optional[str],  # noqa: ARG001
    aws_region: Optional[str],
    json_output: bool,
) -> None:
    """
    List all AWS Glue jobs (ETL jobs) with their status.

    AWS credentials are read from environment variables or AWS credential chain:
    - AWS_ACCESS_KEY_ID
    - AWS_SECRET_ACCESS_KEY
    - AWS_SESSION_TOKEN (optional)
    - AWS_REGION

    Example:
        paradime run aws-glue-list-jobs
    """
    # Note: AWS credentials are set via environment variables by env_click_option decorator
    # boto3 will pick them up automatically from the environment
    # We accept the parameters here to satisfy Click's parameter passing, but don't use them directly

    if not json_output:
        console.info("Listing AWS Glue jobs…")

    try:
        result = list_glue_jobs(region_name=aws_region, json_output=json_output)
        if json_output and result is not None:
            console.json_out(result)
    except Exception as e:
        console.error(f"Failed to list AWS Glue jobs: {e}", exit_code=1)
