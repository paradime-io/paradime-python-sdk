from __future__ import annotations

import sys
from typing import Optional

import click

from paradime.cli import console
from paradime.cli.utils import env_click_option
from paradime.core.scripts.aws_sagemaker import (
    list_sagemaker_pipelines,
    trigger_sagemaker_pipelines,
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
    "--pipeline-names",
    multiple=True,
    help="The name(s) of the SageMaker Pipeline(s) to start",
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
def aws_sagemaker_trigger(
    aws_access_key_id: Optional[str],
    aws_secret_access_key: Optional[str],
    aws_session_token: Optional[str],
    aws_region: Optional[str],
    pipeline_names: tuple,
    wait: bool,
    timeout: int,
    json_output: bool,
) -> None:
    """
    Trigger one or more AWS SageMaker Pipelines.

    AWS credentials are read from environment variables or AWS credential chain:
    - AWS_ACCESS_KEY_ID
    - AWS_SECRET_ACCESS_KEY
    - AWS_SESSION_TOKEN (optional)
    - AWS_REGION

    Example:
        paradime run aws-sagemaker-trigger --pipeline-names my-pipeline --pipeline-names another-pipeline
    """
    if not json_output:
        console.header("AWS SageMaker — Trigger Pipelines")

    try:
        results = trigger_sagemaker_pipelines(
            pipeline_names=list(pipeline_names),
            region_name=aws_region,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token,
            wait_for_completion=wait,
            timeout_minutes=timeout,
        )

        if json_output:
            failed = [r for r in results if "FAILED" in r or "ERROR" in r]
            console.json_out(
                {"results": results, "failed_count": len(failed), "success": len(failed) == 0}
            )
            if failed:
                sys.exit(1)
            return

        # Check if any pipeline executions failed
        failed_pipelines = [result for result in results if "FAILED" in result or "ERROR" in result]
        if failed_pipelines:
            console.error(f"{len(failed_pipelines)} pipeline execution(s) failed.")
            sys.exit(1)

    except Exception as e:
        if json_output:
            console.json_out({"error": str(e), "success": False})
            sys.exit(1)
        console.error(f"SageMaker Pipeline trigger failed: {e}", exit_code=1)


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
def aws_sagemaker_list(
    aws_access_key_id: Optional[str],
    aws_secret_access_key: Optional[str],
    aws_session_token: Optional[str],
    aws_region: Optional[str],
    json_output: bool,
) -> None:
    """
    List all AWS SageMaker Pipelines with their status.

    AWS credentials are read from environment variables or AWS credential chain:
    - AWS_ACCESS_KEY_ID
    - AWS_SECRET_ACCESS_KEY
    - AWS_SESSION_TOKEN (optional)
    - AWS_REGION

    Example:
        paradime run aws-sagemaker-list
    """
    if not json_output:
        console.info("Listing SageMaker Pipelines…")

    try:
        result = list_sagemaker_pipelines(
            region_name=aws_region,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token,
            json_output=json_output,
        )
        if json_output and result is not None:
            console.json_out(result)
    except Exception as e:
        console.error(f"Failed to list SageMaker Pipelines: {e}", exit_code=1)
