import sys
from typing import Optional

import click

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
    "--wait-for-completion/--no-wait-for-completion",
    help="Wait for pipeline executions to complete before returning",
    default=True,
)
@click.option(
    "--timeout-minutes",
    type=int,
    help="Maximum time to wait for pipeline completion (in minutes). Only used with --wait-for-completion.",
    default=1440,
)
def aws_sagemaker_trigger(
    aws_access_key_id: Optional[str],
    aws_secret_access_key: Optional[str],
    aws_session_token: Optional[str],
    aws_region: Optional[str],
    pipeline_names: tuple,
    wait_for_completion: bool,
    timeout_minutes: int,
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
    click.echo(f"Starting {len(pipeline_names)} SageMaker Pipeline(s)...")

    try:
        results = trigger_sagemaker_pipelines(
            pipeline_names=list(pipeline_names),
            region_name=aws_region,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token,
            wait_for_completion=wait_for_completion,
            timeout_minutes=timeout_minutes,
        )

        # Check if any pipeline executions failed
        failed_pipelines = [result for result in results if "FAILED" in result or "ERROR" in result]
        if failed_pipelines:
            sys.exit(1)

    except Exception as e:
        click.echo(f"❌ SageMaker Pipeline trigger failed: {str(e)}")
        raise click.Abort()


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
def aws_sagemaker_list(
    aws_access_key_id: Optional[str],
    aws_secret_access_key: Optional[str],
    aws_session_token: Optional[str],
    aws_region: Optional[str],
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
    click.echo("Listing SageMaker Pipelines...")

    try:
        list_sagemaker_pipelines(
            region_name=aws_region,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token,
        )
    except Exception as e:
        click.echo(f"❌ Failed to list SageMaker Pipelines: {str(e)}")
        raise click.Abort()
