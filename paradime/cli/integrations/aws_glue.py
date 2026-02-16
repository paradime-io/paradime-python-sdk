import sys
from typing import Optional

import click

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
    "--wait-for-completion/--no-wait-for-completion",
    help="Wait for workflow runs to complete before returning",
    default=True,
)
@click.option(
    "--timeout-minutes",
    type=int,
    help="Maximum time to wait for workflow completion (in minutes). Only used with --wait-for-completion.",
    default=1440,
)
def aws_glue_trigger_workflows(
    aws_access_key_id: Optional[str],  # noqa: ARG001
    aws_secret_access_key: Optional[str],  # noqa: ARG001
    aws_session_token: Optional[str],  # noqa: ARG001
    aws_region: Optional[str],
    workflow_names: tuple,
    wait_for_completion: bool,
    timeout_minutes: int,
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

    click.echo(f"Starting {len(workflow_names)} AWS Glue workflow(s)...")

    try:
        results = trigger_glue_workflows(
            workflow_names=list(workflow_names),
            wait_for_completion=wait_for_completion,
            timeout_minutes=timeout_minutes,
            region_name=aws_region,
        )

        # Check if any workflow runs failed
        failed_workflows = [result for result in results if "FAILED" in result or "ERROR" in result]
        if failed_workflows:
            sys.exit(1)

    except Exception as e:
        click.echo(f"❌ AWS Glue workflow trigger failed: {str(e)}")
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
def aws_glue_list_workflows(
    aws_access_key_id: Optional[str],  # noqa: ARG001
    aws_secret_access_key: Optional[str],  # noqa: ARG001
    aws_session_token: Optional[str],  # noqa: ARG001
    aws_region: Optional[str],
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

    click.echo("Listing AWS Glue workflows...")

    try:
        list_glue_workflows(region_name=aws_region)
    except Exception as e:
        click.echo(f"❌ Failed to list AWS Glue workflows: {str(e)}")
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
@click.option(
    "--job-names",
    multiple=True,
    help="The name(s) of the AWS Glue job(s) to trigger",
    required=True,
)
@click.option(
    "--wait-for-completion/--no-wait-for-completion",
    help="Wait for job runs to complete before returning",
    default=True,
)
@click.option(
    "--timeout-minutes",
    type=int,
    help="Maximum time to wait for job completion (in minutes). Only used with --wait-for-completion.",
    default=1440,
)
def aws_glue_trigger_jobs(
    aws_access_key_id: Optional[str],  # noqa: ARG001
    aws_secret_access_key: Optional[str],  # noqa: ARG001
    aws_session_token: Optional[str],  # noqa: ARG001
    aws_region: Optional[str],
    job_names: tuple,
    wait_for_completion: bool,
    timeout_minutes: int,
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

    click.echo(f"Starting {len(job_names)} AWS Glue job(s)...")

    try:
        results = trigger_glue_jobs(
            job_names=list(job_names),
            wait_for_completion=wait_for_completion,
            timeout_minutes=timeout_minutes,
            region_name=aws_region,
        )

        # Check if any job runs failed
        failed_jobs = [result for result in results if "FAILED" in result or "ERROR" in result]
        if failed_jobs:
            sys.exit(1)

    except Exception as e:
        click.echo(f"❌ AWS Glue job trigger failed: {str(e)}")
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
def aws_glue_list_jobs(
    aws_access_key_id: Optional[str],  # noqa: ARG001
    aws_secret_access_key: Optional[str],  # noqa: ARG001
    aws_session_token: Optional[str],  # noqa: ARG001
    aws_region: Optional[str],
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

    click.echo("Listing AWS Glue jobs...")

    try:
        list_glue_jobs(region_name=aws_region)
    except Exception as e:
        click.echo(f"❌ Failed to list AWS Glue jobs: {str(e)}")
        raise click.Abort()
