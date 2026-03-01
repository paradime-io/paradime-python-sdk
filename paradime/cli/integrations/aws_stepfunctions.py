import sys
from typing import Optional

import click

from paradime.cli.utils import env_click_option
from paradime.core.scripts.aws_stepfunctions import (
    list_stepfunctions_state_machines,
    trigger_stepfunctions_executions,
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
    "--state-machine-arns",
    multiple=True,
    help="The ARN(s) of the Step Functions state machine(s) to execute",
    required=True,
)
@click.option(
    "--input-data",
    type=str,
    help="JSON input data to pass to the state machines (optional)",
    required=False,
)
@click.option(
    "--wait-for-completion/--no-wait-for-completion",
    help="Wait for executions to complete before returning",
    default=True,
)
@click.option(
    "--timeout-minutes",
    type=int,
    help="Maximum time to wait for execution completion (in minutes). Only used with --wait-for-completion.",
    default=1440,
)
def aws_stepfunctions_trigger(
    aws_access_key_id: Optional[str],
    aws_secret_access_key: Optional[str],
    aws_session_token: Optional[str],
    aws_region: Optional[str],
    state_machine_arns: tuple,
    input_data: Optional[str],
    wait_for_completion: bool,
    timeout_minutes: int,
) -> None:
    """
    Trigger one or more AWS Step Functions state machine executions.

    AWS credentials are read from environment variables or AWS credential chain:
    - AWS_ACCESS_KEY_ID
    - AWS_SECRET_ACCESS_KEY
    - AWS_SESSION_TOKEN (optional)
    - AWS_REGION

    Example:
        paradime run aws-stepfunctions-trigger --state-machine-arns arn:aws:states:us-east-1:123456789012:stateMachine:MyStateMachine
        paradime run aws-stepfunctions-trigger --state-machine-arns arn:... --input-data '{"key":"value"}'
    """
    import json

    click.echo(f"Starting {len(state_machine_arns)} Step Functions execution(s)...")

    # Parse input data if provided
    input_dict = None
    if input_data:
        try:
            input_dict = json.loads(input_data)
        except json.JSONDecodeError as e:
            click.echo(f"❌ Invalid JSON input data: {str(e)}")
            raise click.Abort()

    try:
        results = trigger_stepfunctions_executions(
            state_machine_arns=list(state_machine_arns),
            input_data=input_dict,
            region_name=aws_region,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token,
            wait_for_completion=wait_for_completion,
            timeout_minutes=timeout_minutes,
        )

        # Check if any executions failed
        failed_executions = [
            result for result in results if "FAILED" in result or "ERROR" in result
        ]
        if failed_executions:
            sys.exit(1)

    except Exception as e:
        click.echo(f"❌ Step Functions execution failed: {str(e)}")
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
def aws_stepfunctions_list(
    aws_access_key_id: Optional[str],
    aws_secret_access_key: Optional[str],
    aws_session_token: Optional[str],
    aws_region: Optional[str],
) -> None:
    """
    List all AWS Step Functions state machines with their status.

    AWS credentials are read from environment variables or AWS credential chain:
    - AWS_ACCESS_KEY_ID
    - AWS_SECRET_ACCESS_KEY
    - AWS_SESSION_TOKEN (optional)
    - AWS_REGION

    Example:
        paradime run aws-stepfunctions-list
    """
    click.echo("Listing Step Functions state machines...")

    try:
        list_stepfunctions_state_machines(
            region_name=aws_region,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token,
        )
    except Exception as e:
        click.echo(f"❌ Failed to list Step Functions state machines: {str(e)}")
        raise click.Abort()
