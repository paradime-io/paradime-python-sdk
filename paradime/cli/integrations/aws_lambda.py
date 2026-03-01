import sys
from typing import Optional

import click

from paradime.cli.utils import env_click_option
from paradime.core.scripts.aws_lambda import list_lambda_functions, trigger_lambda_functions


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
    "--function-names",
    multiple=True,
    help="The name(s) or ARN(s) of the Lambda function(s) to invoke",
    required=True,
)
@click.option(
    "--payload",
    type=str,
    help="JSON payload to pass to the Lambda functions (optional)",
    required=False,
)
@click.option(
    "--invocation-type",
    type=click.Choice(["RequestResponse", "Event", "DryRun"], case_sensitive=False),
    help="Lambda invocation type: RequestResponse (sync), Event (async), or DryRun",
    default="RequestResponse",
)
@click.option(
    "--wait-for-completion/--no-wait-for-completion",
    help="Wait for async invocations to complete before returning (only applies to Event invocation type)",
    default=True,
)
@click.option(
    "--timeout-minutes",
    type=int,
    help="Maximum time to wait for completion (in minutes). Only used with --wait-for-completion.",
    default=15,
)
def aws_lambda_trigger(
    aws_access_key_id: Optional[str],
    aws_secret_access_key: Optional[str],
    aws_session_token: Optional[str],
    aws_region: Optional[str],
    function_names: tuple,
    payload: Optional[str],
    invocation_type: str,
    wait_for_completion: bool,
    timeout_minutes: int,
) -> None:
    """
    Trigger one or more AWS Lambda functions.

    AWS credentials are read from environment variables or AWS credential chain:
    - AWS_ACCESS_KEY_ID
    - AWS_SECRET_ACCESS_KEY
    - AWS_SESSION_TOKEN (optional)
    - AWS_REGION

    Example:
        paradime run aws-lambda-trigger --function-names my-function --invocation-type RequestResponse
        paradime run aws-lambda-trigger --function-names func1 --function-names func2 --payload '{"key":"value"}'
    """
    import json

    click.echo(f"Invoking {len(function_names)} Lambda function(s)...")

    # Parse payload if provided
    payload_dict = None
    if payload:
        try:
            payload_dict = json.loads(payload)
        except json.JSONDecodeError as e:
            click.echo(f"❌ Invalid JSON payload: {str(e)}")
            raise click.Abort()

    try:
        results = trigger_lambda_functions(
            function_names=list(function_names),
            payload=payload_dict,
            invocation_type=invocation_type,
            region_name=aws_region,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token,
            wait_for_completion=wait_for_completion,
            timeout_minutes=timeout_minutes,
        )

        # Check if any invocations failed
        failed_functions = [result for result in results if "FAILED" in result or "ERROR" in result]
        if failed_functions:
            sys.exit(1)

    except Exception as e:
        click.echo(f"❌ Lambda invocation failed: {str(e)}")
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
def aws_lambda_list(
    aws_access_key_id: Optional[str],
    aws_secret_access_key: Optional[str],
    aws_session_token: Optional[str],
    aws_region: Optional[str],
) -> None:
    """
    List all AWS Lambda functions with their status.

    AWS credentials are read from environment variables or AWS credential chain:
    - AWS_ACCESS_KEY_ID
    - AWS_SECRET_ACCESS_KEY
    - AWS_SESSION_TOKEN (optional)
    - AWS_REGION

    Example:
        paradime run aws-lambda-list
    """
    click.echo("Listing Lambda functions...")

    try:
        list_lambda_functions(
            region_name=aws_region,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token,
        )
    except Exception as e:
        click.echo(f"❌ Failed to list Lambda functions: {str(e)}")
        raise click.Abort()
