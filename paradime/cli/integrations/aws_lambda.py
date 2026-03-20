from __future__ import annotations

import sys
from typing import Optional

import click

from paradime.cli import console
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
    "--wait/--no-wait",
    help="Wait for async invocations to complete before returning (only applies to Event invocation type)",
    default=True,
)
@click.option(
    "--timeout",
    type=int,
    help="Maximum time to wait in minutes.",
    default=15,
)
@click.option("--json", "json_output", is_flag=True, help="Output results as JSON.", default=False)
def aws_lambda_trigger(
    aws_access_key_id: Optional[str],
    aws_secret_access_key: Optional[str],
    aws_session_token: Optional[str],
    aws_region: Optional[str],
    function_names: tuple,
    payload: Optional[str],
    invocation_type: str,
    wait: bool,
    timeout: int,
    json_output: bool,
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

    if not json_output:
        console.header("AWS Lambda — Invoke Functions")

    # Parse payload if provided
    payload_dict = None
    if payload:
        try:
            payload_dict = json.loads(payload)
        except json.JSONDecodeError as e:
            console.error(f"Invalid JSON payload: {e}", exit_code=1)

    try:
        results = trigger_lambda_functions(
            function_names=list(function_names),
            payload=payload_dict,
            invocation_type=invocation_type,
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

        # Check if any invocations failed
        failed_functions = [result for result in results if "FAILED" in result or "ERROR" in result]
        if failed_functions:
            console.error(f"{len(failed_functions)} function invocation(s) failed.")
            sys.exit(1)

    except Exception as e:
        if json_output:
            console.json_out({"error": str(e), "success": False})
            sys.exit(1)
        console.error(f"Lambda invocation failed: {e}", exit_code=1)


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
def aws_lambda_list(
    aws_access_key_id: Optional[str],
    aws_secret_access_key: Optional[str],
    aws_session_token: Optional[str],
    aws_region: Optional[str],
    json_output: bool,
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
    if not json_output:
        console.info("Listing Lambda functions…")

    try:
        result = list_lambda_functions(
            region_name=aws_region,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token,
            json_output=json_output,
        )
        if json_output and result is not None:
            console.json_out(result)
    except Exception as e:
        console.error(f"Failed to list Lambda functions: {e}", exit_code=1)
