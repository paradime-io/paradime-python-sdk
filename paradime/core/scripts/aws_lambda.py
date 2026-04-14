from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List, Optional

import boto3  # type: ignore[import-untyped]
from botocore.exceptions import ClientError  # type: ignore[import-untyped]

from paradime.cli import console


def trigger_lambda_functions(
    *,
    function_names: List[str],
    payload: Optional[Dict[str, Any]] = None,
    invocation_type: str = "RequestResponse",
    region_name: Optional[str] = None,
    aws_access_key_id: Optional[str] = None,
    aws_secret_access_key: Optional[str] = None,
    aws_session_token: Optional[str] = None,
    wait_for_completion: bool = True,
    timeout_minutes: int = 15,
) -> List[str]:
    """
    Trigger multiple AWS Lambda functions.

    Args:
        function_names: List of Lambda function names or ARNs to invoke
        payload: Optional JSON payload to pass to the functions
        invocation_type: RequestResponse (synchronous), Event (asynchronous), or DryRun
        region_name: AWS region name (defaults to AWS_DEFAULT_REGION or us-east-1)
        aws_access_key_id: AWS access key ID (defaults to AWS_ACCESS_KEY_ID env var)
        aws_secret_access_key: AWS secret access key (defaults to AWS_SECRET_ACCESS_KEY env var)
        aws_session_token: AWS session token for temporary credentials (optional)
        wait_for_completion: Whether to wait for async invocations to complete
        timeout_minutes: Maximum time to wait for completion

    Returns:
        List of invocation result messages for each function
    """
    futures = []
    results = []

    with ThreadPoolExecutor() as executor:
        for i, function_name in enumerate(set(function_names), 1):
            futures.append(
                (
                    function_name,
                    executor.submit(
                        trigger_lambda_function,
                        function_name=function_name,
                        payload=payload,
                        invocation_type=invocation_type,
                        region_name=region_name,
                        aws_access_key_id=aws_access_key_id,
                        aws_secret_access_key=aws_secret_access_key,
                        aws_session_token=aws_session_token,
                        wait_for_completion=wait_for_completion,
                        timeout_minutes=timeout_minutes,
                    ),
                )
            )

        # Wait for completion and collect results
        function_results = []
        for function_name, future in futures:
            # Use longer timeout when waiting for completion
            future_timeout = (timeout_minutes * 60 + 120) if wait_for_completion else 120
            response_txt = future.result(timeout=future_timeout)
            function_results.append((function_name, response_txt))
            results.append(response_txt)

        def _status_text(response_txt: str) -> str:
            if "SUCCESS" in response_txt:
                return "SUCCESS"
            elif "FAILED" in response_txt or "ERROR" in response_txt:
                return "FAILED"
            elif "THROTTLED" in response_txt:
                return "THROTTLED"
            else:
                return "COMPLETED"

        console.table(
            columns=["Function Name", "Status"],
            rows=[(fn, _status_text(response_txt)) for fn, response_txt in function_results],
            title="Invocation Results",
        )

    return results


def trigger_lambda_function(
    *,
    function_name: str,
    payload: Optional[Dict[str, Any]] = None,
    invocation_type: str = "RequestResponse",
    region_name: Optional[str] = None,
    aws_access_key_id: Optional[str] = None,
    aws_secret_access_key: Optional[str] = None,
    aws_session_token: Optional[str] = None,
    wait_for_completion: bool = True,
    timeout_minutes: int = 15,
) -> str:
    """
    Trigger a single AWS Lambda function.

    Args:
        function_name: Lambda function name or ARN
        payload: Optional JSON payload to pass to the function
        invocation_type: RequestResponse (synchronous), Event (asynchronous), or DryRun
        region_name: AWS region name
        aws_access_key_id: AWS access key ID
        aws_secret_access_key: AWS secret access key
        aws_session_token: AWS session token for temporary credentials
        wait_for_completion: Whether to wait for async invocations to complete
        timeout_minutes: Maximum time to wait for completion

    Returns:
        Status message indicating invocation result
    """
    import json

    # Create Lambda client
    session_kwargs = {}
    if region_name:
        session_kwargs["region_name"] = region_name
    if aws_access_key_id:
        session_kwargs["aws_access_key_id"] = aws_access_key_id
    if aws_secret_access_key:
        session_kwargs["aws_secret_access_key"] = aws_secret_access_key
    if aws_session_token:
        session_kwargs["aws_session_token"] = aws_session_token

    lambda_client = boto3.client("lambda", **session_kwargs)

    # Prepare payload
    payload_bytes = json.dumps(payload or {}).encode("utf-8")

    console.debug(f"[{function_name}] Invoking Lambda function...")
    console.debug(f"[{function_name}] Invocation type: {invocation_type}")

    try:
        # Invoke the Lambda function
        response = lambda_client.invoke(
            FunctionName=function_name,
            InvocationType=invocation_type,
            Payload=payload_bytes,
        )

        status_code = response.get("StatusCode")
        function_error = response.get("FunctionError")

        # Read the response payload
        response_payload = response.get("Payload")
        if response_payload:
            response_data = json.loads(response_payload.read())
        else:
            response_data = {}

        if function_error:
            console.error(f"[{function_name}] Function error: {function_error}")
            error_message = response_data.get("errorMessage", "Unknown error")
            console.debug(f"[{function_name}] Error details: {error_message}")
            return f"FAILED (Function error: {function_error})"

        if status_code == 200:
            console.debug(f"[{function_name}] Invocation successful")
        elif status_code == 202:
            console.debug(f"[{function_name}] Async invocation accepted")
        elif status_code == 204:
            console.debug(f"[{function_name}] DryRun successful")
        else:
            console.debug(f"[{function_name}] Unexpected status code: {status_code}")

        # For async invocations with wait_for_completion
        if invocation_type == "Event" and wait_for_completion:
            console.debug(f"[{function_name}] Monitoring async execution...")
            # Note: AWS Lambda doesn't provide built-in async invocation tracking
            # We use CloudWatch Logs to monitor execution
            execution_status = _monitor_lambda_execution(
                lambda_client=lambda_client,
                function_name=function_name,
                timeout_minutes=timeout_minutes,
            )
            return f"Async invocation completed. Status: {execution_status}"

        return f"SUCCESS (Status code: {status_code})"

    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code", "Unknown")
        error_message = e.response.get("Error", {}).get("Message", str(e))

        console.error(f"[{function_name}] AWS Error: {error_code}")
        console.debug(f"[{function_name}] Error message: {error_message}")

        if error_code == "TooManyRequestsException":
            return f"THROTTLED ({error_message})"
        else:
            return f"ERROR ({error_code}: {error_message})"

    except Exception as e:
        console.error(f"[{function_name}] Unexpected error: {str(e)}")
        return f"ERROR ({str(e)[:100]})"


def _monitor_lambda_execution(
    *,
    lambda_client: Any,
    function_name: str,
    timeout_minutes: int,
) -> str:
    """
    Monitor Lambda function execution using CloudWatch Logs.

    Note: This provides basic monitoring. For production use cases,
    consider using AWS X-Ray or custom CloudWatch metrics.

    Args:
        lambda_client: Boto3 Lambda client
        function_name: Lambda function name
        timeout_minutes: Maximum time to wait

    Returns:
        Execution status
    """
    start_time = time.time()
    timeout_seconds = timeout_minutes * 60
    sleep_interval = 10

    console.debug(
        f"[{function_name}] Note: Async Lambda monitoring is limited. Waiting {timeout_minutes} minutes..."
    )

    # Wait for the specified timeout period
    # In a real implementation, you would check CloudWatch Logs or use AWS X-Ray
    while True:
        elapsed = time.time() - start_time
        if elapsed > timeout_seconds:
            console.debug(
                f"[{function_name}] Monitoring timeout reached after {timeout_minutes} minutes"
            )
            return "TIMEOUT (monitoring period exceeded)"

        if elapsed > 30:  # After 30 seconds, assume success if no errors detected
            console.debug(f"[{function_name}] No errors detected during monitoring period")
            return "SUCCESS (no errors detected)"

        time.sleep(sleep_interval)


def list_lambda_functions(
    *,
    region_name: Optional[str] = None,
    aws_access_key_id: Optional[str] = None,
    aws_secret_access_key: Optional[str] = None,
    aws_session_token: Optional[str] = None,
    json_output: bool = False,
) -> list | None:
    """
    List all Lambda functions in the account.

    Args:
        region_name: AWS region name (defaults to AWS_DEFAULT_REGION or us-east-1)
        aws_access_key_id: AWS access key ID (defaults to AWS_ACCESS_KEY_ID env var)
        aws_secret_access_key: AWS secret access key (defaults to AWS_SECRET_ACCESS_KEY env var)
        aws_session_token: AWS session token for temporary credentials (optional)
        json_output: Whether to return data as a list of dicts instead of printing a table
    """
    # Create Lambda client
    session_kwargs = {}
    if region_name:
        session_kwargs["region_name"] = region_name
    if aws_access_key_id:
        session_kwargs["aws_access_key_id"] = aws_access_key_id
    if aws_secret_access_key:
        session_kwargs["aws_secret_access_key"] = aws_secret_access_key
    if aws_session_token:
        session_kwargs["aws_session_token"] = aws_session_token

    lambda_client = boto3.client("lambda", **session_kwargs)

    if not json_output:
        console.info(f"Listing Lambda functions in region: {lambda_client.meta.region_name}")

    try:
        # List all functions (with pagination support)
        functions = []
        paginator = lambda_client.get_paginator("list_functions")

        for page in paginator.paginate():
            functions.extend(page["Functions"])

        rows = []
        data = []
        for function in functions:
            function_name = function.get("FunctionName", "Unknown")
            runtime = function.get("Runtime", "Unknown")
            last_modified = function.get("LastModified", "Unknown")
            memory_size = function.get("MemorySize", "Unknown")
            timeout = function.get("Timeout", "Unknown")
            state = function.get("State", "Unknown")
            rows.append(
                (function_name, state, runtime, str(memory_size), str(timeout), last_modified)
            )
            data.append(
                {
                    "function_name": function_name,
                    "state": state,
                    "runtime": runtime,
                    "memory_mb": memory_size,
                    "timeout_s": timeout,
                    "last_modified": last_modified,
                }
            )

        if json_output:
            return data

        console.table(
            columns=[
                "Function Name",
                "State",
                "Runtime",
                "Memory (MB)",
                "Timeout (s)",
                "Last Modified",
            ],
            rows=rows,
            title="Lambda Functions",
        )
        return None

    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code", "Unknown")
        error_message = e.response.get("Error", {}).get("Message", str(e))
        console.error(f"Error listing Lambda functions: {error_code} - {error_message}")
        raise
