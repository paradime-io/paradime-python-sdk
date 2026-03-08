import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List, Optional

import boto3  # type: ignore[import-untyped]
from botocore.exceptions import ClientError  # type: ignore[import-untyped]

from paradime.cli import console


def trigger_stepfunctions_executions(
    *,
    state_machine_arns: List[str],
    input_data: Optional[Dict[str, Any]] = None,
    region_name: Optional[str] = None,
    aws_access_key_id: Optional[str] = None,
    aws_secret_access_key: Optional[str] = None,
    aws_session_token: Optional[str] = None,
    wait_for_completion: bool = True,
    timeout_minutes: int = 1440,
) -> List[str]:
    """
    Trigger multiple AWS Step Functions state machine executions.

    Args:
        state_machine_arns: List of Step Functions state machine ARNs to execute
        input_data: Optional JSON input data to pass to the state machines
        region_name: AWS region name (defaults to AWS_DEFAULT_REGION or us-east-1)
        aws_access_key_id: AWS access key ID (defaults to AWS_ACCESS_KEY_ID env var)
        aws_secret_access_key: AWS secret access key (defaults to AWS_SECRET_ACCESS_KEY env var)
        aws_session_token: AWS session token for temporary credentials (optional)
        wait_for_completion: Whether to wait for executions to complete
        timeout_minutes: Maximum time to wait for completion

    Returns:
        List of execution result messages for each state machine
    """
    futures = []
    results = []

    with ThreadPoolExecutor() as executor:
        for i, state_machine_arn in enumerate(set(state_machine_arns), 1):
            # Extract state machine name from ARN for display
            state_machine_name = state_machine_arn.split(":")[-1]

            futures.append(
                (
                    state_machine_name,
                    executor.submit(
                        trigger_stepfunction_execution,
                        state_machine_arn=state_machine_arn,
                        input_data=input_data,
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
        execution_results = []
        for state_machine_name, future in futures:
            # Use longer timeout when waiting for completion
            future_timeout = (timeout_minutes * 60 + 120) if wait_for_completion else 120
            response_txt = future.result(timeout=future_timeout)
            execution_results.append((state_machine_name, response_txt))
            results.append(response_txt)

        def _status_text(response_txt: str) -> str:
            if "SUCCESS" in response_txt or "SUCCEEDED" in response_txt:
                return "SUCCESS"
            elif "FAILED" in response_txt:
                return "FAILED"
            elif "TIMED_OUT" in response_txt:
                return "TIMED OUT"
            elif "ABORTED" in response_txt:
                return "ABORTED"
            else:
                return "COMPLETED"

        console.table(
            columns=["State Machine Name", "Status"],
            rows=[(smn, _status_text(response_txt)) for smn, response_txt in execution_results],
            title="Execution Results",
        )

    return results


def trigger_stepfunction_execution(
    *,
    state_machine_arn: str,
    input_data: Optional[Dict[str, Any]] = None,
    region_name: Optional[str] = None,
    aws_access_key_id: Optional[str] = None,
    aws_secret_access_key: Optional[str] = None,
    aws_session_token: Optional[str] = None,
    wait_for_completion: bool = True,
    timeout_minutes: int = 1440,
) -> str:
    """
    Trigger a single AWS Step Functions state machine execution.

    Args:
        state_machine_arn: Step Functions state machine ARN
        input_data: Optional JSON input data to pass to the state machine
        region_name: AWS region name
        aws_access_key_id: AWS access key ID
        aws_secret_access_key: AWS secret access key
        aws_session_token: AWS session token for temporary credentials
        wait_for_completion: Whether to wait for execution to complete
        timeout_minutes: Maximum time to wait for completion

    Returns:
        Status message indicating execution result
    """
    import json

    # Extract state machine name from ARN
    state_machine_name = state_machine_arn.split(":")[-1]

    # Create Step Functions client
    session_kwargs = {}
    if region_name:
        session_kwargs["region_name"] = region_name
    if aws_access_key_id:
        session_kwargs["aws_access_key_id"] = aws_access_key_id
    if aws_secret_access_key:
        session_kwargs["aws_secret_access_key"] = aws_secret_access_key
    if aws_session_token:
        session_kwargs["aws_session_token"] = aws_session_token

    sfn_client = boto3.client("stepfunctions", **session_kwargs)

    console.debug(f"[{state_machine_name}] Starting execution...")

    try:
        # Prepare input data
        input_json = json.dumps(input_data or {})

        # Start the execution
        response = sfn_client.start_execution(
            stateMachineArn=state_machine_arn,
            input=input_json,
        )

        execution_arn = response.get("executionArn")
        start_date = response.get("startDate")

        console.debug(f"[{state_machine_name}] Execution started")
        console.debug(f"[{state_machine_name}] Execution ARN: {execution_arn}")
        console.debug(f"[{state_machine_name}] Start time: {start_date}")

        if not wait_for_completion:
            return f"Execution started (ARN: {execution_arn})"

        console.debug(f"[{state_machine_name}] Monitoring execution...")

        # Wait for execution completion
        execution_status = _wait_for_execution_completion(
            sfn_client=sfn_client,
            execution_arn=execution_arn,
            state_machine_name=state_machine_name,
            timeout_minutes=timeout_minutes,
        )

        return f"Execution completed. Final status: {execution_status}"

    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code", "Unknown")
        error_message = e.response.get("Error", {}).get("Message", str(e))

        console.error(f"[{state_machine_name}] AWS Error: {error_code}")
        console.debug(f"[{state_machine_name}] Error message: {error_message}")

        return f"ERROR ({error_code}: {error_message})"

    except Exception as e:
        console.error(f"[{state_machine_name}] Unexpected error: {str(e)}")
        return f"ERROR ({str(e)[:100]})"


def _wait_for_execution_completion(
    *,
    sfn_client: Any,
    execution_arn: str,
    state_machine_name: str,
    timeout_minutes: int,
) -> str:
    """
    Poll execution status until completion or timeout.

    Args:
        sfn_client: Boto3 Step Functions client
        execution_arn: Execution ARN
        state_machine_name: State machine name for logging
        timeout_minutes: Maximum time to wait for completion

    Returns:
        Final execution status
    """
    start_time = time.time()
    timeout_seconds = timeout_minutes * 60
    sleep_interval = 10  # Poll every 10 seconds
    counter = 0

    while True:
        elapsed = time.time() - start_time
        if elapsed > timeout_seconds:
            raise Exception(
                f"Timeout waiting for state machine '{state_machine_name}' execution to complete after {timeout_minutes} minutes"
            )

        try:
            # Get execution details
            response = sfn_client.describe_execution(executionArn=execution_arn)

            execution_status = response.get("status", "Unknown")
            stop_date = response.get("stopDate")

            # Log progress every 6 checks (1 minute)
            if counter == 0 or counter % 6 == 0:

                elapsed_min = int(elapsed // 60)
                elapsed_sec = int(elapsed % 60)

                if execution_status == "RUNNING":
                    console.debug(
                        f"[{state_machine_name}] Running... ({elapsed_min}m {elapsed_sec}s elapsed)"
                    )

            # Check if execution is complete
            if execution_status in ["SUCCEEDED", "FAILED", "TIMED_OUT", "ABORTED"]:

                elapsed_min = int(elapsed // 60)
                elapsed_sec = int(elapsed % 60)

                if execution_status == "SUCCEEDED":
                    console.debug(
                        f"[{state_machine_name}] Execution completed successfully ({elapsed_min}m {elapsed_sec}s)"
                    )

                    # Get output if available
                    output = response.get("output")
                    if output:
                        console.debug(f"[{state_machine_name}] Output available")

                    return f"SUCCEEDED (Completed at {stop_date})"

                elif execution_status == "FAILED":
                    console.error(f"[{state_machine_name}] Execution failed")

                    # Get error details if available
                    error = response.get("error", "Unknown error")
                    cause = response.get("cause", "No cause provided")
                    console.debug(f"[{state_machine_name}] Error: {error}")
                    console.debug(f"[{state_machine_name}] Cause: {cause}")

                    return f"FAILED (Error: {error})"

                elif execution_status == "TIMED_OUT":
                    console.debug(f"[{state_machine_name}] Execution timed out")
                    return f"TIMED_OUT (Execution ARN: {execution_arn})"

                elif execution_status == "ABORTED":
                    console.debug(f"[{state_machine_name}] Execution aborted")
                    return f"ABORTED (Execution ARN: {execution_arn})"

            elif execution_status == "RUNNING":
                # Still running, continue waiting
                pass
            else:
                # Unknown state, continue waiting
                pass

            counter += 1
            time.sleep(sleep_interval)

        except ClientError as e:

            error_message = e.response.get("Error", {}).get("Message", str(e))
            console.debug(
                f"[{state_machine_name}] Error checking status: {error_message}. Retrying..."
            )
            time.sleep(sleep_interval)
            continue


def list_stepfunctions_state_machines(
    *,
    region_name: Optional[str] = None,
    aws_access_key_id: Optional[str] = None,
    aws_secret_access_key: Optional[str] = None,
    aws_session_token: Optional[str] = None,
) -> None:
    """
    List all Step Functions state machines in the account.

    Args:
        region_name: AWS region name (defaults to AWS_DEFAULT_REGION or us-east-1)
        aws_access_key_id: AWS access key ID (defaults to AWS_ACCESS_KEY_ID env var)
        aws_secret_access_key: AWS secret access key (defaults to AWS_SECRET_ACCESS_KEY env var)
        aws_session_token: AWS session token for temporary credentials (optional)
    """
    # Create Step Functions client
    session_kwargs = {}
    if region_name:
        session_kwargs["region_name"] = region_name
    if aws_access_key_id:
        session_kwargs["aws_access_key_id"] = aws_access_key_id
    if aws_secret_access_key:
        session_kwargs["aws_secret_access_key"] = aws_secret_access_key
    if aws_session_token:
        session_kwargs["aws_session_token"] = aws_session_token

    sfn_client = boto3.client("stepfunctions", **session_kwargs)

    console.info(f"Listing Step Functions state machines in region: {sfn_client.meta.region_name}")

    try:
        # List all state machines (with pagination support)
        state_machines = []
        paginator = sfn_client.get_paginator("list_state_machines")

        for page in paginator.paginate():
            state_machines.extend(page["stateMachines"])

        rows = []
        for state_machine in state_machines:
            state_machine_name = state_machine.get("name", "Unknown")
            state_machine_arn = state_machine.get("stateMachineArn", "Unknown")
            machine_type = state_machine.get("type", "Unknown")
            creation_date = str(state_machine.get("creationDate", "Unknown"))
            rows.append((state_machine_name, machine_type, creation_date, state_machine_arn))

        console.table(
            columns=["Name", "Type", "Created", "ARN"],
            rows=rows,
            title="Step Functions State Machines",
        )

    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code", "Unknown")
        error_message = e.response.get("Error", {}).get("Message", str(e))
        console.error(
            f"Error listing Step Functions state machines: {error_code} - {error_message}"
        )
        raise
