import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any, List, Optional

import boto3  # type: ignore[import-untyped]
from botocore.exceptions import ClientError  # type: ignore[import-untyped]

from paradime.cli import console


def trigger_sagemaker_pipelines(
    *,
    pipeline_names: List[str],
    region_name: Optional[str] = None,
    aws_access_key_id: Optional[str] = None,
    aws_secret_access_key: Optional[str] = None,
    aws_session_token: Optional[str] = None,
    wait_for_completion: bool = True,
    timeout_minutes: int = 1440,
) -> List[str]:
    """
    Trigger multiple SageMaker Pipelines.

    Args:
        pipeline_names: List of SageMaker Pipeline names to start
        region_name: AWS region name (defaults to AWS_DEFAULT_REGION or us-east-1)
        aws_access_key_id: AWS access key ID (defaults to AWS_ACCESS_KEY_ID env var)
        aws_secret_access_key: AWS secret access key (defaults to AWS_SECRET_ACCESS_KEY env var)
        aws_session_token: AWS session token for temporary credentials (optional)
        wait_for_completion: Whether to wait for pipeline executions to complete
        timeout_minutes: Maximum time to wait for completion

    Returns:
        List of execution result messages for each pipeline
    """
    futures = []
    results = []

    with ThreadPoolExecutor() as executor:
        for i, pipeline_name in enumerate(set(pipeline_names), 1):
            futures.append(
                (
                    pipeline_name,
                    executor.submit(
                        trigger_sagemaker_pipeline,
                        pipeline_name=pipeline_name,
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
        pipeline_results = []
        for pipeline_name, future in futures:
            # Use longer timeout when waiting for completion
            future_timeout = (timeout_minutes * 60 + 120) if wait_for_completion else 120
            response_txt = future.result(timeout=future_timeout)
            pipeline_results.append((pipeline_name, response_txt))
            results.append(response_txt)

        def _status_text(response_txt: str) -> str:
            if "SUCCESS" in response_txt or "SUCCEEDED" in response_txt:
                return "SUCCESS"
            elif "FAILED" in response_txt:
                return "FAILED"
            elif "STOPPED" in response_txt:
                return "STOPPED"
            elif "STOPPING" in response_txt:
                return "STOPPING"
            else:
                return "COMPLETED"

        console.table(
            columns=["Pipeline Name", "Status"],
            rows=[(pn, _status_text(response_txt)) for pn, response_txt in pipeline_results],
            title="Pipeline Execution Results",
        )

    return results


def trigger_sagemaker_pipeline(
    *,
    pipeline_name: str,
    region_name: Optional[str] = None,
    aws_access_key_id: Optional[str] = None,
    aws_secret_access_key: Optional[str] = None,
    aws_session_token: Optional[str] = None,
    wait_for_completion: bool = True,
    timeout_minutes: int = 1440,
) -> str:
    """
    Trigger a single SageMaker Pipeline execution.

    Args:
        pipeline_name: SageMaker Pipeline name
        region_name: AWS region name
        aws_access_key_id: AWS access key ID
        aws_secret_access_key: AWS secret access key
        aws_session_token: AWS session token for temporary credentials
        wait_for_completion: Whether to wait for pipeline execution to complete
        timeout_minutes: Maximum time to wait for completion

    Returns:
        Status message indicating execution result
    """

    # Create SageMaker client
    session_kwargs = {}
    if region_name:
        session_kwargs["region_name"] = region_name
    if aws_access_key_id:
        session_kwargs["aws_access_key_id"] = aws_access_key_id
    if aws_secret_access_key:
        session_kwargs["aws_secret_access_key"] = aws_secret_access_key
    if aws_session_token:
        session_kwargs["aws_session_token"] = aws_session_token

    sagemaker_client = boto3.client("sagemaker", **session_kwargs)

    console.debug(f"[{pipeline_name}] Starting pipeline execution...")

    try:
        # Start the pipeline execution
        response = sagemaker_client.start_pipeline_execution(PipelineName=pipeline_name)

        pipeline_execution_arn = response.get("PipelineExecutionArn")
        execution_display_name = response.get("PipelineExecutionDisplayName", "Unknown")

        console.debug(f"[{pipeline_name}] Pipeline execution started: {execution_display_name}")
        console.debug(f"[{pipeline_name}] Execution ARN: {pipeline_execution_arn}")

        if not wait_for_completion:
            return f"Pipeline execution started (ARN: {pipeline_execution_arn})"

        console.debug(f"[{pipeline_name}] Monitoring pipeline execution...")

        # Wait for pipeline execution completion
        execution_status = _wait_for_pipeline_completion(
            sagemaker_client=sagemaker_client,
            pipeline_execution_arn=pipeline_execution_arn,
            pipeline_name=pipeline_name,
            timeout_minutes=timeout_minutes,
        )

        return f"Pipeline execution completed. Final status: {execution_status}"

    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code", "Unknown")
        error_message = e.response.get("Error", {}).get("Message", str(e))

        console.error(f"[{pipeline_name}] AWS Error: {error_code}")
        console.debug(f"[{pipeline_name}] Error message: {error_message}")

        return f"ERROR ({error_code}: {error_message})"

    except Exception as e:
        console.error(f"[{pipeline_name}] Unexpected error: {str(e)}")
        return f"ERROR ({str(e)[:100]})"


def _wait_for_pipeline_completion(
    *,
    sagemaker_client: Any,
    pipeline_execution_arn: str,
    pipeline_name: str,
    timeout_minutes: int,
) -> str:
    """
    Poll pipeline execution status until completion or timeout.

    Args:
        sagemaker_client: Boto3 SageMaker client
        pipeline_execution_arn: Pipeline execution ARN
        pipeline_name: Pipeline name for logging
        timeout_minutes: Maximum time to wait for completion

    Returns:
        Final execution status
    """
    start_time = time.time()
    timeout_seconds = timeout_minutes * 60
    sleep_interval = 30  # Poll every 30 seconds
    counter = 0

    while True:
        elapsed = time.time() - start_time
        if elapsed > timeout_seconds:
            raise Exception(
                f"Timeout waiting for pipeline '{pipeline_name}' execution to complete after {timeout_minutes} minutes"
            )

        try:
            # Get pipeline execution details
            response = sagemaker_client.describe_pipeline_execution(
                PipelineExecutionArn=pipeline_execution_arn
            )

            execution_status = response.get("PipelineExecutionStatus", "Unknown")
            failure_reason = response.get("FailureReason")

            # Log progress every 2 checks (1 minute)
            if counter == 0 or counter % 2 == 0:

                elapsed_min = int(elapsed // 60)
                elapsed_sec = int(elapsed % 60)

                if execution_status == "Executing":
                    console.debug(
                        f"[{pipeline_name}] Executing... ({elapsed_min}m {elapsed_sec}s elapsed)"
                    )
                elif execution_status == "Stopping":
                    console.debug(
                        f"[{pipeline_name}] Stopping... ({elapsed_min}m {elapsed_sec}s elapsed)"
                    )

            # Check if execution is complete
            if execution_status in ["Succeeded", "Failed", "Stopped"]:

                elapsed_min = int(elapsed // 60)
                elapsed_sec = int(elapsed % 60)

                if execution_status == "Succeeded":
                    console.debug(
                        f"[{pipeline_name}] Pipeline completed successfully ({elapsed_min}m {elapsed_sec}s)"
                    )
                    return f"SUCCEEDED (ARN: {pipeline_execution_arn})"
                elif execution_status == "Failed":
                    console.error(f"[{pipeline_name}] Pipeline execution failed")
                    if failure_reason:
                        console.debug(f"[{pipeline_name}] Failure reason: {failure_reason}")
                    return f"FAILED (Reason: {failure_reason or 'Unknown'})"
                elif execution_status == "Stopped":
                    console.debug(f"[{pipeline_name}] Pipeline execution stopped")
                    return f"STOPPED (ARN: {pipeline_execution_arn})"

            elif execution_status in ["Executing", "Stopping"]:
                # Still running, continue waiting
                pass
            else:
                # Unknown state, continue waiting
                pass

            counter += 1
            time.sleep(sleep_interval)

        except ClientError as e:

            error_message = e.response.get("Error", {}).get("Message", str(e))
            console.debug(f"[{pipeline_name}] Error checking status: {error_message}. Retrying...")
            time.sleep(sleep_interval)
            continue


def list_sagemaker_pipelines(
    *,
    region_name: Optional[str] = None,
    aws_access_key_id: Optional[str] = None,
    aws_secret_access_key: Optional[str] = None,
    aws_session_token: Optional[str] = None,
) -> None:
    """
    List all SageMaker Pipelines in the account.

    Args:
        region_name: AWS region name (defaults to AWS_DEFAULT_REGION or us-east-1)
        aws_access_key_id: AWS access key ID (defaults to AWS_ACCESS_KEY_ID env var)
        aws_secret_access_key: AWS secret access key (defaults to AWS_SECRET_ACCESS_KEY env var)
        aws_session_token: AWS session token for temporary credentials (optional)
    """
    # Create SageMaker client
    session_kwargs = {}
    if region_name:
        session_kwargs["region_name"] = region_name
    if aws_access_key_id:
        session_kwargs["aws_access_key_id"] = aws_access_key_id
    if aws_secret_access_key:
        session_kwargs["aws_secret_access_key"] = aws_secret_access_key
    if aws_session_token:
        session_kwargs["aws_session_token"] = aws_session_token

    sagemaker_client = boto3.client("sagemaker", **session_kwargs)

    console.info(f"Listing SageMaker Pipelines in region: {sagemaker_client.meta.region_name}")

    try:
        # List all pipelines (with pagination support)
        pipelines = []
        paginator = sagemaker_client.get_paginator("list_pipelines")

        for page in paginator.paginate():
            pipelines.extend(page["PipelineSummaries"])

        rows = []
        for pipeline in pipelines:
            pipeline_name = pipeline.get("PipelineName", "Unknown")
            pipeline_display_name = pipeline.get("PipelineDisplayName", pipeline_name)
            creation_time = str(pipeline.get("CreationTime", "Unknown"))
            last_modified_time = str(pipeline.get("LastModifiedTime", "Unknown"))
            rows.append((pipeline_name, pipeline_display_name, creation_time, last_modified_time))

        console.table(
            columns=["Pipeline Name", "Display Name", "Created", "Last Modified"],
            rows=rows,
            title="SageMaker Pipelines",
        )

    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code", "Unknown")
        error_message = e.response.get("Error", {}).get("Message", str(e))
        console.error(f"Error listing SageMaker Pipelines: {error_code} - {error_message}")
        raise
