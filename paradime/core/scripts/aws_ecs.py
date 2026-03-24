from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any, List, Optional

import boto3  # type: ignore[import-untyped]
from botocore.exceptions import ClientError  # type: ignore[import-untyped]

from paradime.cli import console


def _create_ecs_client(
    *,
    region_name: Optional[str] = None,
    aws_access_key_id: Optional[str] = None,
    aws_secret_access_key: Optional[str] = None,
    aws_session_token: Optional[str] = None,
) -> Any:
    """Create a boto3 ECS client with the given credentials."""
    session_kwargs = {}
    if region_name:
        session_kwargs["region_name"] = region_name
    if aws_access_key_id:
        session_kwargs["aws_access_key_id"] = aws_access_key_id
    if aws_secret_access_key:
        session_kwargs["aws_secret_access_key"] = aws_secret_access_key
    if aws_session_token:
        session_kwargs["aws_session_token"] = aws_session_token

    return boto3.client("ecs", **session_kwargs)


def trigger_ecs_tasks(
    *,
    cluster: str,
    task_definitions: List[str],
    launch_type: str = "FARGATE",
    subnets: Optional[List[str]] = None,
    security_groups: Optional[List[str]] = None,
    assign_public_ip: bool = False,
    wait: bool = True,
    timeout_minutes: int = 1440,
    region_name: Optional[str] = None,
    aws_access_key_id: Optional[str] = None,
    aws_secret_access_key: Optional[str] = None,
    aws_session_token: Optional[str] = None,
) -> List[str]:
    """
    Run one or more ECS tasks and optionally wait for completion.

    Args:
        cluster: ECS cluster name or ARN
        task_definitions: List of task definition names or ARNs to run
        launch_type: FARGATE or EC2
        subnets: Subnet IDs for awsvpc network mode
        security_groups: Security group IDs for awsvpc network mode
        assign_public_ip: Whether to assign a public IP (Fargate only)
        wait: Whether to wait for tasks to complete
        timeout_minutes: Maximum time to wait in minutes
        region_name: AWS region name
        aws_access_key_id: AWS access key ID
        aws_secret_access_key: AWS secret access key
        aws_session_token: AWS session token for temporary credentials

    Returns:
        List of status keywords: SUCCESS, FAILED, STOPPED
    """
    ecs_client = _create_ecs_client(
        region_name=region_name,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        aws_session_token=aws_session_token,
    )

    # Build network configuration if subnets are provided
    network_configuration = None
    if subnets:
        awsvpc_config: dict[str, Any] = {"subnets": subnets}
        if security_groups:
            awsvpc_config["securityGroups"] = security_groups
        awsvpc_config["assignPublicIp"] = "ENABLED" if assign_public_ip else "DISABLED"
        network_configuration = {"awsvpcConfiguration": awsvpc_config}

    futures = []
    with ThreadPoolExecutor() as executor:
        for task_def in set(task_definitions):
            futures.append(
                (
                    task_def,
                    executor.submit(
                        _run_single_task,
                        ecs_client=ecs_client,
                        cluster=cluster,
                        task_definition=task_def,
                        launch_type=launch_type,
                        network_configuration=network_configuration,
                        wait=wait,
                        timeout_minutes=timeout_minutes,
                    ),
                )
            )

        task_results = []
        for task_def, future in futures:
            future_timeout = (timeout_minutes * 60 + 120) if wait else 120
            status, task_arn = future.result(timeout=future_timeout)
            task_results.append((task_def, status, task_arn))

    console.table(
        columns=["Task Definition", "Status", "Task ARN"],
        rows=[(td, status, arn) for td, status, arn in task_results],
        title="ECS Task Results",
    )

    return [status for _, status, _ in task_results]


def _run_single_task(
    *,
    ecs_client: Any,
    cluster: str,
    task_definition: str,
    launch_type: str,
    network_configuration: Optional[dict] = None,
    wait: bool = True,
    timeout_minutes: int = 1440,
) -> tuple[str, str]:
    """
    Run a single ECS task and optionally poll until completion.

    Returns:
        Tuple of (status_keyword, task_arn)
    """
    console.debug(f"[{task_definition}] Running task on cluster {cluster}...")

    try:
        run_kwargs: dict[str, Any] = {
            "cluster": cluster,
            "taskDefinition": task_definition,
            "launchType": launch_type.upper(),
            "count": 1,
        }
        if network_configuration:
            run_kwargs["networkConfiguration"] = network_configuration

        response = ecs_client.run_task(**run_kwargs)

        # Check for failures
        failures = response.get("failures", [])
        if failures:
            reason = failures[0].get("reason", "Unknown")
            console.error(f"[{task_definition}] Failed to start task: {reason}")
            return "FAILED", "N/A"

        tasks = response.get("tasks", [])
        if not tasks:
            console.error(f"[{task_definition}] No task returned from run_task")
            return "FAILED", "N/A"

        task_arn = tasks[0]["taskArn"]
        console.debug(f"[{task_definition}] Task started: {task_arn}")

        if not wait:
            console.debug(f"[{task_definition}] Not waiting for completion")
            return "STARTED", task_arn

        # Poll for completion
        return _poll_task(
            ecs_client=ecs_client,
            cluster=cluster,
            task_arn=task_arn,
            task_definition=task_definition,
            timeout_minutes=timeout_minutes,
        )

    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code", "Unknown")
        error_message = e.response.get("Error", {}).get("Message", str(e))
        console.error(f"[{task_definition}] AWS Error: {error_code} - {error_message}")
        return "FAILED", "N/A"

    except Exception as e:
        console.error(f"[{task_definition}] Unexpected error: {str(e)}")
        return "FAILED", "N/A"


def _poll_task(
    *,
    ecs_client: Any,
    cluster: str,
    task_arn: str,
    task_definition: str,
    timeout_minutes: int,
) -> tuple[str, str]:
    """
    Poll an ECS task until it reaches a terminal state or times out.

    Returns:
        Tuple of (status_keyword, task_arn)
    """
    start_time = time.time()
    timeout_seconds = timeout_minutes * 60
    sleep_interval = 10

    while True:
        elapsed = time.time() - start_time
        if elapsed > timeout_seconds:
            console.error(f"[{task_definition}] Timeout after {timeout_minutes} minutes")
            return "FAILED", task_arn

        try:
            response = ecs_client.describe_tasks(cluster=cluster, tasks=[task_arn])
            tasks = response.get("tasks", [])
            if not tasks:
                console.error(f"[{task_definition}] Task not found: {task_arn}")
                return "FAILED", task_arn

            task = tasks[0]
            last_status = task.get("lastStatus", "UNKNOWN")
            console.debug(f"[{task_definition}] Status: {last_status}")

            if last_status == "STOPPED":
                # Check container exit codes
                containers = task.get("containers", [])
                stopped_reason = task.get("stoppedReason", "")

                if stopped_reason:
                    console.debug(f"[{task_definition}] Stopped reason: {stopped_reason}")

                # A task is successful if all containers exited with code 0
                all_success = all(c.get("exitCode") == 0 for c in containers if "exitCode" in c)

                if all_success and containers:
                    console.debug(f"[{task_definition}] Task completed successfully")
                    return "SUCCESS", task_arn
                else:
                    exit_codes = [
                        f"{c.get('name', '?')}={c.get('exitCode', '?')}" for c in containers
                    ]
                    console.error(
                        f"[{task_definition}] Task failed. Exit codes: {', '.join(exit_codes)}. "
                        f"Reason: {stopped_reason}"
                    )
                    return "FAILED", task_arn

        except ClientError as e:
            console.error(f"[{task_definition}] Error polling task: {e}")
            return "FAILED", task_arn

        time.sleep(sleep_interval)


def list_ecs_task_definitions(
    *,
    region_name: Optional[str] = None,
    aws_access_key_id: Optional[str] = None,
    aws_secret_access_key: Optional[str] = None,
    aws_session_token: Optional[str] = None,
    json_output: bool = False,
) -> list | None:
    """
    List all active ECS task definitions.

    Args:
        region_name: AWS region name
        aws_access_key_id: AWS access key ID
        aws_secret_access_key: AWS secret access key
        aws_session_token: AWS session token for temporary credentials
        json_output: Whether to return data as a list of dicts instead of printing a table

    Returns:
        List of dicts when json_output is True, None otherwise.
    """
    ecs_client = _create_ecs_client(
        region_name=region_name,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        aws_session_token=aws_session_token,
    )

    if not json_output:
        console.info(f"Listing ECS task definitions in region: {ecs_client.meta.region_name}")

    try:
        # List all active task definition ARNs (with pagination)
        task_def_arns: list[str] = []
        paginator = ecs_client.get_paginator("list_task_definitions")
        for page in paginator.paginate(status="ACTIVE"):
            task_def_arns.extend(page.get("taskDefinitionArns", []))

        rows = []
        data = []
        for arn in task_def_arns:
            # ARN format: arn:aws:ecs:region:account:task-definition/name:revision
            parts = arn.split("/")[-1] if "/" in arn else arn
            if ":" in parts:
                name, revision = parts.rsplit(":", 1)
            else:
                name = parts
                revision = "latest"

            rows.append((name, revision, "ACTIVE"))
            data.append(
                {
                    "task_definition": name,
                    "revision": revision,
                    "status": "ACTIVE",
                    "arn": arn,
                }
            )

        if json_output:
            return data

        console.table(
            columns=["Task Definition", "Revision", "Status"],
            rows=rows,
            title="ECS Task Definitions",
        )
        return None

    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code", "Unknown")
        error_message = e.response.get("Error", {}).get("Message", str(e))
        console.error(f"Error listing ECS task definitions: {error_code} - {error_message}")
        raise
