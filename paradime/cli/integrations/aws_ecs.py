from __future__ import annotations

import sys
from typing import List, Optional

import click

from paradime.cli import console
from paradime.cli.utils import COMMA_LIST, env_click_option
from paradime.core.scripts.aws_ecs import list_ecs_task_definitions, trigger_ecs_tasks


@click.command(context_settings=dict(max_content_width=160))
@env_click_option(
    "aws-access-key-id",
    "AWS_ACCESS_KEY_ID",
    help="AWS Access Key ID for authentication.",
    required=False,
)
@env_click_option(
    "aws-secret-access-key",
    "AWS_SECRET_ACCESS_KEY",
    help="AWS Secret Access Key for authentication.",
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
    help="AWS region name (e.g., us-east-1).",
    required=False,
)
@click.option(
    "--cluster",
    help="ECS cluster name or ARN.",
    required=True,
)
@click.option(
    "--task-definitions",
    type=COMMA_LIST,
    help="Comma-separated task definition name(s) or ARN(s) to run",
    required=True,
)
@click.option(
    "--launch-type",
    type=click.Choice(["FARGATE", "EC2"], case_sensitive=False),
    help="Launch type for the task.",
    default="FARGATE",
)
@click.option(
    "--subnets",
    type=COMMA_LIST,
    help="Comma-separated subnet ID(s) for awsvpc network mode.",
    required=False,
)
@click.option(
    "--security-groups",
    type=COMMA_LIST,
    help="Comma-separated security group ID(s) for awsvpc network mode.",
    required=False,
)
@click.option(
    "--assign-public-ip/--no-assign-public-ip",
    default=False,
    help="Assign public IP to the task (Fargate only).",
)
@click.option(
    "--wait/--no-wait",
    default=True,
    help="Wait for tasks to complete before returning",
)
@click.option(
    "--timeout",
    type=int,
    help="Maximum time to wait in minutes.",
    default=1440,
)
@click.option("--json", "json_output", is_flag=True, help="Output results as JSON.", default=False)
def aws_ecs_trigger(
    aws_access_key_id: Optional[str],
    aws_secret_access_key: Optional[str],
    aws_session_token: Optional[str],
    aws_region: Optional[str],
    cluster: str,
    task_definitions: List[str],
    launch_type: str,
    subnets: Optional[List[str]],
    security_groups: Optional[List[str]],
    assign_public_ip: bool,
    wait: bool,
    timeout: int,
    json_output: bool,
) -> None:
    """
    Run one or more AWS ECS tasks.

    AWS credentials are read from environment variables or AWS credential chain:
    - AWS_ACCESS_KEY_ID
    - AWS_SECRET_ACCESS_KEY
    - AWS_SESSION_TOKEN (optional)
    - AWS_REGION

    Example:
        paradime run aws-ecs-trigger --cluster my-cluster --task-definitions my-task-def
        paradime run aws-ecs-trigger --cluster my-cluster --task-definitions task1,task2 --subnets subnet-abc
    """
    if not json_output:
        console.header("AWS ECS \u2014 Run Tasks")

    try:
        results = trigger_ecs_tasks(
            cluster=cluster,
            task_definitions=task_definitions,
            launch_type=launch_type,
            subnets=subnets,
            security_groups=security_groups,
            assign_public_ip=assign_public_ip,
            wait=wait,
            timeout_minutes=timeout,
            region_name=aws_region,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token,
        )

        if json_output:
            failed = [r for r in results if r == "FAILED"]
            console.json_out(
                {"results": results, "failed_count": len(failed), "success": len(failed) == 0}
            )
            if failed:
                sys.exit(1)
            return

        failed_tasks = [r for r in results if r == "FAILED"]
        if failed_tasks:
            console.error(f"{len(failed_tasks)} task(s) failed.")
            sys.exit(1)

    except Exception as e:
        if json_output:
            console.json_out({"error": str(e), "success": False})
            sys.exit(1)
        console.error(f"ECS task run failed: {e}", exit_code=1)


@click.command(context_settings=dict(max_content_width=160))
@env_click_option(
    "aws-access-key-id",
    "AWS_ACCESS_KEY_ID",
    help="AWS Access Key ID for authentication.",
    required=False,
)
@env_click_option(
    "aws-secret-access-key",
    "AWS_SECRET_ACCESS_KEY",
    help="AWS Secret Access Key for authentication.",
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
    help="AWS region name (e.g., us-east-1).",
    required=False,
)
@click.option("--json", "json_output", is_flag=True, help="Output results as JSON.", default=False)
def aws_ecs_list(
    aws_access_key_id: Optional[str],
    aws_secret_access_key: Optional[str],
    aws_session_token: Optional[str],
    aws_region: Optional[str],
    json_output: bool,
) -> None:
    """
    List active AWS ECS task definitions.

    AWS credentials are read from environment variables or AWS credential chain:
    - AWS_ACCESS_KEY_ID
    - AWS_SECRET_ACCESS_KEY
    - AWS_SESSION_TOKEN (optional)
    - AWS_REGION

    Example:
        paradime run aws-ecs-list
    """
    if not json_output:
        console.info("Listing ECS task definitions\u2026")

    try:
        result = list_ecs_task_definitions(
            region_name=aws_region,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token,
            json_output=json_output,
        )
        if json_output and result is not None:
            console.json_out(result)
    except Exception as e:
        console.error(f"Failed to list ECS task definitions: {e}", exit_code=1)
