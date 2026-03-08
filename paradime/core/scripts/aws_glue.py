from __future__ import annotations

import os
import time
from concurrent.futures import ThreadPoolExecutor
from typing import List, Optional

import boto3  # type: ignore[import-untyped]
from botocore.exceptions import ClientError, NoCredentialsError  # type: ignore[import-untyped]

from paradime.cli import console


def trigger_glue_workflows(
    *,
    workflow_names: List[str],
    wait_for_completion: bool = True,
    timeout_minutes: int = 1440,
    region_name: Optional[str] = None,
) -> List[str]:
    """
    Trigger multiple AWS Glue workflows.

    Args:
        workflow_names: List of AWS Glue workflow names to trigger
        wait_for_completion: Whether to wait for workflows to complete
        timeout_minutes: Maximum time to wait for completion
        region_name: AWS region name (defaults to AWS_REGION env var or default region)

    Returns:
        List of workflow result messages for each workflow

    Note:
        AWS credentials are read from environment variables or AWS credential chain:
        - AWS_ACCESS_KEY_ID
        - AWS_SECRET_ACCESS_KEY
        - AWS_SESSION_TOKEN (optional)
        - AWS_REGION (or passed as region_name parameter)
    """
    futures = []
    results = []

    with ThreadPoolExecutor() as executor:
        for i, workflow_name in enumerate(set(workflow_names), 1):
            futures.append(
                (
                    workflow_name,
                    executor.submit(
                        trigger_workflow,
                        workflow_name=workflow_name,
                        wait_for_completion=wait_for_completion,
                        timeout_minutes=timeout_minutes,
                        region_name=region_name,
                    ),
                )
            )

        # Wait for completion and collect results
        workflow_results = []
        for workflow_name, future in futures:
            # Use longer timeout when waiting for completion
            future_timeout = (timeout_minutes * 60 + 120) if wait_for_completion else 120
            response_txt = future.result(timeout=future_timeout)
            workflow_results.append((workflow_name, response_txt))
            results.append(response_txt)

        def _status_text(response_txt: str) -> str:
            if "SUCCESS" in response_txt or "COMPLETED" in response_txt:
                return "SUCCESS"
            elif "FAILED" in response_txt or "ERROR" in response_txt:
                return "FAILED"
            elif "STOPPED" in response_txt:
                return "STOPPED"
            elif "RUNNING" in response_txt:
                return "RUNNING"
            else:
                return "TRIGGERED"

        region = region_name or os.environ.get("AWS_REGION", "us-east-1")
        console.table(
            columns=["Workflow", "Status", "Console URL"],
            rows=[
                (
                    wf_name,
                    _status_text(response_txt),
                    f"https://console.aws.amazon.com/glue/home?region={region}#/v2/etl-configuration/workflows/{wf_name}",
                )
                for wf_name, response_txt in workflow_results
            ],
            title="Workflow Results",
        )

    return results


def trigger_workflow(
    *,
    workflow_name: str,
    wait_for_completion: bool = True,
    timeout_minutes: int = 1440,
    region_name: Optional[str] = None,
) -> str:
    """
    Trigger a single AWS Glue workflow.

    Args:
        workflow_name: AWS Glue workflow name
        wait_for_completion: Whether to wait for workflow to complete
        timeout_minutes: Maximum time to wait for completion
        region_name: AWS region name (defaults to AWS_REGION env var or default region)

    Returns:
        Status message indicating workflow result
    """
    # Initialize Glue client
    try:
        region = region_name or os.environ.get("AWS_REGION")
        glue_client = boto3.client("glue", region_name=region)
    except NoCredentialsError:
        error_msg = "AWS credentials not found. Please set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables or configure AWS credentials."
        console.error(f"[{workflow_name}] {error_msg}")
        raise Exception(error_msg)

    # Check workflow status before triggering
    console.debug(f"[{workflow_name}] Checking workflow status...")
    try:
        workflow_response = glue_client.get_workflow(Name=workflow_name)
        workflow_data = workflow_response.get("Workflow", {})
        last_run = workflow_data.get("LastRun", {})

        if last_run:
            workflow_run_state = last_run.get("Status", "UNKNOWN")
            console.debug(f"[{workflow_name}] Last run state: {workflow_run_state}")

            # Warn if a run is currently in progress
            if workflow_run_state == "RUNNING":
                console.debug(f"[{workflow_name}] Warning: A workflow run is currently in progress")

    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code", "Unknown")
        if error_code == "EntityNotFoundException":
            console.error(f"[{workflow_name}] Workflow not found")
            return f"ERROR (workflow '{workflow_name}' not found)"
        else:
            console.debug(
                f"[{workflow_name}] Could not check status: {str(e)[:50]}... Proceeding anyway."
            )

    # Trigger the workflow
    console.debug(f"[{workflow_name}] Triggering workflow...")
    try:
        run_response = glue_client.start_workflow_run(Name=workflow_name)
        run_id = run_response.get("RunId")

        console.debug(f"[{workflow_name}] Workflow triggered successfully (Run ID: {run_id})")

        # Show console link immediately after successful trigger
        region = region_name or os.environ.get("AWS_REGION", "us-east-1")
        console_url = f"https://console.aws.amazon.com/glue/home?region={region}#/v2/etl-configuration/workflows/{workflow_name}"
        console.debug(f"[{workflow_name}] Console: {console_url}")

        if not wait_for_completion:
            return f"Workflow triggered (Run ID: {run_id})"

        console.debug(f"[{workflow_name}] Monitoring workflow progress...")

        # Wait for workflow completion
        workflow_status = _wait_for_workflow_completion(
            glue_client=glue_client,
            workflow_name=workflow_name,
            run_id=run_id,
            timeout_minutes=timeout_minutes,
        )

        return f"Workflow completed. Final status: {workflow_status}"

    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code", "Unknown")
        error_message = e.response.get("Error", {}).get("Message", str(e))

        if error_code == "ConcurrentRunsExceededException":
            console.debug(
                f"[{workflow_name}] Concurrent run limit exceeded. Workflow may be running already."
            )
            return "ERROR (concurrent run limit exceeded - workflow may already be running)"
        else:
            console.error(f"[{workflow_name}] Error: {error_message}")
            return f"ERROR ({error_code}: {error_message})"


def _wait_for_workflow_completion(
    *,
    glue_client: "boto3.client",
    workflow_name: str,
    run_id: str,
    timeout_minutes: int,
) -> str:
    """
    Poll workflow status until completion or timeout.

    Args:
        glue_client: Boto3 Glue client
        workflow_name: AWS Glue workflow name
        run_id: Workflow run ID
        timeout_minutes: Maximum time to wait for completion

    Returns:
        Final workflow status
    """
    start_time = time.time()
    timeout_seconds = timeout_minutes * 60
    sleep_interval = 10  # Poll every 10 seconds (Glue workflows can be long-running)
    counter = 0
    consecutive_failures = 0
    max_consecutive_failures = 5
    run_started = False

    while True:
        elapsed = time.time() - start_time
        if elapsed > timeout_seconds:
            raise Exception(
                f"Timeout waiting for workflow '{workflow_name}' to complete after {timeout_minutes} minutes"
            )

        try:
            # Get workflow run status
            workflow_response = glue_client.get_workflow_run(Name=workflow_name, RunId=run_id)

            run_data = workflow_response.get("Run", {})
            run_status = run_data.get("Status", "UNKNOWN")
            statistics = run_data.get("Statistics", {})

            # Reset failure counter on successful request
            consecutive_failures = 0

            # Track if run has actually started
            if run_status == "RUNNING" and not run_started:
                run_started = True
                console.debug(f"[{workflow_name}] Workflow run started")

            # Log progress every 6 checks (1 minute)
            if counter == 0 or counter % 6 == 0:
                elapsed_min = int(elapsed // 60)
                elapsed_sec = int(elapsed % 60)

                if run_status == "RUNNING":
                    total_actions = statistics.get("TotalActions", 0)
                    succeeded_actions = statistics.get("SucceededActions", 0)
                    failed_actions = statistics.get("FailedActions", 0)
                    running_actions = statistics.get("RunningActions", 0)

                    progress = f"{succeeded_actions}/{total_actions} actions completed"
                    if failed_actions > 0:
                        progress += f", {failed_actions} failed"
                    if running_actions > 0:
                        progress += f", {running_actions} running"

                    console.debug(
                        f"[{workflow_name}] Running... {progress} ({elapsed_min}m {elapsed_sec}s elapsed)"
                    )

            # Check if workflow is complete
            if run_status in ["COMPLETED", "STOPPED", "ERROR"]:
                elapsed_min = int(elapsed // 60)
                elapsed_sec = int(elapsed % 60)

                total_actions = statistics.get("TotalActions", 0)
                succeeded_actions = statistics.get("SucceededActions", 0)
                failed_actions = statistics.get("FailedActions", 0)
                stopped_actions = statistics.get("StoppedActions", 0)

                if run_status == "COMPLETED":
                    console.debug(
                        f"[{workflow_name}] Completed successfully ({elapsed_min}m {elapsed_sec}s)"
                    )
                    console.debug(
                        f"[{workflow_name}] Actions: {succeeded_actions} succeeded, {failed_actions} failed, {stopped_actions} stopped"
                    )
                    return f"SUCCESS (completed - {succeeded_actions}/{total_actions} actions succeeded)"
                elif run_status == "STOPPED":
                    console.debug(f"[{workflow_name}] Workflow stopped")
                    console.debug(
                        f"[{workflow_name}] Actions: {succeeded_actions} succeeded, {failed_actions} failed, {stopped_actions} stopped"
                    )
                    return f"STOPPED (workflow stopped - {succeeded_actions}/{total_actions} actions succeeded)"
                elif run_status == "ERROR":
                    error_message = run_data.get("ErrorMessage", "Unknown error")
                    console.error(f"[{workflow_name}] Workflow failed: {error_message}")
                    console.debug(
                        f"[{workflow_name}] Actions: {succeeded_actions} succeeded, {failed_actions} failed, {stopped_actions} stopped"
                    )
                    return f"FAILED ({failed_actions}/{total_actions} actions failed - {error_message})"

            elif run_status == "RUNNING":
                # Still running, continue waiting
                pass
            else:
                # Continue waiting for unknown states
                pass

            counter += 1
            time.sleep(sleep_interval)

        except ClientError as e:
            consecutive_failures += 1
            if consecutive_failures >= max_consecutive_failures:
                error_message = str(e)
                raise Exception(
                    f"AWS API errors occurred {consecutive_failures} times in a row. Last error: {error_message[:100]}"
                )

            console.debug(
                f"[{workflow_name}] AWS API error: {str(e)[:50]}... Retrying... ({consecutive_failures}/{max_consecutive_failures})"
            )
            time.sleep(
                sleep_interval * min(consecutive_failures, 3)
            )  # Exponential backoff up to 3x
            continue


def trigger_glue_jobs(
    *,
    job_names: List[str],
    wait_for_completion: bool = True,
    timeout_minutes: int = 1440,
    region_name: Optional[str] = None,
) -> List[str]:
    """
    Trigger multiple AWS Glue jobs.

    Args:
        job_names: List of AWS Glue job names to trigger
        wait_for_completion: Whether to wait for jobs to complete
        timeout_minutes: Maximum time to wait for completion
        region_name: AWS region name (defaults to AWS_REGION env var or default region)

    Returns:
        List of job result messages for each job

    Note:
        AWS credentials are read from environment variables or AWS credential chain:
        - AWS_ACCESS_KEY_ID
        - AWS_SECRET_ACCESS_KEY
        - AWS_SESSION_TOKEN (optional)
        - AWS_REGION (or passed as region_name parameter)
    """
    futures = []
    results = []

    with ThreadPoolExecutor() as executor:
        for i, job_name in enumerate(set(job_names), 1):
            futures.append(
                (
                    job_name,
                    executor.submit(
                        trigger_job,
                        job_name=job_name,
                        wait_for_completion=wait_for_completion,
                        timeout_minutes=timeout_minutes,
                        region_name=region_name,
                    ),
                )
            )

        # Wait for completion and collect results
        job_results = []
        for job_name, future in futures:
            # Use longer timeout when waiting for completion
            future_timeout = (timeout_minutes * 60 + 120) if wait_for_completion else 120
            response_txt = future.result(timeout=future_timeout)
            job_results.append((job_name, response_txt))
            results.append(response_txt)

        def _status_text(response_txt: str) -> str:
            if "SUCCESS" in response_txt or "SUCCEEDED" in response_txt:
                return "SUCCESS"
            elif "FAILED" in response_txt or "ERROR" in response_txt:
                return "FAILED"
            elif "STOPPED" in response_txt:
                return "STOPPED"
            elif "RUNNING" in response_txt:
                return "RUNNING"
            else:
                return "TRIGGERED"

        region = region_name or os.environ.get("AWS_REGION", "us-east-1")
        console.table(
            columns=["Job Name", "Status", "Console URL"],
            rows=[
                (
                    jn,
                    _status_text(response_txt),
                    f"https://console.aws.amazon.com/glue/home?region={region}#/v2/etl-configuration/jobs/{jn}",
                )
                for jn, response_txt in job_results
            ],
            title="Job Results",
        )

    return results


def trigger_job(
    *,
    job_name: str,
    wait_for_completion: bool = True,
    timeout_minutes: int = 1440,
    region_name: Optional[str] = None,
) -> str:
    """
    Trigger a single AWS Glue job.

    Args:
        job_name: AWS Glue job name
        wait_for_completion: Whether to wait for job to complete
        timeout_minutes: Maximum time to wait for completion
        region_name: AWS region name (defaults to AWS_REGION env var or default region)

    Returns:
        Status message indicating job result
    """
    # Initialize Glue client
    try:
        region = region_name or os.environ.get("AWS_REGION")
        glue_client = boto3.client("glue", region_name=region)
    except NoCredentialsError:
        error_msg = "AWS credentials not found. Please set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables or configure AWS credentials."
        console.error(f"[{job_name}] {error_msg}")
        raise Exception(error_msg)

    # Check job exists before triggering
    console.debug(f"[{job_name}] Checking job status...")
    try:
        glue_client.get_job(JobName=job_name)
        console.debug(f"[{job_name}] Job found")

    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code", "Unknown")
        if error_code == "EntityNotFoundException":
            console.error(f"[{job_name}] Job not found")
            return f"ERROR (job '{job_name}' not found)"
        else:
            console.debug(
                f"[{job_name}] Could not check status: {str(e)[:50]}... Proceeding anyway."
            )

    # Trigger the job
    console.debug(f"[{job_name}] Triggering job...")
    try:
        run_response = glue_client.start_job_run(JobName=job_name)
        run_id = run_response.get("JobRunId")

        console.debug(f"[{job_name}] Job triggered successfully (Run ID: {run_id})")

        # Show console link immediately after successful trigger
        region = region_name or os.environ.get("AWS_REGION", "us-east-1")
        console_url = f"https://console.aws.amazon.com/glue/home?region={region}#/v2/etl-configuration/jobs/{job_name}"
        console.debug(f"[{job_name}] Console: {console_url}")

        if not wait_for_completion:
            return f"Job triggered (Run ID: {run_id})"

        console.debug(f"[{job_name}] Monitoring job progress...")

        # Wait for job completion
        job_status = _wait_for_job_completion(
            glue_client=glue_client,
            job_name=job_name,
            run_id=run_id,
            timeout_minutes=timeout_minutes,
        )

        return f"Job completed. Final status: {job_status}"

    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code", "Unknown")
        error_message = e.response.get("Error", {}).get("Message", str(e))

        if error_code == "ConcurrentRunsExceededException":
            console.debug(
                f"[{job_name}] Concurrent run limit exceeded. Job may be running already."
            )
            return "ERROR (concurrent run limit exceeded - job may already be running)"
        else:
            console.error(f"[{job_name}] Error: {error_message}")
            return f"ERROR ({error_code}: {error_message})"


def _wait_for_job_completion(
    *,
    glue_client: "boto3.client",
    job_name: str,
    run_id: str,
    timeout_minutes: int,
) -> str:
    """
    Poll job status until completion or timeout.

    Args:
        glue_client: Boto3 Glue client
        job_name: AWS Glue job name
        run_id: Job run ID
        timeout_minutes: Maximum time to wait for completion

    Returns:
        Final job status
    """
    start_time = time.time()
    timeout_seconds = timeout_minutes * 60
    sleep_interval = 5  # Poll every 5 seconds
    counter = 0
    consecutive_failures = 0
    max_consecutive_failures = 5
    run_started = False

    while True:
        elapsed = time.time() - start_time
        if elapsed > timeout_seconds:
            raise Exception(
                f"Timeout waiting for job '{job_name}' to complete after {timeout_minutes} minutes"
            )

        try:
            # Get job run status
            job_run_response = glue_client.get_job_run(JobName=job_name, RunId=run_id)

            run_data = job_run_response.get("JobRun", {})
            run_state = run_data.get("JobRunState", "UNKNOWN")

            # Reset failure counter on successful request
            consecutive_failures = 0

            # Track if run has actually started
            if run_state == "RUNNING" and not run_started:
                run_started = True
                console.debug(f"[{job_name}] Job run started")

            # Log progress every 6 checks (30 seconds)
            if counter == 0 or counter % 6 == 0:
                elapsed_min = int(elapsed // 60)
                elapsed_sec = int(elapsed % 60)

                if run_state == "RUNNING":
                    console.debug(
                        f"[{job_name}] Running... ({elapsed_min}m {elapsed_sec}s elapsed)"
                    )

            # Check if job is complete
            if run_state in ["SUCCEEDED", "FAILED", "STOPPED", "TIMEOUT", "ERROR"]:
                elapsed_min = int(elapsed // 60)
                elapsed_sec = int(elapsed % 60)

                if run_state == "SUCCEEDED":
                    console.debug(
                        f"[{job_name}] Completed successfully ({elapsed_min}m {elapsed_sec}s)"
                    )
                    return "SUCCESS (completed)"
                elif run_state == "FAILED":
                    error_message = run_data.get("ErrorMessage", "Unknown error")
                    console.error(f"[{job_name}] Job failed: {error_message}")
                    return f"FAILED ({error_message})"
                elif run_state == "STOPPED":
                    console.debug(f"[{job_name}] Job stopped")
                    return "STOPPED (job stopped)"
                elif run_state == "TIMEOUT":
                    console.debug(f"[{job_name}] Job timed out")
                    return "TIMEOUT (job timed out)"
                elif run_state == "ERROR":
                    error_message = run_data.get("ErrorMessage", "Unknown error")
                    console.error(f"[{job_name}] Job error: {error_message}")
                    return f"ERROR ({error_message})"

            elif run_state in ["RUNNING", "STARTING", "STOPPING"]:
                # Still running, continue waiting
                pass
            else:
                # Continue waiting for unknown states
                pass

            counter += 1
            time.sleep(sleep_interval)

        except ClientError as e:
            consecutive_failures += 1
            if consecutive_failures >= max_consecutive_failures:
                error_message = str(e)
                raise Exception(
                    f"AWS API errors occurred {consecutive_failures} times in a row. Last error: {error_message[:100]}"
                )

            console.debug(
                f"[{job_name}] AWS API error: {str(e)[:50]}... Retrying... ({consecutive_failures}/{max_consecutive_failures})"
            )
            time.sleep(
                sleep_interval * min(consecutive_failures, 3)
            )  # Exponential backoff up to 3x
            continue


def list_glue_jobs(
    *,
    region_name: Optional[str] = None,
    json_output: bool = False,
) -> list | None:
    """
    List all AWS Glue jobs with their status.

    Args:
        region_name: AWS region name (defaults to AWS_REGION env var or default region)
        json_output: Whether to return data as a list of dicts instead of printing a table
    """
    # Initialize Glue client
    try:
        region = region_name or os.environ.get("AWS_REGION")
        glue_client = boto3.client("glue", region_name=region)
    except NoCredentialsError:
        error_msg = "AWS credentials not found. Please set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables or configure AWS credentials."
        console.error(error_msg)
        raise Exception(error_msg)

    if not json_output:
        console.info(f"Listing AWS Glue jobs in region: {region or 'default'}")

    try:
        # List all jobs
        jobs_response = glue_client.get_jobs()
        jobs = jobs_response.get("Jobs", [])

        # Handle pagination
        while "NextToken" in jobs_response:
            jobs_response = glue_client.get_jobs(NextToken=jobs_response["NextToken"])
            jobs.extend(jobs_response.get("Jobs", []))

        if not jobs:
            if not json_output:
                console.info("No jobs found.")
            return [] if json_output else None

        rows = []
        data = []
        for job in jobs:
            job_name = job.get("Name", "Unknown")

            try:
                # Get job runs to show last run status
                runs_response = glue_client.get_job_runs(JobName=job_name, MaxResults=1)
                job_runs = runs_response.get("JobRuns", [])

                if job_runs:
                    last_run = job_runs[0]
                    run_state = last_run.get("JobRunState", "Unknown")
                    started_on = str(last_run.get("StartedOn", "Never"))
                else:
                    run_state = "Never Run"
                    started_on = "Never"

                region_str = region_name or os.environ.get("AWS_REGION", "us-east-1")
                console_url = f"https://console.aws.amazon.com/glue/home?region={region_str}#/v2/etl-configuration/jobs/{job_name}"
                rows.append((job_name, run_state, started_on, console_url))
                data.append(
                    {
                        "job_name": job_name,
                        "last_run_status": run_state,
                        "started_on": started_on,
                        "console_url": console_url,
                    }
                )

            except ClientError as e:
                rows.append((job_name, "Error", str(e)[:50], ""))
                data.append(
                    {
                        "job_name": job_name,
                        "last_run_status": "Error",
                        "started_on": str(e)[:50],
                        "console_url": "",
                    }
                )

        if json_output:
            return data

        console.table(
            columns=["Job Name", "Last Run Status", "Started On", "Console URL"],
            rows=rows,
            title="AWS Glue Jobs",
        )
        return None

    except ClientError as e:
        error_message = e.response.get("Error", {}).get("Message", str(e))
        console.error(f"Error listing jobs: {error_message}")
        raise


def list_glue_workflows(
    *,
    region_name: Optional[str] = None,
    json_output: bool = False,
) -> list | None:
    """
    List all AWS Glue workflows with their status.

    Args:
        region_name: AWS region name (defaults to AWS_REGION env var or default region)
        json_output: Whether to return data as a list of dicts instead of printing a table
    """
    # Initialize Glue client
    try:
        region = region_name or os.environ.get("AWS_REGION")
        glue_client = boto3.client("glue", region_name=region)
    except NoCredentialsError:
        error_msg = "AWS credentials not found. Please set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables or configure AWS credentials."
        console.error(error_msg)
        raise Exception(error_msg)

    if not json_output:
        console.info(f"Listing AWS Glue workflows in region: {region or 'default'}")

    try:
        # List all workflows
        workflows_response = glue_client.list_workflows()
        workflow_names = workflows_response.get("Workflows", [])

        # Handle pagination
        while "NextToken" in workflows_response:
            workflows_response = glue_client.list_workflows(
                NextToken=workflows_response["NextToken"]
            )
            workflow_names.extend(workflows_response.get("Workflows", []))

        if not workflow_names:
            if not json_output:
                console.info("No workflows found.")
            return [] if json_output else None

        rows = []
        data = []
        for workflow_name in workflow_names:
            try:
                workflow_response = glue_client.get_workflow(Name=workflow_name)
                workflow_data = workflow_response.get("Workflow", {})

                last_run = workflow_data.get("LastRun", {})

                if last_run:
                    run_status = last_run.get("Status", "Unknown")
                    statistics = last_run.get("Statistics", {})
                    started_on = str(last_run.get("StartedOn", "Never"))
                    total_actions = statistics.get("TotalActions", 0)
                    succeeded_actions = statistics.get("SucceededActions", 0)
                    failed_actions = statistics.get("FailedActions", 0)
                    actions_summary = (
                        f"{succeeded_actions}/{total_actions} (failed: {failed_actions})"
                    )
                else:
                    run_status = "Never Run"
                    started_on = "Never"
                    actions_summary = "N/A"
                    total_actions = 0
                    succeeded_actions = 0
                    failed_actions = 0

                region_str = region_name or os.environ.get("AWS_REGION", "us-east-1")
                console_url = f"https://console.aws.amazon.com/glue/home?region={region_str}#/v2/etl-configuration/workflows/{workflow_name}"
                rows.append((workflow_name, run_status, started_on, actions_summary, console_url))
                data.append(
                    {
                        "workflow": workflow_name,
                        "last_run_status": run_status,
                        "started_on": started_on,
                        "actions": actions_summary,
                        "console_url": console_url,
                    }
                )

            except ClientError as e:
                rows.append((workflow_name, "Error", "", str(e)[:50], ""))
                data.append(
                    {
                        "workflow": workflow_name,
                        "last_run_status": "Error",
                        "started_on": "",
                        "actions": str(e)[:50],
                        "console_url": "",
                    }
                )

        if json_output:
            return data

        console.table(
            columns=["Workflow", "Last Run Status", "Started On", "Actions", "Console URL"],
            rows=rows,
            title="AWS Glue Workflows",
        )
        return None

    except ClientError as e:
        error_message = e.response.get("Error", {}).get("Message", str(e))
        console.error(f"Error listing workflows: {error_message}")
        raise
