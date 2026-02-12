import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor
from typing import List, Optional

import boto3
from botocore.exceptions import ClientError, NoCredentialsError

logging.basicConfig(format="%(asctime)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)


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

    # Add visual separator and header
    print(f"\n{'='*60}")
    print("🚀 TRIGGERING AWS GLUE WORKFLOWS")
    print(f"{'='*60}")

    with ThreadPoolExecutor() as executor:
        for i, workflow_name in enumerate(set(workflow_names), 1):
            print(f"\n[{i}/{len(set(workflow_names))}] ⚙️  {workflow_name}")
            print(f"{'-'*40}")

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

        # Add separator for live progress section
        print(f"\n{'='*60}")
        print("⚡ LIVE PROGRESS")
        print(f"{'='*60}")

        # Wait for completion and collect results
        workflow_results = []
        for workflow_name, future in futures:
            # Use longer timeout when waiting for completion
            future_timeout = (timeout_minutes * 60 + 120) if wait_for_completion else 120
            response_txt = future.result(timeout=future_timeout)
            workflow_results.append((workflow_name, response_txt))
            results.append(response_txt)

        # Display results as simple table
        print(f"\n{'='*80}")
        print("📊 WORKFLOW RESULTS")
        print(f"{'='*80}")
        print(f"{'WORKFLOW':<35} {'STATUS':<10} {'CONSOLE'}")
        print(f"{'-'*35} {'-'*10} {'-'*45}")

        for workflow_name, response_txt in workflow_results:
            # Format result with emoji
            if "SUCCESS" in response_txt or "COMPLETED" in response_txt:
                status = "✅ SUCCESS"
            elif "FAILED" in response_txt or "ERROR" in response_txt:
                status = "❌ FAILED"
            elif "STOPPED" in response_txt:
                status = "🛑 STOPPED"
            elif "RUNNING" in response_txt:
                status = "🔄 RUNNING"
            else:
                status = "ℹ️  TRIGGERED"

            # Get region for console URL
            region = region_name or os.environ.get("AWS_REGION", "us-east-1")
            console_url = f"https://console.aws.amazon.com/glue/home?region={region}#/v2/etl-configuration/workflows/{workflow_name}"
            print(f"{workflow_name:<35} {status:<10} {console_url}")

        print(f"{'='*80}\n")

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
    import datetime

    timestamp = datetime.datetime.now().strftime("%H:%M:%S")

    # Initialize Glue client
    try:
        region = region_name or os.environ.get("AWS_REGION")
        glue_client = boto3.client("glue", region_name=region)
    except NoCredentialsError:
        error_msg = "AWS credentials not found. Please set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables or configure AWS credentials."
        print(f"{timestamp} ❌ [{workflow_name}] {error_msg}")
        raise Exception(error_msg)

    # Check workflow status before triggering
    print(f"{timestamp} 🔍 [{workflow_name}] Checking workflow status...")
    try:
        workflow_response = glue_client.get_workflow(Name=workflow_name)
        workflow_data = workflow_response.get("Workflow", {})
        last_run = workflow_data.get("LastRun", {})

        if last_run:
            workflow_run_state = last_run.get("Status", "UNKNOWN")
            print(f"{timestamp} 📊 [{workflow_name}] Last run state: {workflow_run_state}")

            # Warn if a run is currently in progress
            if workflow_run_state == "RUNNING":
                print(
                    f"{timestamp} ⚠️  [{workflow_name}] Warning: A workflow run is currently in progress"
                )

    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code", "Unknown")
        if error_code == "EntityNotFoundException":
            print(f"{timestamp} ❌ [{workflow_name}] Workflow not found")
            return f"ERROR (workflow '{workflow_name}' not found)"
        else:
            print(
                f"{timestamp} ⚠️  [{workflow_name}] Could not check status: {str(e)[:50]}... Proceeding anyway."
            )

    # Trigger the workflow
    print(f"{timestamp} 🚀 [{workflow_name}] Triggering workflow...")
    try:
        run_response = glue_client.start_workflow_run(Name=workflow_name)
        run_id = run_response.get("RunId")

        print(f"{timestamp} ✅ [{workflow_name}] Workflow triggered successfully (Run ID: {run_id})")

        # Show console link immediately after successful trigger
        region = region_name or os.environ.get("AWS_REGION", "us-east-1")
        console_url = f"https://console.aws.amazon.com/glue/home?region={region}#/v2/etl-configuration/workflows/{workflow_name}"
        print(f"{timestamp} 🔗 [{workflow_name}] Console: {console_url}")

        if not wait_for_completion:
            return f"Workflow triggered (Run ID: {run_id})"

        print(f"{timestamp} ⏳ [{workflow_name}] Monitoring workflow progress...")

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
            print(
                f"{timestamp} ⚠️  [{workflow_name}] Concurrent run limit exceeded. Workflow may be running already."
            )
            return f"ERROR (concurrent run limit exceeded - workflow may already be running)"
        else:
            print(f"{timestamp} ❌ [{workflow_name}] Error: {error_message}")
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
                import datetime

                timestamp = datetime.datetime.now().strftime("%H:%M:%S")
                print(f"{timestamp} 🔄 [{workflow_name}] Workflow run started")

            # Log progress every 6 checks (1 minute)
            if counter == 0 or counter % 6 == 0:
                import datetime

                timestamp = datetime.datetime.now().strftime("%H:%M:%S")
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

                    print(
                        f"{timestamp} 🔄 [{workflow_name}] Running... {progress} ({elapsed_min}m {elapsed_sec}s elapsed)"
                    )

            # Check if workflow is complete
            if run_status in ["COMPLETED", "STOPPED", "ERROR"]:
                import datetime

                timestamp = datetime.datetime.now().strftime("%H:%M:%S")
                elapsed_min = int(elapsed // 60)
                elapsed_sec = int(elapsed % 60)

                total_actions = statistics.get("TotalActions", 0)
                succeeded_actions = statistics.get("SucceededActions", 0)
                failed_actions = statistics.get("FailedActions", 0)
                stopped_actions = statistics.get("StoppedActions", 0)

                if run_status == "COMPLETED":
                    print(
                        f"{timestamp} ✅ [{workflow_name}] Completed successfully ({elapsed_min}m {elapsed_sec}s)"
                    )
                    print(
                        f"{timestamp} 📊 [{workflow_name}] Actions: {succeeded_actions} succeeded, {failed_actions} failed, {stopped_actions} stopped"
                    )
                    return f"SUCCESS (completed - {succeeded_actions}/{total_actions} actions succeeded)"
                elif run_status == "STOPPED":
                    print(f"{timestamp} 🛑 [{workflow_name}] Workflow stopped")
                    print(
                        f"{timestamp} 📊 [{workflow_name}] Actions: {succeeded_actions} succeeded, {failed_actions} failed, {stopped_actions} stopped"
                    )
                    return f"STOPPED (workflow stopped - {succeeded_actions}/{total_actions} actions succeeded)"
                elif run_status == "ERROR":
                    error_message = run_data.get("ErrorMessage", "Unknown error")
                    print(f"{timestamp} ❌ [{workflow_name}] Workflow failed: {error_message}")
                    print(
                        f"{timestamp} 📊 [{workflow_name}] Actions: {succeeded_actions} succeeded, {failed_actions} failed, {stopped_actions} stopped"
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

            import datetime

            timestamp = datetime.datetime.now().strftime("%H:%M:%S")
            print(
                f"{timestamp} ⚠️  [{workflow_name}] AWS API error: {str(e)[:50]}... Retrying... ({consecutive_failures}/{max_consecutive_failures})"
            )
            time.sleep(
                sleep_interval * min(consecutive_failures, 3)
            )  # Exponential backoff up to 3x
            continue


def list_glue_workflows(
    *,
    region_name: Optional[str] = None,
) -> None:
    """
    List all AWS Glue workflows with their status.

    Args:
        region_name: AWS region name (defaults to AWS_REGION env var or default region)
    """
    # Initialize Glue client
    try:
        region = region_name or os.environ.get("AWS_REGION")
        glue_client = boto3.client("glue", region_name=region)
    except NoCredentialsError:
        error_msg = "AWS credentials not found. Please set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables or configure AWS credentials."
        print(f"\n❌ {error_msg}")
        raise Exception(error_msg)

    print(f"\n🔍 Listing AWS Glue workflows in region: {region or 'default'}")

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
            print("\n📋 No workflows found.")
            return

        print(f"\n{'='*80}")
        print(f"📋 FOUND {len(workflow_names)} WORKFLOW(S)")
        print(f"{'='*80}")

        # Get details for each workflow
        for i, workflow_name in enumerate(workflow_names, 1):
            try:
                workflow_response = glue_client.get_workflow(Name=workflow_name)
                workflow_data = workflow_response.get("Workflow", {})

                last_run = workflow_data.get("LastRun", {})

                if last_run:
                    run_status = last_run.get("Status", "Unknown")
                    statistics = last_run.get("Statistics", {})
                    started_on = last_run.get("StartedOn", "Never")
                    completed_on = last_run.get("CompletedOn", "N/A")

                    # Format status with emoji
                    status_emoji = (
                        "🔄"
                        if run_status == "RUNNING"
                        else (
                            "✅"
                            if run_status == "COMPLETED"
                            else "🛑" if run_status == "STOPPED" else "❌" if run_status == "ERROR" else "❓"
                        )
                    )

                    total_actions = statistics.get("TotalActions", 0)
                    succeeded_actions = statistics.get("SucceededActions", 0)
                    failed_actions = statistics.get("FailedActions", 0)
                else:
                    run_status = "Never Run"
                    status_emoji = "⚪"
                    started_on = "Never"
                    completed_on = "N/A"
                    total_actions = 0
                    succeeded_actions = 0
                    failed_actions = 0

                # Create console deep link
                region = region_name or os.environ.get("AWS_REGION", "us-east-1")
                console_url = f"https://console.aws.amazon.com/glue/home?region={region}#/v2/etl-configuration/workflows/{workflow_name}"

                print(f"\n[{i}/{len(workflow_names)}] ⚙️  {workflow_name}")
                print(f"{'-'*50}")
                print(f"   {status_emoji} Last Run Status: {run_status}")
                if last_run:
                    print(f"   🕐 Started: {started_on}")
                    if run_status != "RUNNING":
                        print(f"   🕐 Completed: {completed_on}")
                    if total_actions > 0:
                        print(
                            f"   📊 Actions: {succeeded_actions} succeeded, {failed_actions} failed (Total: {total_actions})"
                        )
                print(f"   🔗 Console: {console_url}")

            except ClientError as e:
                print(f"\n[{i}/{len(workflow_names)}] ⚙️  {workflow_name}")
                print(f"{'-'*50}")
                print(f"   ⚠️  Could not retrieve details: {str(e)[:50]}")

        print(f"\n{'='*80}\n")

    except ClientError as e:
        error_message = e.response.get("Error", {}).get("Message", str(e))
        print(f"\n❌ Error listing workflows: {error_message}")
        raise
