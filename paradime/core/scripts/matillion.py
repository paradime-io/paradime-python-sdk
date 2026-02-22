import logging
import time
from concurrent.futures import ThreadPoolExecutor
from typing import List, Optional

import requests

from paradime.core.scripts.utils import handle_http_error

logging.basicConfig(format="%(asctime)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)


def trigger_matillion_pipeline(
    *,
    base_url: str,
    api_token: str,
    pipeline_ids: List[str],
    environment: str,
    wait_for_completion: bool = True,
    timeout_minutes: int = 1440,
) -> List[str]:
    """
    Trigger execution for multiple Matillion pipelines.

    Args:
        base_url: Matillion instance base URL (e.g., https://your-instance.matillion.com)
        api_token: Matillion API token
        pipeline_ids: List of Matillion pipeline IDs to execute
        environment: Matillion environment name
        wait_for_completion: Whether to wait for executions to complete
        timeout_minutes: Maximum time to wait for completion

    Returns:
        List of execution result messages for each pipeline
    """
    futures = []
    results = []

    # Add visual separator and header
    print(f"\n{'='*60}")
    print("🚀 TRIGGERING MATILLION PIPELINES")
    print(f"{'='*60}")

    with ThreadPoolExecutor() as executor:
        for i, pipeline_id in enumerate(set(pipeline_ids), 1):
            print(f"\n[{i}/{len(set(pipeline_ids))}] 📊 {pipeline_id}")
            print(f"{'-'*40}")

            futures.append(
                (
                    pipeline_id,
                    executor.submit(
                        trigger_single_pipeline,
                        base_url=base_url,
                        api_token=api_token,
                        pipeline_id=pipeline_id,
                        environment=environment,
                        wait_for_completion=wait_for_completion,
                        timeout_minutes=timeout_minutes,
                    ),
                )
            )

        # Add separator for live progress section
        print(f"\n{'='*60}")
        print("⚡ LIVE PROGRESS")
        print(f"{'='*60}")

        # Wait for completion and collect results
        pipeline_results = []
        for pipeline_id, future in futures:
            # Use longer timeout when waiting for completion
            future_timeout = (timeout_minutes * 60 + 120) if wait_for_completion else 120
            response_txt = future.result(timeout=future_timeout)
            pipeline_results.append((pipeline_id, response_txt))
            results.append(response_txt)

        # Display results as simple table
        print(f"\n{'='*80}")
        print("📊 EXECUTION RESULTS")
        print(f"{'='*80}")
        print(f"{'PIPELINE':<30} {'STATUS':<10} {'DASHBOARD'}")
        print(f"{'-'*30} {'-'*10} {'-'*45}")

        for pipeline_id, response_txt in pipeline_results:
            # Format result with emoji
            if "SUCCESS" in response_txt:
                status = "✅ SUCCESS"
            elif "FAILED" in response_txt:
                status = "❌ FAILED"
            elif "RUNNING" in response_txt:
                status = "🔄 RUNNING"
            else:
                status = "ℹ️ COMPLETED"

            dashboard_url = f"{base_url}/pipelines/{pipeline_id}"
            print(f"{pipeline_id:<30} {status:<10} {dashboard_url}")

        print(f"{'='*80}\n")

    return results


def trigger_single_pipeline(
    *,
    base_url: str,
    api_token: str,
    pipeline_id: str,
    environment: str,
    wait_for_completion: bool = True,
    timeout_minutes: int = 1440,
) -> str:
    """
    Trigger execution for a single Matillion pipeline.

    Args:
        base_url: Matillion instance base URL
        api_token: Matillion API token
        pipeline_id: Matillion pipeline ID
        environment: Matillion environment name
        wait_for_completion: Whether to wait for execution to complete
        timeout_minutes: Maximum time to wait for completion

    Returns:
        Status message indicating execution result
    """
    headers = {
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/json",
    }

    import datetime

    timestamp = datetime.datetime.now().strftime("%H:%M:%S")

    # Check pipeline status before attempting execution
    print(f"{timestamp} 🔍 [{pipeline_id}] Checking pipeline status...")
    try:
        status_response = requests.get(
            f"{base_url}/api/v1/pipelines/{pipeline_id}",
            headers=headers,
        )

        if status_response.status_code == 200:
            status_data = status_response.json()
            pipeline_name = status_data.get("name", pipeline_id)
            pipeline_type = status_data.get("type", "unknown")

            print(f"{timestamp} 📊 [{pipeline_id}] Name: {pipeline_name} | Type: {pipeline_type}")

    except Exception as e:
        print(
            f"{timestamp} ⚠️  [{pipeline_id}] Could not check status: {str(e)[:50]}... Proceeding anyway."
        )

    # Trigger the pipeline execution
    execution_payload = {
        "environment": environment,
    }

    print(f"{timestamp} 🚀 [{pipeline_id}] Triggering pipeline execution...")
    execution_response = requests.post(
        f"{base_url}/api/v1/pipelines/{pipeline_id}/execute",
        json=execution_payload,
        headers=headers,
    )

    handle_http_error(
        execution_response,
        f"Error triggering execution for pipeline '{pipeline_id}':",
    )

    execution_data = execution_response.json()
    execution_id = execution_data.get("executionId") or execution_data.get("id")

    # Show dashboard link immediately after successful trigger
    dashboard_url = f"{base_url}/pipelines/{pipeline_id}/executions/{execution_id}"
    print(f"{timestamp} 🔗 [{pipeline_id}] Dashboard: {dashboard_url}")

    if not wait_for_completion:
        return f"Pipeline triggered. Execution ID: {execution_id}"

    print(f"{timestamp} ⏳ [{pipeline_id}] Monitoring execution progress...")

    # Wait for execution completion
    execution_status = _wait_for_execution_completion(
        base_url=base_url,
        api_token=api_token,
        pipeline_id=pipeline_id,
        execution_id=execution_id,
        timeout_minutes=timeout_minutes,
    )

    return f"Execution completed. Final status: {execution_status}"


def _wait_for_execution_completion(
    *,
    base_url: str,
    api_token: str,
    pipeline_id: str,
    execution_id: str,
    timeout_minutes: int,
) -> str:
    """
    Poll execution status until completion or timeout.

    Args:
        base_url: Matillion instance base URL
        api_token: Matillion API token
        pipeline_id: Matillion pipeline ID
        execution_id: Execution ID to monitor
        timeout_minutes: Maximum time to wait for completion

    Returns:
        Final execution status
    """
    headers = {
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/json",
    }
    start_time = time.time()
    timeout_seconds = timeout_minutes * 60
    sleep_interval = 5  # Poll every 5 seconds
    counter = 0

    while True:
        elapsed = time.time() - start_time
        if elapsed > timeout_seconds:
            raise Exception(
                f"Timeout waiting for pipeline '{pipeline_id}' execution to complete after {timeout_minutes} minutes"
            )

        try:
            # Get execution status
            execution_response = requests.get(
                f"{base_url}/api/v1/pipelines/{pipeline_id}/executions/{execution_id}",
                headers=headers,
            )

            if execution_response.status_code != 200:
                raise Exception(
                    f"Execution status check failed with HTTP {execution_response.status_code}"
                )

            execution_data = execution_response.json()
            status = execution_data.get("status", "unknown")

            # Log progress every 6 checks (30 seconds)
            if counter == 0 or counter % 6 == 0:
                import datetime

                timestamp = datetime.datetime.now().strftime("%H:%M:%S")
                elapsed_min = int(elapsed // 60)
                elapsed_sec = int(elapsed % 60)
                if status == "RUNNING":
                    print(
                        f"{timestamp} 🔄 [{pipeline_id}] Running... ({elapsed_min}m {elapsed_sec}s elapsed)"
                    )
                elif status == "PENDING":
                    print(
                        f"{timestamp} ⏳ [{pipeline_id}] Pending... ({elapsed_min}m {elapsed_sec}s elapsed)"
                    )

            # Check if execution is complete
            if status == "SUCCESS":
                import datetime

                timestamp = datetime.datetime.now().strftime("%H:%M:%S")
                elapsed_min = int(elapsed // 60)
                elapsed_sec = int(elapsed % 60)
                print(
                    f"{timestamp} ✅ [{pipeline_id}] Completed successfully ({elapsed_min}m {elapsed_sec}s)"
                )
                return "SUCCESS"

            elif status == "FAILED":
                import datetime

                timestamp = datetime.datetime.now().strftime("%H:%M:%S")
                print(f"{timestamp} ❌ [{pipeline_id}] Execution failed")
                error_message = execution_data.get("error", "No error details available")
                return f"FAILED: {error_message}"

            elif status == "CANCELLED":
                import datetime

                timestamp = datetime.datetime.now().strftime("%H:%M:%S")
                print(f"{timestamp} ⚠️  [{pipeline_id}] Execution was cancelled")
                return "CANCELLED"

            elif status in ["RUNNING", "PENDING"]:
                # Still running, continue waiting
                pass

            else:
                # Unknown status, continue waiting
                pass

            counter += 1
            time.sleep(sleep_interval)

        except requests.exceptions.RequestException as e:
            import datetime

            timestamp = datetime.datetime.now().strftime("%H:%M:%S")
            print(f"{timestamp} ⚠️  [{pipeline_id}] Network error: {str(e)[:50]}... Retrying.")
            time.sleep(sleep_interval)
            continue


def list_matillion_pipelines(
    *,
    base_url: str,
    api_token: str,
    environment: Optional[str] = None,
) -> None:
    """
    List all Matillion pipelines with their IDs and status.

    Args:
        base_url: Matillion instance base URL
        api_token: Matillion API token
        environment: Optional environment name to filter pipelines
    """
    headers = {
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/json",
    }

    # Build URL based on whether environment is provided
    if environment:
        url = f"{base_url}/api/v1/environments/{environment}/pipelines"
        print(f"\n🔍 Listing pipelines for environment: {environment}")
    else:
        url = f"{base_url}/api/v1/pipelines"
        print("\n🔍 Listing all pipelines")

    pipelines_response = requests.get(
        url,
        headers=headers,
    )

    handle_http_error(pipelines_response, "Error getting pipelines:")

    pipelines_data = pipelines_response.json()

    if not pipelines_data or (isinstance(pipelines_data, dict) and "pipelines" in pipelines_data):
        pipelines = pipelines_data.get("pipelines", []) if isinstance(pipelines_data, dict) else []
    else:
        pipelines = pipelines_data if isinstance(pipelines_data, list) else []

    if not pipelines:
        print("No pipelines found.")
        return

    print(f"\n{'='*80}")
    print(f"📋 FOUND {len(pipelines)} PIPELINE(S)")
    print(f"{'='*80}")

    for i, pipeline in enumerate(pipelines, 1):
        pipeline_id = pipeline.get("id", "Unknown")
        name = pipeline.get("name", "Unknown")
        pipeline_type = pipeline.get("type", "Unknown")
        environment_name = pipeline.get("environment", "Unknown")

        # Get last execution info if available
        last_execution = pipeline.get("lastExecution", {})
        last_status = last_execution.get("status", "Never run")
        last_run_at = last_execution.get("startedAt", "Never")

        # Format status with emoji
        status_emoji = (
            "✅"
            if last_status == "SUCCESS"
            else (
                "❌"
                if last_status == "FAILED"
                else "🔄" if last_status == "RUNNING" else "⏸️" if last_status == "PENDING" else "❓"
            )
        )

        # Create dashboard deep link
        dashboard_url = f"{base_url}/pipelines/{pipeline_id}"

        print(f"\n[{i}/{len(pipelines)}] 📊 {pipeline_id}")
        print(f"{'-'*50}")
        print(f"   Name: {name}")
        print(f"   Type: {pipeline_type}")
        print(f"   Environment: {environment_name}")
        print(f"   {status_emoji} Last Status: {last_status}")
        if last_run_at != "Never":
            print(f"   🕐 Last Run: {last_run_at}")
        print(f"   🔗 Dashboard: {dashboard_url}")

    print(f"\n{'='*80}\n")
