import logging
import time
from concurrent.futures import ThreadPoolExecutor
from typing import List, Optional, Dict, Any
from datetime import datetime

import requests

from paradime.core.scripts.utils import handle_http_error

logging.basicConfig(format="%(asctime)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)


def trigger_airflow_dags(
    *,
    base_url: str,
    username: str,
    password: str,
    dag_ids: List[str],
    dag_run_conf: Optional[Dict[str, Any]] = None,
    wait_for_completion: bool = True,
    timeout_minutes: int = 1440,
    show_logs: bool = True,
) -> List[str]:
    """
    Trigger multiple Airflow DAG runs.

    Args:
        base_url: Airflow base URL (e.g., https://your-airflow.com or MWAA webserver URL)
        username: Airflow username (or API key)
        password: Airflow password (or API secret)
        dag_ids: List of DAG IDs to trigger
        dag_run_conf: Optional configuration to pass to DAG runs
        wait_for_completion: Whether to wait for DAG runs to complete
        timeout_minutes: Maximum time to wait for completion
        show_logs: Whether to display task logs during execution

    Returns:
        List of DAG run result messages
    """
    futures = []
    results = []

    # Add visual separator and header
    print(f"\n{'='*60}")
    print("🚀 TRIGGERING AIRFLOW DAGS")
    print(f"{'='*60}")

    with ThreadPoolExecutor() as executor:
        for i, dag_id in enumerate(set(dag_ids), 1):
            print(f"\n[{i}/{len(set(dag_ids))}] 🔄 {dag_id}")
            print(f"{'-'*40}")

            futures.append(
                (
                    dag_id,
                    executor.submit(
                        trigger_dag_run,
                        base_url=base_url,
                        username=username,
                        password=password,
                        dag_id=dag_id,
                        dag_run_conf=dag_run_conf,
                        wait_for_completion=wait_for_completion,
                        timeout_minutes=timeout_minutes,
                        show_logs=show_logs,
                    ),
                )
            )

        # Add separator for live progress section
        print(f"\n{'='*60}")
        print("⚡ LIVE PROGRESS")
        print(f"{'='*60}")

        # Wait for completion and collect results
        dag_results = []
        for dag_id, future in futures:
            # Use longer timeout when waiting for completion
            future_timeout = (timeout_minutes * 60 + 120) if wait_for_completion else 120
            response_txt = future.result(timeout=future_timeout)
            dag_results.append((dag_id, response_txt))
            results.append(response_txt)

        # Display results as simple table
        print(f"\n{'='*80}")
        print("📊 DAG RUN RESULTS")
        print(f"{'='*80}")
        print(f"{'DAG ID':<30} {'STATUS':<15}")
        print(f"{'-'*30} {'-'*15}")

        for dag_id, response_txt in dag_results:
            # Format result with emoji
            if "SUCCESS" in response_txt:
                status = "✅ SUCCESS"
            elif "FAILED" in response_txt:
                status = "❌ FAILED"
            elif "RUNNING" in response_txt:
                status = "🔄 RUNNING"
            else:
                status = "ℹ️ COMPLETED"

            print(f"{dag_id:<30} {status:<15}")

        print(f"{'='*80}\n")

    return results


def trigger_dag_run(
    *,
    base_url: str,
    username: str,
    password: str,
    dag_id: str,
    dag_run_conf: Optional[Dict[str, Any]] = None,
    wait_for_completion: bool = True,
    timeout_minutes: int = 1440,
    show_logs: bool = True,
) -> str:
    """
    Trigger a single Airflow DAG run.

    Args:
        base_url: Airflow base URL
        username: Airflow username (or API key)
        password: Airflow password (or API secret)
        dag_id: DAG ID to trigger
        dag_run_conf: Optional configuration to pass to DAG run
        wait_for_completion: Whether to wait for DAG run to complete
        timeout_minutes: Maximum time to wait for completion
        show_logs: Whether to display task logs during execution

    Returns:
        Status message indicating DAG run result
    """
    # Ensure base URL doesn't have trailing slash
    base_url = base_url.rstrip("/")

    # Build API URL
    api_base = f"{base_url}/api/v1"
    auth = (username, password)
    headers = {"Content-Type": "application/json"}

    timestamp = datetime.now().strftime("%H:%M:%S")

    # Check DAG status before attempting trigger
    print(f"{timestamp} 🔍 [{dag_id}] Checking DAG status...")
    try:
        dag_response = requests.get(
            f"{api_base}/dags/{dag_id}",
            auth=auth,
            headers=headers,
        )

        if dag_response.status_code == 200:
            dag_data = dag_response.json()
            is_paused = dag_data.get("is_paused", False)
            is_active = dag_data.get("is_active", True)

            print(
                f"{timestamp} 📊 [{dag_id}] Active: {is_active} | Paused: {is_paused}"
            )

            # Handle paused DAGs
            if is_paused:
                print(
                    f"{timestamp} ⚠️  [{dag_id}] Warning: DAG is paused - run will queue until unpaused"
                )

            # Handle inactive DAGs
            if not is_active:
                print(
                    f"{timestamp} ⚠️  [{dag_id}] Warning: DAG is not active"
                )

    except Exception as e:
        print(
            f"{timestamp} ⚠️  [{dag_id}] Could not check status: {str(e)[:50]}... Proceeding anyway."
        )

    # Prepare DAG run payload
    dag_run_payload = {
        "conf": dag_run_conf or {},
    }

    print(f"{timestamp} 🚀 [{dag_id}] Triggering DAG run...")
    dag_run_response = requests.post(
        f"{api_base}/dags/{dag_id}/dagRuns",
        json=dag_run_payload,
        auth=auth,
        headers=headers,
    )

    handle_http_error(
        dag_run_response,
        f"Error triggering DAG run for '{dag_id}':",
    )

    dag_run_data = dag_run_response.json()
    dag_run_id = dag_run_data.get("dag_run_id")
    logical_date = dag_run_data.get("logical_date", "")

    print(
        f"{timestamp} ✅ [{dag_id}] DAG run triggered successfully (ID: {dag_run_id})"
    )

    # Show Airflow UI link
    ui_url = f"{base_url}/dags/{dag_id}/grid?dag_run_id={dag_run_id}"
    print(f"{timestamp} 🔗 [{dag_id}] Airflow UI: {ui_url}")

    if not wait_for_completion:
        return f"DAG run triggered (ID: {dag_run_id})"

    print(f"{timestamp} ⏳ [{dag_id}] Monitoring DAG run progress...")

    # Wait for DAG run completion
    dag_run_status = _wait_for_dag_completion(
        api_base=api_base,
        auth=auth,
        headers=headers,
        dag_id=dag_id,
        dag_run_id=dag_run_id,
        timeout_minutes=timeout_minutes,
        show_logs=show_logs,
    )

    return f"DAG run completed. Final status: {dag_run_status}"


def _wait_for_dag_completion(
    *,
    api_base: str,
    auth: tuple,
    headers: dict,
    dag_id: str,
    dag_run_id: str,
    timeout_minutes: int,
    show_logs: bool,
) -> str:
    """
    Poll DAG run status until completion or timeout.

    Args:
        api_base: Airflow API base URL
        auth: Authentication tuple (username, password)
        headers: Request headers
        dag_id: DAG ID
        dag_run_id: DAG run ID
        timeout_minutes: Maximum time to wait for completion
        show_logs: Whether to display task logs

    Returns:
        Final DAG run status
    """
    start_time = time.time()
    timeout_seconds = timeout_minutes * 60
    sleep_interval = 10  # Poll every 10 seconds
    counter = 0
    task_states_logged = set()  # Track which task states we've logged

    while True:
        elapsed = time.time() - start_time
        if elapsed > timeout_seconds:
            raise Exception(
                f"Timeout waiting for DAG '{dag_id}' run '{dag_run_id}' to complete after {timeout_minutes} minutes"
            )

        try:
            # Get DAG run status
            dag_run_response = requests.get(
                f"{api_base}/dags/{dag_id}/dagRuns/{dag_run_id}",
                auth=auth,
                headers=headers,
            )

            if dag_run_response.status_code != 200:
                raise Exception(
                    f"DAG run status check failed with HTTP {dag_run_response.status_code}"
                )

            dag_run_data = dag_run_response.json()
            state = dag_run_data.get("state", "unknown")

            # Get task instances for progress tracking
            task_instances_response = requests.get(
                f"{api_base}/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances",
                auth=auth,
                headers=headers,
            )

            task_instances = []
            if task_instances_response.status_code == 200:
                task_instances_data = task_instances_response.json()
                task_instances = task_instances_data.get("task_instances", [])

            # Log progress
            timestamp = datetime.now().strftime("%H:%M:%S")
            elapsed_min = int(elapsed // 60)
            elapsed_sec = int(elapsed % 60)

            # Count task states
            task_states = {}
            for task in task_instances:
                task_state = task.get("state", "unknown")
                task_states[task_state] = task_states.get(task_state, 0) + 1

            # Log progress every 6 checks (~1 minute)
            if counter == 0 or counter % 6 == 0:
                if state == "running":
                    state_summary = ", ".join(
                        [f"{count} {state}" for state, count in task_states.items()]
                    )
                    print(
                        f"{timestamp} 🔄 [{dag_id}] Running... ({state_summary}) ({elapsed_min}m {elapsed_sec}s elapsed)"
                    )
                elif state == "queued":
                    print(
                        f"{timestamp} ⏳ [{dag_id}] Queued... ({elapsed_min}m {elapsed_sec}s elapsed)"
                    )

            # Display logs for completed tasks (only once per task)
            if show_logs:
                for task in task_instances:
                    task_id = task.get("task_id")
                    task_state = task.get("state")
                    task_key = f"{task_id}:{task_state}"

                    # Only fetch logs for completed tasks we haven't logged yet
                    if (
                        task_state in ["success", "failed"]
                        and task_key not in task_states_logged
                    ):
                        _fetch_and_display_task_logs(
                            api_base=api_base,
                            auth=auth,
                            headers=headers,
                            dag_id=dag_id,
                            dag_run_id=dag_run_id,
                            task_id=task_id,
                            task_state=task_state,
                        )
                        task_states_logged.add(task_key)

            # Check if DAG run is complete
            if state in ["success", "failed"]:
                timestamp = datetime.now().strftime("%H:%M:%S")
                elapsed_min = int(elapsed // 60)
                elapsed_sec = int(elapsed % 60)

                if state == "success":
                    print(
                        f"{timestamp} ✅ [{dag_id}] Completed successfully ({elapsed_min}m {elapsed_sec}s)"
                    )
                    return f"SUCCESS (completed at run ID: {dag_run_id})"
                elif state == "failed":
                    # Log any remaining task logs
                    if show_logs:
                        for task in task_instances:
                            task_id = task.get("task_id")
                            task_state = task.get("state")
                            task_key = f"{task_id}:{task_state}"
                            if task_key not in task_states_logged:
                                _fetch_and_display_task_logs(
                                    api_base=api_base,
                                    auth=auth,
                                    headers=headers,
                                    dag_id=dag_id,
                                    dag_run_id=dag_run_id,
                                    task_id=task_id,
                                    task_state=task_state,
                                )
                                task_states_logged.add(task_key)

                    print(f"{timestamp} ❌ [{dag_id}] DAG run failed")
                    return f"FAILED (run ID: {dag_run_id})"

            elif state in ["running", "queued"]:
                # Still running, continue waiting
                pass
            else:
                # Continue waiting for unknown states
                pass

            counter += 1
            time.sleep(sleep_interval)

        except requests.exceptions.RequestException as e:
            timestamp = datetime.now().strftime("%H:%M:%S")
            print(
                f"{timestamp} ⚠️  [{dag_id}] Network error: {str(e)[:50]}... Retrying."
            )
            time.sleep(sleep_interval)
            continue


def _fetch_and_display_task_logs(
    *,
    api_base: str,
    auth: tuple,
    headers: dict,
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    task_state: str,
) -> None:
    """
    Fetch and display logs for a specific task instance.

    Args:
        api_base: Airflow API base URL
        auth: Authentication tuple (username, password)
        headers: Request headers
        dag_id: DAG ID
        dag_run_id: DAG run ID
        task_id: Task ID
        task_state: Task state (for display purposes)
    """
    timestamp = datetime.now().strftime("%H:%M:%S")

    try:
        # Fetch task logs - try attempt 1 first
        task_try = 1
        logs_response = requests.get(
            f"{api_base}/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/logs/{task_try}",
            auth=auth,
            headers=headers,
        )

        if logs_response.status_code == 200:
            # Get the log content
            log_content = logs_response.text

            # Display logs with formatting
            state_emoji = "✅" if task_state == "success" else "❌"
            print(f"\n{timestamp} {state_emoji} [{dag_id}] Task '{task_id}' {task_state.upper()}")
            print(f"{'-'*60}")
            print("📝 TASK LOGS:")
            print(f"{'-'*60}")

            # Display the logs (limit to last 50 lines for readability)
            log_lines = log_content.split("\n")
            if len(log_lines) > 50:
                print(f"... (showing last 50 lines of {len(log_lines)} total lines)")
                log_lines = log_lines[-50:]

            for line in log_lines:
                if line.strip():  # Only print non-empty lines
                    print(f"  {line}")

            print(f"{'-'*60}\n")
        else:
            # Log retrieval failed, just note the task state
            print(
                f"{timestamp} ⚠️  [{dag_id}] Task '{task_id}' {task_state} (logs unavailable)"
            )

    except Exception as e:
        print(
            f"{timestamp} ⚠️  [{dag_id}] Could not fetch logs for task '{task_id}': {str(e)[:50]}"
        )


def list_airflow_dags(
    *,
    base_url: str,
    username: str,
    password: str,
    only_active: bool = True,
) -> None:
    """
    List all Airflow DAGs with their IDs and status.

    Args:
        base_url: Airflow base URL
        username: Airflow username (or API key)
        password: Airflow password (or API secret)
        only_active: Whether to only show active (non-paused) DAGs
    """
    # Ensure base URL doesn't have trailing slash
    base_url = base_url.rstrip("/")

    api_base = f"{base_url}/api/v1"
    auth = (username, password)
    headers = {"Content-Type": "application/json"}

    # Build URL and parameters
    params = {}
    if only_active:
        params["only_active"] = "true"
        print("\n🔍 Listing active DAGs")
    else:
        print("\n🔍 Listing all DAGs")

    dags_response = requests.get(
        f"{api_base}/dags",
        auth=auth,
        headers=headers,
        params=params,
    )

    handle_http_error(dags_response, "Error getting DAGs:")

    dags_data = dags_response.json()

    if "dags" not in dags_data:
        print("No DAGs found.")
        return

    dags = dags_data["dags"]

    print(f"\n{'='*80}")
    print(f"📋 FOUND {len(dags)} DAG(S)")
    print(f"{'='*80}")

    for i, dag in enumerate(dags, 1):
        dag_id = dag.get("dag_id", "Unknown")
        is_paused = dag.get("is_paused", False)
        is_active = dag.get("is_active", True)
        description = dag.get("description", "No description")

        # Get last DAG run info if available
        last_run = dag.get("last_parsed_time", "Never")

        # Format status with emoji
        paused_emoji = "⏸️" if is_paused else "▶️"
        active_emoji = "✅" if is_active else "❌"

        # Create UI link
        ui_url = f"{base_url}/dags/{dag_id}/grid"

        print(f"\n[{i}/{len(dags)}] 🔄 {dag_id}")
        print(f"{'-'*50}")
        print(f"   Description: {description}")
        print(f"   {paused_emoji} Paused: {is_paused}")
        print(f"   {active_emoji} Active: {is_active}")
        print(f"   📅 Last Parsed: {last_run}")
        print(f"   🔗 UI: {ui_url}")

    print(f"\n{'='*80}\n")
