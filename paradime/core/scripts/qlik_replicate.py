"""
Qlik Replicate task trigger module.

This module provides functions to trigger, monitor, and manage Qlik Replicate tasks
via the Qlik Enterprise Manager REST API.

Usage with environment variables:
    import os
    from paradime.core.scripts.qlik_replicate import trigger_qlik_replicate_tasks

    # Set environment variables
    QLIK_HOST = os.getenv("QLIK_HOST")  # e.g., "https://your-qlik-server.com"
    QLIK_USERNAME = os.getenv("QLIK_USERNAME")
    QLIK_PASSWORD = os.getenv("QLIK_PASSWORD")
    QLIK_SERVER_NAME = os.getenv("QLIK_SERVER_NAME")  # Replicate server name

    # Trigger tasks
    results = trigger_qlik_replicate_tasks(
        host=QLIK_HOST,
        username=QLIK_USERNAME,
        password=QLIK_PASSWORD,
        server_name=QLIK_SERVER_NAME,
        task_names=["task1", "task2"],
        run_option="RESUME_PROCESSING",  # Options: RESUME_PROCESSING, RELOAD_TARGET, RECOVER
        wait_for_completion=True,
        timeout_minutes=60,
    )

Run options:
    - RESUME_PROCESSING: Resume task from where it left off (default)
    - RELOAD_TARGET: Reload all target tables
    - RECOVER: Recover task from error state

Authentication:
    Uses Qlik Enterprise Manager API with username/password authentication.
    Session tokens are automatically managed and expire after 5 minutes of inactivity.
"""

import logging
import time
from concurrent.futures import ThreadPoolExecutor
from typing import List

import requests

from paradime.core.scripts.utils import handle_http_error

logging.basicConfig(format="%(asctime)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)


def trigger_qlik_replicate_tasks(
    *,
    host: str,
    username: str,
    password: str,
    server_name: str,
    task_names: List[str],
    run_option: str = "RESUME_PROCESSING",
    wait_for_completion: bool = True,
    timeout_minutes: int = 1440,
) -> List[str]:
    """
    Trigger multiple Qlik Replicate tasks.

    Args:
        host: Qlik Enterprise Manager host URL
        username: Username for authentication
        password: Password for authentication
        server_name: Replicate server name
        task_names: List of task names to run
        run_option: Run option (RESUME_PROCESSING, RELOAD_TARGET, RECOVER)
        wait_for_completion: Whether to wait for tasks to complete
        timeout_minutes: Maximum time to wait for completion

    Returns:
        List of task result messages
    """
    # Get session token
    session_token = _login(host=host, username=username, password=password)

    futures = []
    results = []

    # Add visual separator and header
    print(f"\n{'='*60}")
    print("🚀 TRIGGERING QLIK REPLICATE TASKS")
    print(f"{'='*60}")

    with ThreadPoolExecutor() as executor:
        for i, task_name in enumerate(set(task_names), 1):
            print(f"\n[{i}/{len(set(task_names))}] 🔌 {task_name}")
            print(f"{'-'*40}")

            futures.append(
                (
                    task_name,
                    executor.submit(
                        trigger_task_run,
                        host=host,
                        session_token=session_token,
                        server_name=server_name,
                        task_name=task_name,
                        run_option=run_option,
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
        task_results = []
        for task_name, future in futures:
            # Use longer timeout when waiting for completion
            future_timeout = (timeout_minutes * 60 + 120) if wait_for_completion else 120
            response_txt = future.result(timeout=future_timeout)
            task_results.append((task_name, response_txt))
            results.append(response_txt)

        # Display results as simple table
        print(f"\n{'='*80}")
        print("📊 TASK RESULTS")
        print(f"{'='*80}")
        print(f"{'TASK':<30} {'STATUS':<10} {'SERVER'}")
        print(f"{'-'*30} {'-'*10} {'-'*20}")

        for task_name, response_txt in task_results:
            # Format result with emoji
            if "RUNNING" in response_txt:
                status = "✅ RUNNING"
            elif "STOPPED" in response_txt:
                status = "⏸️ STOPPED"
            elif "ERROR" in response_txt:
                status = "❌ ERROR"
            elif "RECOVERY" in response_txt:
                status = "🔄 RECOVERY"
            else:
                status = "ℹ️ COMPLETED"

            print(f"{task_name:<30} {status:<10} {server_name}")

        print(f"{'='*80}\n")

    return results


def trigger_task_run(
    *,
    host: str,
    session_token: str,
    server_name: str,
    task_name: str,
    run_option: str = "RESUME_PROCESSING",
    wait_for_completion: bool = True,
    timeout_minutes: int = 1440,
) -> str:
    """
    Trigger run for a single Qlik Replicate task.

    Args:
        host: Qlik Enterprise Manager host URL
        session_token: Session token from login
        server_name: Replicate server name
        task_name: Task name to run
        run_option: Run option (RESUME_PROCESSING, RELOAD_TARGET, RECOVER)
        wait_for_completion: Whether to wait for task to complete
        timeout_minutes: Maximum time to wait for completion

    Returns:
        Status message indicating task result
    """
    base_url = f"{host}/attunityenterprisemanager/api/v1"
    headers = {
        "Content-Type": "application/json",
        "EnterpriseManager.APISessionID": session_token,
    }

    import datetime

    timestamp = datetime.datetime.now().strftime("%H:%M:%S")

    # Check task status before attempting run
    print(f"{timestamp} 🔍 [{task_name}] Checking task status...")
    try:
        status_response = requests.get(
            f"{base_url}/servers/{server_name}/tasks/{task_name}",
            headers=headers,
        )

        if status_response.status_code == 200:
            task_data = status_response.json()
            current_state = task_data.get("state", "unknown")

            print(f"{timestamp} 📊 [{task_name}] Current state: {current_state}")

            # Handle tasks that are already running
            if current_state == "RUNNING":
                print(f"{timestamp} ℹ️  [{task_name}] Task is already running")

    except Exception as e:
        print(
            f"{timestamp} ⚠️  [{task_name}] Could not check status: {str(e)[:50]}... Proceeding anyway."
        )

    # Trigger the task run
    print(f"{timestamp} 🚀 [{task_name}] Triggering task with option: {run_option}...")
    run_response = requests.post(
        f"{base_url}/servers/{server_name}/tasks/{task_name}",
        params={"action": "run", "option": run_option},
        headers=headers,
    )

    handle_http_error(
        run_response,
        f"Error triggering task '{task_name}':",
    )

    print(f"{timestamp} ✅ [{task_name}] Task triggered successfully")

    if not wait_for_completion:
        return run_response.text

    print(f"{timestamp} ⏳ [{task_name}] Monitoring task progress...")

    # Wait for task completion
    task_status = _wait_for_task_completion(
        host=host,
        session_token=session_token,
        server_name=server_name,
        task_name=task_name,
        timeout_minutes=timeout_minutes,
    )

    return f"Task completed. Final status: {task_status}"


def _login(*, host: str, username: str, password: str) -> str:
    """
    Authenticate with Qlik Enterprise Manager and get session token.

    Args:
        host: Qlik Enterprise Manager host URL
        username: Username for authentication
        password: Password for authentication

    Returns:
        Session token for API calls
    """
    base_url = f"{host}/attunityenterprisemanager/api/v1"
    login_url = f"{base_url}/login"

    headers = {"Content-Type": "application/json"}

    payload = {"username": username, "password": password}

    response = requests.post(login_url, json=payload, headers=headers)

    handle_http_error(response, "Error authenticating with Qlik Enterprise Manager:")

    # Extract session token from response headers
    session_token = response.headers.get("EnterpriseManager.APISessionID")

    if not session_token:
        raise Exception("Failed to obtain session token from login response")

    return session_token


def _wait_for_task_completion(
    *,
    host: str,
    session_token: str,
    server_name: str,
    task_name: str,
    timeout_minutes: int,
) -> str:
    """
    Poll task status until completion or timeout.

    Args:
        host: Qlik Enterprise Manager host URL
        session_token: Session token from login
        server_name: Replicate server name
        task_name: Task name
        timeout_minutes: Maximum time to wait for completion

    Returns:
        Final task status
    """
    base_url = f"{host}/attunityenterprisemanager/api/v1"
    headers = {
        "Content-Type": "application/json",
        "EnterpriseManager.APISessionID": session_token,
    }

    start_time = time.time()
    timeout_seconds = timeout_minutes * 60
    sleep_interval = 5  # Poll every 5 seconds
    counter = 0

    while True:
        elapsed = time.time() - start_time
        if elapsed > timeout_seconds:
            raise Exception(
                f"Timeout waiting for task '{task_name}' to complete after {timeout_minutes} minutes"
            )

        try:
            # Get task details to check status
            task_response = requests.get(
                f"{base_url}/servers/{server_name}/tasks/{task_name}",
                headers=headers,
            )

            if task_response.status_code != 200:
                raise Exception(f"Task status check failed with HTTP {task_response.status_code}")

            task_data = task_response.json()
            task_state = task_data.get("state", "unknown")

            # Log progress every 6 checks (30 seconds)
            if counter == 0 or counter % 6 == 0:
                import datetime

                timestamp = datetime.datetime.now().strftime("%H:%M:%S")
                elapsed_min = int(elapsed // 60)
                elapsed_sec = int(elapsed % 60)

                if task_state == "RUNNING":
                    print(
                        f"{timestamp} 🔄 [{task_name}] Running... ({elapsed_min}m {elapsed_sec}s elapsed)"
                    )
                elif task_state == "STARTING":
                    print(
                        f"{timestamp} ⏳ [{task_name}] Starting... ({elapsed_min}m {elapsed_sec}s elapsed)"
                    )

            # Check if task is complete
            if task_state in ["STOPPED", "ERROR", "RECOVERY"]:
                import datetime

                timestamp = datetime.datetime.now().strftime("%H:%M:%S")
                elapsed_min = int(elapsed // 60)
                elapsed_sec = int(elapsed % 60)

                if task_state == "STOPPED":
                    print(
                        f"{timestamp} ✅ [{task_name}] Task stopped successfully ({elapsed_min}m {elapsed_sec}s)"
                    )
                    return "STOPPED (task completed normally)"
                elif task_state == "ERROR":
                    print(f"{timestamp} ❌ [{task_name}] Task encountered an error")
                    return "ERROR (task failed)"
                elif task_state == "RECOVERY":
                    print(f"{timestamp} 🔄 [{task_name}] Task in recovery mode")
                    return "RECOVERY (task is recovering)"

            elif task_state in ["RUNNING", "STARTING"]:
                # Still running, continue waiting
                pass
            else:
                # Continue waiting for unknown states
                pass

            counter += 1
            time.sleep(sleep_interval)

        except requests.exceptions.RequestException as e:
            import datetime

            timestamp = datetime.datetime.now().strftime("%H:%M:%S")
            print(f"{timestamp} ⚠️  [{task_name}] Network error: {str(e)[:50]}... Retrying.")
            time.sleep(sleep_interval)
            continue


def stop_qlik_replicate_tasks(
    *,
    host: str,
    username: str,
    password: str,
    server_name: str,
    task_names: List[str],
    timeout_seconds: int = 30,
) -> List[str]:
    """
    Stop multiple Qlik Replicate tasks.

    Args:
        host: Qlik Enterprise Manager host URL
        username: Username for authentication
        password: Password for authentication
        server_name: Replicate server name
        task_names: List of task names to stop
        timeout_seconds: Timeout for stop operation

    Returns:
        List of stop result messages
    """
    # Get session token
    session_token = _login(host=host, username=username, password=password)

    base_url = f"{host}/attunityenterprisemanager/api/v1"
    headers = {
        "Content-Type": "application/json",
        "EnterpriseManager.APISessionID": session_token,
    }

    results = []

    print(f"\n{'='*60}")
    print("🛑 STOPPING QLIK REPLICATE TASKS")
    print(f"{'='*60}")

    for i, task_name in enumerate(set(task_names), 1):
        print(f"\n[{i}/{len(set(task_names))}] 🔌 {task_name}")
        print(f"{'-'*40}")

        import datetime

        timestamp = datetime.datetime.now().strftime("%H:%M:%S")

        try:
            print(f"{timestamp} 🛑 [{task_name}] Stopping task...")
            stop_response = requests.post(
                f"{base_url}/servers/{server_name}/tasks/{task_name}",
                params={"action": "stop", "timeout": str(timeout_seconds)},
                headers=headers,
            )

            handle_http_error(stop_response, f"Error stopping task '{task_name}':")

            print(f"{timestamp} ✅ [{task_name}] Task stopped successfully")
            results.append(f"STOPPED: {task_name}")

        except Exception as e:
            print(f"{timestamp} ❌ [{task_name}] Error stopping task: {str(e)[:100]}")
            results.append(f"ERROR: {task_name} - {str(e)[:100]}")

    print(f"\n{'='*60}\n")

    return results


def list_qlik_replicate_tasks(
    *,
    host: str,
    username: str,
    password: str,
    server_name: str,
) -> None:
    """
    List all Qlik Replicate tasks with their status.

    Args:
        host: Qlik Enterprise Manager host URL
        username: Username for authentication
        password: Password for authentication
        server_name: Replicate server name
    """
    # Get session token
    session_token = _login(host=host, username=username, password=password)

    base_url = f"{host}/attunityenterprisemanager/api/v1"
    headers = {
        "Content-Type": "application/json",
        "EnterpriseManager.APISessionID": session_token,
    }

    print(f"\n🔍 Listing tasks for server: {server_name}")

    tasks_response = requests.get(
        f"{base_url}/servers/{server_name}/tasks",
        headers=headers,
    )

    handle_http_error(tasks_response, "Error getting tasks:")

    tasks_data = tasks_response.json()

    if not tasks_data or not isinstance(tasks_data, list):
        print("No tasks found.")
        return

    print(f"\n{'='*80}")
    print(f"📋 FOUND {len(tasks_data)} TASK(S)")
    print(f"{'='*80}")

    for i, task in enumerate(tasks_data, 1):
        task_name = task.get("name", "Unknown")
        task_state = task.get("state", "Unknown")
        source = task.get("source_name", "Unknown")
        target = task.get("target_name", "Unknown")

        # Format state with emoji
        state_emoji = (
            "🔄"
            if task_state == "RUNNING"
            else (
                "⏸️"
                if task_state == "STOPPED"
                else "❌" if task_state == "ERROR" else "🔄" if task_state == "RECOVERY" else "❓"
            )
        )

        print(f"\n[{i}/{len(tasks_data)}] 🔌 {task_name}")
        print(f"{'-'*50}")
        print(f"   {state_emoji} State: {task_state}")
        print(f"   Source: {source}")
        print(f"   Target: {target}")
        print(f"   Server: {server_name}")

    print(f"\n{'='*80}\n")
