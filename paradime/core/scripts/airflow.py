from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import requests

from paradime.cli import console
from paradime.core.scripts.utils import handle_http_error


def _get_gcp_bearer_token() -> str:
    """
    Get GCP bearer token for Cloud Composer authentication.

    Uses Application Default Credentials (ADC) or service account key.
    Requires google-auth library to be installed.

    Returns:
        Bearer token for authentication
    """
    try:
        from google.auth import default  # type: ignore[import-not-found]
        from google.auth.transport.requests import Request  # type: ignore[import-not-found]

        credentials, project = default()
        credentials.refresh(Request())
        return credentials.token  # type: ignore[no-any-return]
    except ImportError:
        raise ImportError(
            "google-auth library is required for GCP Cloud Composer authentication. "
            "Install it with: pip install google-auth"
        )
    except Exception as e:
        raise Exception(f"Failed to get GCP bearer token: {str(e)}")


def _get_auth_headers(
    *,
    username: Optional[str] = None,
    password: Optional[str] = None,
    use_gcp_auth: bool = False,
    bearer_token: Optional[str] = None,
) -> Tuple[Optional[tuple], dict]:
    """
    Get authentication headers based on the authentication method.

    Args:
        username: Username for basic auth
        password: Password for basic auth
        use_gcp_auth: Whether to use GCP Cloud Composer authentication
        bearer_token: Optional bearer token for token-based auth

    Returns:
        Tuple of (auth_tuple, headers) where auth_tuple is for basic auth
        and headers contain bearer token if applicable
    """
    headers = {"Content-Type": "application/json"}

    if use_gcp_auth:
        # Use GCP authentication (Cloud Composer)
        token = bearer_token or _get_gcp_bearer_token()
        headers["Authorization"] = f"Bearer {token}"
        return None, headers
    elif bearer_token:
        # Use provided bearer token
        headers["Authorization"] = f"Bearer {bearer_token}"
        return None, headers
    else:
        # Use basic authentication
        if not username or not password:
            raise ValueError(
                "username and password are required for basic authentication. "
                "For GCP Cloud Composer, use use_gcp_auth=True instead."
            )
        return (username, password), headers


def trigger_airflow_dags(
    *,
    base_url: str,
    username: Optional[str] = None,
    password: Optional[str] = None,
    dag_ids: List[str],
    dag_run_conf: Optional[Dict[str, Any]] = None,
    logical_date: Optional[str] = None,
    wait_for_completion: bool = True,
    timeout_minutes: int = 1440,
    show_logs: bool = True,
    use_gcp_auth: bool = False,
    bearer_token: Optional[str] = None,
) -> List[str]:
    """
    Trigger multiple Airflow DAG runs.

    Args:
        base_url: Airflow base URL (e.g., https://your-airflow.com, MWAA webserver URL, or Cloud Composer URL)
        username: Airflow username (or API key). Not required for GCP Cloud Composer.
        password: Airflow password (or API secret). Not required for GCP Cloud Composer.
        dag_ids: List of DAG IDs to trigger
        dag_run_conf: Optional configuration to pass to DAG runs
        logical_date: Optional logical date for the DAG run in ISO format (e.g., "2024-01-01T00:00:00Z"). Defaults to current timestamp.
        wait_for_completion: Whether to wait for DAG runs to complete
        timeout_minutes: Maximum time to wait for completion
        show_logs: Whether to display task logs during execution
        use_gcp_auth: Whether to use GCP Cloud Composer authentication (uses Application Default Credentials)
        bearer_token: Optional bearer token for token-based authentication

    Returns:
        List of DAG run result messages
    """
    futures = []
    results = []

    with ThreadPoolExecutor() as executor:
        for i, dag_id in enumerate(set(dag_ids), 1):
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
                        logical_date=logical_date,
                        wait_for_completion=wait_for_completion,
                        timeout_minutes=timeout_minutes,
                        show_logs=show_logs,
                        use_gcp_auth=use_gcp_auth,
                        bearer_token=bearer_token,
                    ),
                )
            )

        # Wait for completion and collect results
        dag_results = []
        for dag_id, future in futures:
            # Use longer timeout when waiting for completion
            future_timeout = (timeout_minutes * 60 + 120) if wait_for_completion else 120
            response_txt = future.result(timeout=future_timeout)
            dag_results.append((dag_id, response_txt))
            results.append(response_txt)

        def _status_text(response_txt: str) -> str:
            if "SUCCESS" in response_txt:
                return "SUCCESS"
            elif "FAILED" in response_txt:
                return "FAILED"
            elif "RUNNING" in response_txt:
                return "RUNNING"
            else:
                return "COMPLETED"

        console.table(
            columns=["DAG ID", "Status"],
            rows=[(dag_id, _status_text(response_txt)) for dag_id, response_txt in dag_results],
            title="DAG Run Results",
        )

    return results


def trigger_dag_run(
    *,
    base_url: str,
    username: Optional[str] = None,
    password: Optional[str] = None,
    dag_id: str,
    dag_run_conf: Optional[Dict[str, Any]] = None,
    logical_date: Optional[str] = None,
    wait_for_completion: bool = True,
    timeout_minutes: int = 1440,
    show_logs: bool = True,
    use_gcp_auth: bool = False,
    bearer_token: Optional[str] = None,
) -> str:
    """
    Trigger a single Airflow DAG run.

    Args:
        base_url: Airflow base URL
        username: Airflow username (or API key). Not required for GCP Cloud Composer.
        password: Airflow password (or API secret). Not required for GCP Cloud Composer.
        dag_id: DAG ID to trigger
        dag_run_conf: Optional configuration to pass to DAG run
        logical_date: Optional logical date for the DAG run in ISO format. Defaults to current timestamp.
        wait_for_completion: Whether to wait for DAG run to complete
        timeout_minutes: Maximum time to wait for completion
        show_logs: Whether to display task logs during execution
        use_gcp_auth: Whether to use GCP Cloud Composer authentication
        bearer_token: Optional bearer token for token-based authentication

    Returns:
        Status message indicating DAG run result
    """
    # Ensure base URL doesn't have trailing slash
    base_url = base_url.rstrip("/")

    # Build API URL - try v2 first, fallback to v1 for older Airflow versions
    api_base = f"{base_url}/api/v2"

    # Get authentication credentials
    auth, headers = _get_auth_headers(
        username=username,
        password=password,
        use_gcp_auth=use_gcp_auth,
        bearer_token=bearer_token,
    )

    # Check DAG status before attempting trigger
    console.debug(f"[{dag_id}] Checking DAG status...")
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

            console.debug(f"[{dag_id}] Active: {is_active} | Paused: {is_paused}")

            # Handle paused DAGs
            if is_paused:
                console.debug(f"[{dag_id}] Warning: DAG is paused - run will queue until unpaused")

            # Handle inactive DAGs
            if not is_active:
                console.debug(f"[{dag_id}] Warning: DAG is not active")

    except Exception as e:
        console.debug(f"[{dag_id}] Could not check status: {str(e)[:50]}... Proceeding anyway.")

    # Prepare DAG run payload
    # For Airflow v2 API, we need to provide a logical_date (execution_date)
    # Use provided logical_date or generate one using current timestamp
    if logical_date is None:
        logical_date = datetime.now().isoformat() + "Z"

    dag_run_payload = {
        "logical_date": logical_date,
        "conf": dag_run_conf or {},
    }

    console.debug(f"[{dag_id}] Triggering DAG run...")
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

    console.debug(f"[{dag_id}] DAG run triggered successfully (ID: {dag_run_id})")

    # Show Airflow UI link
    ui_url = f"{base_url}/dags/{dag_id}/grid?dag_run_id={dag_run_id}"
    console.debug(f"[{dag_id}] Airflow UI: {ui_url}")

    if not wait_for_completion:
        return f"DAG run triggered (ID: {dag_run_id})"

    console.debug(f"[{dag_id}] Monitoring DAG run progress...")

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
    auth: Optional[tuple],
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
        auth: Optional authentication tuple (username, password) for basic auth
        headers: Request headers (may contain bearer token)
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

            elapsed_min = int(elapsed // 60)
            elapsed_sec = int(elapsed % 60)

            # Count task states
            task_states: Dict[str, int] = {}
            for task in task_instances:
                task_state = task.get("state", "unknown")
                task_states[task_state] = task_states.get(task_state, 0) + 1

            # Log progress every 6 checks (~1 minute)
            if counter == 0 or counter % 6 == 0:
                if state == "running":
                    state_summary = ", ".join(
                        [f"{count} {state}" for state, count in task_states.items()]
                    )
                    console.debug(
                        f"[{dag_id}] Running... ({state_summary}) ({elapsed_min}m {elapsed_sec}s elapsed)"
                    )
                elif state == "queued":
                    console.debug(f"[{dag_id}] Queued... ({elapsed_min}m {elapsed_sec}s elapsed)")

            # Display logs for completed tasks (only once per task)
            if show_logs:
                for task in task_instances:
                    task_id = task.get("task_id")
                    task_state = task.get("state")
                    task_key = f"{task_id}:{task_state}"

                    # Only fetch logs for completed tasks we haven't logged yet
                    if task_state in ["success", "failed"] and task_key not in task_states_logged:
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
                elapsed_min = int(elapsed // 60)
                elapsed_sec = int(elapsed % 60)

                if state == "success":
                    console.debug(
                        f"[{dag_id}] Completed successfully ({elapsed_min}m {elapsed_sec}s)"
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

                    console.debug(f"[{dag_id}] DAG run failed")
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
            console.debug(f"[{dag_id}] Network error: {str(e)[:50]}... Retrying.")
            time.sleep(sleep_interval)
            continue


def _fetch_and_display_task_logs(
    *,
    api_base: str,
    auth: Optional[tuple],
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
        auth: Optional authentication tuple (username, password) for basic auth
        headers: Request headers (may contain bearer token)
        dag_id: DAG ID
        dag_run_id: DAG run ID
        task_id: Task ID
        task_state: Task state (for display purposes)
    """
    try:
        # Fetch task logs - try attempt 1 first
        task_try = 1
        logs_response = requests.get(
            f"{api_base}/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/logs/{task_try}",
            auth=auth,
            headers=headers,
        )

        if logs_response.status_code == 200:
            console.debug(f"[{dag_id}] Task '{task_id}' {task_state.upper()}")

            # Parse log content - Airflow v2 API returns JSON structured logs
            import json

            try:
                log_data = json.loads(logs_response.text)
                log_entries = log_data.get("content", [])

                formatted_lines = []
                for entry in log_entries:
                    if isinstance(entry, dict):
                        # Extract timestamp and event/message
                        entry_timestamp = entry.get("timestamp", "")
                        event = entry.get("event", "")
                        level = entry.get("level", "INFO").upper()

                        # Format timestamp to match Airflow's format [YYYY-MM-DD HH:MM:SS]
                        if entry_timestamp:
                            try:
                                from datetime import datetime as dt

                                parsed_time = dt.fromisoformat(
                                    entry_timestamp.replace("Z", "+00:00")
                                )
                                formatted_time = parsed_time.strftime("%Y-%m-%d %H:%M:%S")
                            except Exception:
                                formatted_time = entry_timestamp[:19].replace("T", " ")
                        else:
                            formatted_time = ""

                        # Skip GitHub Actions group markers
                        if event.startswith("::group::") or event.startswith("::endgroup::"):
                            continue

                        # Format the log line
                        if formatted_time and event:
                            formatted_lines.append(f"[{formatted_time}] {level} - {event}")

                # Display the formatted logs
                if formatted_lines:
                    # Limit to last 100 lines for readability
                    if len(formatted_lines) > 100:
                        console.debug(
                            f"... (showing last 100 lines of {len(formatted_lines)} total lines)"
                        )
                        formatted_lines = formatted_lines[-100:]

                    for line in formatted_lines:
                        console.debug(f"  {line}")
                else:
                    # Fallback to raw text if no structured logs found
                    console.debug(f"  {logs_response.text[:2000]}")

            except json.JSONDecodeError:
                # Fallback to plain text parsing for older Airflow versions
                log_lines = logs_response.text.split("\n")
                if len(log_lines) > 100:
                    console.debug(f"... (showing last 100 lines of {len(log_lines)} total lines)")
                    log_lines = log_lines[-100:]

                for line in log_lines:
                    if line.strip():
                        console.debug(f"  {line}")

        else:
            # Log retrieval failed, just note the task state
            console.debug(f"[{dag_id}] Task '{task_id}' {task_state} (logs unavailable)")

    except Exception as e:
        console.debug(f"[{dag_id}] Could not fetch logs for task '{task_id}': {str(e)[:50]}")


def list_airflow_dags(
    *,
    base_url: str,
    username: Optional[str] = None,
    password: Optional[str] = None,
    only_active: bool = True,
    use_gcp_auth: bool = False,
    bearer_token: Optional[str] = None,
    json_output: bool = False,
) -> list | None:
    """
    List all Airflow DAGs with their IDs and status.

    Args:
        base_url: Airflow base URL
        username: Airflow username (or API key). Not required for GCP Cloud Composer.
        password: Airflow password (or API secret). Not required for GCP Cloud Composer.
        only_active: Whether to only show active (non-paused) DAGs
        use_gcp_auth: Whether to use GCP Cloud Composer authentication
        bearer_token: Optional bearer token for token-based authentication
        json_output: Whether to return data as a list of dicts instead of printing a table
    """
    # Ensure base URL doesn't have trailing slash
    base_url = base_url.rstrip("/")

    api_base = f"{base_url}/api/v2"

    # Get authentication credentials
    auth, headers = _get_auth_headers(
        username=username,
        password=password,
        use_gcp_auth=use_gcp_auth,
        bearer_token=bearer_token,
    )

    # Build URL and parameters
    params = {}
    if not json_output:
        if only_active:
            params["only_active"] = "true"
            console.info("Listing active DAGs")
        else:
            console.info("Listing all DAGs")
    else:
        if only_active:
            params["only_active"] = "true"

    dags_response = requests.get(
        f"{api_base}/dags",
        auth=auth,
        headers=headers,
        params=params,
    )

    handle_http_error(dags_response, "Error getting DAGs:")

    dags_data = dags_response.json()

    if "dags" not in dags_data:
        if not json_output:
            console.info("No DAGs found.")
        return [] if json_output else None

    dags = dags_data["dags"]

    rows = []
    data = []
    for dag in dags:
        dag_id = dag.get("dag_id", "Unknown")
        is_paused = dag.get("is_paused", False)
        is_active = dag.get("is_active", True)
        last_run = dag.get("last_parsed_time", "Never")
        ui_url = f"{base_url}/dags/{dag_id}/grid"
        rows.append((dag_id, str(is_active), str(is_paused), last_run, ui_url))
        data.append(
            {
                "dag_id": dag_id,
                "active": is_active,
                "paused": is_paused,
                "last_parsed": last_run,
                "ui_url": ui_url,
            }
        )

    if json_output:
        return data

    console.table(
        columns=["DAG ID", "Active", "Paused", "Last Parsed", "UI URL"],
        rows=rows,
        title="Airflow DAGs",
    )
    return None
