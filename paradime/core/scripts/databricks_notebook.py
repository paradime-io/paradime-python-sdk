from __future__ import annotations

import json
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Optional

import requests

from paradime.cli import console
from paradime.core.scripts.utils import handle_http_error


def trigger_databricks_notebooks(
    *,
    host: str,
    token: str,
    notebook_paths: List[str],
    cluster_id: str,
    parameters: Optional[str] = None,
    wait: bool = True,
    timeout_minutes: int = 1440,
) -> List[str]:
    """
    Trigger notebook runs on Databricks via the runs/submit API.

    Args:
        host: Databricks workspace URL (e.g. https://adb-xxxx.azuredatabricks.net)
        token: Databricks personal access token
        notebook_paths: List of notebook paths to run
        cluster_id: Existing cluster ID to run notebooks on
        parameters: Optional JSON string of base parameters for the notebooks
        wait: Whether to wait for notebook runs to complete
        timeout_minutes: Maximum time to wait in minutes

    Returns:
        List of status keywords: SUCCESS, FAILED, CANCELLED, TIMEDOUT
    """
    base_url = host.rstrip("/")

    # Parse parameters
    base_params: Dict[str, str] = {}
    if parameters:
        try:
            base_params = json.loads(parameters)
        except json.JSONDecodeError as e:
            raise Exception(f"Invalid JSON parameters: {e}")

    futures = []
    results: List[str] = []

    with ThreadPoolExecutor() as executor:
        for notebook_path in notebook_paths:
            futures.append(
                (
                    notebook_path,
                    executor.submit(
                        _run_notebook,
                        base_url=base_url,
                        token=token,
                        notebook_path=notebook_path,
                        cluster_id=cluster_id,
                        base_params=base_params,
                        wait=wait,
                        timeout_minutes=timeout_minutes,
                    ),
                )
            )

        notebook_results = []
        for notebook_path, future in futures:
            future_timeout = (timeout_minutes * 60 + 120) if wait else 120
            result = future.result(timeout=future_timeout)
            notebook_results.append((notebook_path, result["status"], result["run_url"]))
            results.append(result["status"])

        console.table(
            columns=["Notebook", "Status", "Run URL"],
            rows=[
                (path, status, run_url)
                for path, status, run_url in notebook_results
            ],
            title="Notebook Run Results",
        )

    return results


def _run_notebook(
    *,
    base_url: str,
    token: str,
    notebook_path: str,
    cluster_id: str,
    base_params: Dict[str, str],
    wait: bool,
    timeout_minutes: int,
) -> Dict[str, str]:
    """
    Submit and optionally wait for a single notebook run.

    Returns:
        Dict with 'status' and 'run_url' keys.
    """
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    # Build the submit payload
    payload: Dict = {
        "run_name": f"paradime-{notebook_path.split('/')[-1]}",
        "existing_cluster_id": cluster_id,
        "notebook_task": {
            "notebook_path": notebook_path,
        },
    }
    if base_params:
        payload["notebook_task"]["base_parameters"] = base_params

    # Submit the run
    console.debug(f"[{notebook_path}] Submitting notebook run...")
    submit_response = requests.post(
        f"{base_url}/api/2.1/jobs/runs/submit",
        headers=headers,
        json=payload,
    )
    handle_http_error(submit_response, f"Error submitting notebook run for '{notebook_path}':")

    run_id = submit_response.json().get("run_id")
    if not run_id:
        raise Exception(f"No run_id returned for notebook '{notebook_path}'")

    console.debug(f"[{notebook_path}] Submitted run_id={run_id}")

    # Get the run page URL
    run_url = _get_run_url(base_url=base_url, token=token, run_id=run_id)
    console.debug(f"[{notebook_path}] Run URL: {run_url}")

    if not wait:
        return {"status": "TRIGGERED", "run_url": run_url}

    console.debug(f"[{notebook_path}] Waiting for completion...")
    status = _wait_for_run(
        base_url=base_url,
        token=token,
        run_id=run_id,
        notebook_path=notebook_path,
        timeout_minutes=timeout_minutes,
    )
    return {"status": status, "run_url": run_url}


def _get_run_url(*, base_url: str, token: str, run_id: int) -> str:
    """Fetch the run_page_url for a given run_id."""
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    response = requests.get(
        f"{base_url}/api/2.1/jobs/runs/get",
        headers=headers,
        params={"run_id": run_id},
    )
    if response.status_code == 200:
        return response.json().get("run_page_url", f"{base_url}/#job/run/{run_id}")
    return f"{base_url}/#job/run/{run_id}"


def _wait_for_run(
    *,
    base_url: str,
    token: str,
    run_id: int,
    notebook_path: str,
    timeout_minutes: int,
) -> str:
    """
    Poll the Databricks runs/get API until the run reaches a terminal state.

    Returns:
        Status keyword: SUCCESS, FAILED, CANCELLED, TIMEDOUT
    """
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    start_time = time.time()
    timeout_seconds = timeout_minutes * 60
    sleep_interval = 5
    counter = 0

    while True:
        elapsed = time.time() - start_time
        if elapsed > timeout_seconds:
            console.debug(f"[{notebook_path}] Timed out after {timeout_minutes} minutes")
            return "TIMEDOUT"

        try:
            response = requests.get(
                f"{base_url}/api/2.1/jobs/runs/get",
                headers=headers,
                params={"run_id": run_id},
            )

            if response.status_code != 200:
                console.debug(
                    f"[{notebook_path}] Status check returned HTTP {response.status_code}, retrying..."
                )
                time.sleep(sleep_interval)
                counter += 1
                continue

            run_data = response.json()
            state = run_data.get("state", {})
            life_cycle_state = state.get("life_cycle_state", "UNKNOWN")
            result_state = state.get("result_state")

            # Log progress periodically (every ~30 seconds)
            if counter == 0 or counter % 6 == 0:
                elapsed_min = int(elapsed // 60)
                elapsed_sec = int(elapsed % 60)
                console.debug(
                    f"[{notebook_path}] State: {life_cycle_state} "
                    f"({elapsed_min}m {elapsed_sec}s elapsed)"
                )

            # Terminal states
            if life_cycle_state in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
                elapsed_min = int(elapsed // 60)
                elapsed_sec = int(elapsed % 60)

                if result_state == "SUCCESS":
                    console.debug(
                        f"[{notebook_path}] Completed successfully "
                        f"({elapsed_min}m {elapsed_sec}s)"
                    )
                    return "SUCCESS"
                elif result_state == "CANCELED":
                    console.debug(f"[{notebook_path}] Run was cancelled")
                    return "CANCELLED"
                elif result_state == "TIMEDOUT":
                    console.debug(f"[{notebook_path}] Run timed out on Databricks side")
                    return "TIMEDOUT"
                else:
                    # FAILED or INTERNAL_ERROR
                    state_message = state.get("state_message", "")
                    console.debug(
                        f"[{notebook_path}] Run failed: {result_state} — {state_message}"
                    )
                    return "FAILED"

        except requests.exceptions.RequestException as e:
            console.debug(f"[{notebook_path}] Network error: {str(e)[:80]}... Retrying.")

        counter += 1
        time.sleep(sleep_interval)
