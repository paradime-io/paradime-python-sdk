from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import List, Optional

import requests

from paradime.cli import console
from paradime.core.scripts.utils import handle_http_error


def trigger_databricks_workflows(
    *,
    host: str,
    token: str,
    job_ids: List[str],
    wait_for_completion: bool = True,
    timeout_minutes: int = 1440,
) -> List[str]:
    """
    Trigger Databricks workflow (job) runs for multiple jobs.

    Args:
        host: Databricks workspace URL (e.g. https://adb-xxxx.azuredatabricks.net)
        token: Databricks personal access token
        job_ids: List of Databricks job IDs to trigger
        wait_for_completion: Whether to wait for runs to complete
        timeout_minutes: Maximum time to wait for completion

    Returns:
        List of run result messages for each job
    """
    host = host.rstrip("/")
    futures = []
    results = []

    with ThreadPoolExecutor() as executor:
        for i, job_id in enumerate(set(job_ids), 1):
            futures.append(
                (
                    job_id,
                    executor.submit(
                        _trigger_single_workflow,
                        host=host,
                        token=token,
                        job_id=job_id,
                        wait_for_completion=wait_for_completion,
                        timeout_minutes=timeout_minutes,
                    ),
                )
            )

        # Wait for completion and collect results
        job_results = []
        for job_id, future in futures:
            future_timeout = (timeout_minutes * 60 + 120) if wait_for_completion else 120
            response_txt = future.result(timeout=future_timeout)
            job_results.append((job_id, response_txt))
            results.append(response_txt)

        def _status_text(response_txt: str) -> str:
            if "SUCCESS" in response_txt:
                return "SUCCESS"
            elif "FAILED" in response_txt:
                return "FAILED"
            elif "CANCELLED" in response_txt:
                return "CANCELLED"
            elif "TIMEDOUT" in response_txt:
                return "TIMEDOUT"
            elif "RUNNING" in response_txt:
                return "RUNNING"
            else:
                return "COMPLETED"

        def _run_url(response_txt: str) -> str:
            # Extract run URL from response text if present
            for part in response_txt.split():
                if part.startswith("http"):
                    return part
            return ""

        console.table(
            columns=["Job ID", "Status", "Run URL"],
            rows=[
                (job_id, _status_text(response_txt), _run_url(response_txt))
                for job_id, response_txt in job_results
            ],
            title="Workflow Run Results",
        )

    return results


def _trigger_single_workflow(
    *,
    host: str,
    token: str,
    job_id: str,
    wait_for_completion: bool = True,
    timeout_minutes: int = 1440,
) -> str:
    """
    Trigger a single Databricks workflow (job) run.

    Args:
        host: Databricks workspace URL
        token: Databricks personal access token
        job_id: Databricks job ID to trigger
        wait_for_completion: Whether to wait for run to complete
        timeout_minutes: Maximum time to wait for completion

    Returns:
        Status message indicating run result
    """
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    # Trigger the job run
    console.debug(f"[{job_id}] Triggering workflow run...")
    run_response = requests.post(
        f"{host}/api/2.1/jobs/run-now",
        json={"job_id": int(job_id)},
        headers=headers,
    )

    handle_http_error(
        run_response,
        f"Error triggering workflow run for job '{job_id}':",
    )

    run_data = run_response.json()
    run_id = run_data.get("run_id")

    console.debug(f"[{job_id}] Workflow run triggered (Run ID: {run_id})")

    # Get run page URL
    run_page_url = _get_run_page_url(
        host=host,
        token=token,
        run_id=run_id,
    )
    if run_page_url:
        console.debug(f"[{job_id}] Run URL: {run_page_url}")

    if not wait_for_completion:
        return f"Workflow triggered (Run ID: {run_id}) {run_page_url or ''}"

    console.debug(f"[{job_id}] Monitoring workflow run progress...")

    # Wait for run completion
    run_status = _wait_for_run_completion(
        host=host,
        token=token,
        job_id=job_id,
        run_id=run_id,
        timeout_minutes=timeout_minutes,
    )

    return f"Workflow completed. Final status: {run_status} {run_page_url or ''}"


def _get_run_page_url(
    *,
    host: str,
    token: str,
    run_id: int,
) -> Optional[str]:
    """
    Fetch the run_page_url for a given run ID.

    Args:
        host: Databricks workspace URL
        token: Databricks personal access token
        run_id: Run ID

    Returns:
        The run page URL or None if unavailable
    """
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    try:
        response = requests.get(
            f"{host}/api/2.1/jobs/runs/get",
            params={"run_id": run_id},
            headers=headers,
        )
        if response.status_code == 200:
            return response.json().get("run_page_url")
    except Exception:
        pass
    return None


def _wait_for_run_completion(
    *,
    host: str,
    token: str,
    job_id: str,
    run_id: int,
    timeout_minutes: int,
) -> str:
    """
    Poll run status until completion or timeout.

    Args:
        host: Databricks workspace URL
        token: Databricks personal access token
        job_id: Job ID for logging
        run_id: Run ID to monitor
        timeout_minutes: Maximum time to wait for completion

    Returns:
        Final run status keyword (SUCCESS, FAILED, CANCELLED, TIMEDOUT)
    """
    headers = {
        "Authorization": f"Bearer {token}",
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
                f"Timeout waiting for job '{job_id}' run {run_id} to complete "
                f"after {timeout_minutes} minutes"
            )

        try:
            run_response = requests.get(
                f"{host}/api/2.1/jobs/runs/get",
                params={"run_id": run_id},
                headers=headers,
            )

            if run_response.status_code != 200:
                raise Exception(
                    f"Run status check failed with HTTP {run_response.status_code}"
                )

            run_data = run_response.json()
            state = run_data.get("state", {})
            life_cycle_state = state.get("life_cycle_state", "UNKNOWN")
            result_state = state.get("result_state")

            elapsed_min = int(elapsed // 60)
            elapsed_sec = int(elapsed % 60)

            # Log progress every 6 checks (~30 seconds)
            if counter == 0 or counter % 6 == 0:
                if life_cycle_state in ("PENDING", "RUNNING", "TERMINATING"):
                    console.debug(
                        f"[{job_id}] {life_cycle_state}... ({elapsed_min}m {elapsed_sec}s elapsed)"
                    )

            # Check if run has terminated
            if life_cycle_state == "TERMINATED":
                elapsed_min = int(elapsed // 60)
                elapsed_sec = int(elapsed % 60)

                if result_state == "SUCCESS":
                    console.debug(
                        f"[{job_id}] Completed successfully ({elapsed_min}m {elapsed_sec}s)"
                    )
                    return "SUCCESS"
                elif result_state == "FAILED":
                    state_message = state.get("state_message", "No details available")
                    console.debug(f"[{job_id}] Run failed: {state_message}")
                    return "FAILED"
                elif result_state == "TIMEDOUT":
                    console.debug(f"[{job_id}] Run timed out")
                    return "TIMEDOUT"
                elif result_state == "CANCELED":
                    console.debug(f"[{job_id}] Run was cancelled")
                    return "CANCELLED"
                elif result_state == "MAXIMUM_CONCURRENT_RUNS_REACHED":
                    console.debug(f"[{job_id}] Maximum concurrent runs reached")
                    return "FAILED"
                else:
                    console.debug(f"[{job_id}] Terminated with result: {result_state}")
                    return result_state or "COMPLETED"

            elif life_cycle_state == "SKIPPED":
                console.debug(f"[{job_id}] Run was skipped")
                return "FAILED"

            elif life_cycle_state == "INTERNAL_ERROR":
                state_message = state.get("state_message", "No details available")
                console.debug(f"[{job_id}] Internal error: {state_message}")
                return "FAILED"

            counter += 1
            time.sleep(sleep_interval)

        except requests.exceptions.RequestException as e:
            console.debug(f"[{job_id}] Network error: {str(e)[:50]}... Retrying.")
            time.sleep(sleep_interval)
            continue


def list_databricks_jobs(
    *,
    host: str,
    token: str,
    json_output: bool = False,
) -> list | None:
    """
    List all Databricks jobs.

    Args:
        host: Databricks workspace URL
        token: Databricks personal access token
        json_output: Whether to return data as a list of dicts instead of printing a table

    Returns:
        List of job dicts when json_output=True, else None
    """
    host = host.rstrip("/")

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    if not json_output:
        console.info("Listing Databricks jobs...")

    jobs_response = requests.get(
        f"{host}/api/2.1/jobs/list",
        headers=headers,
    )

    handle_http_error(jobs_response, "Error listing Databricks jobs:")

    jobs_data = jobs_response.json()
    jobs = jobs_data.get("jobs", [])

    if not jobs:
        if not json_output:
            console.info("No jobs found.")
        return [] if json_output else None

    rows = []
    data = []
    for job in jobs:
        job_id = str(job.get("job_id", "Unknown"))
        name = job.get("settings", {}).get("name", "Unknown")
        created_time = job.get("created_time")
        if created_time:
            # Databricks returns created_time in milliseconds
            created_str = datetime.fromtimestamp(created_time / 1000).strftime(
                "%Y-%m-%d %H:%M:%S"
            )
        else:
            created_str = "Unknown"

        rows.append((job_id, name, created_str))
        data.append(
            {
                "job_id": job_id,
                "name": name,
                "created_time": created_str,
            }
        )

    if json_output:
        return data

    console.table(
        columns=["Job ID", "Name", "Created"],
        rows=rows,
        title="Databricks Jobs",
    )
    return None
