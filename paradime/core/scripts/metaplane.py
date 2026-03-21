from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor
from typing import List

import requests

from paradime.cli import console
from paradime.core.scripts.utils import handle_http_error


def trigger_metaplane_monitors(
    *,
    api_key: str,
    monitor_ids: List[str],
    wait: bool = True,
    timeout_minutes: int = 1440,
) -> List[str]:
    """
    Trigger runs for multiple Metaplane monitors.

    Args:
        api_key: Metaplane API key
        monitor_ids: List of Metaplane monitor IDs to trigger
        wait: Whether to wait for monitor runs to complete
        timeout_minutes: Maximum time to wait for completion

    Returns:
        List of result status keywords for each monitor run
    """
    base_url = "https://api.metaplane.dev/api/v1"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    unique_ids = list(set(monitor_ids))

    # Trigger all monitors in a single API call
    console.debug(f"Triggering {len(unique_ids)} monitor(s)...")
    trigger_response = requests.post(
        f"{base_url}/monitors/trigger",
        json={"monitorIds": unique_ids},
        headers=headers,
    )

    handle_http_error(trigger_response, "Error triggering Metaplane monitors:")

    trigger_data = trigger_response.json()
    runs = trigger_data.get("runs", [])

    if not runs:
        console.warning("No runs returned from Metaplane trigger.")
        return []

    console.info(f"Triggered {len(runs)} monitor run(s).")

    if not wait:
        console.table(
            columns=["Monitor ID", "Run ID"],
            rows=[(r.get("monitorId", ""), r.get("runId", "")) for r in runs],
            title="Triggered Monitor Runs",
        )
        return [f"TRIGGERED (runId={r.get('runId', '')})" for r in runs]

    # Poll each run in parallel
    futures = []
    results: List[str] = []

    with ThreadPoolExecutor() as executor:
        for run_info in runs:
            run_id = run_info.get("runId", "")
            monitor_id = run_info.get("monitorId", "")
            futures.append(
                (
                    monitor_id,
                    run_id,
                    executor.submit(
                        _wait_for_run_completion,
                        api_key=api_key,
                        run_id=run_id,
                        monitor_id=monitor_id,
                        timeout_minutes=timeout_minutes,
                    ),
                )
            )

        run_results = []
        for monitor_id, run_id, future in futures:
            future_timeout = timeout_minutes * 60 + 120
            result = future.result(timeout=future_timeout)
            run_results.append((monitor_id, run_id, result))
            results.append(result)

    def _status_text(result: str) -> str:
        if "SUCCESS" in result:
            return "SUCCESS"
        elif "FAILED" in result:
            return "FAILED"
        else:
            return "UNKNOWN"

    console.table(
        columns=["Monitor ID", "Status", "Result"],
        rows=[
            (mid, _status_text(result), result)
            for mid, _rid, result in run_results
        ],
        title="Monitor Run Results",
    )

    return results


def _wait_for_run_completion(
    *,
    api_key: str,
    run_id: str,
    monitor_id: str,
    timeout_minutes: int,
) -> str:
    """
    Poll a monitor run status until completion or timeout.

    Args:
        api_key: Metaplane API key
        run_id: The run ID to poll
        monitor_id: The monitor ID (for logging)
        timeout_minutes: Maximum time to wait for completion

    Returns:
        Status keyword: SUCCESS or FAILED
    """
    base_url = "https://api.metaplane.dev/api/v1"
    headers = {
        "Authorization": f"Bearer {api_key}",
    }
    start_time = time.time()
    timeout_seconds = timeout_minutes * 60
    sleep_interval = 5
    counter = 0

    while True:
        elapsed = time.time() - start_time
        if elapsed > timeout_seconds:
            raise Exception(
                f"Timeout waiting for monitor '{monitor_id}' run '{run_id}' to complete "
                f"after {timeout_minutes} minutes"
            )

        try:
            run_response = requests.get(
                f"{base_url}/monitors/runs/{run_id}",
                headers=headers,
            )

            if run_response.status_code != 200:
                console.debug(
                    f"[{monitor_id}] HTTP {run_response.status_code} checking run status. Retrying..."
                )
                time.sleep(sleep_interval)
                continue

            run_data = run_response.json()
            status = run_data.get("status", "unknown")
            result = run_data.get("result")

            # Log progress every 6 checks (~30 seconds)
            if counter == 0 or counter % 6 == 0:
                elapsed_min = int(elapsed // 60)
                elapsed_sec = int(elapsed % 60)
                if status == "running":
                    console.debug(
                        f"[{monitor_id}] Running... ({elapsed_min}m {elapsed_sec}s elapsed)"
                    )

            if status == "completed":
                elapsed_min = int(elapsed // 60)
                elapsed_sec = int(elapsed % 60)
                if result == "pass":
                    console.debug(
                        f"[{monitor_id}] Completed — pass ({elapsed_min}m {elapsed_sec}s)"
                    )
                    return "SUCCESS"
                else:
                    console.debug(
                        f"[{monitor_id}] Completed — {result} ({elapsed_min}m {elapsed_sec}s)"
                    )
                    return f"FAILED (result={result})"

            elif status == "failed":
                console.error(f"[{monitor_id}] Run failed")
                return "FAILED"

            counter += 1
            time.sleep(sleep_interval)

        except requests.exceptions.RequestException as e:
            console.debug(f"[{monitor_id}] Network error: {str(e)[:50]}... Retrying.")
            time.sleep(sleep_interval)
            continue


def list_metaplane_monitors(
    *,
    api_key: str,
    json_output: bool = False,
) -> list | None:
    """
    List all Metaplane monitors with their IDs and status.

    Args:
        api_key: Metaplane API key
        json_output: Whether to return data as a list of dicts instead of printing a table
    """
    base_url = "https://api.metaplane.dev/api/v1"
    headers = {
        "Authorization": f"Bearer {api_key}",
    }

    monitors_response = requests.get(
        f"{base_url}/monitors",
        headers=headers,
    )

    handle_http_error(monitors_response, "Error listing Metaplane monitors:")

    monitors_data = monitors_response.json()
    monitors = monitors_data.get("monitors", [])

    if not monitors:
        if not json_output:
            console.info("No monitors found.")
        return [] if json_output else None

    rows = []
    data = []
    for monitor in monitors:
        monitor_id = monitor.get("id", "Unknown")
        name = monitor.get("name", "Unknown")
        monitor_type = monitor.get("type", "Unknown")
        status = monitor.get("status", "Unknown")

        rows.append((monitor_id, name, monitor_type, status))
        data.append(
            {
                "monitor_id": monitor_id,
                "name": name,
                "type": monitor_type,
                "status": status,
            }
        )

    if json_output:
        return data

    console.table(
        columns=["Monitor ID", "Name", "Type", "Status"],
        rows=rows,
        title="Metaplane Monitors",
    )
    return None
