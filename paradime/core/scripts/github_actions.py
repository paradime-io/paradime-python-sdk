from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests

from paradime.cli import console
from paradime.core.scripts.utils import handle_http_error

BASE_URL = "https://api.github.com"


def _get_headers(token: str) -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }


def trigger_github_workflows(
    *,
    token: str,
    repo: str,
    workflow_ids: List[str],
    ref: str = "main",
    inputs: Optional[Dict[str, Any]] = None,
    wait: bool = True,
    timeout_minutes: int = 1440,
) -> List[str]:
    """
    Trigger multiple GitHub Actions workflow dispatches.

    Args:
        token: GitHub personal access token or fine-grained token
        repo: GitHub repository in 'owner/repo' format
        workflow_ids: List of workflow IDs or filenames to trigger
        ref: Git reference (branch/tag) to trigger the workflow on
        inputs: Optional workflow inputs
        wait: Whether to wait for workflow runs to complete
        timeout_minutes: Maximum time to wait in minutes

    Returns:
        List of status keywords: SUCCESS, FAILED, CANCELLED
    """
    futures = []
    results = []

    with ThreadPoolExecutor() as executor:
        for i, workflow_id in enumerate(set(workflow_ids), 1):
            futures.append(
                (
                    workflow_id,
                    executor.submit(
                        _trigger_single_workflow,
                        token=token,
                        repo=repo,
                        workflow_id=workflow_id,
                        ref=ref,
                        inputs=inputs,
                        wait=wait,
                        timeout_minutes=timeout_minutes,
                    ),
                )
            )

        workflow_results = []
        for workflow_id, future in futures:
            future_timeout = (timeout_minutes * 60 + 120) if wait else 120
            response_txt = future.result(timeout=future_timeout)
            workflow_results.append((workflow_id, response_txt))
            results.append(response_txt)

        def _status_text(response_txt: str) -> str:
            if "SUCCESS" in response_txt:
                return "SUCCESS"
            elif "FAILED" in response_txt:
                return "FAILED"
            elif "CANCELLED" in response_txt:
                return "CANCELLED"
            else:
                return "COMPLETED"

        console.table(
            columns=["Workflow", "Status", "URL"],
            rows=[
                (
                    wid,
                    _status_text(response_txt),
                    f"https://github.com/{repo}/actions",
                )
                for wid, response_txt in workflow_results
            ],
            title="Workflow Dispatch Results",
        )

    return results


def _trigger_single_workflow(
    *,
    token: str,
    repo: str,
    workflow_id: str,
    ref: str = "main",
    inputs: Optional[Dict[str, Any]] = None,
    wait: bool = True,
    timeout_minutes: int = 1440,
) -> str:
    """Trigger a single GitHub Actions workflow dispatch and optionally wait for completion."""
    headers = _get_headers(token)

    payload: Dict[str, Any] = {"ref": ref}
    if inputs:
        payload["inputs"] = inputs

    dispatch_time = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    console.debug(f"[{workflow_id}] Triggering workflow dispatch on ref '{ref}'...")

    dispatch_response = requests.post(
        f"{BASE_URL}/repos/{repo}/actions/workflows/{workflow_id}/dispatches",
        headers=headers,
        json=payload,
    )

    handle_http_error(
        dispatch_response,
        f"Error triggering workflow dispatch for '{workflow_id}':",
    )

    console.debug(f"[{workflow_id}] Workflow dispatch triggered successfully")

    if not wait:
        return f"Workflow dispatch triggered for '{workflow_id}'"

    console.debug(f"[{workflow_id}] Waiting for workflow run to appear...")

    run_id = _find_triggered_run(
        token=token,
        repo=repo,
        workflow_id=workflow_id,
        dispatch_time=dispatch_time,
        timeout_seconds=120,
    )

    if run_id is None:
        console.debug(
            f"[{workflow_id}] Could not find the triggered run. "
            "Workflow was dispatched but run tracking is unavailable."
        )
        return f"Workflow dispatch triggered for '{workflow_id}' (run not found for tracking)"

    run_url = f"https://github.com/{repo}/actions/runs/{run_id}"
    console.debug(f"[{workflow_id}] Found run {run_id}: {run_url}")

    status = _wait_for_run_completion(
        token=token,
        repo=repo,
        run_id=run_id,
        workflow_id=workflow_id,
        timeout_minutes=timeout_minutes,
    )

    return f"Workflow run completed. Final status: {status}"


def _find_triggered_run(
    *,
    token: str,
    repo: str,
    workflow_id: str,
    dispatch_time: str,
    timeout_seconds: int = 120,
) -> Optional[int]:
    """Poll the workflow runs endpoint to find the run triggered by our dispatch."""
    headers = _get_headers(token)
    start_time = time.time()
    sleep_interval = 5

    while True:
        elapsed = time.time() - start_time
        if elapsed > timeout_seconds:
            return None

        try:
            response = requests.get(
                f"{BASE_URL}/repos/{repo}/actions/workflows/{workflow_id}/runs",
                headers=headers,
                params={"per_page": 5, "event": "workflow_dispatch"},
            )

            if response.status_code == 200:
                runs = response.json().get("workflow_runs", [])
                for run in runs:
                    created_at = run.get("created_at", "")
                    if created_at >= dispatch_time:
                        return run.get("id")

        except requests.exceptions.RequestException:
            pass

        time.sleep(sleep_interval)


def _wait_for_run_completion(
    *,
    token: str,
    repo: str,
    run_id: int,
    workflow_id: str,
    timeout_minutes: int,
) -> str:
    """Poll a workflow run until it completes or times out."""
    headers = _get_headers(token)
    start_time = time.time()
    timeout_seconds = timeout_minutes * 60
    sleep_interval = 10
    counter = 0

    while True:
        elapsed = time.time() - start_time
        if elapsed > timeout_seconds:
            raise Exception(
                f"Timeout waiting for workflow '{workflow_id}' run {run_id} to complete "
                f"after {timeout_minutes} minutes"
            )

        try:
            response = requests.get(
                f"{BASE_URL}/repos/{repo}/actions/runs/{run_id}",
                headers=headers,
            )

            if response.status_code != 200:
                raise Exception(
                    f"Run status check failed with HTTP {response.status_code}"
                )

            run_data = response.json()
            status = run_data.get("status", "unknown")
            conclusion = run_data.get("conclusion")

            elapsed_min = int(elapsed // 60)
            elapsed_sec = int(elapsed % 60)

            if counter == 0 or counter % 6 == 0:
                if status == "in_progress":
                    console.debug(
                        f"[{workflow_id}] In progress... "
                        f"({elapsed_min}m {elapsed_sec}s elapsed)"
                    )
                elif status == "queued":
                    console.debug(
                        f"[{workflow_id}] Queued... "
                        f"({elapsed_min}m {elapsed_sec}s elapsed)"
                    )

            if status == "completed":
                run_url = f"https://github.com/{repo}/actions/runs/{run_id}"

                if conclusion == "success":
                    console.debug(
                        f"[{workflow_id}] Completed successfully "
                        f"({elapsed_min}m {elapsed_sec}s)"
                    )
                    return f"SUCCESS (run {run_id}: {run_url})"
                elif conclusion == "failure":
                    console.error(f"[{workflow_id}] Workflow run failed")
                    return f"FAILED (run {run_id}: {run_url})"
                elif conclusion == "cancelled":
                    console.debug(f"[{workflow_id}] Workflow run was cancelled")
                    return f"CANCELLED (run {run_id}: {run_url})"
                else:
                    console.debug(
                        f"[{workflow_id}] Completed with conclusion: {conclusion}"
                    )
                    return f"FAILED (run {run_id}, conclusion: {conclusion})"

            counter += 1
            time.sleep(sleep_interval)

        except requests.exceptions.RequestException as e:
            console.debug(f"[{workflow_id}] Network error: {str(e)[:50]}... Retrying.")
            time.sleep(sleep_interval)
            continue


def list_github_workflows(
    *,
    token: str,
    repo: str,
    json_output: bool = False,
) -> list | None:
    """List all GitHub Actions workflows in a repository."""
    headers = _get_headers(token)

    response = requests.get(
        f"{BASE_URL}/repos/{repo}/actions/workflows",
        headers=headers,
    )

    handle_http_error(response, "Error listing workflows:")

    data = response.json()
    workflows = data.get("workflows", [])

    if not workflows:
        if not json_output:
            console.info("No workflows found.")
        return [] if json_output else None

    rows = []
    result_data = []
    for workflow in workflows:
        wf_id = str(workflow.get("id", "Unknown"))
        name = workflow.get("name", "Unknown")
        state = workflow.get("state", "Unknown")
        path = workflow.get("path", "Unknown")
        rows.append((wf_id, name, state, path))
        result_data.append(
            {
                "workflow_id": wf_id,
                "name": name,
                "state": state,
                "path": path,
            }
        )

    if json_output:
        return result_data

    console.table(
        columns=["Workflow ID", "Name", "State", "Path"],
        rows=rows,
        title="GitHub Actions Workflows",
    )
    return None
