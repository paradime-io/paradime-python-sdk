from __future__ import annotations

import itertools
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests
from rich.live import Live
from rich.text import Text

from paradime.cli import console
from paradime.core.scripts.utils import handle_http_error

BASE_URL = "https://api.github.com"

# Braille spinner frames
_BRAILLE_FRAMES = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]


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


def _fetch_jobs(*, headers: Dict[str, str], repo: str, run_id: int) -> List[Dict[str, Any]]:
    """Fetch jobs (with steps) for a workflow run."""
    try:
        response = requests.get(
            f"{BASE_URL}/repos/{repo}/actions/runs/{run_id}/jobs",
            headers=headers,
        )
        if response.status_code == 200:
            return response.json().get("jobs", [])
    except requests.exceptions.RequestException:
        pass
    return []


def _build_step_display(
    *,
    jobs: List[Dict[str, Any]],
    workflow_id: str,
    spinner_frame: str,
) -> Text:
    """Build a Rich Text renderable showing all job steps with status icons."""
    lines = Text()

    for job in jobs:
        job_name = job.get("name", "unknown")
        job_status = job.get("status", "queued")
        job_conclusion = job.get("conclusion")

        steps = job.get("steps", [])
        if not steps:
            icon = _step_icon(job_status, job_conclusion, spinner_frame)
            lines.append("  ")
            lines.append_text(icon)
            lines.append(f"  {job_name}\n")
            continue

        for step in steps:
            step_name = step.get("name", "unknown")
            status = step.get("status", "queued")
            conclusion = step.get("conclusion")

            # Skip queued steps that haven't started
            if status == "queued":
                continue

            icon = _step_icon(status, conclusion, spinner_frame)
            lines.append("  ")
            lines.append_text(icon)
            lines.append(f"  {job_name} > {step_name}\n")

    return lines


def _step_icon(status: str, conclusion: Optional[str], spinner_frame: str) -> Text:
    """Return a Rich Text icon for a step's status/conclusion."""
    if status == "completed":
        if conclusion == "success":
            return Text("✓", style="bold green")
        elif conclusion == "failure":
            return Text("✗", style="bold red")
        elif conclusion in ("skipped", "cancelled"):
            return Text("⊘", style="dim")
        else:
            return Text("?", style="bold yellow")
    elif status == "in_progress":
        return Text(spinner_frame, style="cyan")
    else:
        return Text("○", style="dim")


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
    api_interval = 10
    spinner_interval = 0.1
    spinner = itertools.cycle(_BRAILLE_FRAMES)

    # Shared state between API polling and Live rendering
    current_jobs: List[Dict[str, Any]] = []
    run_completed = threading.Event()
    run_result: List[str] = []  # single-element list to pass result from thread
    lock = threading.Lock()

    def _poll_api() -> None:
        """Background thread that polls the GitHub API."""
        nonlocal current_jobs
        counter = 0

        while not run_completed.is_set():
            elapsed = time.time() - start_time
            if elapsed > timeout_seconds:
                with lock:
                    run_result.append("TIMEOUT")
                run_completed.set()
                return

            try:
                response = requests.get(
                    f"{BASE_URL}/repos/{repo}/actions/runs/{run_id}",
                    headers=headers,
                )

                if response.status_code != 200:
                    with lock:
                        run_result.append("ERROR")
                    run_completed.set()
                    return

                run_data = response.json()
                status = run_data.get("status", "unknown")
                conclusion = run_data.get("conclusion")

                # Fetch jobs
                jobs = _fetch_jobs(headers=headers, repo=repo, run_id=run_id)
                with lock:
                    current_jobs = jobs

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
                            f"[{workflow_id}] Queued... " f"({elapsed_min}m {elapsed_sec}s elapsed)"
                        )

                if status == "completed":
                    # Final fetch
                    final_jobs = _fetch_jobs(headers=headers, repo=repo, run_id=run_id)
                    with lock:
                        current_jobs = final_jobs

                    run_url = f"https://github.com/{repo}/actions/runs/{run_id}"
                    if conclusion == "success":
                        with lock:
                            run_result.append(f"SUCCESS (run {run_id}: {run_url})")
                    elif conclusion == "failure":
                        with lock:
                            run_result.append(f"FAILED (run {run_id}: {run_url})")
                    elif conclusion == "cancelled":
                        with lock:
                            run_result.append(f"CANCELLED (run {run_id}: {run_url})")
                    else:
                        with lock:
                            run_result.append(f"FAILED (run {run_id}, conclusion: {conclusion})")
                    run_completed.set()
                    return

                counter += 1

            except requests.exceptions.RequestException as e:
                console.debug(f"[{workflow_id}] Network error: {str(e)[:50]}... Retrying.")

            # Sleep in small increments so we can exit quickly
            for _ in range(int(api_interval / 0.5)):
                if run_completed.is_set():
                    return
                time.sleep(0.5)

    poll_thread = threading.Thread(target=_poll_api, daemon=True)
    poll_thread.start()

    with Live(
        Text("  Waiting for workflow to start..."), console=console.console, refresh_per_second=10
    ) as live:
        while not run_completed.is_set():
            frame = next(spinner)
            with lock:
                jobs_snapshot = list(current_jobs)

            if jobs_snapshot:
                display = _build_step_display(
                    jobs=jobs_snapshot,
                    workflow_id=workflow_id,
                    spinner_frame=frame,
                )
                live.update(display)
            else:
                waiting = Text()
                waiting.append(f"  {frame}", style="cyan")
                waiting.append("  Waiting for workflow to start...")
                live.update(waiting)

            time.sleep(spinner_interval)

    poll_thread.join(timeout=5)

    # Print final static output of all steps
    if current_jobs:
        final_display = _build_step_display(
            jobs=current_jobs,
            workflow_id=workflow_id,
            spinner_frame=" ",
        )
        console.console.print(final_display)

    if not run_result:
        raise Exception(
            f"Timeout waiting for workflow '{workflow_id}' run {run_id} to complete "
            f"after {timeout_minutes} minutes"
        )

    result = run_result[0]

    if result == "TIMEOUT":
        raise Exception(
            f"Timeout waiting for workflow '{workflow_id}' run {run_id} to complete "
            f"after {timeout_minutes} minutes"
        )
    elif result == "ERROR":
        raise Exception(f"Run status check failed for workflow '{workflow_id}' run {run_id}")

    if "FAILED" in result:
        console.error(f"\\[{workflow_id}] Workflow run failed")

    return result


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
