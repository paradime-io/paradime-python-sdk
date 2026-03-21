from __future__ import annotations

import base64
import time
from concurrent.futures import ThreadPoolExecutor
from typing import List, Optional

import requests

from paradime.cli import console
from paradime.core.scripts.utils import handle_http_error


def trigger_azure_pipelines(
    *,
    organization: str,
    project: str,
    pat: str,
    pipeline_ids: List[str],
    branch: Optional[str] = None,
    wait_for_completion: bool = True,
    timeout_minutes: int = 1440,
) -> List[str]:
    """
    Trigger runs for multiple Azure DevOps Pipelines.

    Args:
        organization: Azure DevOps organization name
        project: Azure DevOps project name
        pat: Personal Access Token for authentication
        pipeline_ids: List of pipeline IDs to trigger
        branch: Optional branch name to run the pipeline on
        wait_for_completion: Whether to wait for runs to complete
        timeout_minutes: Maximum time to wait for completion

    Returns:
        List of run result messages for each pipeline
    """
    auth_headers = _get_auth_headers(pat)

    futures = []
    results = []

    with ThreadPoolExecutor() as executor:
        for i, pipeline_id in enumerate(set(pipeline_ids), 1):
            futures.append(
                (
                    pipeline_id,
                    executor.submit(
                        trigger_pipeline_run,
                        auth_headers=auth_headers,
                        organization=organization,
                        project=project,
                        pipeline_id=pipeline_id,
                        branch=branch,
                        wait_for_completion=wait_for_completion,
                        timeout_minutes=timeout_minutes,
                    ),
                )
            )

        pipeline_results = []
        for pipeline_id, future in futures:
            future_timeout = (timeout_minutes * 60 + 120) if wait_for_completion else 120
            response_txt = future.result(timeout=future_timeout)
            pipeline_results.append((pipeline_id, response_txt))
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
            columns=["Pipeline", "Status", "URL"],
            rows=[
                (
                    pid,
                    _status_text(response_txt),
                    f"https://dev.azure.com/{organization}/{project}/_build?definitionId={pid}",
                )
                for pid, response_txt in pipeline_results
            ],
            title="Pipeline Run Results",
        )

    return results


def trigger_pipeline_run(
    *,
    auth_headers: dict,
    organization: str,
    project: str,
    pipeline_id: str,
    branch: Optional[str] = None,
    wait_for_completion: bool = True,
    timeout_minutes: int = 1440,
) -> str:
    """
    Trigger a run for a single Azure DevOps Pipeline.

    Args:
        auth_headers: Authentication headers (Basic auth)
        organization: Azure DevOps organization name
        project: Azure DevOps project name
        pipeline_id: Pipeline ID to trigger
        branch: Optional branch name to run the pipeline on
        wait_for_completion: Whether to wait for run to complete
        timeout_minutes: Maximum time to wait for completion

    Returns:
        Status message indicating run result
    """
    base_url = f"https://dev.azure.com/{organization}/{project}/_apis/pipelines"
    api_version = "7.1"

    body: dict = {}
    if branch:
        ref_name = branch if branch.startswith("refs/") else f"refs/heads/{branch}"
        body = {"resources": {"repositories": {"self": {"refName": ref_name}}}}

    run_url = f"{base_url}/{pipeline_id}/runs?api-version={api_version}"

    console.debug(f"[{pipeline_id}] Triggering pipeline run...")

    run_response = requests.post(
        run_url,
        json=body,
        headers=auth_headers,
    )

    handle_http_error(
        run_response,
        f"Error triggering pipeline run for pipeline '{pipeline_id}':",
    )

    run_data = run_response.json()
    run_id = run_data.get("id")
    run_state = run_data.get("state", "unknown")

    console.debug(f"[{pipeline_id}] Pipeline run triggered (Run ID: {run_id}, State: {run_state})")

    portal_url = (
        f"https://dev.azure.com/{organization}/{project}/_build/results?buildId={run_id}"
    )
    console.debug(f"[{pipeline_id}] Portal: {portal_url}")

    if not wait_for_completion:
        return f"Pipeline run triggered (Run ID: {run_id})"

    console.debug(f"[{pipeline_id}] Monitoring pipeline run progress...")

    run_status = _wait_for_pipeline_completion(
        auth_headers=auth_headers,
        organization=organization,
        project=project,
        pipeline_id=pipeline_id,
        run_id=run_id,
        timeout_minutes=timeout_minutes,
    )

    return f"Pipeline run completed. Final status: {run_status}"


def _get_auth_headers(pat: str) -> dict:
    """Build authentication headers for Azure DevOps REST API."""
    credentials = base64.b64encode(f":{pat}".encode("utf-8")).decode("utf-8")
    return {
        "Authorization": f"Basic {credentials}",
        "Content-Type": "application/json",
    }


def _wait_for_pipeline_completion(
    *,
    auth_headers: dict,
    organization: str,
    project: str,
    pipeline_id: str,
    run_id: int,
    timeout_minutes: int,
) -> str:
    """Poll pipeline run status until completion or timeout."""
    base_url = f"https://dev.azure.com/{organization}/{project}/_apis/pipelines"
    api_version = "7.1"
    start_time = time.time()
    timeout_seconds = timeout_minutes * 60
    sleep_interval = 5
    counter = 0

    run_url = f"{base_url}/{pipeline_id}/runs/{run_id}?api-version={api_version}"

    while True:
        elapsed = time.time() - start_time
        if elapsed > timeout_seconds:
            raise Exception(
                f"Timeout waiting for pipeline '{pipeline_id}' run '{run_id}' "
                f"to complete after {timeout_minutes} minutes"
            )

        try:
            run_response = requests.get(run_url, headers=auth_headers)

            if run_response.status_code != 200:
                console.debug(
                    f"[{pipeline_id}] HTTP {run_response.status_code} error. Retrying..."
                )
                time.sleep(sleep_interval)
                continue

            run_data = run_response.json()
            run_state = run_data.get("state", "unknown")
            run_result = run_data.get("result")

            if counter == 0 or counter % 6 == 0:
                elapsed_min = int(elapsed // 60)
                elapsed_sec = int(elapsed % 60)
                if run_state == "inProgress":
                    console.debug(
                        f"[{pipeline_id}] Pipeline running... "
                        f"({elapsed_min}m {elapsed_sec}s elapsed)"
                    )

            if run_state == "completed":
                elapsed_min = int(elapsed // 60)
                elapsed_sec = int(elapsed % 60)

                if run_result == "succeeded":
                    console.debug(
                        f"[{pipeline_id}] Completed successfully ({elapsed_min}m {elapsed_sec}s)"
                    )
                    return f"SUCCESS (run ID: {run_id})"
                elif run_result == "failed":
                    console.error(f"[{pipeline_id}] Pipeline failed")
                    return f"FAILED (run ID: {run_id})"
                elif run_result == "canceled":
                    console.debug(f"[{pipeline_id}] Pipeline cancelled")
                    return f"CANCELLED (run ID: {run_id})"
                else:
                    console.debug(
                        f"[{pipeline_id}] Completed with result: {run_result} "
                        f"({elapsed_min}m {elapsed_sec}s)"
                    )
                    return f"COMPLETED (result: {run_result}, run ID: {run_id})"

            counter += 1
            time.sleep(sleep_interval)

        except requests.exceptions.RequestException as e:
            console.debug(f"[{pipeline_id}] Network error: {str(e)[:50]}... Retrying.")
            time.sleep(sleep_interval)
            continue


def list_azure_pipelines(
    *,
    organization: str,
    project: str,
    pat: str,
    json_output: bool = False,
) -> list | None:
    """List all pipelines in an Azure DevOps project."""
    auth_headers = _get_auth_headers(pat)
    base_url = f"https://dev.azure.com/{organization}/{project}/_apis/pipelines"
    api_version = "7.1"

    pipelines_url = f"{base_url}?api-version={api_version}"

    pipelines_response = requests.get(pipelines_url, headers=auth_headers)
    handle_http_error(pipelines_response, "Error getting pipelines:")

    pipelines_data = pipelines_response.json()

    if "value" not in pipelines_data or not pipelines_data["value"]:
        if not json_output:
            console.info("No pipelines found.")
        return [] if json_output else None

    pipelines = pipelines_data["value"]

    rows = []
    data = []
    for pipeline in pipelines:
        pipeline_id = str(pipeline.get("id", "Unknown"))
        name = pipeline.get("name", "Unknown")
        folder = pipeline.get("folder", "\\")
        portal_url = (
            f"https://dev.azure.com/{organization}/{project}/_build?definitionId={pipeline_id}"
        )
        rows.append((pipeline_id, name, folder, portal_url))
        data.append(
            {
                "pipeline_id": pipeline_id,
                "name": name,
                "folder": folder,
                "url": portal_url,
            }
        )

    if json_output:
        return data

    console.table(
        columns=["Pipeline ID", "Name", "Folder", "URL"],
        rows=rows,
        title="Azure DevOps Pipelines",
    )
    return None
