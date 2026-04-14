from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor
from typing import List, Optional

import requests

from paradime.cli import console
from paradime.core.scripts.utils import handle_http_error


def trigger_airbyte_jobs(
    *,
    client_id: str,
    client_secret: str,
    connection_ids: List[str],
    job_type: str,
    workspace_id: Optional[str] = None,
    wait_for_completion: bool = True,
    timeout_minutes: int = 1440,
    base_url: str = "https://api.airbyte.com/v1",
    use_cloud_auth: bool = True,
) -> List[str]:
    """
    Trigger jobs for multiple Airbyte connections.

    Args:
        client_id: Airbyte client ID (or username for server)
        client_secret: Airbyte client secret (or password for server)
        connection_ids: List of Airbyte connection IDs
        job_type: Type of job ('sync' or 'reset')
        workspace_id: Optional workspace ID
        wait_for_completion: Whether to wait for jobs to complete
        timeout_minutes: Maximum time to wait for completion
        base_url: Airbyte API base URL (default: Airbyte Cloud)
        use_cloud_auth: Whether to use cloud authentication (OAuth) or server auth (basic)

    Returns:
        List of job result messages for each connection
    """
    # Get authentication headers
    if use_cloud_auth:
        access_token = _get_access_token(client_id, client_secret, base_url)
        auth_headers = {"Authorization": f"Bearer {access_token}"}
    else:
        auth_headers = {"Authorization": f"Bearer {client_id}:{client_secret}"}

    futures = []
    results = []

    unique_ids = list(set(connection_ids))

    with ThreadPoolExecutor() as executor:
        for connection_id in unique_ids:
            futures.append(
                (
                    connection_id,
                    executor.submit(
                        trigger_connection_job,
                        auth_headers=auth_headers,
                        connection_id=connection_id,
                        job_type=job_type,
                        workspace_id=workspace_id,
                        wait_for_completion=wait_for_completion,
                        timeout_minutes=timeout_minutes,
                        base_url=base_url,
                    ),
                )
            )

        connection_results = []
        for connection_id, future in futures:
            future_timeout = (timeout_minutes * 60 + 120) if wait_for_completion else 120
            response_txt = future.result(timeout=future_timeout)
            connection_results.append((connection_id, response_txt))
            results.append(response_txt)

    def _status_text(resp: str) -> str:
        if "SUCCESS" in resp:
            return "✓ Success"
        if "FAILED" in resp:
            return "✗ Failed"
        if "CANCELLED" in resp:
            return "✗ Cancelled"
        if "INCOMPLETE" in resp:
            return "⚠ Incomplete"
        if "RUNNING" in resp:
            return "Running"
        return "Completed"

    console.table(
        columns=["Connection", "Status", "Job Type"],
        rows=[
            (conn_id, _status_text(resp), job_type.upper()) for conn_id, resp in connection_results
        ],
        title="Job Results",
    )

    return results


def trigger_connection_job(
    *,
    auth_headers: dict,
    connection_id: str,
    job_type: str,
    workspace_id: Optional[str] = None,
    wait_for_completion: bool = True,
    timeout_minutes: int = 1440,
    base_url: str = "https://api.airbyte.com/v1",
) -> str:
    """
    Trigger job for a single Airbyte connection.

    Args:
        auth_headers: Authentication headers (Bearer token or Basic auth)
        connection_id: Airbyte connection ID
        job_type: Type of job ('sync' or 'reset')
        workspace_id: Optional workspace ID
        wait_for_completion: Whether to wait for job to complete
        timeout_minutes: Maximum time to wait for completion
        base_url: Airbyte API base URL

    Returns:
        Status message indicating job result
    """
    headers = {**auth_headers, "Content-Type": "application/json"}

    if job_type not in ["sync", "reset"]:
        raise ValueError(f"Invalid job type: {job_type}. Must be 'sync' or 'reset'")

    console.debug(f"[{connection_id}] Checking connection status…")
    try:
        connection_response = requests.get(
            f"{base_url}/connections/{connection_id}",
            headers=headers,
        )

        if connection_response.status_code == 200:
            connection_data = connection_response.json()
            connection_status = connection_data.get("status", "unknown")
            console.debug(f"[{connection_id}] Status: {connection_status}")

            if connection_status != "active":
                console.debug(f"[{connection_id}] Warning: Connection is {connection_status}")

    except Exception as e:
        console.debug(f"[{connection_id}] Could not check status: {str(e)[:50]}… Proceeding.")

    job_payload: dict = {
        "connectionId": connection_id,
        "jobType": job_type,
    }

    if workspace_id:
        job_payload["workspaceId"] = workspace_id

    console.debug(f"[{connection_id}] Triggering {job_type} job…")
    job_response = requests.post(
        f"{base_url}/jobs",
        json=job_payload,
        headers=headers,
    )

    handle_http_error(
        job_response,
        f"Error triggering {job_type} job for connection '{connection_id}':",
    )

    job_data = job_response.json()
    job_id = job_data.get("jobId")

    console.debug(f"[{connection_id}] Job triggered successfully (ID: {job_id})")

    if not wait_for_completion:
        return f"{job_type.upper()} job triggered (ID: {job_id})"

    console.debug(f"[{connection_id}] Monitoring job progress…")

    job_status = _wait_for_job_completion(
        auth_headers=auth_headers,
        job_id=job_id,
        connection_id=connection_id,
        timeout_minutes=timeout_minutes,
        base_url=base_url,
    )

    return f"Job completed. Final status: {job_status}"


def _get_access_token(
    client_id: str, client_secret: str, base_url: str = "https://api.airbyte.com/v1"
) -> str:
    """
    Get access token using client credentials (Airbyte Cloud only).

    Args:
        client_id: Airbyte client ID
        client_secret: Airbyte client secret
        base_url: Airbyte API base URL

    Returns:
        Access token for API calls
    """
    token_url = f"{base_url}/applications/token"

    payload = {
        "client_id": client_id,
        "client_secret": client_secret,
    }

    headers = {"Content-Type": "application/json"}

    response = requests.post(token_url, json=payload, headers=headers)

    handle_http_error(response, "Error getting access token:")

    token_data = response.json()
    return token_data.get("access_token")


def _wait_for_job_completion(
    *,
    auth_headers: dict,
    job_id: str,
    connection_id: str,
    timeout_minutes: int,
    base_url: str,
) -> str:
    """
    Poll job status until completion or timeout.

    Args:
        auth_headers: Authentication headers (Bearer token or Basic auth)
        job_id: Airbyte job ID
        connection_id: Connection ID for logging
        timeout_minutes: Maximum time to wait for completion
        base_url: Airbyte API base URL

    Returns:
        Final job status
    """
    headers = {**auth_headers, "Content-Type": "application/json"}

    start_time = time.time()
    timeout_seconds = timeout_minutes * 60
    sleep_interval = 5
    counter = 0
    consecutive_failures = 0
    max_consecutive_failures = 5

    while True:
        elapsed = time.time() - start_time
        if elapsed > timeout_seconds:
            raise Exception(
                f"Timeout waiting for job '{job_id}' to complete after {timeout_minutes} minutes"
            )

        try:
            job_response = requests.get(
                f"{base_url}/jobs/{job_id}",
                headers=headers,
            )

            if job_response.status_code != 200:
                consecutive_failures += 1
                if consecutive_failures >= max_consecutive_failures:
                    raise Exception(
                        f"Job status check failed {consecutive_failures} times in a row. "
                        f"Last HTTP status: {job_response.status_code}"
                    )
                console.debug(
                    f"[{connection_id}] HTTP {job_response.status_code} error. "
                    f"Retrying… ({consecutive_failures}/{max_consecutive_failures})"
                )
                time.sleep(sleep_interval * min(consecutive_failures, 3))
                continue

            job_data = job_response.json()
            job_status = job_data.get("status", "unknown")
            consecutive_failures = 0

            elapsed_min = int(elapsed // 60)
            elapsed_sec = int(elapsed % 60)
            if counter == 0 or counter % 6 == 0:
                if job_status in ("running", "pending"):
                    console.debug(
                        f"[{connection_id}] Job {job_status}… ({elapsed_min}m {elapsed_sec}s elapsed)"
                    )

            if job_status in ["succeeded", "failed", "cancelled", "incomplete"]:
                if job_status == "succeeded":
                    console.debug(
                        f"[{connection_id}] Job completed successfully "
                        f"({elapsed_min}m {elapsed_sec}s)"
                    )
                    return f"SUCCESS (completed at job ID: {job_id})"
                elif job_status == "failed":
                    console.debug(f"[{connection_id}] Job failed")
                    return f"FAILED (job ID: {job_id})"
                elif job_status == "cancelled":
                    console.debug(f"[{connection_id}] Job cancelled")
                    return f"CANCELLED (job ID: {job_id})"
                elif job_status == "incomplete":
                    console.debug(f"[{connection_id}] Job incomplete")
                    return f"INCOMPLETE (job ID: {job_id})"

            counter += 1
            time.sleep(sleep_interval)

        except requests.exceptions.RequestException as e:
            consecutive_failures += 1
            if consecutive_failures >= max_consecutive_failures:
                raise Exception(
                    f"Network errors occurred {consecutive_failures} times in a row. "
                    f"Last error: {str(e)[:100]}"
                )
            console.debug(
                f"[{connection_id}] Network error: {str(e)[:50]}… "
                f"Retrying… ({consecutive_failures}/{max_consecutive_failures})"
            )
            time.sleep(sleep_interval * min(consecutive_failures, 3))
            continue


def list_airbyte_connections(
    *,
    client_id: str,
    client_secret: str,
    workspace_id: Optional[str] = None,
    base_url: str = "https://api.airbyte.com/v1",
    use_cloud_auth: bool = True,
    json_output: bool = False,
) -> list | None:
    """
    List all Airbyte connections with their IDs and status.

    Args:
        client_id: Airbyte client ID (or username for server)
        client_secret: Airbyte client secret (or password for server)
        workspace_id: Optional workspace ID to filter connections
        base_url: Airbyte API base URL (default: Airbyte Cloud)
        use_cloud_auth: Whether to use cloud authentication (OAuth) or server auth (basic)
        json_output: Whether to return data as a list of dicts instead of printing a table
    """
    if use_cloud_auth:
        access_token = _get_access_token(client_id, client_secret, base_url)
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        }
    else:
        headers = {
            "Authorization": f"Bearer {client_id}:{client_secret}",
            "Content-Type": "application/json",
        }

    url = f"{base_url}/connections"
    params = {}

    if workspace_id:
        params["workspaceId"] = workspace_id

    connections_response = requests.get(url, headers=headers, params=params)

    handle_http_error(connections_response, "Error getting connections:")

    connections_data = connections_response.json()

    if "data" not in connections_data:
        if not json_output:
            console.info("No connections found.")
        return [] if json_output else None

    connections = connections_data["data"]

    if json_output:
        return [
            {
                "id": conn.get("connectionId", "Unknown"),
                "name": conn.get("name", "Unknown"),
                "status": conn.get("status", "Unknown"),
                "source_id": conn.get("sourceId", "Unknown"),
                "destination_id": conn.get("destinationId", "Unknown"),
            }
            for conn in connections
        ]

    console.table(
        columns=["ID", "Name", "Status", "Source ID", "Destination ID"],
        rows=[
            (
                conn.get("connectionId", "Unknown"),
                conn.get("name", "Unknown"),
                conn.get("status", "Unknown"),
                conn.get("sourceId", "Unknown"),
                conn.get("destinationId", "Unknown"),
            )
            for conn in connections
        ],
        title=f"Airbyte Connections ({len(connections)} found)",
    )
    return None
