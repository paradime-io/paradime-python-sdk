import logging
import time
from concurrent.futures import ThreadPoolExecutor
from typing import List, Optional

import requests

from paradime.core.scripts.utils import handle_http_error

logging.basicConfig(format="%(asctime)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)


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
        # Cloud: OAuth 2.0 - get access token using client credentials
        access_token = _get_access_token(client_id, client_secret, base_url)
        auth_headers = {"Authorization": f"Bearer {access_token}"}
    else:
        # Server: Use client_id and client_secret directly as bearer token
        # For Airbyte Server, client_id is typically the API key and client_secret is the secret
        auth_headers = {"Authorization": f"Bearer {client_id}:{client_secret}"}

    futures = []
    results = []

    # Add visual separator and header
    print(f"\n{'='*60}")
    print("🚀 TRIGGERING AIRBYTE JOBS")
    print(f"{'='*60}")

    with ThreadPoolExecutor() as executor:
        for i, connection_id in enumerate(set(connection_ids), 1):
            print(f"\n[{i}/{len(set(connection_ids))}] 🔌 {connection_id}")
            print(f"{'-'*40}")

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

        # Add separator for live progress section
        print(f"\n{'='*60}")
        print("⚡ LIVE PROGRESS")
        print(f"{'='*60}")

        # Wait for completion and collect results
        connection_results = []
        for connection_id, future in futures:
            # Use longer timeout when waiting for completion
            future_timeout = (timeout_minutes * 60 + 120) if wait_for_completion else 120
            response_txt = future.result(timeout=future_timeout)
            connection_results.append((connection_id, response_txt))
            results.append(response_txt)

        # Display results as simple table
        print(f"\n{'='*80}")
        print("📊 JOB RESULTS")
        print(f"{'='*80}")
        print(f"{'CONNECTION':<25} {'STATUS':<10} {'JOB TYPE':<10}")
        print(f"{'-'*25} {'-'*10} {'-'*10}")

        for connection_id, response_txt in connection_results:
            # Format result with emoji
            if "SUCCESS" in response_txt:
                status = "✅ SUCCESS"
            elif "FAILED" in response_txt:
                status = "❌ FAILED"
            elif "CANCELLED" in response_txt:
                status = "🚫 CANCELLED"
            elif "INCOMPLETE" in response_txt:
                status = "⚠️ INCOMPLETE"
            elif "RUNNING" in response_txt:
                status = "🔄 RUNNING"
            else:
                status = "ℹ️ COMPLETED"

            print(f"{connection_id:<25} {status:<10} {job_type.upper()}")

        print(f"{'='*80}\n")

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

    import datetime

    timestamp = datetime.datetime.now().strftime("%H:%M:%S")

    # Validate job type
    if job_type not in ["sync", "reset"]:
        raise ValueError(f"Invalid job type: {job_type}. Must be 'sync' or 'reset'")

    # Check connection status before attempting job
    print(f"{timestamp} 🔍 [{connection_id}] Checking connection status...")
    try:
        connection_response = requests.get(
            f"{base_url}/connections/{connection_id}",
            headers=headers,
        )

        if connection_response.status_code == 200:
            connection_data = connection_response.json()
            connection_status = connection_data.get("status", "unknown")

            print(f"{timestamp} 📊 [{connection_id}] Status: {connection_status}")

            # Handle inactive connections
            if connection_status != "active":
                print(
                    f"{timestamp} ⚠️  [{connection_id}] Warning: Connection is {connection_status}"
                )

    except Exception as e:
        print(
            f"{timestamp} ⚠️  [{connection_id}] Could not check status: {str(e)[:50]}... Proceeding anyway."
        )

    # Prepare job payload
    job_payload = {
        "connectionId": connection_id,
        "jobType": job_type,
    }

    if workspace_id:
        job_payload["workspaceId"] = workspace_id

    print(f"{timestamp} 🚀 [{connection_id}] Triggering {job_type} job...")
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

    print(f"{timestamp} ✅ [{connection_id}] Job triggered successfully (ID: {job_id})")

    if not wait_for_completion:
        return f"{job_type.upper()} job triggered (ID: {job_id})"

    print(f"{timestamp} ⏳ [{connection_id}] Monitoring job progress...")

    # Wait for job completion
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
            # Get job status
            job_response = requests.get(
                f"{base_url}/jobs/{job_id}",
                headers=headers,
            )

            if job_response.status_code != 200:
                consecutive_failures += 1
                if consecutive_failures >= max_consecutive_failures:
                    raise Exception(
                        f"Job status check failed {consecutive_failures} times in a row. Last HTTP status: {job_response.status_code}"
                    )

                import datetime

                timestamp = datetime.datetime.now().strftime("%H:%M:%S")
                print(
                    f"{timestamp} ⚠️  [{connection_id}] HTTP {job_response.status_code} error. Retrying... ({consecutive_failures}/{max_consecutive_failures})"
                )
                time.sleep(
                    sleep_interval * min(consecutive_failures, 3)
                )  # Exponential backoff up to 3x
                continue

            job_data = job_response.json()
            job_status = job_data.get("status", "unknown")

            # Reset failure counter on successful request
            consecutive_failures = 0

            # Log progress every 6 checks (1 minute)
            if counter == 0 or counter % 6 == 0:
                import datetime

                timestamp = datetime.datetime.now().strftime("%H:%M:%S")
                elapsed_min = int(elapsed // 60)
                elapsed_sec = int(elapsed % 60)

                if job_status == "running":
                    print(
                        f"{timestamp} 🔄 [{connection_id}] Job running... ({elapsed_min}m {elapsed_sec}s elapsed)"
                    )
                elif job_status == "pending":
                    print(
                        f"{timestamp} ⏳ [{connection_id}] Job pending... ({elapsed_min}m {elapsed_sec}s elapsed)"
                    )

            # Check if job is complete
            if job_status in ["succeeded", "failed", "cancelled", "incomplete"]:
                import datetime

                timestamp = datetime.datetime.now().strftime("%H:%M:%S")
                elapsed_min = int(elapsed // 60)
                elapsed_sec = int(elapsed % 60)

                if job_status == "succeeded":
                    print(
                        f"{timestamp} ✅ [{connection_id}] Job completed successfully ({elapsed_min}m {elapsed_sec}s)"
                    )
                    return f"SUCCESS (completed at job ID: {job_id})"
                elif job_status == "failed":
                    print(f"{timestamp} ❌ [{connection_id}] Job failed")
                    return f"FAILED (job ID: {job_id})"
                elif job_status == "cancelled":
                    print(f"{timestamp} 🚫 [{connection_id}] Job cancelled")
                    return f"CANCELLED (job ID: {job_id})"
                elif job_status == "incomplete":
                    print(f"{timestamp} ⚠️ [{connection_id}] Job incomplete")
                    return f"INCOMPLETE (job ID: {job_id})"

            elif job_status in ["running", "pending"]:
                # Still running, continue waiting
                pass
            else:
                # Continue waiting for unknown states
                pass

            counter += 1
            time.sleep(sleep_interval)

        except requests.exceptions.RequestException as e:
            consecutive_failures += 1
            if consecutive_failures >= max_consecutive_failures:
                raise Exception(
                    f"Network errors occurred {consecutive_failures} times in a row. Last error: {str(e)[:100]}"
                )

            import datetime

            timestamp = datetime.datetime.now().strftime("%H:%M:%S")
            print(
                f"{timestamp} ⚠️  [{connection_id}] Network error: {str(e)[:50]}... Retrying... ({consecutive_failures}/{max_consecutive_failures})"
            )
            time.sleep(
                sleep_interval * min(consecutive_failures, 3)
            )  # Exponential backoff up to 3x
            continue


def list_airbyte_connections(
    *,
    client_id: str,
    client_secret: str,
    workspace_id: Optional[str] = None,
    base_url: str = "https://api.airbyte.com/v1",
    use_cloud_auth: bool = True,
) -> None:
    """
    List all Airbyte connections with their IDs and status.

    Args:
        client_id: Airbyte client ID (or username for server)
        client_secret: Airbyte client secret (or password for server)
        workspace_id: Optional workspace ID to filter connections
        base_url: Airbyte API base URL (default: Airbyte Cloud)
        use_cloud_auth: Whether to use cloud authentication (OAuth) or server auth (basic)
    """
    # Get authentication headers
    if use_cloud_auth:
        # Cloud: OAuth 2.0 - get access token using client credentials
        access_token = _get_access_token(client_id, client_secret, base_url)
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        }
    else:
        # Server: Use client_id and client_secret directly as bearer token
        # For Airbyte Server, client_id is typically the API key and client_secret is the secret
        headers = {
            "Authorization": f"Bearer {client_id}:{client_secret}",
            "Content-Type": "application/json",
        }

    # Build URL and parameters
    url = f"{base_url}/connections"
    params = {}

    if workspace_id:
        params["workspaceId"] = workspace_id
        print(f"\n🔍 Listing connections for workspace: {workspace_id}")
    else:
        print("\n🔍 Listing all connections")

    connections_response = requests.get(url, headers=headers, params=params)

    handle_http_error(connections_response, "Error getting connections:")

    connections_data = connections_response.json()

    if "data" not in connections_data:
        print("No connections found.")
        return

    connections = connections_data["data"]

    print(f"\n{'='*80}")
    print(f"📋 FOUND {len(connections)} CONNECTION(S)")
    print(f"{'='*80}")

    for i, connection in enumerate(connections, 1):
        connection_id = connection.get("connectionId", "Unknown")
        name = connection.get("name", "Unknown")
        status = connection.get("status", "Unknown")

        source_id = connection.get("sourceId", "Unknown")
        destination_id = connection.get("destinationId", "Unknown")

        # Format status with emoji
        status_emoji = (
            "✅"
            if status == "active"
            else "⏸️" if status == "inactive" else "❌" if status == "deprecated" else "❓"
        )

        print(f"\n[{i}/{len(connections)}] 🔌 {connection_id}")
        print(f"{'-'*50}")
        print(f"   Name: {name}")
        print(f"   {status_emoji} Status: {status}")
        print(f"   Source ID: {source_id}")
        print(f"   Destination ID: {destination_id}")

    print(f"\n{'='*80}\n")
