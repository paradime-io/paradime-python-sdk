import time
from concurrent.futures import ThreadPoolExecutor
from typing import List

import requests

from paradime.cli import console
from paradime.core.scripts.utils import handle_http_error

HIGHTOUCH_BASE_URL = "https://api.hightouch.com/api/v1"


def _get_auth_headers(api_token: str) -> dict:
    """
    Get authentication headers using the provided API token.

    Args:
        api_token: Hightouch API token.

    Returns:
        Dictionary with Authorization header.
    """
    return {
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/json",
    }


def trigger_hightouch_syncs(
    *,
    api_token: str,
    sync_ids: List[str],
    full_resync: bool = False,
    wait_for_completion: bool = True,
    timeout_minutes: int = 1440,
) -> List[str]:
    """
    Trigger syncs for multiple Hightouch syncs.

    Args:
        api_token: Hightouch API token.
        sync_ids: List of Hightouch sync IDs to trigger.
        full_resync: Whether to resync all rows (ignoring previously synced rows).
        wait_for_completion: Whether to wait for syncs to complete.
        timeout_minutes: Maximum time to wait for completion.

    Returns:
        List of sync result messages for each sync.
    """
    auth_headers = _get_auth_headers(api_token)
    futures = []
    results = []

    with ThreadPoolExecutor() as executor:
        for i, sync_id in enumerate(set(sync_ids), 1):
            futures.append(
                (
                    sync_id,
                    executor.submit(
                        trigger_single_sync,
                        auth_headers=auth_headers,
                        sync_id=sync_id,
                        full_resync=full_resync,
                        wait_for_completion=wait_for_completion,
                        timeout_minutes=timeout_minutes,
                    ),
                )
            )

        sync_results = []
        for sync_id, future in futures:
            future_timeout = (timeout_minutes * 60 + 120) if wait_for_completion else 120
            response_txt = future.result(timeout=future_timeout)
            sync_results.append((sync_id, response_txt))
            results.append(response_txt)

        def _status_text(response_txt: str) -> str:
            if "SUCCESS" in response_txt:
                return "SUCCESS"
            elif "FAILED" in response_txt:
                return "FAILED"
            elif "CANCELLED" in response_txt:
                return "CANCELLED"
            elif "WARNING" in response_txt:
                return "WARNING"
            elif "INTERRUPTED" in response_txt:
                return "INTERRUPTED"
            elif "ABORTED" in response_txt:
                return "ABORTED"
            else:
                return "COMPLETED"

        console.table(
            columns=["Sync ID", "Status"],
            rows=[(sid, _status_text(response_txt)) for sid, response_txt in sync_results],
            title="Sync Results",
        )

    return results


def trigger_single_sync(
    *,
    auth_headers: dict,
    sync_id: str,
    full_resync: bool = False,
    wait_for_completion: bool = True,
    timeout_minutes: int = 1440,
) -> str:
    """
    Trigger a single Hightouch sync.

    Args:
        auth_headers: Authentication headers (Bearer token).
        sync_id: Hightouch sync ID.
        full_resync: Whether to resync all rows.
        wait_for_completion: Whether to wait for sync to complete.
        timeout_minutes: Maximum time to wait for completion.

    Returns:
        Status message indicating sync result.
    """

    console.debug(f"[{sync_id}] Triggering sync (full_resync={full_resync})...")

    trigger_payload = {"fullResync": full_resync}

    trigger_response = requests.post(
        f"{HIGHTOUCH_BASE_URL}/syncs/{sync_id}/trigger",
        json=trigger_payload,
        headers=auth_headers,
    )

    handle_http_error(
        trigger_response,
        f"Error triggering sync '{sync_id}':",
    )

    trigger_data = trigger_response.json()
    sync_request_id = trigger_data.get("id")

    console.debug(f"[{sync_id}] Sync triggered successfully (request ID: {sync_request_id})")

    if not wait_for_completion:
        return f"Sync triggered (request ID: {sync_request_id})"

    console.debug(f"[{sync_id}] Monitoring sync progress...")

    sync_status = _wait_for_sync_completion(
        auth_headers=auth_headers,
        sync_id=sync_id,
        timeout_minutes=timeout_minutes,
    )

    return f"Sync completed. Final status: {sync_status}"


def _wait_for_sync_completion(
    *,
    auth_headers: dict,
    sync_id: str,
    timeout_minutes: int,
) -> str:
    """
    Poll sync status until completion or timeout.

    Args:
        auth_headers: Authentication headers (Bearer token).
        sync_id: Hightouch sync ID.
        timeout_minutes: Maximum time to wait for completion.

    Returns:
        Final sync status.
    """
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
                f"Timeout waiting for sync '{sync_id}' to complete after {timeout_minutes} minutes"
            )

        try:
            runs_response = requests.get(
                f"{HIGHTOUCH_BASE_URL}/syncs/{sync_id}/runs",
                headers=auth_headers,
                params={"limit": "1", "orderBy": "createdAt"},
            )

            if runs_response.status_code != 200:
                consecutive_failures += 1
                if consecutive_failures >= max_consecutive_failures:
                    raise Exception(
                        f"Sync status check failed {consecutive_failures} times in a row. "
                        f"Last HTTP status: {runs_response.status_code}"
                    )

                console.debug(
                    f"[{sync_id}] HTTP {runs_response.status_code} error. "
                    f"Retrying... ({consecutive_failures}/{max_consecutive_failures})"
                )
                time.sleep(sleep_interval * min(consecutive_failures, 3))
                continue

            runs_data = runs_response.json()

            # Response is a list of sync runs
            if isinstance(runs_data, list) and len(runs_data) > 0:
                latest_run = runs_data[0]
            elif isinstance(runs_data, dict) and "data" in runs_data:
                runs_list = runs_data["data"]
                if len(runs_list) > 0:
                    latest_run = runs_list[0]
                else:
                    time.sleep(sleep_interval)
                    counter += 1
                    continue
            else:
                time.sleep(sleep_interval)
                counter += 1
                continue

            run_status = latest_run.get("status", "unknown")
            consecutive_failures = 0

            if counter == 0 or counter % 6 == 0:

                elapsed_min = int(elapsed // 60)
                elapsed_sec = int(elapsed % 60)

                if run_status in ["processing", "queued"]:
                    console.debug(
                        f"[{sync_id}] Status: {run_status}... "
                        f"({elapsed_min}m {elapsed_sec}s elapsed)"
                    )

            if run_status in [
                "success",
                "failed",
                "cancelled",
                "warning",
                "interrupted",
                "aborted",
            ]:

                elapsed_min = int(elapsed // 60)
                elapsed_sec = int(elapsed % 60)

                if run_status == "success":
                    console.debug(
                        f"[{sync_id}] Sync completed successfully "
                        f"({elapsed_min}m {elapsed_sec}s)"
                    )
                    return "SUCCESS"
                elif run_status == "warning":
                    console.debug(
                        f"[{sync_id}] Sync completed with warnings "
                        f"({elapsed_min}m {elapsed_sec}s)"
                    )
                    return "WARNING"
                elif run_status == "failed":
                    console.error(f"[{sync_id}] Sync failed")
                    return "FAILED"
                elif run_status == "cancelled":
                    console.debug(f"[{sync_id}] Sync cancelled")
                    return "CANCELLED"
                elif run_status == "interrupted":
                    console.debug(f"[{sync_id}] Sync interrupted")
                    return "INTERRUPTED"
                elif run_status == "aborted":
                    console.debug(f"[{sync_id}] Sync aborted")
                    return "ABORTED"

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
                f"[{sync_id}] Network error: {str(e)[:50]}... "
                f"Retrying... ({consecutive_failures}/{max_consecutive_failures})"
            )
            time.sleep(sleep_interval * min(consecutive_failures, 3))
            continue


def trigger_hightouch_sync_sequences(
    *,
    api_token: str,
    sync_sequence_ids: List[str],
    wait_for_completion: bool = True,
    timeout_minutes: int = 1440,
) -> List[str]:
    """
    Trigger runs for multiple Hightouch sync sequences.

    Args:
        api_token: Hightouch API token.
        sync_sequence_ids: List of Hightouch sync sequence IDs to trigger.
        wait_for_completion: Whether to wait for sequences to complete.
        timeout_minutes: Maximum time to wait for completion.

    Returns:
        List of sequence result messages.
    """
    auth_headers = _get_auth_headers(api_token)
    futures = []
    results = []

    with ThreadPoolExecutor() as executor:
        for i, sequence_id in enumerate(set(sync_sequence_ids), 1):
            futures.append(
                (
                    sequence_id,
                    executor.submit(
                        trigger_single_sync_sequence,
                        auth_headers=auth_headers,
                        sync_sequence_id=sequence_id,
                        wait_for_completion=wait_for_completion,
                        timeout_minutes=timeout_minutes,
                    ),
                )
            )

        sequence_results = []
        for sequence_id, future in futures:
            future_timeout = (timeout_minutes * 60 + 120) if wait_for_completion else 120
            response_txt = future.result(timeout=future_timeout)
            sequence_results.append((sequence_id, response_txt))
            results.append(response_txt)

        def _status_text(response_txt: str) -> str:
            if "SUCCESS" in response_txt:
                return "SUCCESS"
            elif "FAILED" in response_txt:
                return "FAILED"
            elif "CANCELLED" in response_txt:
                return "CANCELLED"
            elif "WARNING" in response_txt:
                return "WARNING"
            elif "INTERRUPTED" in response_txt:
                return "INTERRUPTED"
            elif "ABORTED" in response_txt:
                return "ABORTED"
            else:
                return "COMPLETED"

        console.table(
            columns=["Sequence ID", "Status"],
            rows=[(sid, _status_text(response_txt)) for sid, response_txt in sequence_results],
            title="Sequence Results",
        )

    return results


def trigger_single_sync_sequence(
    *,
    auth_headers: dict,
    sync_sequence_id: str,
    wait_for_completion: bool = True,
    timeout_minutes: int = 1440,
) -> str:
    """
    Trigger a single Hightouch sync sequence.

    Args:
        auth_headers: Authentication headers (Bearer token).
        sync_sequence_id: Hightouch sync sequence ID.
        wait_for_completion: Whether to wait for sequence to complete.
        timeout_minutes: Maximum time to wait for completion.

    Returns:
        Status message indicating sequence result.
    """

    console.debug(f"[{sync_sequence_id}] Triggering sync sequence...")

    trigger_response = requests.post(
        f"{HIGHTOUCH_BASE_URL}/sync-sequences/{sync_sequence_id}/trigger",
        headers=auth_headers,
    )

    handle_http_error(
        trigger_response,
        f"Error triggering sync sequence '{sync_sequence_id}':",
    )

    trigger_data = trigger_response.json()
    sequence_run_id = trigger_data.get("id")

    console.debug(
        f"[{sync_sequence_id}] Sync sequence triggered successfully " f"(run ID: {sequence_run_id})"
    )

    if not wait_for_completion:
        return f"Sync sequence triggered (run ID: {sequence_run_id})"

    console.debug(f"[{sync_sequence_id}] Monitoring sequence progress...")

    sequence_status = _wait_for_sync_sequence_completion(
        auth_headers=auth_headers,
        sync_sequence_id=sync_sequence_id,
        timeout_minutes=timeout_minutes,
    )

    return f"Sync sequence completed. Final status: {sequence_status}"


def _wait_for_sync_sequence_completion(
    *,
    auth_headers: dict,
    sync_sequence_id: str,
    timeout_minutes: int,
) -> str:
    """
    Poll sync sequence status until completion or timeout.

    Args:
        auth_headers: Authentication headers (Bearer token).
        sync_sequence_id: Hightouch sync sequence ID.
        timeout_minutes: Maximum time to wait for completion.

    Returns:
        Final sequence status.
    """
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
                f"Timeout waiting for sync sequence '{sync_sequence_id}' to complete "
                f"after {timeout_minutes} minutes"
            )

        try:
            runs_response = requests.get(
                f"{HIGHTOUCH_BASE_URL}/sync-sequences/{sync_sequence_id}/runs",
                headers=auth_headers,
                params={"limit": "1", "orderBy": "createdAt"},
            )

            if runs_response.status_code != 200:
                consecutive_failures += 1
                if consecutive_failures >= max_consecutive_failures:
                    raise Exception(
                        f"Sequence status check failed {consecutive_failures} times in a row. "
                        f"Last HTTP status: {runs_response.status_code}"
                    )

                console.debug(
                    f"[{sync_sequence_id}] HTTP {runs_response.status_code} error. "
                    f"Retrying... ({consecutive_failures}/{max_consecutive_failures})"
                )
                time.sleep(sleep_interval * min(consecutive_failures, 3))
                continue

            runs_data = runs_response.json()

            if isinstance(runs_data, list) and len(runs_data) > 0:
                latest_run = runs_data[0]
            elif isinstance(runs_data, dict) and "data" in runs_data:
                runs_list = runs_data["data"]
                if len(runs_list) > 0:
                    latest_run = runs_list[0]
                else:
                    time.sleep(sleep_interval)
                    counter += 1
                    continue
            else:
                time.sleep(sleep_interval)
                counter += 1
                continue

            run_status = latest_run.get("status", "unknown")
            consecutive_failures = 0

            if counter == 0 or counter % 6 == 0:

                elapsed_min = int(elapsed // 60)
                elapsed_sec = int(elapsed % 60)

                if run_status in ["processing", "queued", "running"]:
                    console.debug(
                        f"[{sync_sequence_id}] Status: {run_status}... "
                        f"({elapsed_min}m {elapsed_sec}s elapsed)"
                    )

            if run_status in [
                "success",
                "failed",
                "cancelled",
                "warning",
                "interrupted",
                "aborted",
            ]:

                elapsed_min = int(elapsed // 60)
                elapsed_sec = int(elapsed % 60)

                if run_status == "success":
                    console.debug(
                        f"[{sync_sequence_id}] Sequence completed successfully "
                        f"({elapsed_min}m {elapsed_sec}s)"
                    )
                    return "SUCCESS"
                elif run_status == "warning":
                    console.debug(
                        f"[{sync_sequence_id}] Sequence completed with warnings "
                        f"({elapsed_min}m {elapsed_sec}s)"
                    )
                    return "WARNING"
                elif run_status == "failed":
                    console.error(f"[{sync_sequence_id}] Sequence failed")
                    return "FAILED"
                elif run_status == "cancelled":
                    console.debug(f"[{sync_sequence_id}] Sequence cancelled")
                    return "CANCELLED"
                elif run_status == "interrupted":
                    console.debug(f"[{sync_sequence_id}] Sequence interrupted")
                    return "INTERRUPTED"
                elif run_status == "aborted":
                    console.debug(f"[{sync_sequence_id}] Sequence aborted")
                    return "ABORTED"

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
                f"[{sync_sequence_id}] Network error: {str(e)[:50]}... "
                f"Retrying... ({consecutive_failures}/{max_consecutive_failures})"
            )
            time.sleep(sleep_interval * min(consecutive_failures, 3))
            continue


def list_hightouch_syncs(*, api_token: str) -> None:
    """
    List all Hightouch syncs with their IDs and status.

    Args:
        api_token: Hightouch API token.
    """
    auth_headers = _get_auth_headers(api_token)

    console.info("Listing all Hightouch syncs")

    syncs_response = requests.get(
        f"{HIGHTOUCH_BASE_URL}/syncs",
        headers=auth_headers,
    )

    handle_http_error(syncs_response, "Error getting syncs:")

    syncs_data = syncs_response.json()

    # Handle both list and dict response formats
    if isinstance(syncs_data, list):
        syncs = syncs_data
    elif isinstance(syncs_data, dict) and "data" in syncs_data:
        syncs = syncs_data["data"]
    else:
        console.info("No syncs found.")
        return

    if not syncs:
        console.info("No syncs found.")
        return

    rows = []
    for sync in syncs:
        sync_id = sync.get("id", "Unknown")
        slug = sync.get("slug", "N/A")
        name = sync.get("name", "Unknown")
        status = sync.get("status", "Unknown")
        schedule_type = (
            sync.get("schedule", {}).get("type", "Unknown")
            if isinstance(sync.get("schedule"), dict)
            else "Unknown"
        )
        model_id = sync.get("modelId", "Unknown")
        destination_id = sync.get("destinationId", "Unknown")
        last_run_at = sync.get("lastRunAt", "Never")
        rows.append(
            (
                str(sync_id),
                name,
                slug,
                status,
                schedule_type,
                str(model_id),
                str(destination_id),
                str(last_run_at),
            )
        )

    console.table(
        columns=[
            "Sync ID",
            "Name",
            "Slug",
            "Status",
            "Schedule",
            "Model ID",
            "Destination ID",
            "Last Run",
        ],
        rows=rows,
        title="Hightouch Syncs",
    )


def list_hightouch_sync_sequences(*, api_token: str) -> None:
    """
    List all Hightouch sync sequences with their IDs and status.

    Args:
        api_token: Hightouch API token.
    """
    auth_headers = _get_auth_headers(api_token)

    console.info("Listing all Hightouch sync sequences")

    sequences_response = requests.get(
        f"{HIGHTOUCH_BASE_URL}/sync-sequences",
        headers=auth_headers,
    )

    handle_http_error(sequences_response, "Error getting sync sequences:")

    sequences_data = sequences_response.json()

    # Handle both list and dict response formats
    if isinstance(sequences_data, list):
        sequences = sequences_data
    elif isinstance(sequences_data, dict) and "data" in sequences_data:
        sequences = sequences_data["data"]
    else:
        console.info("No sync sequences found.")
        return

    if not sequences:
        console.info("No sync sequences found.")
        return

    rows = []
    for sequence in sequences:
        sequence_id = sequence.get("id", "Unknown")
        name = sequence.get("name", "Unknown")
        status = sequence.get("status", "Unknown")
        schedule_type = (
            sequence.get("schedule", {}).get("type", "Unknown")
            if isinstance(sequence.get("schedule"), dict)
            else "Unknown"
        )
        last_run_at = sequence.get("lastRunAt", "Never")

        # Get sync IDs in the sequence
        syncs_in_sequence = sequence.get("syncs", [])
        sync_count = len(syncs_in_sequence) if isinstance(syncs_in_sequence, list) else 0

        rows.append(
            (str(sequence_id), name, status, schedule_type, str(sync_count), str(last_run_at))
        )

    console.table(
        columns=["Sequence ID", "Name", "Status", "Schedule", "Syncs", "Last Run"],
        rows=rows,
        title="Hightouch Sync Sequences",
    )
