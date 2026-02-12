import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor
from typing import List, Optional

import requests

from paradime.core.scripts.utils import handle_http_error

logging.basicConfig(format="%(asctime)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

HIGHTOUCH_BASE_URL = "https://api.hightouch.com/api/v1"


def _get_auth_headers() -> dict:
    """
    Get authentication headers using the HIGHTOUCH_API_TOKEN environment variable.

    Returns:
        Dictionary with Authorization header.

    Raises:
        ValueError: If HIGHTOUCH_API_TOKEN is not set.
    """
    api_token = os.environ.get("HIGHTOUCH_API_TOKEN")
    if not api_token:
        raise ValueError(
            "HIGHTOUCH_API_TOKEN environment variable is not set. "
            "Create an API key in your Hightouch workspace settings and set it as HIGHTOUCH_API_TOKEN."
        )
    return {
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/json",
    }


def trigger_hightouch_syncs(
    *,
    sync_ids: List[str],
    full_resync: bool = False,
    wait_for_completion: bool = True,
    timeout_minutes: int = 1440,
) -> List[str]:
    """
    Trigger syncs for multiple Hightouch syncs.

    Args:
        sync_ids: List of Hightouch sync IDs to trigger.
        full_resync: Whether to resync all rows (ignoring previously synced rows).
        wait_for_completion: Whether to wait for syncs to complete.
        timeout_minutes: Maximum time to wait for completion.

    Returns:
        List of sync result messages for each sync.
    """
    auth_headers = _get_auth_headers()
    futures = []
    results = []

    print(f"\n{'='*60}")
    print("🚀 TRIGGERING HIGHTOUCH SYNCS")
    print(f"{'='*60}")

    with ThreadPoolExecutor() as executor:
        for i, sync_id in enumerate(set(sync_ids), 1):
            print(f"\n[{i}/{len(set(sync_ids))}] 🔌 Sync {sync_id}")
            print(f"{'-'*40}")

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

        print(f"\n{'='*60}")
        print("⚡ LIVE PROGRESS")
        print(f"{'='*60}")

        sync_results = []
        for sync_id, future in futures:
            future_timeout = (timeout_minutes * 60 + 120) if wait_for_completion else 120
            response_txt = future.result(timeout=future_timeout)
            sync_results.append((sync_id, response_txt))
            results.append(response_txt)

        print(f"\n{'='*80}")
        print("📊 SYNC RESULTS")
        print(f"{'='*80}")
        print(f"{'SYNC ID':<25} {'STATUS':<15}")
        print(f"{'-'*25} {'-'*15}")

        for sync_id, response_txt in sync_results:
            if "SUCCESS" in response_txt:
                status = "✅ SUCCESS"
            elif "FAILED" in response_txt:
                status = "❌ FAILED"
            elif "CANCELLED" in response_txt:
                status = "🚫 CANCELLED"
            elif "WARNING" in response_txt:
                status = "⚠️ WARNING"
            elif "INTERRUPTED" in response_txt:
                status = "⚠️ INTERRUPTED"
            elif "ABORTED" in response_txt:
                status = "🚫 ABORTED"
            else:
                status = "ℹ️ COMPLETED"

            print(f"{sync_id:<25} {status:<15}")

        print(f"{'='*80}\n")

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
    import datetime

    timestamp = datetime.datetime.now().strftime("%H:%M:%S")

    print(f"{timestamp} 🚀 [{sync_id}] Triggering sync (full_resync={full_resync})...")

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

    print(f"{timestamp} ✅ [{sync_id}] Sync triggered successfully (request ID: {sync_request_id})")

    if not wait_for_completion:
        return f"Sync triggered (request ID: {sync_request_id})"

    print(f"{timestamp} ⏳ [{sync_id}] Monitoring sync progress...")

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
                params={"limit": 1, "orderBy": "createdAt"},
            )

            if runs_response.status_code != 200:
                consecutive_failures += 1
                if consecutive_failures >= max_consecutive_failures:
                    raise Exception(
                        f"Sync status check failed {consecutive_failures} times in a row. "
                        f"Last HTTP status: {runs_response.status_code}"
                    )

                import datetime

                timestamp = datetime.datetime.now().strftime("%H:%M:%S")
                print(
                    f"{timestamp} ⚠️  [{sync_id}] HTTP {runs_response.status_code} error. "
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
                import datetime

                timestamp = datetime.datetime.now().strftime("%H:%M:%S")
                elapsed_min = int(elapsed // 60)
                elapsed_sec = int(elapsed % 60)

                if run_status in ["processing", "queued"]:
                    print(
                        f"{timestamp} 🔄 [{sync_id}] Status: {run_status}... "
                        f"({elapsed_min}m {elapsed_sec}s elapsed)"
                    )

            if run_status in ["success", "failed", "cancelled", "warning", "interrupted", "aborted"]:
                import datetime

                timestamp = datetime.datetime.now().strftime("%H:%M:%S")
                elapsed_min = int(elapsed // 60)
                elapsed_sec = int(elapsed % 60)

                if run_status == "success":
                    print(
                        f"{timestamp} ✅ [{sync_id}] Sync completed successfully "
                        f"({elapsed_min}m {elapsed_sec}s)"
                    )
                    return "SUCCESS"
                elif run_status == "warning":
                    print(
                        f"{timestamp} ⚠️  [{sync_id}] Sync completed with warnings "
                        f"({elapsed_min}m {elapsed_sec}s)"
                    )
                    return "WARNING"
                elif run_status == "failed":
                    print(f"{timestamp} ❌ [{sync_id}] Sync failed")
                    return "FAILED"
                elif run_status == "cancelled":
                    print(f"{timestamp} 🚫 [{sync_id}] Sync cancelled")
                    return "CANCELLED"
                elif run_status == "interrupted":
                    print(f"{timestamp} ⚠️  [{sync_id}] Sync interrupted")
                    return "INTERRUPTED"
                elif run_status == "aborted":
                    print(f"{timestamp} 🚫 [{sync_id}] Sync aborted")
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

            import datetime

            timestamp = datetime.datetime.now().strftime("%H:%M:%S")
            print(
                f"{timestamp} ⚠️  [{sync_id}] Network error: {str(e)[:50]}... "
                f"Retrying... ({consecutive_failures}/{max_consecutive_failures})"
            )
            time.sleep(sleep_interval * min(consecutive_failures, 3))
            continue


def trigger_hightouch_sync_sequences(
    *,
    sync_sequence_ids: List[str],
    wait_for_completion: bool = True,
    timeout_minutes: int = 1440,
) -> List[str]:
    """
    Trigger runs for multiple Hightouch sync sequences.

    Args:
        sync_sequence_ids: List of Hightouch sync sequence IDs to trigger.
        wait_for_completion: Whether to wait for sequences to complete.
        timeout_minutes: Maximum time to wait for completion.

    Returns:
        List of sequence result messages.
    """
    auth_headers = _get_auth_headers()
    futures = []
    results = []

    print(f"\n{'='*60}")
    print("🚀 TRIGGERING HIGHTOUCH SYNC SEQUENCES")
    print(f"{'='*60}")

    with ThreadPoolExecutor() as executor:
        for i, sequence_id in enumerate(set(sync_sequence_ids), 1):
            print(f"\n[{i}/{len(set(sync_sequence_ids))}] 🔌 Sequence {sequence_id}")
            print(f"{'-'*40}")

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

        print(f"\n{'='*60}")
        print("⚡ LIVE PROGRESS")
        print(f"{'='*60}")

        sequence_results = []
        for sequence_id, future in futures:
            future_timeout = (timeout_minutes * 60 + 120) if wait_for_completion else 120
            response_txt = future.result(timeout=future_timeout)
            sequence_results.append((sequence_id, response_txt))
            results.append(response_txt)

        print(f"\n{'='*80}")
        print("📊 SEQUENCE RESULTS")
        print(f"{'='*80}")
        print(f"{'SEQUENCE ID':<25} {'STATUS':<15}")
        print(f"{'-'*25} {'-'*15}")

        for sequence_id, response_txt in sequence_results:
            if "SUCCESS" in response_txt:
                status = "✅ SUCCESS"
            elif "FAILED" in response_txt:
                status = "❌ FAILED"
            elif "CANCELLED" in response_txt:
                status = "🚫 CANCELLED"
            elif "WARNING" in response_txt:
                status = "⚠️ WARNING"
            elif "INTERRUPTED" in response_txt:
                status = "⚠️ INTERRUPTED"
            elif "ABORTED" in response_txt:
                status = "🚫 ABORTED"
            else:
                status = "ℹ️ COMPLETED"

            print(f"{sequence_id:<25} {status:<15}")

        print(f"{'='*80}\n")

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
    import datetime

    timestamp = datetime.datetime.now().strftime("%H:%M:%S")

    print(f"{timestamp} 🚀 [{sync_sequence_id}] Triggering sync sequence...")

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

    print(
        f"{timestamp} ✅ [{sync_sequence_id}] Sync sequence triggered successfully "
        f"(run ID: {sequence_run_id})"
    )

    if not wait_for_completion:
        return f"Sync sequence triggered (run ID: {sequence_run_id})"

    print(f"{timestamp} ⏳ [{sync_sequence_id}] Monitoring sequence progress...")

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
                params={"limit": 1, "orderBy": "createdAt"},
            )

            if runs_response.status_code != 200:
                consecutive_failures += 1
                if consecutive_failures >= max_consecutive_failures:
                    raise Exception(
                        f"Sequence status check failed {consecutive_failures} times in a row. "
                        f"Last HTTP status: {runs_response.status_code}"
                    )

                import datetime

                timestamp = datetime.datetime.now().strftime("%H:%M:%S")
                print(
                    f"{timestamp} ⚠️  [{sync_sequence_id}] HTTP {runs_response.status_code} error. "
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
                import datetime

                timestamp = datetime.datetime.now().strftime("%H:%M:%S")
                elapsed_min = int(elapsed // 60)
                elapsed_sec = int(elapsed % 60)

                if run_status in ["processing", "queued", "running"]:
                    print(
                        f"{timestamp} 🔄 [{sync_sequence_id}] Status: {run_status}... "
                        f"({elapsed_min}m {elapsed_sec}s elapsed)"
                    )

            if run_status in ["success", "failed", "cancelled", "warning", "interrupted", "aborted"]:
                import datetime

                timestamp = datetime.datetime.now().strftime("%H:%M:%S")
                elapsed_min = int(elapsed // 60)
                elapsed_sec = int(elapsed % 60)

                if run_status == "success":
                    print(
                        f"{timestamp} ✅ [{sync_sequence_id}] Sequence completed successfully "
                        f"({elapsed_min}m {elapsed_sec}s)"
                    )
                    return "SUCCESS"
                elif run_status == "warning":
                    print(
                        f"{timestamp} ⚠️  [{sync_sequence_id}] Sequence completed with warnings "
                        f"({elapsed_min}m {elapsed_sec}s)"
                    )
                    return "WARNING"
                elif run_status == "failed":
                    print(f"{timestamp} ❌ [{sync_sequence_id}] Sequence failed")
                    return "FAILED"
                elif run_status == "cancelled":
                    print(f"{timestamp} 🚫 [{sync_sequence_id}] Sequence cancelled")
                    return "CANCELLED"
                elif run_status == "interrupted":
                    print(f"{timestamp} ⚠️  [{sync_sequence_id}] Sequence interrupted")
                    return "INTERRUPTED"
                elif run_status == "aborted":
                    print(f"{timestamp} 🚫 [{sync_sequence_id}] Sequence aborted")
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

            import datetime

            timestamp = datetime.datetime.now().strftime("%H:%M:%S")
            print(
                f"{timestamp} ⚠️  [{sync_sequence_id}] Network error: {str(e)[:50]}... "
                f"Retrying... ({consecutive_failures}/{max_consecutive_failures})"
            )
            time.sleep(sleep_interval * min(consecutive_failures, 3))
            continue


def list_hightouch_syncs() -> None:
    """
    List all Hightouch syncs with their IDs and status.
    """
    auth_headers = _get_auth_headers()

    print("\n🔍 Listing all Hightouch syncs")

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
        print("No syncs found.")
        return

    if not syncs:
        print("No syncs found.")
        return

    print(f"\n{'='*80}")
    print(f"📋 FOUND {len(syncs)} SYNC(S)")
    print(f"{'='*80}")

    for i, sync in enumerate(syncs, 1):
        sync_id = sync.get("id", "Unknown")
        slug = sync.get("slug", "N/A")
        name = sync.get("name", "Unknown")
        status = sync.get("status", "Unknown")
        schedule_type = sync.get("schedule", {}).get("type", "Unknown") if isinstance(sync.get("schedule"), dict) else "Unknown"
        model_id = sync.get("modelId", "Unknown")
        destination_id = sync.get("destinationId", "Unknown")
        last_run_at = sync.get("lastRunAt", "Never")

        # Format status with emoji
        status_emoji = (
            "✅"
            if status in ["success", "active"]
            else "❌" if status == "failed" else "⚠️" if status == "warning" else "❓"
        )

        print(f"\n[{i}/{len(syncs)}] 🔌 {sync_id}")
        print(f"{'-'*50}")
        print(f"   Name: {name}")
        print(f"   Slug: {slug}")
        print(f"   {status_emoji} Status: {status}")
        print(f"   Schedule: {schedule_type}")
        print(f"   Model ID: {model_id}")
        print(f"   Destination ID: {destination_id}")
        if last_run_at != "Never":
            print(f"   Last Run: {last_run_at}")

    print(f"\n{'='*80}\n")


def list_hightouch_sync_sequences() -> None:
    """
    List all Hightouch sync sequences with their IDs and status.
    """
    auth_headers = _get_auth_headers()

    print("\n🔍 Listing all Hightouch sync sequences")

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
        print("No sync sequences found.")
        return

    if not sequences:
        print("No sync sequences found.")
        return

    print(f"\n{'='*80}")
    print(f"📋 FOUND {len(sequences)} SYNC SEQUENCE(S)")
    print(f"{'='*80}")

    for i, sequence in enumerate(sequences, 1):
        sequence_id = sequence.get("id", "Unknown")
        name = sequence.get("name", "Unknown")
        status = sequence.get("status", "Unknown")
        schedule_type = sequence.get("schedule", {}).get("type", "Unknown") if isinstance(sequence.get("schedule"), dict) else "Unknown"
        last_run_at = sequence.get("lastRunAt", "Never")

        # Get sync IDs in the sequence
        syncs_in_sequence = sequence.get("syncs", [])
        sync_count = len(syncs_in_sequence) if isinstance(syncs_in_sequence, list) else 0

        # Format status with emoji
        status_emoji = (
            "✅"
            if status in ["success", "active"]
            else "❌" if status == "failed" else "⚠️" if status == "warning" else "❓"
        )

        print(f"\n[{i}/{len(sequences)}] 🔗 {sequence_id}")
        print(f"{'-'*50}")
        print(f"   Name: {name}")
        print(f"   {status_emoji} Status: {status}")
        print(f"   Schedule: {schedule_type}")
        print(f"   Syncs in Sequence: {sync_count}")
        if last_run_at != "Never":
            print(f"   Last Run: {last_run_at}")

    print(f"\n{'='*80}\n")
