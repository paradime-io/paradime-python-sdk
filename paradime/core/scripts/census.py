import logging
import time
from concurrent.futures import ThreadPoolExecutor
from typing import List, Optional

import requests

from paradime.core.scripts.utils import handle_http_error

logging.basicConfig(format="%(asctime)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)


def trigger_census_syncs(
    *,
    api_token: str,
    sync_ids: List[str],
    force_full_sync: bool = False,
    wait_for_completion: bool = True,
    timeout_minutes: int = 1440,
) -> List[str]:
    """
    Trigger sync for multiple Census syncs.

    Args:
        api_token: Census API token
        sync_ids: List of Census sync IDs to trigger
        force_full_sync: Whether to force a full sync (default: False)
        wait_for_completion: Whether to wait for syncs to complete
        timeout_minutes: Maximum time to wait for completion

    Returns:
        List of sync result messages for each sync
    """
    futures = []
    results = []

    # Add visual separator and header
    print(f"\n{'='*60}")
    print("🚀 TRIGGERING CENSUS SYNCS")
    print(f"{'='*60}")

    with ThreadPoolExecutor() as executor:
        for i, sync_id in enumerate(set(sync_ids), 1):
            print(f"\n[{i}/{len(set(sync_ids))}] 🔌 {sync_id}")
            print(f"{'-'*40}")

            futures.append(
                (
                    sync_id,
                    executor.submit(
                        trigger_sync,
                        api_token=api_token,
                        sync_id=sync_id,
                        force_full_sync=force_full_sync,
                        wait_for_completion=wait_for_completion,
                        timeout_minutes=timeout_minutes,
                    ),
                )
            )

        # Add separator for live progress section
        print(f"\n{'='*60}")
        print("⚡ LIVE PROGRESS")
        print(f"{'='*60}")

        # Wait for completion and collect results
        sync_results = []
        for sync_id, future in futures:
            # Use longer timeout when waiting for completion
            future_timeout = (timeout_minutes * 60 + 120) if wait_for_completion else 120
            response_txt = future.result(timeout=future_timeout)
            sync_results.append((sync_id, response_txt))
            results.append(response_txt)

        # Display results as simple table
        print(f"\n{'='*80}")
        print("📊 SYNC RESULTS")
        print(f"{'='*80}")
        print(f"{'SYNC ID':<25} {'STATUS':<15} {'DASHBOARD'}")
        print(f"{'-'*25} {'-'*15} {'-'*45}")

        for sync_id, response_txt in sync_results:
            # Format result with emoji
            if "SUCCESS" in response_txt or "COMPLETED" in response_txt:
                status = "✅ SUCCESS"
            elif "FAILED" in response_txt:
                status = "❌ FAILED"
            elif "CANCELLED" in response_txt:
                status = "🚫 CANCELLED"
            elif "WORKING" in response_txt:
                status = "🔄 WORKING"
            else:
                status = "ℹ️ TRIGGERED"

            dashboard_url = f"https://app.getcensus.com/syncs/{sync_id}"
            print(f"{sync_id:<25} {status:<15} {dashboard_url}")

        print(f"{'='*80}\n")

    return results


def trigger_sync(
    *,
    api_token: str,
    sync_id: str,
    force_full_sync: bool = False,
    wait_for_completion: bool = True,
    timeout_minutes: int = 1440,
) -> str:
    """
    Trigger sync for a single Census sync.

    Args:
        api_token: Census API token
        sync_id: Census sync ID
        force_full_sync: Whether to force a full sync
        wait_for_completion: Whether to wait for sync to complete
        timeout_minutes: Maximum time to wait for completion

    Returns:
        Status message indicating sync result
    """
    base_url = "https://app.getcensus.com/api/v1"
    headers = {
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/json",
    }

    import datetime

    timestamp = datetime.datetime.now().strftime("%H:%M:%S")

    # Check sync status before attempting trigger
    print(f"{timestamp} 🔍 [{sync_id}] Checking sync status...")
    try:
        sync_response = requests.get(
            f"{base_url}/syncs/{sync_id}",
            headers=headers,
        )

        if sync_response.status_code == 200:
            sync_data = sync_response.json().get("data", {})
            sync_status = sync_data.get("status", "unknown")
            schedule_frequency = sync_data.get("schedule", {}).get("frequency", "unknown")

            print(
                f"{timestamp} 📊 [{sync_id}] Status: {sync_status} | Schedule: {schedule_frequency}"
            )

            # Handle paused syncs
            if sync_status == "paused":
                print(f"{timestamp} ⚠️  [{sync_id}] Warning: Sync is paused")

    except Exception as e:
        print(
            f"{timestamp} ⚠️  [{sync_id}] Could not check status: {str(e)[:50]}... Proceeding anyway."
        )

    # Trigger the sync
    sync_payload = {}
    if force_full_sync:
        sync_payload["force_full_sync"] = True

    print(f"{timestamp} 🚀 [{sync_id}] Triggering sync...")
    trigger_response = requests.post(
        f"{base_url}/syncs/{sync_id}/trigger",
        json=sync_payload if sync_payload else None,
        headers=headers,
    )

    handle_http_error(
        trigger_response,
        f"Error triggering sync for sync '{sync_id}':",
    )

    trigger_data = trigger_response.json()
    sync_run_id = trigger_data.get("data", {}).get("sync_run_id")

    if not sync_run_id:
        print(f"{timestamp} ⚠️  [{sync_id}] No sync_run_id returned in response")
        return "Sync triggered but no run ID returned"

    print(f"{timestamp} ✅ [{sync_id}] Sync triggered successfully (Run ID: {sync_run_id})")

    # Show dashboard link immediately after successful trigger
    dashboard_url = f"https://app.getcensus.com/syncs/{sync_id}/sync-history/{sync_run_id}"
    print(f"{timestamp} 🔗 [{sync_id}] Dashboard: {dashboard_url}")

    if not wait_for_completion:
        return f"Sync triggered (Run ID: {sync_run_id})"

    print(f"{timestamp} ⏳ [{sync_id}] Monitoring sync progress...")

    # Wait for sync completion
    sync_status = _wait_for_sync_completion(
        api_token=api_token,
        sync_run_id=sync_run_id,
        sync_id=sync_id,
        timeout_minutes=timeout_minutes,
    )

    return f"Sync completed. Final status: {sync_status}"


def _wait_for_sync_completion(
    *,
    api_token: str,
    sync_run_id: str,
    sync_id: str,
    timeout_minutes: int,
) -> str:
    """
    Poll sync run status until completion or timeout.

    Args:
        api_token: Census API token
        sync_run_id: Census sync run ID
        sync_id: Sync ID for logging
        timeout_minutes: Maximum time to wait for completion

    Returns:
        Final sync status
    """
    base_url = "https://app.getcensus.com/api/v1"
    headers = {
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/json",
    }
    start_time = time.time()
    timeout_seconds = timeout_minutes * 60
    sleep_interval = 5  # Poll every 5 seconds
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
            # Get sync run details to check status
            sync_run_response = requests.get(
                f"{base_url}/sync_runs/{sync_run_id}",
                headers=headers,
            )

            if sync_run_response.status_code != 200:
                consecutive_failures += 1
                if consecutive_failures >= max_consecutive_failures:
                    raise Exception(
                        f"Sync run status check failed {consecutive_failures} times in a row. Last HTTP status: {sync_run_response.status_code}"
                    )

                import datetime

                timestamp = datetime.datetime.now().strftime("%H:%M:%S")
                print(
                    f"{timestamp} ⚠️  [{sync_id}] HTTP {sync_run_response.status_code} error. Retrying... ({consecutive_failures}/{max_consecutive_failures})"
                )
                time.sleep(
                    sleep_interval * min(consecutive_failures, 3)
                )  # Exponential backoff up to 3x
                continue

            sync_run_data = sync_run_response.json()

            if "data" not in sync_run_data:
                time.sleep(sleep_interval)
                continue

            data = sync_run_data["data"]
            status = data.get("status", "unknown")

            # Reset failure counter on successful request
            consecutive_failures = 0

            # Log progress every 6 checks (30 seconds)
            if counter == 0 or counter % 6 == 0:
                import datetime

                timestamp = datetime.datetime.now().strftime("%H:%M:%S")
                elapsed_min = int(elapsed // 60)
                elapsed_sec = int(elapsed % 60)

                if status == "working":
                    records_processed = data.get("records_processed", 0)
                    records_updated = data.get("records_updated", 0)
                    records_failed = data.get("records_failed", 0)
                    print(
                        f"{timestamp} 🔄 [{sync_id}] Working... (Processed: {records_processed}, Updated: {records_updated}, Failed: {records_failed}) ({elapsed_min}m {elapsed_sec}s elapsed)"
                    )
                elif status in ["queued", "pending"]:
                    print(
                        f"{timestamp} ⏳ [{sync_id}] Queued... ({elapsed_min}m {elapsed_sec}s elapsed)"
                    )

            # Check if sync is complete
            if status in ["completed", "failed", "cancelled"]:
                import datetime

                timestamp = datetime.datetime.now().strftime("%H:%M:%S")
                elapsed_min = int(elapsed // 60)
                elapsed_sec = int(elapsed % 60)

                records_processed = data.get("records_processed", 0)
                records_updated = data.get("records_updated", 0)
                records_failed = data.get("records_failed", 0)

                if status == "completed":
                    print(
                        f"{timestamp} ✅ [{sync_id}] Completed successfully (Processed: {records_processed}, Updated: {records_updated}, Failed: {records_failed}) ({elapsed_min}m {elapsed_sec}s)"
                    )
                    return f"SUCCESS (Run ID: {sync_run_id}, Processed: {records_processed}, Updated: {records_updated})"
                elif status == "failed":
                    error_message = data.get("error_message", "No error message")
                    print(
                        f"{timestamp} ❌ [{sync_id}] Sync failed: {error_message[:100]}"
                    )
                    return f"FAILED (Run ID: {sync_run_id}, Error: {error_message[:100]})"
                elif status == "cancelled":
                    print(f"{timestamp} 🚫 [{sync_id}] Sync cancelled")
                    return f"CANCELLED (Run ID: {sync_run_id})"

            elif status in ["working", "queued", "pending"]:
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
                f"{timestamp} ⚠️  [{sync_id}] Network error: {str(e)[:50]}... Retrying... ({consecutive_failures}/{max_consecutive_failures})"
            )
            time.sleep(
                sleep_interval * min(consecutive_failures, 3)
            )  # Exponential backoff up to 3x
            continue


def list_census_syncs(
    *,
    api_token: str,
) -> None:
    """
    List all Census syncs with their IDs and status.

    Args:
        api_token: Census API token
    """
    base_url = "https://app.getcensus.com/api/v1"
    headers = {
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/json",
    }

    print("\n🔍 Listing all Census syncs")

    # Get all syncs - paginate if necessary
    all_syncs = []
    page = 1
    per_page = 100

    while True:
        syncs_response = requests.get(
            f"{base_url}/syncs",
            headers=headers,
            params={"page": page, "per_page": per_page, "order": "desc"},
        )

        handle_http_error(syncs_response, "Error getting syncs:")

        syncs_data = syncs_response.json()

        if "data" not in syncs_data:
            break

        syncs = syncs_data["data"]
        if not syncs:
            break

        all_syncs.extend(syncs)

        # Check if there are more pages
        if len(syncs) < per_page:
            break

        page += 1

    if not all_syncs:
        print("No syncs found.")
        return

    print(f"\n{'='*80}")
    print(f"📋 FOUND {len(all_syncs)} SYNC(S)")
    print(f"{'='*80}")

    for i, sync in enumerate(all_syncs, 1):
        sync_id = sync.get("id", "Unknown")
        label = sync.get("label", "Unnamed")
        status = sync.get("status", "Unknown")

        source_name = sync.get("source_attributes", {}).get("connection", {}).get("name", "Unknown")
        destination_name = sync.get("destination_attributes", {}).get("connection", {}).get("name", "Unknown")

        schedule = sync.get("schedule", {})
        schedule_frequency = schedule.get("frequency", "manual")

        # Get last run info
        last_run = sync.get("last_run", {})
        last_run_status = last_run.get("status", "never_run")
        last_run_completed_at = last_run.get("completed_at", "N/A")

        # Format status with emoji
        status_emoji = (
            "✅" if status == "active"
            else "⏸️" if status == "paused"
            else "❌" if status == "archived"
            else "❓"
        )

        last_run_emoji = (
            "✅" if last_run_status == "completed"
            else "❌" if last_run_status == "failed"
            else "🔄" if last_run_status == "working"
            else "⏳" if last_run_status == "queued"
            else "➖"
        )

        # Create dashboard deep link
        dashboard_url = f"https://app.getcensus.com/syncs/{sync_id}"

        print(f"\n[{i}/{len(all_syncs)}] 🔄 {sync_id}")
        print(f"{'-'*50}")
        print(f"   Label: {label}")
        print(f"   {status_emoji} Status: {status}")
        print(f"   Source: {source_name}")
        print(f"   Destination: {destination_name}")
        print(f"   Schedule: {schedule_frequency}")
        print(f"   {last_run_emoji} Last Run: {last_run_status}")
        if last_run_completed_at != "N/A":
            print(f"   Completed At: {last_run_completed_at}")
        print(f"   🔗 Dashboard: {dashboard_url}")

    print(f"\n{'='*80}\n")
