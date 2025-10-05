import logging
import time
from concurrent.futures import ThreadPoolExecutor
from typing import List, Optional

import requests

from paradime.core.scripts.utils import handle_http_error

logging.basicConfig(format="%(asctime)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)


def trigger_fivetran_sync(
    *,
    api_key: str,
    api_secret: str,
    connector_ids: List[str],
    force: bool = False,
    wait_for_completion: bool = True,
    timeout_minutes: int = 1440,
) -> List[str]:
    """
    Trigger sync for multiple Fivetran connectors.

    Args:
        api_key: Fivetran API key
        api_secret: Fivetran API secret
        connector_ids: List of Fivetran connector IDs to sync
        force: Whether to force restart ongoing syncs
        wait_for_completion: Whether to wait for syncs to complete
        timeout_minutes: Maximum time to wait for completion

    Returns:
        List of sync result messages for each connector
    """
    futures = []
    results = []

    # Add visual separator and header
    print(f"\n{'='*60}")
    print("🚀 TRIGGERING FIVETRAN CONNECTORS")
    print(f"{'='*60}")

    with ThreadPoolExecutor() as executor:
        for i, connector_id in enumerate(set(connector_ids), 1):
            print(f"\n[{i}/{len(set(connector_ids))}] 🔌 {connector_id}")
            print(f"{'-'*40}")

            futures.append(
                (
                    connector_id,
                    executor.submit(
                        trigger_connector_sync,
                        api_key=api_key,
                        api_secret=api_secret,
                        connector_id=connector_id,
                        force=force,
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
        connector_results = []
        for connector_id, future in futures:
            # Use longer timeout when waiting for completion
            future_timeout = (timeout_minutes * 60 + 120) if wait_for_completion else 120
            response_txt = future.result(timeout=future_timeout)
            connector_results.append((connector_id, response_txt))
            results.append(response_txt)

        # Display results as simple table
        print(f"\n{'='*80}")
        print("📊 SYNC RESULTS")
        print(f"{'='*80}")
        print(f"{'CONNECTOR':<25} {'STATUS':<10} {'DASHBOARD'}")
        print(f"{'-'*25} {'-'*10} {'-'*45}")

        for connector_id, response_txt in connector_results:
            # Format result with emoji
            if "SUCCESS" in response_txt:
                status = "✅ SUCCESS"
            elif "FAILED" in response_txt:
                status = "❌ FAILED"
            elif "PAUSED" in response_txt:
                status = "⚠️ PAUSED"
            elif "RESCHEDULED" in response_txt:
                status = "⏳ RESCHEDULED"
            else:
                status = "ℹ️ COMPLETED"

            dashboard_url = f"https://fivetran.com/dashboard/connections/{connector_id}"
            print(f"{connector_id:<25} {status:<10} {dashboard_url}")

        print(f"{'='*80}\n")

    return results


def trigger_connector_sync(
    *,
    api_key: str,
    api_secret: str,
    connector_id: str,
    force: bool = False,
    wait_for_completion: bool = True,
    timeout_minutes: int = 1440,
) -> str:
    """
    Trigger sync for a single Fivetran connector.

    Args:
        api_key: Fivetran API key
        api_secret: Fivetran API secret
        connector_id: Fivetran connector ID
        force: Whether to force restart ongoing syncs
        wait_for_completion: Whether to wait for sync to complete
        timeout_minutes: Maximum time to wait for completion

    Returns:
        Status message indicating sync result
    """
    base_url = "https://api.fivetran.com/v1"
    auth = (api_key, api_secret)

    import datetime

    timestamp = datetime.datetime.now().strftime("%H:%M:%S")

    # Check connector status before attempting sync
    print(f"{timestamp} 🔍 [{connector_id}] Checking connector status...")
    try:
        status_response = requests.get(
            f"{base_url}/connectors/{connector_id}",
            auth=auth,
            headers={"Content-Type": "application/json"},
        )

        if status_response.status_code == 200:
            status_data = status_response.json().get("data", {})
            current_sync_state = status_data.get("status", {}).get("sync_state", "unknown")
            current_setup_state = status_data.get("status", {}).get("setup_state", "unknown")

            print(
                f"{timestamp} 📊 [{connector_id}] State: {current_sync_state} | Setup: {current_setup_state}"
            )

            # Handle paused connectors
            if current_sync_state == "paused":
                if not force:
                    print(
                        f"{timestamp} ⚠️  [{connector_id}] Connector is paused - use --force to override"
                    )
                    return "PAUSED (connector is paused - use --force to attempt override)"
                else:
                    print(f"{timestamp} 🔄 [{connector_id}] Forcing sync on paused connector...")

            # Handle broken setup state
            if current_setup_state == "broken":
                print(
                    f"{timestamp} ⚠️  [{connector_id}] Warning: Broken setup detected - sync may fail"
                )

    except Exception as e:
        print(
            f"{timestamp} ⚠️  [{connector_id}] Could not check status: {str(e)[:50]}... Proceeding anyway."
        )

    # Trigger the sync
    sync_payload = {"force": force} if force else {}

    print(f"{timestamp} 🚀 [{connector_id}] Triggering sync...")
    sync_response = requests.post(
        f"{base_url}/connectors/{connector_id}/sync",
        json=sync_payload,
        auth=auth,
        headers={"Content-Type": "application/json"},
    )

    handle_http_error(
        sync_response,
        f"Error triggering sync for connector '{connector_id}':",
    )

    # Show dashboard link immediately after successful trigger
    dashboard_url = f"https://fivetran.com/dashboard/connections/{connector_id}"
    print(f"{timestamp} 🔗 [{connector_id}] Dashboard: {dashboard_url}")

    if not wait_for_completion:
        return sync_response.text

    print(f"{timestamp} ⏳ [{connector_id}] Monitoring sync progress...")

    # Wait for sync completion
    sync_status = _wait_for_sync_completion(
        api_key=api_key,
        api_secret=api_secret,
        connector_id=connector_id,
        timeout_minutes=timeout_minutes,
    )

    return f"Sync completed. Final status: {sync_status}"


def _wait_for_sync_completion(
    *,
    api_key: str,
    api_secret: str,
    connector_id: str,
    timeout_minutes: int,
) -> str:
    """
    Poll connector status until sync completion or timeout.

    Args:
        api_key: Fivetran API key
        api_secret: Fivetran API secret
        connector_id: Fivetran connector ID
        timeout_minutes: Maximum time to wait for completion

    Returns:
        Final sync status
    """
    base_url = "https://api.fivetran.com/v1"
    auth = (api_key, api_secret)
    start_time = time.time()
    timeout_seconds = timeout_minutes * 60
    sleep_interval = 5  # Poll every 5 seconds
    counter = 0

    # Get initial state to track when sync actually completes
    initial_succeeded_at = None
    initial_failed_at = None
    sync_started = False

    try:
        initial_response = requests.get(
            f"{base_url}/connectors/{connector_id}",
            auth=auth,
            headers={"Content-Type": "application/json"},
        )
        if initial_response.status_code == 200:
            initial_data = initial_response.json().get("data", {})
            initial_succeeded_at = initial_data.get("succeeded_at")
            initial_failed_at = initial_data.get("failed_at")
    except Exception as e:
        print(
            f"    ⚠️ [{connector_id}] Could not get initial state: {str(e)[:50]}... Proceeding anyway."
        )

    while True:
        elapsed = time.time() - start_time
        if elapsed > timeout_seconds:
            raise Exception(
                f"Timeout waiting for connector '{connector_id}' sync to complete after {timeout_minutes} minutes"
            )

        try:
            # Get connector details to check sync status
            connector_response = requests.get(
                f"{base_url}/connectors/{connector_id}",
                auth=auth,
                headers={"Content-Type": "application/json"},
            )

            if connector_response.status_code != 200:
                raise Exception(
                    f"Connector status check failed with HTTP {connector_response.status_code}"
                )

            connector_data = connector_response.json()

            if "data" not in connector_data:
                time.sleep(sleep_interval)
                continue

            data = connector_data["data"]
            sync_state = data.get("status", {}).get("sync_state", "unknown")
            succeeded_at = data.get("succeeded_at")
            failed_at = data.get("failed_at")

            # Track if sync has actually started
            if sync_state == "syncing" and not sync_started:
                sync_started = True
                import datetime

                timestamp = datetime.datetime.now().strftime("%H:%M:%S")
                print(f"{timestamp} 🔄 [{connector_id}] Sync started")

            # Log progress every 6 checks (1 minute)
            if counter == 0 or counter % 6 == 0:
                import datetime

                timestamp = datetime.datetime.now().strftime("%H:%M:%S")
                elapsed_min = int(elapsed // 60)
                elapsed_sec = int(elapsed % 60)
                if sync_state == "syncing":
                    print(
                        f"{timestamp} 🔄 [{connector_id}] Syncing... ({elapsed_min}m {elapsed_sec}s elapsed)"
                    )
                elif not sync_started:
                    print(
                        f"{timestamp} ⏳ [{connector_id}] Waiting to start... ({elapsed_min}m {elapsed_sec}s elapsed)"
                    )

            # Check if sync is complete - but only if we've seen it start or timestamps changed
            if sync_state in ["scheduled", "rescheduled"]:
                # Only consider sync complete if:
                # 1. We saw it start (sync_started=True), OR
                # 2. The timestamps have changed from initial values
                sync_completed = (
                    sync_started
                    or succeeded_at != initial_succeeded_at
                    or failed_at != initial_failed_at
                )

                if sync_completed:
                    import datetime

                    timestamp = datetime.datetime.now().strftime("%H:%M:%S")
                    elapsed_min = int(elapsed // 60)
                    elapsed_sec = int(elapsed % 60)

                    # Check if we had a recent success or failure
                    if succeeded_at and succeeded_at != initial_succeeded_at:
                        print(
                            f"{timestamp} ✅ [{connector_id}] Completed successfully ({elapsed_min}m {elapsed_sec}s)"
                        )
                        return f"SUCCESS (completed at {succeeded_at})"
                    elif failed_at and failed_at != initial_failed_at:
                        print(f"{timestamp} ❌ [{connector_id}] Sync failed")
                        return f"FAILED (failed at {failed_at})"
                    elif sync_state == "rescheduled":
                        print(
                            f"{timestamp} ⏳ [{connector_id}] Rescheduled ({elapsed_min}m {elapsed_sec}s)"
                        )
                        return "RESCHEDULED (sync will resume automatically)"
                    else:
                        print(
                            f"{timestamp} ✅ [{connector_id}] Completed ({elapsed_min}m {elapsed_sec}s)"
                        )
                        return f"COMPLETED (sync state: {sync_state})"

            elif sync_state == "syncing":
                # Still syncing, continue waiting
                pass

            elif sync_state == "paused":
                import datetime

                timestamp = datetime.datetime.now().strftime("%H:%M:%S")
                print(f"{timestamp} ⚠️  [{connector_id}] Sync is paused")
                return f"PAUSED (sync state: {sync_state})"

            else:
                # Continue waiting for unknown states
                pass

            counter += 1
            time.sleep(sleep_interval)

        except requests.exceptions.RequestException as e:
            import datetime

            timestamp = datetime.datetime.now().strftime("%H:%M:%S")
            print(f"{timestamp} ⚠️  [{connector_id}] Network error: {str(e)[:50]}... Retrying.")
            time.sleep(sleep_interval)
            continue


def list_fivetran_connectors(
    *,
    api_key: str,
    api_secret: str,
    group_id: Optional[str] = None,
) -> None:
    """
    List all Fivetran connectors with their IDs and status.

    Args:
        api_key: Fivetran API key
        api_secret: Fivetran API secret
        group_id: Optional group ID to filter connectors
    """
    base_url = "https://api.fivetran.com/v1"
    auth = (api_key, api_secret)

    # Build URL based on whether group_id is provided
    if group_id:
        url = f"{base_url}/groups/{group_id}/connectors"
        print(f"\n🔍 Listing connectors for group: {group_id}")
    else:
        url = f"{base_url}/connectors"
        print("\n🔍 Listing all connectors")

    connectors_response = requests.get(
        url,
        auth=auth,
        headers={"Content-Type": "application/json"},
    )

    handle_http_error(connectors_response, "Error getting connectors:")

    connectors_data = connectors_response.json()

    if "data" not in connectors_data or "items" not in connectors_data["data"]:
        print("No connectors found.")
        return

    connectors = connectors_data["data"]["items"]

    print(f"\n{'='*80}")
    print(f"📋 FOUND {len(connectors)} CONNECTOR(S)")
    print(f"{'='*80}")

    for i, connector in enumerate(connectors, 1):
        connector_id = connector.get("id", "Unknown")
        service = connector.get("service", "Unknown")
        schema = connector.get("schema", "Unknown")

        status = connector.get("status", {})
        sync_state = status.get("sync_state", "Unknown")
        setup_state = status.get("setup_state", "Unknown")

        succeeded_at = connector.get("succeeded_at", "Never")
        failed_at = connector.get("failed_at", "Never")

        # Format sync state with emoji
        sync_emoji = (
            "🔄"
            if sync_state == "syncing"
            else (
                "✅"
                if sync_state in ["scheduled", "rescheduled"]
                else "⏸️" if sync_state == "paused" else "❓"
            )
        )
        setup_emoji = (
            "✅" if setup_state == "connected" else "❌" if setup_state == "broken" else "⚠️"
        )

        # Create dashboard deep link
        dashboard_url = f"https://fivetran.com/dashboard/connections/{connector_id}"

        print(f"\n[{i}/{len(connectors)}] 🔌 {connector_id}")
        print(f"{'-'*50}")
        print(f"   Service: {service}")
        print(f"   Schema: {schema}")
        print(f"   {sync_emoji} Sync State: {sync_state}")
        print(f"   {setup_emoji} Setup State: {setup_state}")
        if succeeded_at != "Never":
            print(f"   ✅ Last Success: {succeeded_at}")
        if failed_at != "Never":
            print(f"   ❌ Last Failure: {failed_at}")
        print(f"   🔗 Dashboard: {dashboard_url}")

    print(f"\n{'='*80}\n")
