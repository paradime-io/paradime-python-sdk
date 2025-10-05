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
    
    with ThreadPoolExecutor() as executor:
        for connector_id in set(connector_ids):
            logger.info(f"Triggering Fivetran connector sync: {connector_id}")
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
        
        for connector_id, future in futures:
            # Use longer timeout when waiting for completion
            future_timeout = (timeout_minutes * 60 + 120) if wait_for_completion else 120
            response_txt = future.result(timeout=future_timeout)
            logger.info(f"Sync result for connector {connector_id}: {response_txt}")
            results.append(response_txt)
    
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
    
    # Check connector status before attempting sync
    logger.info(f"Checking connector status before triggering sync: {connector_id}")
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
            
            logger.info(f"Current state - sync: {current_sync_state}, setup: {current_setup_state}")
            
            # Handle paused connectors
            if current_sync_state == "paused":
                if not force:
                    logger.warning(f"⚠️ Connector '{connector_id}' is paused. Sync cannot be triggered unless --force is used.")
                    return "PAUSED (connector is paused - use --force to attempt override)"
                else:
                    logger.info(f"🔄 Attempting to force sync on paused connector '{connector_id}'...")
            
            # Handle broken setup state
            if current_setup_state == "broken":
                logger.warning(f"⚠️ Connector '{connector_id}' has broken setup. Sync may fail.")
                
    except Exception as e:
        logger.warning(f"Could not check connector status: {e}. Proceeding with sync attempt...")
    
    # Trigger the sync
    sync_payload = {"force": force} if force else {}
    
    logger.info(f"Triggering sync for connector: {connector_id}")
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
    logger.info(f"🔗 View connector in Fivetran dashboard: {dashboard_url}")
    
    if not wait_for_completion:
        return sync_response.text
    
    logger.info(f"Waiting for sync completion for connector: {connector_id}")
    
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
    sleep_interval = 10  # Poll every 10 seconds
    counter = 0
    
    # Get initial state to track when sync actually completes
    initial_succeeded_at = None
    initial_failed_at = None
    sync_started = False
    
    logger.info(f"Getting initial state for connector {connector_id} to track sync progress...")
    
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
            initial_sync_state = initial_data.get("status", {}).get("sync_state", "unknown")
            logger.info(f"Initial sync state: {initial_sync_state}, last success: {initial_succeeded_at}, last failure: {initial_failed_at}")
    except Exception as e:
        logger.warning(f"Could not get initial state: {e}")
    
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
                logger.error(f"❌ Failed to get connector status: HTTP {connector_response.status_code}")
                raise Exception(
                    f"Connector status check failed with HTTP {connector_response.status_code}"
                )
            
            connector_data = connector_response.json()
            
            if "data" not in connector_data:
                logger.warning(f"No data field in connector response for {connector_id}")
                time.sleep(sleep_interval)
                continue
                
            data = connector_data["data"]
            sync_state = data.get("status", {}).get("sync_state", "unknown")
            setup_state = data.get("status", {}).get("setup_state", "unknown")
            succeeded_at = data.get("succeeded_at")
            failed_at = data.get("failed_at")
            
            # Log progress on first check and then every 6 checks (1 minute)
            if counter == 0 or counter % 6 == 0:
                logger.info(f"Connector {connector_id} sync state: {sync_state}, setup state: {setup_state}")
            
            # Track if sync has actually started
            if sync_state == "syncing" and not sync_started:
                sync_started = True
                logger.info(f"🔄 Sync started for connector {connector_id}")
            
            # Check if sync is complete - but only if we've seen it start or timestamps changed
            if sync_state in ["scheduled", "rescheduled"]:
                # Only consider sync complete if:
                # 1. We saw it start (sync_started=True), OR
                # 2. The timestamps have changed from initial values
                sync_completed = (
                    sync_started or 
                    succeeded_at != initial_succeeded_at or 
                    failed_at != initial_failed_at
                )
                
                if sync_completed:
                    elapsed_min = int(elapsed // 60)
                    elapsed_sec = int(elapsed % 60)
                    
                    # Check if we had a recent success or failure
                    if succeeded_at and succeeded_at != initial_succeeded_at:
                        logger.info(
                            f"✅ Connector '{connector_id}' sync completed successfully in {elapsed_min}m {elapsed_sec}s"
                        )
                        return f"SUCCESS (completed at {succeeded_at})"
                    elif failed_at and failed_at != initial_failed_at:
                        logger.error(f"❌ Connector '{connector_id}' sync failed")
                        return f"FAILED (failed at {failed_at})"
                    elif sync_state == "rescheduled":
                        logger.warning(f"⏳ Connector '{connector_id}' sync was rescheduled in {elapsed_min}m {elapsed_sec}s")
                        return f"RESCHEDULED (sync will resume automatically)"
                    else:
                        logger.info(
                            f"✅ Connector '{connector_id}' sync completed in {elapsed_min}m {elapsed_sec}s"
                        )
                        return f"COMPLETED (sync state: {sync_state})"
                else:
                    # Sync hasn't started yet or no change detected, continue waiting
                    if not sync_started and counter > 0:
                        logger.info(f"⏳ Waiting for sync to start for connector {connector_id}...")
            
            elif sync_state == "syncing":
                # Still syncing, continue waiting
                pass
            
            elif sync_state == "paused":
                logger.warning(f"⚠️ Connector '{connector_id}' sync is paused")
                return f"PAUSED (sync state: {sync_state})"
            
            else:
                logger.warning(f"⚠️ Connector '{connector_id}' has unknown sync state: {sync_state}")
                # Continue waiting for unknown states
            
            counter += 1
            time.sleep(sleep_interval)
            
        except requests.exceptions.RequestException as e:
            logger.warning(f"Network error checking connector status for {connector_id}: {e}")
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
        logger.info(f"Listing connectors for group: {group_id}")
    else:
        url = f"{base_url}/connectors"
        logger.info("Listing all connectors")
    
    connectors_response = requests.get(
        url,
        auth=auth,
        headers={"Content-Type": "application/json"},
    )
    
    handle_http_error(connectors_response, "Error getting connectors:")
    
    connectors_data = connectors_response.json()
    
    if "data" not in connectors_data or "items" not in connectors_data["data"]:
        logger.info("No connectors found.")
        return
    
    connectors = connectors_data["data"]["items"]
    
    logger.info(f"\n{'='*80}")
    logger.info(f"Found {len(connectors)} connector(s):")
    logger.info(f"{'='*80}")
    
    for connector in connectors:
        connector_id = connector.get("id", "Unknown")
        service = connector.get("service", "Unknown")
        schema = connector.get("schema", "Unknown")
        
        status = connector.get("status", {})
        sync_state = status.get("sync_state", "Unknown")
        setup_state = status.get("setup_state", "Unknown")
        
        succeeded_at = connector.get("succeeded_at", "Never")
        failed_at = connector.get("failed_at", "Never")
        
        # Format sync state with emoji
        sync_emoji = "🔄" if sync_state == "syncing" else "✅" if sync_state in ["scheduled", "rescheduled"] else "⏸️" if sync_state == "paused" else "❓"
        setup_emoji = "✅" if setup_state == "connected" else "❌" if setup_state == "broken" else "⚠️"
        
        # Create dashboard deep link
        dashboard_url = f"https://fivetran.com/dashboard/connections/{connector_id}"
        
        logger.info(f"\n📊 Connector ID: {connector_id}")
        logger.info(f"   Service: {service}")
        logger.info(f"   Schema: {schema}")
        logger.info(f"   {sync_emoji} Sync State: {sync_state}")
        logger.info(f"   {setup_emoji} Setup State: {setup_state}")
        if succeeded_at != "Never":
            logger.info(f"   ✅ Last Success: {succeeded_at}")
        if failed_at != "Never":
            logger.info(f"   ❌ Last Failure: {failed_at}")
        logger.info(f"   🔗 Dashboard: {dashboard_url}")
        logger.info(f"   {'-'*40}")
    
    logger.info(f"{'='*80}\n")