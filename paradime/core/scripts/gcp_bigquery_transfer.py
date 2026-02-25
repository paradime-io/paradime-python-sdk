import datetime
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any, List

from paradime.core.scripts.gcp_utils import get_gcp_credentials

logging.basicConfig(format="%(asctime)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)


def _get_transfer_client(service_account_key_file: str) -> Any:
    from google.cloud import bigquery_datatransfer_v1

    credentials = get_gcp_credentials(service_account_key_file)
    return bigquery_datatransfer_v1.DataTransferServiceClient(
        credentials=credentials, transport="rest"
    )


def _resolve_scheduled_query_name(
    client: Any,
    project: str,
    location: str,
    display_name: str,
) -> str:
    """Resolve a scheduled query display name to its full resource name."""
    from google.cloud import bigquery_datatransfer_v1

    parent = f"projects/{project}/locations/{location}"
    configs = client.list_transfer_configs(
        request=bigquery_datatransfer_v1.ListTransferConfigsRequest(
            parent=parent,
            data_source_ids=["scheduled_query"],
        )
    )

    matches = []
    for config in configs:
        if config.display_name == display_name:
            matches.append(config)

    if len(matches) == 0:
        raise Exception(
            f"No scheduled query found with display name '{display_name}' "
            f"in project '{project}', location '{location}'"
        )
    if len(matches) > 1:
        names = [m.name for m in matches]
        raise Exception(
            f"Multiple scheduled queries found with display name '{display_name}': {names}. "
            f"Please ensure display names are unique."
        )

    return matches[0].name


def trigger_bigquery_transfer(
    *,
    service_account_key_file: str,
    project: str,
    location: str,
    scheduled_query_names: List[str],
    wait_for_completion: bool = True,
    timeout_minutes: int = 1440,
) -> List[str]:
    """Trigger BigQuery scheduled queries by display name."""
    futures = []
    results = []

    print(f"\n{'='*60}")
    print("🚀 TRIGGERING BIGQUERY SCHEDULED QUERIES")
    print(f"{'='*60}")

    with ThreadPoolExecutor() as executor:
        for i, name in enumerate(set(scheduled_query_names), 1):
            print(f"\n[{i}/{len(set(scheduled_query_names))}] 📋 {name}")
            print(f"{'-'*40}")

            futures.append(
                (
                    name,
                    executor.submit(
                        _trigger_single_scheduled_query,
                        service_account_key_file=service_account_key_file,
                        project=project,
                        location=location,
                        display_name=name,
                        wait_for_completion=wait_for_completion,
                        timeout_minutes=timeout_minutes,
                    ),
                )
            )

        print(f"\n{'='*60}")
        print("⚡ LIVE PROGRESS")
        print(f"{'='*60}")

        query_results = []
        for name, future in futures:
            future_timeout = (timeout_minutes * 60 + 120) if wait_for_completion else 120
            response_txt = future.result(timeout=future_timeout)
            query_results.append((name, response_txt))
            results.append(response_txt)

        print(f"\n{'='*80}")
        print("📊 RESULTS")
        print(f"{'='*80}")
        print(f"{'SCHEDULED QUERY':<40} {'STATUS'}")
        print(f"{'-'*40} {'-'*30}")

        for name, response_txt in query_results:
            if "SUCCEEDED" in response_txt:
                status = "✅ SUCCEEDED"
            elif "FAILED" in response_txt:
                status = "❌ FAILED"
            elif "CANCELLED" in response_txt:
                status = "⚠️ CANCELLED"
            else:
                status = "ℹ️ TRIGGERED"
            print(f"{name:<40} {status}")

        print(f"{'='*80}\n")

    return results


def _trigger_single_scheduled_query(
    *,
    service_account_key_file: str,
    project: str,
    location: str,
    display_name: str,
    wait_for_completion: bool,
    timeout_minutes: int,
) -> str:
    from google.cloud import bigquery_datatransfer_v1
    from google.protobuf.timestamp_pb2 import Timestamp

    timestamp = datetime.datetime.now().strftime("%H:%M:%S")

    client = _get_transfer_client(service_account_key_file)

    print(f"{timestamp} 🔍 [{display_name}] Resolving scheduled query name...")
    config_name = _resolve_scheduled_query_name(client, project, location, display_name)
    print(f"{timestamp} ✅ [{display_name}] Resolved to: {config_name}")

    now = Timestamp()
    now.FromDatetime(datetime.datetime.now(datetime.timezone.utc))

    print(f"{timestamp} 🚀 [{display_name}] Triggering manual run...")
    response = client.start_manual_transfer_runs(
        request=bigquery_datatransfer_v1.StartManualTransferRunsRequest(
            parent=config_name,
            requested_run_time=now,
        )
    )

    if not response.runs:
        raise Exception(f"No runs were created for scheduled query '{display_name}'")

    run_name = response.runs[0].name
    print(f"{timestamp} ✅ [{display_name}] Run started: {run_name}")

    if not wait_for_completion:
        return f"TRIGGERED (run: {run_name})"

    print(f"{timestamp} ⏳ [{display_name}] Monitoring run progress...")
    return _wait_for_transfer_run(
        client=client,
        run_name=run_name,
        display_name=display_name,
        timeout_minutes=timeout_minutes,
    )


def _wait_for_transfer_run(
    *,
    client: Any,
    run_name: str,
    display_name: str,
    timeout_minutes: int,
) -> str:
    from google.cloud import bigquery_datatransfer_v1

    start_time = time.time()
    timeout_seconds = timeout_minutes * 60
    sleep_interval = 10
    counter = 0

    terminal_states = {
        bigquery_datatransfer_v1.TransferState.SUCCEEDED,
        bigquery_datatransfer_v1.TransferState.FAILED,
        bigquery_datatransfer_v1.TransferState.CANCELLED,
    }

    while True:
        elapsed = time.time() - start_time
        if elapsed > timeout_seconds:
            raise Exception(
                f"Timeout waiting for scheduled query '{display_name}' after {timeout_minutes} minutes"
            )

        try:
            run = client.get_transfer_run(name=run_name)
            state = run.state

            if counter == 0 or counter % 6 == 0:
                timestamp = datetime.datetime.now().strftime("%H:%M:%S")
                elapsed_min = int(elapsed // 60)
                elapsed_sec = int(elapsed % 60)
                state_name = bigquery_datatransfer_v1.TransferState(state).name
                print(
                    f"{timestamp} 🔄 [{display_name}] State: {state_name} "
                    f"({elapsed_min}m {elapsed_sec}s elapsed)"
                )

            if state in terminal_states:
                timestamp = datetime.datetime.now().strftime("%H:%M:%S")
                elapsed_min = int(elapsed // 60)
                elapsed_sec = int(elapsed % 60)
                state_name = bigquery_datatransfer_v1.TransferState(state).name

                if state == bigquery_datatransfer_v1.TransferState.SUCCEEDED:
                    print(
                        f"{timestamp} ✅ [{display_name}] Completed successfully "
                        f"({elapsed_min}m {elapsed_sec}s)"
                    )
                    return f"SUCCEEDED ({elapsed_min}m {elapsed_sec}s)"
                elif state == bigquery_datatransfer_v1.TransferState.FAILED:
                    error_msg = run.error_status.message if run.error_status else "Unknown error"
                    print(f"{timestamp} ❌ [{display_name}] Failed: {error_msg}")
                    return f"FAILED: {error_msg}"
                else:
                    print(f"{timestamp} ⚠️ [{display_name}] {state_name}")
                    return "CANCELLED"

        except Exception as e:
            timestamp = datetime.datetime.now().strftime("%H:%M:%S")
            print(f"{timestamp} ⚠️ [{display_name}] Error checking status: {str(e)[:80]}...")

        counter += 1
        time.sleep(sleep_interval)


def list_bigquery_transfers(
    *,
    service_account_key_file: str,
    project: str,
    location: str,
) -> None:
    """List all BigQuery scheduled queries."""
    from google.cloud import bigquery_datatransfer_v1

    client = _get_transfer_client(service_account_key_file)
    parent = f"projects/{project}/locations/{location}"

    print(f"\n🔍 Listing scheduled queries in {parent}")

    configs = client.list_transfer_configs(
        request=bigquery_datatransfer_v1.ListTransferConfigsRequest(
            parent=parent,
            data_source_ids=["scheduled_query"],
        )
    )

    config_list = list(configs)
    if not config_list:
        print("No scheduled queries found.")
        return

    print(f"\n{'='*80}")
    print(f"📋 FOUND {len(config_list)} SCHEDULED QUERY/QUERIES")
    print(f"{'='*80}")

    for i, config in enumerate(config_list, 1):
        state_name = bigquery_datatransfer_v1.TransferState(config.state).name
        state_emoji = (
            "✅"
            if state_name == "SUCCEEDED"
            else (
                "❌"
                if state_name == "FAILED"
                else "🔄" if state_name == "RUNNING" else "⏳" if state_name == "PENDING" else "❓"
            )
        )

        print(f"\n[{i}/{len(config_list)}] 📋 {config.display_name}")
        print(f"{'-'*50}")
        print(f"   Resource Name: {config.name}")
        print(f"   {state_emoji} State: {state_name}")
        print(f"   Schedule: {config.schedule}")
        print(f"   Destination Dataset: {config.destination_dataset_id}")
        if config.next_run_time:
            print(f"   Next Run: {config.next_run_time}")
        if config.update_time:
            print(f"   Last Updated: {config.update_time}")

    print(f"\n{'='*80}\n")
