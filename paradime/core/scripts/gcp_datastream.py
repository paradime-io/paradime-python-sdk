import datetime
import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Any, List

from paradime.core.scripts.gcp_utils import get_gcp_credentials

logging.basicConfig(format="%(asctime)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)


def _get_datastream_client(service_account_key_file: str) -> Any:
    from google.cloud import datastream_v1

    credentials = get_gcp_credentials(service_account_key_file)
    return datastream_v1.DatastreamClient(credentials=credentials, transport="rest")


def _resolve_stream_name(
    client: Any,
    project: str,
    location: str,
    display_name: str,
) -> str:
    """Resolve a stream display name to its full resource name."""
    parent = f"projects/{project}/locations/{location}"
    streams = client.list_streams(parent=parent)

    matches = []
    for stream in streams:
        if stream.display_name == display_name:
            matches.append(stream)

    if len(matches) == 0:
        raise Exception(
            f"No Datastream stream found with display name '{display_name}' "
            f"in project '{project}', location '{location}'"
        )
    if len(matches) > 1:
        names = [m.name for m in matches]
        raise Exception(
            f"Multiple streams found with display name '{display_name}': {names}. "
            f"Please ensure display names are unique."
        )

    return matches[0].name


def trigger_datastream(
    *,
    service_account_key_file: str,
    project: str,
    location: str,
    stream_names: List[str],
    action: str = "start",
    wait_for_completion: bool = True,
    timeout_minutes: int = 60,
) -> List[str]:
    """Start, pause, or resume Datastream streams by display name."""
    futures = []
    results = []

    action_label = action.upper()

    print(f"\n{'='*60}")
    print(f"🚀 {action_label} DATASTREAM STREAMS")
    print(f"{'='*60}")

    with ThreadPoolExecutor() as executor:
        for i, name in enumerate(set(stream_names), 1):
            print(f"\n[{i}/{len(set(stream_names))}] 🌊 {name}")
            print(f"{'-'*40}")

            futures.append(
                (
                    name,
                    executor.submit(
                        _trigger_single_stream,
                        service_account_key_file=service_account_key_file,
                        project=project,
                        location=location,
                        display_name=name,
                        action=action,
                        wait_for_completion=wait_for_completion,
                        timeout_minutes=timeout_minutes,
                    ),
                )
            )

        print(f"\n{'='*60}")
        print("⚡ LIVE PROGRESS")
        print(f"{'='*60}")

        stream_results = []
        for name, future in futures:
            future_timeout = (timeout_minutes * 60 + 120) if wait_for_completion else 120
            response_txt = future.result(timeout=future_timeout)
            stream_results.append((name, response_txt))
            results.append(response_txt)

        print(f"\n{'='*80}")
        print("📊 RESULTS")
        print(f"{'='*80}")
        print(f"{'STREAM':<40} {'STATUS'}")
        print(f"{'-'*40} {'-'*30}")

        for name, response_txt in stream_results:
            if "SUCCESS" in response_txt or "RUNNING" in response_txt or "PAUSED" in response_txt:
                status = f"✅ {response_txt}"
            elif "FAILED" in response_txt:
                status = f"❌ {response_txt}"
            else:
                status = f"ℹ️ {response_txt}"
            print(f"{name:<40} {status}")

        print(f"{'='*80}\n")

    return results


def _trigger_single_stream(
    *,
    service_account_key_file: str,
    project: str,
    location: str,
    display_name: str,
    action: str,
    wait_for_completion: bool,
    timeout_minutes: int,
) -> str:
    from google.cloud import datastream_v1
    from google.protobuf import field_mask_pb2

    timestamp = datetime.datetime.now().strftime("%H:%M:%S")

    client = _get_datastream_client(service_account_key_file)

    print(f"{timestamp} 🔍 [{display_name}] Resolving stream name...")
    stream_resource_name = _resolve_stream_name(client, project, location, display_name)
    print(f"{timestamp} ✅ [{display_name}] Resolved to: {stream_resource_name}")

    # Determine target state
    if action in ("start", "resume"):
        target_state = datastream_v1.Stream.State.RUNNING
    elif action == "pause":
        target_state = datastream_v1.Stream.State.PAUSED
    else:
        raise Exception(f"Unsupported action: {action}. Use 'start', 'pause', or 'resume'.")

    print(f"{timestamp} 🚀 [{display_name}] Setting stream state to {action.upper()}...")

    stream = datastream_v1.Stream(
        name=stream_resource_name,
        state=target_state,
    )
    update_mask = field_mask_pb2.FieldMask(paths=["state"])

    operation = client.update_stream(
        stream=stream,
        update_mask=update_mask,
    )

    if not wait_for_completion:
        print(f"{timestamp} ✅ [{display_name}] State change initiated (not waiting)")
        return f"TRIGGERED ({action.upper()})"

    print(f"{timestamp} ⏳ [{display_name}] Waiting for state change to complete...")

    try:
        result = operation.result(timeout=timeout_minutes * 60)
        timestamp = datetime.datetime.now().strftime("%H:%M:%S")
        final_state = datastream_v1.Stream.State(result.state).name
        print(f"{timestamp} ✅ [{display_name}] Stream is now {final_state}")
        return f"SUCCESS ({final_state})"
    except Exception as e:
        timestamp = datetime.datetime.now().strftime("%H:%M:%S")
        error_msg = str(e)[:200]
        print(f"{timestamp} ❌ [{display_name}] State change failed: {error_msg}")
        return f"FAILED: {error_msg}"


def list_datastream_streams(
    *,
    service_account_key_file: str,
    project: str,
    location: str,
) -> None:
    """List all Datastream streams."""
    from google.cloud import datastream_v1

    client = _get_datastream_client(service_account_key_file)
    parent = f"projects/{project}/locations/{location}"

    print(f"\n🔍 Listing Datastream streams in {parent}")

    streams = list(client.list_streams(parent=parent))

    if not streams:
        print("No Datastream streams found.")
        return

    print(f"\n{'='*80}")
    print(f"📋 FOUND {len(streams)} STREAM(S)")
    print(f"{'='*80}")

    for i, stream in enumerate(streams, 1):
        state_name = datastream_v1.Stream.State(stream.state).name
        state_emoji = (
            "✅"
            if state_name == "RUNNING"
            else (
                "⏸️"
                if state_name == "PAUSED"
                else (
                    "🔄"
                    if state_name in ("STARTING", "DRAINING")
                    else (
                        "⏳"
                        if state_name == "NOT_STARTED"
                        else "❌" if state_name in ("FAILED", "FAILED_PERMANENTLY") else "❓"
                    )
                )
            )
        )

        display = stream.display_name or stream.name.split("/")[-1]

        source_type = "N/A"
        if stream.source_config and stream.source_config.source_connection_profile:
            source_type = stream.source_config.source_connection_profile.split("/")[-1]

        dest_type = "N/A"
        if stream.destination_config and stream.destination_config.destination_connection_profile:
            dest_type = stream.destination_config.destination_connection_profile.split("/")[-1]

        print(f"\n[{i}/{len(streams)}] 🌊 {display}")
        print(f"{'-'*50}")
        print(f"   Resource Name: {stream.name}")
        print(f"   {state_emoji} State: {state_name}")
        print(f"   Source: {source_type}")
        print(f"   Destination: {dest_type}")
        if stream.create_time:
            print(f"   Created: {stream.create_time}")
        if stream.update_time:
            print(f"   Last Updated: {stream.update_time}")

    print(f"\n{'='*80}\n")
