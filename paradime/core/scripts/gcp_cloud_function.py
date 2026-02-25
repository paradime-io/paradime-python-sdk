import datetime
import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Any, List, Optional

from paradime.core.scripts.gcp_utils import get_gcp_credentials, get_gcp_id_token_credentials

logging.basicConfig(format="%(asctime)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)


def _get_functions_client(service_account_key_file: str) -> Any:
    from google.cloud import functions_v2

    credentials = get_gcp_credentials(service_account_key_file)
    return functions_v2.FunctionServiceClient(credentials=credentials, transport="rest")


def _resolve_function_url(
    client: Any,
    project: str,
    location: str,
    function_name: str,
) -> str:
    """Resolve a function name to its invocation URL."""
    full_name = f"projects/{project}/locations/{location}/functions/{function_name}"
    function = client.get_function(name=full_name)

    url = function.service_config.uri
    if not url:
        raise Exception(
            f"Function '{function_name}' does not have an HTTP trigger URL. "
            f"Only HTTP-triggered functions can be invoked."
        )
    return url


def trigger_cloud_functions(
    *,
    service_account_key_file: str,
    project: str,
    location: str,
    function_names: List[str],
    payload: Optional[str] = None,
    wait_for_completion: bool = True,
    timeout_minutes: int = 30,
) -> List[str]:
    """Trigger Cloud Functions by name."""
    futures = []
    results = []

    print(f"\n{'='*60}")
    print("🚀 TRIGGERING CLOUD FUNCTIONS")
    print(f"{'='*60}")

    with ThreadPoolExecutor() as executor:
        for i, name in enumerate(set(function_names), 1):
            print(f"\n[{i}/{len(set(function_names))}] ⚡ {name}")
            print(f"{'-'*40}")

            futures.append(
                (
                    name,
                    executor.submit(
                        _trigger_single_function,
                        service_account_key_file=service_account_key_file,
                        project=project,
                        location=location,
                        function_name=name,
                        payload=payload,
                        wait_for_completion=wait_for_completion,
                        timeout_minutes=timeout_minutes,
                    ),
                )
            )

        print(f"\n{'='*60}")
        print("⚡ LIVE PROGRESS")
        print(f"{'='*60}")

        function_results = []
        for name, future in futures:
            future_timeout = (timeout_minutes * 60 + 120) if wait_for_completion else 120
            response_txt = future.result(timeout=future_timeout)
            function_results.append((name, response_txt))
            results.append(response_txt)

        print(f"\n{'='*80}")
        print("📊 RESULTS")
        print(f"{'='*80}")
        print(f"{'FUNCTION':<40} {'STATUS'}")
        print(f"{'-'*40} {'-'*30}")

        for name, response_txt in function_results:
            if "SUCCESS" in response_txt:
                status = "✅ SUCCESS"
            elif "FAILED" in response_txt:
                status = "❌ FAILED"
            else:
                status = "ℹ️ TRIGGERED"
            print(f"{name:<40} {status}")

        print(f"{'='*80}\n")

    return results


def _trigger_single_function(
    *,
    service_account_key_file: str,
    project: str,
    location: str,
    function_name: str,
    payload: Optional[str],
    wait_for_completion: bool,
    timeout_minutes: int,
) -> str:
    import json

    from google.auth.transport.requests import AuthorizedSession

    timestamp = datetime.datetime.now().strftime("%H:%M:%S")

    # Resolve function name to URL
    print(f"{timestamp} 🔍 [{function_name}] Resolving function URL...")
    client = _get_functions_client(service_account_key_file)
    function_url = _resolve_function_url(client, project, location, function_name)
    print(f"{timestamp} ✅ [{function_name}] URL: {function_url}")

    # Create ID token credentials for invocation
    credentials = get_gcp_id_token_credentials(service_account_key_file, function_url)
    session = AuthorizedSession(credentials)

    # Parse payload
    request_body = None
    if payload:
        try:
            request_body = json.loads(payload)
        except json.JSONDecodeError:
            request_body = {"data": payload}

    print(f"{timestamp} 🚀 [{function_name}] Invoking function...")

    timeout_seconds = timeout_minutes * 60

    if not wait_for_completion:
        # Fire and forget - use a very short timeout
        try:
            response = session.post(
                function_url,
                json=request_body,
                timeout=5,
            )
            timestamp = datetime.datetime.now().strftime("%H:%M:%S")
            print(f"{timestamp} ✅ [{function_name}] Request sent (status: {response.status_code})")
            return f"TRIGGERED (status: {response.status_code})"
        except Exception:
            timestamp = datetime.datetime.now().strftime("%H:%M:%S")
            print(f"{timestamp} ✅ [{function_name}] Request sent (fire-and-forget)")
            return "TRIGGERED (fire-and-forget)"

    # Synchronous invocation - wait for response
    response = session.post(
        function_url,
        json=request_body,
        timeout=timeout_seconds,
    )

    timestamp = datetime.datetime.now().strftime("%H:%M:%S")

    if response.status_code == 200:
        response_text = response.text[:200] if response.text else ""
        print(f"{timestamp} ✅ [{function_name}] Completed successfully (HTTP 200)")
        if response_text:
            print(f"{timestamp} 📄 [{function_name}] Response: {response_text}")
        return f"SUCCESS (HTTP {response.status_code})"
    else:
        error_text = response.text[:200] if response.text else ""
        print(
            f"{timestamp} ❌ [{function_name}] Failed with HTTP {response.status_code}: {error_text}"
        )
        return f"FAILED (HTTP {response.status_code}: {error_text})"


def list_cloud_functions(
    *,
    service_account_key_file: str,
    project: str,
    location: str,
) -> None:
    """List all Cloud Functions in the specified project and location."""
    from google.cloud import functions_v2

    client = _get_functions_client(service_account_key_file)
    parent = f"projects/{project}/locations/{location}"

    print(f"\n🔍 Listing Cloud Functions in {parent}")

    functions_list = list(client.list_functions(parent=parent))

    if not functions_list:
        print("No Cloud Functions found.")
        return

    print(f"\n{'='*80}")
    print(f"📋 FOUND {len(functions_list)} FUNCTION(S)")
    print(f"{'='*80}")

    for i, func in enumerate(functions_list, 1):
        name = func.name.split("/")[-1]
        state = functions_v2.Function.State(func.state).name
        state_emoji = (
            "✅" if state == "ACTIVE"
            else "🔄" if state == "DEPLOYING"
            else "❌" if state == "FAILED"
            else "❓"
        )

        url = func.service_config.uri if func.service_config else "N/A"
        runtime = func.build_config.runtime if func.build_config else "N/A"
        entry_point = func.build_config.entry_point if func.build_config else "N/A"

        print(f"\n[{i}/{len(functions_list)}] ⚡ {name}")
        print(f"{'-'*50}")
        print(f"   {state_emoji} State: {state}")
        print(f"   Runtime: {runtime}")
        print(f"   Entry Point: {entry_point}")
        print(f"   URL: {url}")
        if func.update_time:
            print(f"   Last Updated: {func.update_time}")

    print(f"\n{'='*80}\n")
