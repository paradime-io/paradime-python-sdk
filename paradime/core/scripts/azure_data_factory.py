import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List, Optional

import requests

from paradime.cli import console
from paradime.core.scripts.utils import handle_http_error


def trigger_adf_pipeline_runs(
    *,
    tenant_id: str,
    client_id: str,
    client_secret: str,
    subscription_id: str,
    resource_group: str,
    factory_name: str,
    pipeline_names: List[str],
    parameters: Optional[Dict[str, Any]] = None,
    wait_for_completion: bool = True,
    timeout_minutes: int = 1440,
) -> List[str]:
    """
    Trigger runs for multiple Azure Data Factory pipelines.

    Args:
        tenant_id: Azure AD tenant ID
        client_id: Azure AD application (service principal) client ID
        client_secret: Azure AD application client secret
        subscription_id: Azure subscription ID
        resource_group: Azure resource group name
        factory_name: Azure Data Factory name
        pipeline_names: List of pipeline names to trigger
        parameters: Optional parameters to pass to the pipelines
        wait_for_completion: Whether to wait for runs to complete
        timeout_minutes: Maximum time to wait for completion

    Returns:
        List of run result messages for each pipeline
    """
    # Get authentication token
    access_token = _get_access_token(tenant_id, client_id, client_secret)
    auth_headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }

    futures = []
    results = []

    with ThreadPoolExecutor() as executor:
        for i, pipeline_name in enumerate(set(pipeline_names), 1):
            futures.append(
                (
                    pipeline_name,
                    executor.submit(
                        trigger_pipeline_run,
                        auth_headers=auth_headers,
                        subscription_id=subscription_id,
                        resource_group=resource_group,
                        factory_name=factory_name,
                        pipeline_name=pipeline_name,
                        parameters=parameters,
                        wait_for_completion=wait_for_completion,
                        timeout_minutes=timeout_minutes,
                    ),
                )
            )

        # Wait for completion and collect results
        pipeline_results = []
        for pipeline_name, future in futures:
            # Use longer timeout when waiting for completion
            future_timeout = (timeout_minutes * 60 + 120) if wait_for_completion else 120
            response_txt = future.result(timeout=future_timeout)
            pipeline_results.append((pipeline_name, response_txt))
            results.append(response_txt)

        def _status_text(response_txt: str) -> str:
            if "SUCCEEDED" in response_txt:
                return "SUCCEEDED"
            elif "FAILED" in response_txt:
                return "FAILED"
            elif "CANCELLED" in response_txt:
                return "CANCELLED"
            elif "CANCELLING" in response_txt:
                return "CANCELLING"
            elif "QUEUED" in response_txt:
                return "QUEUED"
            elif "IN_PROGRESS" in response_txt:
                return "IN PROGRESS"
            else:
                return "COMPLETED"

        console.table(
            columns=["Pipeline", "Status"],
            rows=[(pn, _status_text(response_txt)) for pn, response_txt in pipeline_results],
            title="Pipeline Run Results",
        )

    return results


def trigger_pipeline_run(
    *,
    auth_headers: dict,
    subscription_id: str,
    resource_group: str,
    factory_name: str,
    pipeline_name: str,
    parameters: Optional[Dict[str, Any]] = None,
    wait_for_completion: bool = True,
    timeout_minutes: int = 1440,
) -> str:
    """
    Trigger a run for a single Azure Data Factory pipeline.

    Args:
        auth_headers: Authentication headers (Bearer token)
        subscription_id: Azure subscription ID
        resource_group: Azure resource group name
        factory_name: Azure Data Factory name
        pipeline_name: Pipeline name to trigger
        parameters: Optional parameters to pass to the pipeline
        wait_for_completion: Whether to wait for run to complete
        timeout_minutes: Maximum time to wait for completion

    Returns:
        Status message indicating run result
    """

    base_url = "https://management.azure.com"
    api_version = "2018-06-01"

    # Trigger the pipeline run
    create_run_url = (
        f"{base_url}/subscriptions/{subscription_id}"
        f"/resourceGroups/{resource_group}"
        f"/providers/Microsoft.DataFactory/factories/{factory_name}"
        f"/pipelines/{pipeline_name}/createRun"
        f"?api-version={api_version}"
    )

    console.debug(f"[{pipeline_name}] Triggering pipeline run...")

    run_payload = parameters if parameters else {}

    run_response = requests.post(
        create_run_url,
        json=run_payload,
        headers=auth_headers,
    )

    handle_http_error(
        run_response,
        f"Error triggering pipeline run for '{pipeline_name}':",
    )

    run_data = run_response.json()
    run_id = run_data.get("runId")

    console.debug(f"[{pipeline_name}] Pipeline run triggered (Run ID: {run_id})")

    # Show Azure portal link
    portal_url = (
        f"https://adf.azure.com/en/monitoring/pipelineruns/{run_id}"
        f"?factory=/subscriptions/{subscription_id}"
        f"/resourceGroups/{resource_group}"
        f"/providers/Microsoft.DataFactory/factories/{factory_name}"
    )
    console.debug(f"[{pipeline_name}] Portal: {portal_url}")

    if not wait_for_completion:
        return f"Pipeline run triggered (Run ID: {run_id})"

    console.debug(f"[{pipeline_name}] Monitoring pipeline run progress...")

    # Wait for run completion
    run_status = _wait_for_pipeline_completion(
        auth_headers=auth_headers,
        subscription_id=subscription_id,
        resource_group=resource_group,
        factory_name=factory_name,
        run_id=run_id,
        pipeline_name=pipeline_name,
        timeout_minutes=timeout_minutes,
    )

    return f"Pipeline run completed. Final status: {run_status}"


def _get_access_token(tenant_id: str, client_id: str, client_secret: str) -> str:
    """
    Get Azure AD access token using client credentials flow.

    Args:
        tenant_id: Azure AD tenant ID
        client_id: Azure AD application client ID
        client_secret: Azure AD application client secret

    Returns:
        Access token for Azure Management API calls
    """
    token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"

    payload = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
        "scope": "https://management.azure.com/.default",
    }

    response = requests.post(token_url, data=payload)

    handle_http_error(response, "Error getting Azure AD access token:")

    token_data = response.json()
    return token_data["access_token"]


def _wait_for_pipeline_completion(
    *,
    auth_headers: dict,
    subscription_id: str,
    resource_group: str,
    factory_name: str,
    run_id: str,
    pipeline_name: str,
    timeout_minutes: int,
) -> str:
    """
    Poll pipeline run status until completion or timeout.

    Args:
        auth_headers: Authentication headers (Bearer token)
        subscription_id: Azure subscription ID
        resource_group: Azure resource group name
        factory_name: Azure Data Factory name
        run_id: Pipeline run ID
        pipeline_name: Pipeline name for logging
        timeout_minutes: Maximum time to wait for completion

    Returns:
        Final pipeline run status
    """
    base_url = "https://management.azure.com"
    api_version = "2018-06-01"
    start_time = time.time()
    timeout_seconds = timeout_minutes * 60
    sleep_interval = 5
    counter = 0
    consecutive_failures = 0
    max_consecutive_failures = 5

    run_url = (
        f"{base_url}/subscriptions/{subscription_id}"
        f"/resourceGroups/{resource_group}"
        f"/providers/Microsoft.DataFactory/factories/{factory_name}"
        f"/pipelineruns/{run_id}"
        f"?api-version={api_version}"
    )

    while True:
        elapsed = time.time() - start_time
        if elapsed > timeout_seconds:
            raise Exception(
                f"Timeout waiting for pipeline '{pipeline_name}' run '{run_id}' "
                f"to complete after {timeout_minutes} minutes"
            )

        try:
            run_response = requests.get(
                run_url,
                headers=auth_headers,
            )

            if run_response.status_code != 200:
                consecutive_failures += 1
                if consecutive_failures >= max_consecutive_failures:
                    raise Exception(
                        f"Pipeline run status check failed {consecutive_failures} times in a row. "
                        f"Last HTTP status: {run_response.status_code}"
                    )

                console.debug(
                    f"[{pipeline_name}] HTTP {run_response.status_code} error. "
                    f"Retrying... ({consecutive_failures}/{max_consecutive_failures})"
                )
                time.sleep(sleep_interval * min(consecutive_failures, 3))
                continue

            run_data = run_response.json()
            run_status = run_data.get("status", "unknown")

            # Reset failure counter on successful request
            consecutive_failures = 0

            # Log progress every 6 checks (30 seconds)
            if counter == 0 or counter % 6 == 0:

                elapsed_min = int(elapsed // 60)
                elapsed_sec = int(elapsed % 60)

                if run_status == "InProgress":
                    console.debug(
                        f"[{pipeline_name}] Pipeline running... "
                        f"({elapsed_min}m {elapsed_sec}s elapsed)"
                    )
                elif run_status == "Queued":
                    console.debug(
                        f"[{pipeline_name}] Pipeline queued... "
                        f"({elapsed_min}m {elapsed_sec}s elapsed)"
                    )

            # Check if run is complete
            if run_status in ["Succeeded", "Failed", "Cancelled"]:

                elapsed_min = int(elapsed // 60)
                elapsed_sec = int(elapsed % 60)

                if run_status == "Succeeded":
                    console.debug(
                        f"[{pipeline_name}] Pipeline completed successfully "
                        f"({elapsed_min}m {elapsed_sec}s)"
                    )
                    return f"SUCCEEDED (run ID: {run_id})"
                elif run_status == "Failed":
                    message = run_data.get("message", "No error message available")
                    console.error(f"[{pipeline_name}] Pipeline failed: {message}")
                    return f"FAILED (run ID: {run_id})"
                elif run_status == "Cancelled":
                    console.debug(f"[{pipeline_name}] Pipeline cancelled")
                    return f"CANCELLED (run ID: {run_id})"

            elif run_status == "Cancelling":

                console.debug(f"[{pipeline_name}] Pipeline is being cancelled...")

            elif run_status in ["InProgress", "Queued"]:
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
                    f"Network errors occurred {consecutive_failures} times in a row. "
                    f"Last error: {str(e)[:100]}"
                )

            console.debug(
                f"[{pipeline_name}] Network error: {str(e)[:50]}... "
                f"Retrying... ({consecutive_failures}/{max_consecutive_failures})"
            )
            time.sleep(sleep_interval * min(consecutive_failures, 3))
            continue


def list_adf_pipelines(
    *,
    tenant_id: str,
    client_id: str,
    client_secret: str,
    subscription_id: str,
    resource_group: str,
    factory_name: str,
) -> None:
    """
    List all pipelines in an Azure Data Factory.

    Args:
        tenant_id: Azure AD tenant ID
        client_id: Azure AD application (service principal) client ID
        client_secret: Azure AD application client secret
        subscription_id: Azure subscription ID
        resource_group: Azure resource group name
        factory_name: Azure Data Factory name
    """
    # Get authentication token
    access_token = _get_access_token(tenant_id, client_id, client_secret)
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }

    base_url = "https://management.azure.com"
    api_version = "2018-06-01"

    pipelines_url = (
        f"{base_url}/subscriptions/{subscription_id}"
        f"/resourceGroups/{resource_group}"
        f"/providers/Microsoft.DataFactory/factories/{factory_name}"
        f"/pipelines?api-version={api_version}"
    )

    console.info(f"Listing pipelines for factory: {factory_name}")

    pipelines_response = requests.get(pipelines_url, headers=headers)

    handle_http_error(pipelines_response, "Error getting pipelines:")

    pipelines_data = pipelines_response.json()

    if "value" not in pipelines_data or not pipelines_data["value"]:
        console.info("No pipelines found.")
        return

    pipelines = pipelines_data["value"]

    rows = []
    for pipeline in pipelines:
        name = pipeline.get("name", "Unknown")
        pipeline_type = pipeline.get("type", "Unknown")
        description = pipeline.get("properties", {}).get("description", "")
        activities = pipeline.get("properties", {}).get("activities", [])
        pipeline_parameters = pipeline.get("properties", {}).get("parameters", {})
        param_names = ", ".join(pipeline_parameters.keys()) if pipeline_parameters else ""
        rows.append((name, pipeline_type, str(len(activities)), param_names, description))

    console.table(
        columns=["Name", "Type", "Activities", "Parameters", "Description"],
        rows=rows,
        title=f"ADF Pipelines ({factory_name})",
    )
