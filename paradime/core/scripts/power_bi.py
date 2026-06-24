from __future__ import annotations

import base64
import json
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Final, List, Optional

import msal  # type: ignore[import-untyped]
import requests

from paradime.cli import console
from paradime.core.scripts.utils import handle_http_error

POWER_BI_HOST: Final = "https://api.powerbi.com"
REFRESH_POLL_INTERVAL_SECONDS: Final = 10


@dataclass(frozen=True)
class Dataset:
    id: str
    name: str
    is_refreshable: bool


@dataclass(frozen=True)
class RefreshResult:
    dataset_name: str
    dataset_id: str
    request_id: Optional[str]
    # One of: "Triggered" (not waited), "Completed", "Failed", "Disabled", "Timeout"
    status: str
    error: Optional[str] = None

    @property
    def is_failure(self) -> bool:
        return self.status in ("Failed", "Disabled", "Timeout")


def trigger_power_bi_refreshes(
    *,
    tenant_id: str,
    client_id: str,
    client_secret: str,
    group_id: str,
    dataset_names: List[str],
    refresh_request_body_b64: Optional[str],
    wait: bool = False,
    timeout_minutes: int = 60,
) -> List[RefreshResult]:
    """
    Trigger a Power BI refresh for each named dataset.

    When ``wait`` is False (default), this returns as soon as each refresh has been
    accepted by Power BI (fire-and-forget). When ``wait`` is True, it polls each
    dataset's refresh history until the refresh reaches a terminal state or
    ``timeout_minutes`` elapses, and reports the outcome per dataset.
    """
    access_token = get_access_token(tenant_id, client_id, client_secret)

    datasets = get_power_bi_datasets(
        access_token=access_token,
        group_id=group_id,
    )

    # Resolve names to ids up front so a bad name fails fast before anything is triggered.
    targets = []
    for dataset_name in dataset_names:
        if dataset_name not in datasets:
            raise Exception(
                f"Unable to find dataset: '{dataset_name}' in datasets. Available are: {list(datasets.keys())}"
            )
        targets.append((dataset_name, datasets[dataset_name].id))

    refresh_request_body = None
    if refresh_request_body_b64:
        try:
            refresh_request_body = json.loads(base64.b64decode(refresh_request_body_b64))
        except Exception as e:
            raise Exception(f"Could not decode refresh body: {e}")

    def _run_one(dataset_name: str, dataset_id: str) -> RefreshResult:
        console.debug(f"Refreshing Power BI dataset: {dataset_name} ({dataset_id})")
        try:
            request_id = _refresh_power_bi_dataset(
                access_token=access_token,
                group_id=group_id,
                dataset_id=dataset_id,
                refresh_request_body=refresh_request_body,
            )
        except Exception as e:
            return RefreshResult(
                dataset_name=dataset_name,
                dataset_id=dataset_id,
                request_id=None,
                status="Failed",
                error=f"Failed to trigger refresh: {e}",
            )

        if not wait:
            return RefreshResult(
                dataset_name=dataset_name,
                dataset_id=dataset_id,
                request_id=request_id,
                status="Triggered",
            )

        return _wait_for_power_bi_refresh(
            access_token=access_token,
            group_id=group_id,
            dataset_name=dataset_name,
            dataset_id=dataset_id,
            request_id=request_id,
            timeout_minutes=timeout_minutes,
        )

    # The poll loop enforces the timeout internally; allow a small buffer on the future.
    future_timeout = (timeout_minutes * 60 + 60) if wait else 60

    results: List[RefreshResult] = []
    with ThreadPoolExecutor() as executor:
        futures = [
            (name, executor.submit(_run_one, name, dataset_id)) for name, dataset_id in targets
        ]
        for name, future in futures:
            results.append(future.result(timeout=future_timeout))

    return results


def _refresh_power_bi_dataset(
    *,
    access_token: str,
    group_id: str,
    dataset_id: str,
    refresh_request_body: Optional[dict],
) -> Optional[str]:
    """Trigger a refresh and return the Power BI ``RequestId`` of the triggered refresh."""
    refresh_response = requests.post(
        f"{POWER_BI_HOST}/v1.0/myorg/groups/{group_id}/datasets/{dataset_id}/refreshes",
        headers={"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"},
        json=refresh_request_body or {},
    )
    handle_http_error(refresh_response)
    # Power BI returns the id of the triggered refresh in the RequestId response header.
    return refresh_response.headers.get("RequestId") or refresh_response.headers.get(
        "x-ms-request-id"
    )


def _wait_for_power_bi_refresh(
    *,
    access_token: str,
    group_id: str,
    dataset_name: str,
    dataset_id: str,
    request_id: Optional[str],
    timeout_minutes: int,
) -> RefreshResult:
    """Poll a dataset's refresh history until the triggered refresh reaches a terminal state."""
    start_time = time.time()
    timeout_seconds = timeout_minutes * 60
    counter = 0

    while True:
        elapsed = time.time() - start_time
        if elapsed > timeout_seconds:
            return RefreshResult(
                dataset_name=dataset_name,
                dataset_id=dataset_id,
                request_id=request_id,
                status="Timeout",
                error=f"Refresh did not complete within {timeout_minutes} minutes",
            )

        try:
            history_response = requests.get(
                f"{POWER_BI_HOST}/v1.0/myorg/groups/{group_id}/datasets/{dataset_id}/refreshes?$top=10",
                headers={
                    "Authorization": f"Bearer {access_token}",
                    "Content-Type": "application/json",
                },
            )
            if history_response.status_code != 200:
                console.debug(
                    f"[{dataset_name}] Refresh history check returned HTTP "
                    f"{history_response.status_code}. Retrying."
                )
                time.sleep(REFRESH_POLL_INTERVAL_SECONDS)
                continue

            entries = history_response.json().get("value", [])
            entry = None
            if request_id:
                entry = next((e for e in entries if e.get("requestId") == request_id), None)
            # Fall back to the most recent refresh if we could not capture the request id.
            if entry is None and not request_id and entries:
                entry = entries[0]

            if entry is None:
                # Refresh not yet visible in history; keep waiting.
                time.sleep(REFRESH_POLL_INTERVAL_SECONDS)
                continue

            status = entry.get("status", "Unknown")

            if counter == 0 or counter % 6 == 0:
                elapsed_min, elapsed_sec = int(elapsed // 60), int(elapsed % 60)
                console.debug(
                    f"[{dataset_name}] Refresh status: {status} "
                    f"({elapsed_min}m {elapsed_sec}s elapsed)"
                )

            if status == "Completed":
                return RefreshResult(
                    dataset_name=dataset_name,
                    dataset_id=dataset_id,
                    request_id=request_id,
                    status="Completed",
                )
            if status == "Failed":
                return RefreshResult(
                    dataset_name=dataset_name,
                    dataset_id=dataset_id,
                    request_id=request_id,
                    status="Failed",
                    error=_parse_refresh_error(entry.get("serviceExceptionJson")),
                )
            if status == "Disabled":
                return RefreshResult(
                    dataset_name=dataset_name,
                    dataset_id=dataset_id,
                    request_id=request_id,
                    status="Disabled",
                    error="Refresh is disabled for this dataset",
                )

            # "Unknown" means the refresh is still in progress.
            counter += 1
            time.sleep(REFRESH_POLL_INTERVAL_SECONDS)

        except requests.exceptions.RequestException as e:
            console.debug(f"[{dataset_name}] Network error: {str(e)[:50]}... Retrying.")
            time.sleep(REFRESH_POLL_INTERVAL_SECONDS)
            continue


def _parse_refresh_error(service_exception_json: Optional[str]) -> str:
    """Extract a human-readable message from Power BI's serviceExceptionJson, if present."""
    if not service_exception_json:
        return "Refresh failed"
    try:
        data = json.loads(service_exception_json)
        return data.get("errorDescription") or data.get("errorCode") or service_exception_json
    except Exception:
        return service_exception_json


def get_power_bi_datasets(
    *,
    access_token: str,
    group_id: str,
) -> dict[str, Dataset]:
    """Get the datasets for the Power BI API."""
    datasets_response = requests.get(
        f"{POWER_BI_HOST}/v1.0/myorg/groups/{group_id}/datasets",
        headers={"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"},
    )
    handle_http_error(datasets_response)
    response_json = datasets_response.json()
    if "value" not in response_json:
        raise ValueError(f"Invalid response from datasets list: {datasets_response.text}")
    datasets = {}
    for dataset in response_json["value"]:
        datasets[dataset.get("name")] = Dataset(
            id=dataset.get("id"),
            name=dataset.get("name"),
            is_refreshable=dataset.get("isRefreshable"),
        )
    return datasets


def get_access_token(tenant_id: str, client_id: str, client_secret: str) -> str:
    authority_url = "https://login.microsoftonline.com/" + tenant_id
    scope = ["https://analysis.windows.net/powerbi/api/.default"]

    app = msal.ConfidentialClientApplication(
        client_id, authority=authority_url, client_credential=client_secret
    )
    access_token_response = app.acquire_token_for_client(scopes=scope)
    if "access_token" not in access_token_response:
        raise Exception(
            f"Could not get access token for Power BI API. Please double check your credentials: {access_token_response}"
        )
    return access_token_response.get("access_token")
