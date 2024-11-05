import base64
import json
from concurrent.futures import ThreadPoolExecutor
from typing import List, Optional

import msal  # type: ignore[import-untyped]
import requests

from paradime.client.paradime_cli_client import logger
from paradime.core.scripts.utils import handle_http_error


def trigger_power_bi_refreshes(
    *,
    host: str,
    tenant_id: str,
    client_id: str,
    client_secret: str,
    group_id: str,
    dataset_ids: List[str],
    refresh_request_body_b64: Optional[str],
) -> None:
    access_token = _get_access_token(tenant_id, client_id, client_secret)

    # Refresh the dataset
    refresh_request_body = None
    if refresh_request_body_b64:
        try:
            refresh_request_body = json.loads(base64.b64decode(refresh_request_body_b64))
        except Exception as e:
            logger.warning(f"Could not decode refresh body: {e}")

    futures = []
    with ThreadPoolExecutor() as executor:
        for dataset_id in set(dataset_ids):
            logger.info(f"Triggering refresh for dataset: {dataset_id}...")
            futures.append(
                (
                    dataset_id,
                    executor.submit(
                        _refresh_power_bi_dataset,
                        host=host,
                        access_token=access_token,
                        group_id=group_id,
                        dataset_id=dataset_id,
                        refresh_request_body=refresh_request_body,
                    ),
                )
            )
        for dataset_id, future in futures:
            response_txt = future.result(timeout=60)
            logger.info(f"{dataset_id}: {response_txt}")


def _refresh_power_bi_dataset(
    *,
    host: str,
    access_token: str,
    group_id: str,
    dataset_id: str,
    refresh_request_body: Optional[dict],
) -> str:
    refresh_response = requests.post(
        f"{host}/v1.0/myorg/groups/{group_id}/datasets/{dataset_id}/refreshes",
        headers={"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"},
        json=refresh_request_body or {},
    )
    handle_http_error(refresh_response)
    return refresh_response.text


def get_power_bi_datasets(
    *,
    host: str,
    tenant_id: str,
    client_id: str,
    client_secret: str,
    group_id: str,
) -> dict:
    """Get the datasets for the Power BI API."""
    access_token = _get_access_token(tenant_id, client_id, client_secret)
    datasets_response = requests.get(
        f"{host}/v1.0/myorg/groups/{group_id}/datasets",
        headers={"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"},
    )
    handle_http_error(datasets_response)
    return datasets_response.json()


def _get_access_token(tenant_id: str, client_id: str, client_secret: str) -> str:
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
