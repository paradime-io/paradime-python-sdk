from __future__ import annotations

import base64
import json
import logging
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Final, List, Optional

import msal  # type: ignore[import-untyped]
import requests

from paradime.core.scripts.utils import handle_http_error

logging.basicConfig(format="%(asctime)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)


POWER_BI_HOST: Final = "https://api.powerbi.com"


@dataclass(frozen=True)
class Dataset:
    id: str
    name: str
    is_refreshable: bool


def trigger_power_bi_refreshes(
    *,
    tenant_id: str,
    client_id: str,
    client_secret: str,
    group_id: str,
    dataset_names: List[str],
    refresh_request_body_b64: Optional[str],
) -> None:
    access_token = get_access_token(tenant_id, client_id, client_secret)

    datasets = get_power_bi_datasets(
        access_token=access_token,
        group_id=group_id,
    )

    dataset_ids = set()
    for dataset_name in dataset_names:
        if dataset_name not in datasets:
            raise Exception(
                f"Unable to find dataset: '{dataset_name}' in datasets. Available are: {list(datasets.keys())}"
            )
        dataset_ids.add(datasets[dataset_name].id)

    # Refresh the dataset
    refresh_request_body = None
    if refresh_request_body_b64:
        try:
            refresh_request_body = json.loads(base64.b64decode(refresh_request_body_b64))
        except Exception as e:
            raise Exception(f"Could not decode refresh body: {e}")

    futures = []
    with ThreadPoolExecutor() as executor:
        for dataset_id in dataset_ids:
            logger.info(f"Refreshing Power BI dataset: {dataset_id}")
            futures.append(
                (
                    dataset_id,
                    executor.submit(
                        _refresh_power_bi_dataset,
                        access_token=access_token,
                        group_id=group_id,
                        dataset_id=dataset_id,
                        refresh_request_body=refresh_request_body,
                    ),
                )
            )
        for dataset_id, future in futures:
            future.result(timeout=60)


def _refresh_power_bi_dataset(
    *,
    access_token: str,
    group_id: str,
    dataset_id: str,
    refresh_request_body: Optional[dict],
) -> str:
    refresh_response = requests.post(
        f"{POWER_BI_HOST}/v1.0/myorg/groups/{group_id}/datasets/{dataset_id}/refreshes",
        headers={"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"},
        json=refresh_request_body or {},
    )
    handle_http_error(refresh_response)
    return refresh_response.text


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
