import logging
from concurrent.futures import ThreadPoolExecutor
from typing import List

import requests

from paradime.core.scripts.utils import handle_http_error

logging.basicConfig(format="%(asctime)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)


def trigger_tableau_refresh(
    *,
    host: str,
    personal_access_token_name: str,
    personal_access_token_secret: str,
    site_name: str,
    workbook_names: List[str],
    api_version: str,
) -> None:
    auth_response = requests.post(
        f"{host}/api/{api_version}/auth/signin",
        json={
            "credentials": {
                "personalAccessTokenName": personal_access_token_name,
                "personalAccessTokenSecret": personal_access_token_secret,
                "site": {"contentUrl": site_name},
            }
        },
        headers={"Accept": "application/json", "Content-Type": "application/json"},
    )
    handle_http_error(auth_response)

    # Extract token to use for subsequent calls
    auth_token: str = auth_response.json()["credentials"]["token"]
    site_id: str = auth_response.json()["credentials"]["site"]["id"]

    # call refresh for the workbooks async
    futures = []
    with ThreadPoolExecutor() as executor:
        for workbook_name in set(workbook_names):
            logger.info(f"Refreshing Tableau workbook: {workbook_name}")
            futures.append(
                (
                    workbook_name,
                    executor.submit(
                        _trigger_workbook_refresh,
                        host=host,
                        auth_token=auth_token,
                        site_id=site_id,
                        api_version=api_version,
                        workbook_name=workbook_name,
                    ),
                )
            )
        for workbook_name, future in futures:
            response_txt = future.result(timeout=60)
            logger.info(f"Refreshed Tableau workbook: {workbook_name} - {response_txt}")


def _trigger_workbook_refresh(
    *,
    host: str,
    auth_token: str,
    site_id: str,
    api_version: str,
    workbook_name: str,
) -> str:
    # find the workbook id
    workbook_response = requests.get(
        f"{host}/api/{api_version}/sites/{site_id}/workbooks",
        headers={
            "Accept": "application/json",
            "Content-Type": "application/json",
            "X-Tableau-Auth": auth_token,
        },
        params={"filter": f"name:eq:{workbook_name}"},
    )
    handle_http_error(workbook_response, f"Error searching for '{workbook_name}:'")

    workbooks_data = workbook_response.json()
    try:
        workbook_id = workbooks_data["workbooks"]["workbook"][0]["id"]
    except KeyError:
        raise Exception(f"Could not find workbook with name '{workbook_name}'")

    # Refresh the workbook
    refresh_trigger = requests.post(
        f"{host}/api/{api_version}/sites/{site_id}/workbooks/{workbook_id}/refresh",
        json={},
        headers={
            "Accept": "application/json",
            "Content-Type": "application/json",
            "X-Tableau-Auth": auth_token,
        },
    )
    handle_http_error(
        refresh_trigger, f"Error triggering refresh for '{workbook_name}' ({workbook_id}):"
    )

    return refresh_trigger.text
