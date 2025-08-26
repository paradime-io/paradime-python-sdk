import logging
import re
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
    workbook_names: List[str],  # Can now be names OR IDs
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
                        trigger_workbook_refresh,
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

def trigger_workbook_refresh(
    *,
    host: str,
    auth_token: str,
    site_id: str,
    api_version: str,
    workbook_name: str,  # Can be name OR ID
) -> str:
    workbook_id = None
    
    def _is_uuid_format(value: str) -> bool:
        """Check if input string matches UUID format."""
        uuid_pattern = r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        return bool(re.match(uuid_pattern, value, re.IGNORECASE))
    
    def _get_workbook_from_response(workbooks_data: dict) -> str | None:
        """Extract workbook ID from API response."""
        if "workbooks" not in workbooks_data or "workbook" not in workbooks_data["workbooks"]:
            return None
            
        workbook = workbooks_data["workbooks"]["workbook"]
        if isinstance(workbook, list) and len(workbook) > 0:
            return workbook[0]["id"]
        elif isinstance(workbook, dict):
            return workbook["id"]
        return None
    
    # If input looks like an ID, try direct access first
    if _is_uuid_format(workbook_name):
        try:
            logger.info(f"Input appears to be an ID, trying direct access: '{workbook_name}'")
            direct_workbook_response = requests.get(
                f"{host}/api/{api_version}/sites/{site_id}/workbooks/{workbook_name}",
                headers={
                    "Accept": "application/json",
                    "Content-Type": "application/json",
                    "X-Tableau-Auth": auth_token,
                },
            )
            
            if direct_workbook_response.status_code == 200:
                workbook_data = direct_workbook_response.json()
                if "workbook" in workbook_data:
                    workbook_id = workbook_data["workbook"]["id"]
                    logger.info(f"Found workbook by direct ID access: '{workbook_name}' -> confirmed ID: {workbook_id}")
            else:
                logger.warning(f"Could not access workbook directly by ID '{workbook_name}': HTTP {direct_workbook_response.status_code}")
        except Exception as e:
            logger.warning(f"Could not access workbook by direct ID '{workbook_name}': {e}")
    
    # If not found by direct ID access (or input wasn't an ID), try searching by name
    if workbook_id is None:
        try:
            logger.info(f"Searching for workbook by name: '{workbook_name}'")
            workbook_response = requests.get(
                f"{host}/api/{api_version}/sites/{site_id}/workbooks",
                headers={
                    "Accept": "application/json",
                    "Content-Type": "application/json",
                    "X-Tableau-Auth": auth_token,
                },
                params={"filter": f"name:eq:{workbook_name}"},
            )
            handle_http_error(workbook_response, f"Error searching for workbook by name '{workbook_name}':")
            
            workbooks_data = workbook_response.json()
            workbook_id = _get_workbook_from_response(workbooks_data)
            if workbook_id:
                logger.info(f"Found workbook by name: '{workbook_name}' -> ID: {workbook_id}")
        except Exception as e:
            logger.warning(f"Could not find workbook by name '{workbook_name}': {e}")
    
    # If still not found and input wasn't a UUID, try searching all workbooks for an ID match
    if workbook_id is None and not _is_uuid_format(workbook_name):
        try:
            logger.info(f"Searching all workbooks for potential ID match: '{workbook_name}'")
            all_workbooks_response = requests.get(
                f"{host}/api/{api_version}/sites/{site_id}/workbooks",
                headers={
                    "Accept": "application/json",
                    "Content-Type": "application/json",
                    "X-Tableau-Auth": auth_token,
                },
            )
            handle_http_error(all_workbooks_response, f"Error getting all workbooks while searching for '{workbook_name}':")
            
            workbooks_data = all_workbooks_response.json()
            if "workbooks" in workbooks_data and "workbook" in workbooks_data["workbooks"]:
                workbooks_list = workbooks_data["workbooks"]["workbook"]
                if isinstance(workbooks_list, dict):
                    workbooks_list = [workbooks_list]
                
                for workbook in workbooks_list:
                    if workbook.get("id") == workbook_name:
                        workbook_id = workbook["id"]
                        logger.info(f"Found workbook by ID in all workbooks search: '{workbook_name}' -> confirmed ID: {workbook_id}")
                        break
        except Exception as e:
            logger.warning(f"Could not search all workbooks for ID '{workbook_name}': {e}")
    
    # If still not found, raise an exception
    if workbook_id is None:
        raise Exception(f"Could not find workbook with name or ID '{workbook_name}'. Please check that the workbook exists and you have permission to access it.")
    
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
        refresh_trigger, f"Error triggering refresh for workbook '{workbook_name}' (ID: {workbook_id}):"
    )
    return refresh_trigger.text