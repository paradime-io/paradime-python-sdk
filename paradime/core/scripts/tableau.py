import logging
import re
import time
from concurrent.futures import ThreadPoolExecutor
from typing import List, Optional

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
    workbook_names: List[str],  # Can now be names OR UUIDs
    api_version: str,
    wait_for_completion: bool = False,
    timeout_minutes: int = 30,
) -> List[str]:
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
    results = []
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
                        wait_for_completion=wait_for_completion,
                        timeout_minutes=timeout_minutes,
                    ),
                )
            )
        for workbook_name, future in futures:
            # Use longer timeout when waiting for completion
            future_timeout = (timeout_minutes * 60 + 120) if wait_for_completion else 120
            response_txt = future.result(timeout=future_timeout)
            results.append(response_txt)
            logger.info(f"Refreshed Tableau workbook: {workbook_name} - {response_txt}")
    return results


def trigger_workbook_refresh(
    *,
    host: str,
    auth_token: str,
    site_id: str,
    api_version: str,
    workbook_name: str,  # Can be name OR UUID
    wait_for_completion: bool = False,
    timeout_minutes: int = 30,
) -> str:
    workbook_uuid = None

    def _is_uuid_format(value: str) -> bool:
        """Check if input string matches UUID format."""
        uuid_pattern = r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
        return bool(re.match(uuid_pattern, value, re.IGNORECASE))

    def _get_workbook_from_response(workbooks_data: dict) -> Optional[str]:
        """Extract workbook UUID from API response."""
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
                    workbook_uuid = workbook_data["workbook"]["id"]
                    logger.info(
                        f"Found workbook by direct UUID access: '{workbook_name}' -> confirmed UUID: {workbook_uuid}"
                    )
            else:
                logger.warning(
                    f"Could not access workbook directly by UUID '{workbook_name}': HTTP {direct_workbook_response.status_code}"
                )
        except Exception as e:
            logger.warning(f"Could not access workbook by direct UUID '{workbook_name}': {e}")

    # If not found by direct ID access (or input wasn't an ID), try searching by name
    if workbook_uuid is None:
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
            handle_http_error(
                workbook_response, f"Error searching for workbook by name '{workbook_name}':"
            )

            workbooks_data = workbook_response.json()
            workbook_uuid = _get_workbook_from_response(workbooks_data)
            if workbook_uuid:
                logger.info(f"Found workbook by name: '{workbook_name}' -> UUID: {workbook_uuid}")
        except Exception as e:
            logger.warning(f"Could not find workbook by name '{workbook_name}': {e}")

    # If still not found and input wasn't a UUID, try searching all workbooks for an ID match
    if workbook_uuid is None and not _is_uuid_format(workbook_name):
        try:
            logger.info(f"Searching all workbooks for potential UUID match: '{workbook_name}'")
            all_workbooks_response = requests.get(
                f"{host}/api/{api_version}/sites/{site_id}/workbooks",
                headers={
                    "Accept": "application/json",
                    "Content-Type": "application/json",
                    "X-Tableau-Auth": auth_token,
                },
            )
            handle_http_error(
                all_workbooks_response,
                f"Error getting all workbooks while searching for '{workbook_name}':",
            )

            workbooks_data = all_workbooks_response.json()
            if "workbooks" in workbooks_data and "workbook" in workbooks_data["workbooks"]:
                workbooks_list = workbooks_data["workbooks"]["workbook"]
                if isinstance(workbooks_list, dict):
                    workbooks_list = [workbooks_list]

                for workbook in workbooks_list:
                    if workbook.get("id") == workbook_name:
                        workbook_uuid = workbook["id"]
                        logger.info(
                            f"Found workbook by UUID in all workbooks search: '{workbook_name}' -> confirmed UUID: {workbook_uuid}"
                        )
                        break
        except Exception as e:
            logger.warning(f"Could not search all workbooks for UUID '{workbook_name}': {e}")

    # If still not found, raise an exception
    if workbook_uuid is None:
        raise Exception(
            f"Could not find workbook with name or UUID '{workbook_name}'. Please check that the workbook exists and you have permission to access it."
        )

    # Refresh the workbook
    refresh_trigger = requests.post(
        f"{host}/api/{api_version}/sites/{site_id}/workbooks/{workbook_uuid}/refresh",
        json={},
        headers={
            "Accept": "application/json",
            "Content-Type": "application/json",
            "X-Tableau-Auth": auth_token,
        },
    )
    handle_http_error(
        refresh_trigger,
        f"Error triggering refresh for workbook '{workbook_name}' (UUID: {workbook_uuid}):",
    )

    if not wait_for_completion:
        return refresh_trigger.text

    # Extract job ID from response to monitor completion
    job_id = _extract_job_id_from_response(refresh_trigger.text)
    if not job_id:
        logger.warning(
            f"Could not extract job ID from refresh response for workbook '{workbook_name}'. Cannot monitor completion."
        )
        return refresh_trigger.text

    logger.info(f"Monitoring refresh job {job_id} for workbook '{workbook_name}'...")

    # Wait for job completion
    job_status = _wait_for_job_completion(
        host=host,
        auth_token=auth_token,
        site_id=site_id,
        api_version=api_version,
        job_id=job_id,
        resource_type="Workbook",
        resource_name=workbook_name,
        timeout_minutes=timeout_minutes,
    )

    return f"Refresh completed. Job status: {job_status}"


def _extract_job_id_from_response(response_text: str) -> Optional[str]:
    """Extract job ID from refresh response JSON or XML."""
    try:
        # Try JSON first (newer API versions)
        import json

        data = json.loads(response_text)
        if "job" in data and "id" in data["job"]:
            return data["job"]["id"]
    except (json.JSONDecodeError, KeyError):
        pass

    try:
        # Fallback to XML parsing (older API versions)
        import xml.etree.ElementTree as ET

        root = ET.fromstring(response_text)

        # Look for job element in response
        job_elem = root.find(".//{http://onlinehelp.tableau.com/ts-api}job")
        if job_elem is not None:
            return job_elem.get("id")
    except Exception as e:
        logger.warning(f"Could not parse job ID from response: {e}")

    return None


def _wait_for_job_completion(
    *,
    host: str,
    auth_token: str,
    site_id: str,
    api_version: str,
    job_id: str,
    resource_type: str,
    resource_name: str,
    timeout_minutes: int,
) -> str:
    """Poll job status until completion or timeout."""
    start_time = time.time()
    timeout_seconds = timeout_minutes * 60
    sleep_interval = 5  # Poll every 5 seconds
    counter = 0

    while True:
        elapsed = time.time() - start_time
        if elapsed > timeout_seconds:
            raise Exception(
                f"Timeout waiting for {resource_type.lower()} '{resource_name}' refresh job {job_id} to complete after {timeout_minutes} minutes"
            )

        try:
            job_response = requests.get(
                f"{host}/api/{api_version}/sites/{site_id}/jobs/{job_id}",
                headers={
                    "Accept": "application/json",
                    "Content-Type": "application/json",
                    "X-Tableau-Auth": auth_token,
                },
            )

            if job_response.status_code != 200:
                logger.error(f"❌ Failed to get job status: HTTP {job_response.status_code}")
                logger.error("The refresh was triggered successfully, but job monitoring failed.")
                logger.error(
                    "The refresh may still be running on Tableau Server. Check the server directly for job status."
                )
                raise Exception(
                    f"Job monitoring failed with HTTP {job_response.status_code}. The refresh job was triggered but monitoring failed."
                )

            # Parse job status from JSON or XML response
            progress = "0"
            finish_code = None
            started_at = None
            completed_at = None

            try:
                # Try JSON first (newer API versions)
                import json

                data = json.loads(job_response.text)
                if "job" in data:
                    job = data["job"]
                    progress = str(job.get("progress", "0"))
                    finish_code = job.get("finishCode")
                    started_at = job.get("startedAt")
                    completed_at = job.get("completedAt")
                else:
                    logger.warning(f"No job element found in JSON response for job {job_id}")
                    time.sleep(sleep_interval)
                    continue
            except (json.JSONDecodeError, KeyError):
                # Fallback to XML parsing (older API versions)
                try:
                    import xml.etree.ElementTree as ET

                    root = ET.fromstring(job_response.text)
                    job_elem = root.find(".//{http://onlinehelp.tableau.com/ts-api}job")

                    if job_elem is None:
                        logger.warning(
                            f"Could not find job element in XML response for job {job_id}"
                        )
                        time.sleep(sleep_interval)
                        continue

                    progress = job_elem.get("progress", "0")
                    finish_code = job_elem.get("finishCode")
                    started_at = job_elem.get("startedAt")
                    completed_at = job_elem.get("completedAt")
                except Exception as e:
                    logger.warning(f"Could not parse job status response for {job_id}: {e}")
                    time.sleep(sleep_interval)
                    continue

            # Log progress on first check and then every 5 checks (2.5 minutes)
            if counter == 0 or counter % 5 == 0:
                if started_at:
                    logger.info(f"Job {job_id} progress: {progress}% (started at {started_at})")
                else:
                    logger.info(f"Job {job_id} progress: {progress}% (waiting to start...)")

            # Check if job is complete
            if finish_code is not None:
                if finish_code == "0":
                    elapsed_min = int(elapsed // 60)
                    elapsed_sec = int(elapsed % 60)
                    logger.info(
                        f"✅ {resource_type} '{resource_name}' refresh completed successfully in {elapsed_min}m {elapsed_sec}s"
                    )
                    return f"SUCCESS (finished at {completed_at})"
                elif finish_code == "1":
                    logger.error(f"❌ {resource_type} '{resource_name}' refresh failed")
                    return f"FAILED (finish code: {finish_code})"
                elif finish_code == "2":
                    logger.warning(f"⚠️ {resource_type} '{resource_name}' refresh was canceled")
                    return f"CANCELED (finish code: {finish_code})"
                else:
                    logger.warning(
                        f"⚠️ {resource_type} '{resource_name}' refresh finished with unknown code: {finish_code}"
                    )
                    return f"UNKNOWN (finish code: {finish_code})"

            counter += 1
            time.sleep(sleep_interval)

        except requests.exceptions.RequestException as e:
            logger.warning(f"Network error polling job status for {job_id}: {e}")
            time.sleep(sleep_interval)
            continue


def trigger_tableau_datasource_refresh(
    *,
    host: str,
    personal_access_token_name: str,
    personal_access_token_secret: str,
    site_name: str,
    datasource_names: List[str],  # Can now be names OR UUIDs
    api_version: str,
    wait_for_completion: bool = False,
    timeout_minutes: int = 30,
) -> List[str]:
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
    # call refresh for the datasources async
    futures = []
    results = []
    with ThreadPoolExecutor() as executor:
        for datasource_name in set(datasource_names):
            logger.info(f"Refreshing Tableau data source: {datasource_name}")
            futures.append(
                (
                    datasource_name,
                    executor.submit(
                        trigger_datasource_refresh,
                        host=host,
                        auth_token=auth_token,
                        site_id=site_id,
                        api_version=api_version,
                        datasource_name=datasource_name,
                        wait_for_completion=wait_for_completion,
                        timeout_minutes=timeout_minutes,
                    ),
                )
            )
        for datasource_name, future in futures:
            # Use longer timeout when waiting for completion
            future_timeout = (timeout_minutes * 60 + 120) if wait_for_completion else 120
            response_txt = future.result(timeout=future_timeout)
            results.append(response_txt)
            logger.info(f"Refreshed Tableau data source: {datasource_name} - {response_txt}")
    return results


def trigger_datasource_refresh(
    *,
    host: str,
    auth_token: str,
    site_id: str,
    api_version: str,
    datasource_name: str,  # Can be name OR UUID
    wait_for_completion: bool = False,
    timeout_minutes: int = 30,
) -> str:
    datasource_uuid = None

    def _is_uuid_format(value: str) -> bool:
        """Check if input string matches UUID format."""
        uuid_pattern = r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
        return bool(re.match(uuid_pattern, value, re.IGNORECASE))

    def _get_datasource_from_response(datasources_data: dict) -> Optional[str]:
        """Extract data source UUID from API response."""
        if (
            "datasources" not in datasources_data
            or "datasource" not in datasources_data["datasources"]
        ):
            return None

        datasource = datasources_data["datasources"]["datasource"]
        if isinstance(datasource, list) and len(datasource) > 0:
            return datasource[0]["id"]
        elif isinstance(datasource, dict):
            return datasource["id"]
        return None

    # If input looks like an ID, try direct access first
    if _is_uuid_format(datasource_name):
        try:
            logger.info(f"Input appears to be a UUID, trying direct access: '{datasource_name}'")
            direct_datasource_response = requests.get(
                f"{host}/api/{api_version}/sites/{site_id}/datasources/{datasource_name}",
                headers={
                    "Accept": "application/json",
                    "Content-Type": "application/json",
                    "X-Tableau-Auth": auth_token,
                },
            )

            if direct_datasource_response.status_code == 200:
                datasource_data = direct_datasource_response.json()
                if "datasource" in datasource_data:
                    datasource_uuid = datasource_data["datasource"]["id"]
                    logger.info(
                        f"Found data source by direct UUID access: '{datasource_name}' -> confirmed UUID: {datasource_uuid}"
                    )
            else:
                logger.warning(
                    f"Could not access data source directly by UUID '{datasource_name}': HTTP {direct_datasource_response.status_code}"
                )
        except Exception as e:
            logger.warning(f"Could not access data source by direct UUID '{datasource_name}': {e}")

    # If not found by direct ID access (or input wasn't an ID), try searching by name
    if datasource_uuid is None:
        try:
            logger.info(f"Searching for data source by name: '{datasource_name}'")
            datasource_response = requests.get(
                f"{host}/api/{api_version}/sites/{site_id}/datasources",
                headers={
                    "Accept": "application/json",
                    "Content-Type": "application/json",
                    "X-Tableau-Auth": auth_token,
                },
                params={"filter": f"name:eq:{datasource_name}"},
            )
            handle_http_error(
                datasource_response, f"Error searching for data source by name '{datasource_name}':"
            )

            datasources_data = datasource_response.json()
            datasource_uuid = _get_datasource_from_response(datasources_data)
            if datasource_uuid:
                logger.info(
                    f"Found data source by name: '{datasource_name}' -> UUID: {datasource_uuid}"
                )
        except Exception as e:
            logger.warning(f"Could not find data source by name '{datasource_name}': {e}")

    # If still not found and input wasn't a UUID, try searching all datasources for an ID match
    if datasource_uuid is None and not _is_uuid_format(datasource_name):
        try:
            logger.info(f"Searching all data sources for potential UUID match: '{datasource_name}'")
            all_datasources_response = requests.get(
                f"{host}/api/{api_version}/sites/{site_id}/datasources",
                headers={
                    "Accept": "application/json",
                    "Content-Type": "application/json",
                    "X-Tableau-Auth": auth_token,
                },
            )
            handle_http_error(
                all_datasources_response,
                f"Error getting all data sources while searching for '{datasource_name}':",
            )

            datasources_data = all_datasources_response.json()
            if (
                "datasources" in datasources_data
                and "datasource" in datasources_data["datasources"]
            ):
                datasources_list = datasources_data["datasources"]["datasource"]
                if isinstance(datasources_list, dict):
                    datasources_list = [datasources_list]

                for datasource in datasources_list:
                    if datasource.get("id") == datasource_name:
                        datasource_uuid = datasource["id"]
                        logger.info(
                            f"Found data source by UUID in all data sources search: '{datasource_name}' -> confirmed UUID: {datasource_uuid}"
                        )
                        break
        except Exception as e:
            logger.warning(f"Could not search all data sources for UUID '{datasource_name}': {e}")

    # If still not found, raise an exception
    if datasource_uuid is None:
        raise Exception(
            f"Could not find data source with name or UUID '{datasource_name}'. Please check that the data source exists and you have permission to access it."
        )

    # Refresh the data source
    refresh_trigger = requests.post(
        f"{host}/api/{api_version}/sites/{site_id}/datasources/{datasource_uuid}/refresh",
        json={},
        headers={
            "Accept": "application/json",
            "Content-Type": "application/json",
            "X-Tableau-Auth": auth_token,
        },
    )
    handle_http_error(
        refresh_trigger,
        f"Error triggering refresh for data source '{datasource_name}' (UUID: {datasource_uuid}):",
    )

    if not wait_for_completion:
        return refresh_trigger.text

    # Extract job ID from response to monitor completion
    job_id = _extract_job_id_from_response(refresh_trigger.text)
    if not job_id:
        logger.warning(
            f"Could not extract job ID from refresh response for data source '{datasource_name}'. Cannot monitor completion."
        )
        return refresh_trigger.text

    logger.info(f"Monitoring refresh job {job_id} for data source '{datasource_name}'...")

    # Wait for job completion
    job_status = _wait_for_job_completion(
        host=host,
        auth_token=auth_token,
        site_id=site_id,
        api_version=api_version,
        job_id=job_id,
        resource_type="Data source",
        resource_name=datasource_name,
        timeout_minutes=timeout_minutes,
    )

    return f"Refresh completed. Job status: {job_status}"


def list_tableau_workbooks(
    *,
    host: str,
    personal_access_token_name: str,
    personal_access_token_secret: str,
    site_name: str,
    api_version: str,
) -> None:
    """List all Tableau workbooks with their names and UUIDs."""
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

    auth_token: str = auth_response.json()["credentials"]["token"]
    site_id: str = auth_response.json()["credentials"]["site"]["id"]

    # Get all workbooks
    workbooks_response = requests.get(
        f"{host}/api/{api_version}/sites/{site_id}/workbooks",
        headers={
            "Accept": "application/json",
            "Content-Type": "application/json",
            "X-Tableau-Auth": auth_token,
        },
    )
    handle_http_error(workbooks_response, "Error getting workbooks:")

    workbooks_data = workbooks_response.json()

    if "workbooks" not in workbooks_data or "workbook" not in workbooks_data["workbooks"]:
        logger.info("No workbooks found.")
        return

    workbooks = workbooks_data["workbooks"]["workbook"]
    if isinstance(workbooks, dict):
        workbooks = [workbooks]

    logger.info(f"Found {len(workbooks)} workbook(s):")
    for workbook in workbooks:
        name = workbook.get("name", "Unknown")
        wb_uuid = workbook.get("id", "Unknown")
        project_name = (
            workbook.get("project", {}).get("name", "Unknown")
            if isinstance(workbook.get("project"), dict)
            else "Unknown"
        )
        logger.info(f"  {name} | {wb_uuid} | Project: {project_name}")


def list_tableau_datasources(
    *,
    host: str,
    personal_access_token_name: str,
    personal_access_token_secret: str,
    site_name: str,
    api_version: str,
) -> None:
    """List all Tableau data sources with their names and UUIDs."""
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

    auth_token: str = auth_response.json()["credentials"]["token"]
    site_id: str = auth_response.json()["credentials"]["site"]["id"]

    # Get all data sources
    datasources_response = requests.get(
        f"{host}/api/{api_version}/sites/{site_id}/datasources",
        headers={
            "Accept": "application/json",
            "Content-Type": "application/json",
            "X-Tableau-Auth": auth_token,
        },
    )
    handle_http_error(datasources_response, "Error getting data sources:")

    datasources_data = datasources_response.json()

    if "datasources" not in datasources_data or "datasource" not in datasources_data["datasources"]:
        logger.info("No data sources found.")
        return

    datasources = datasources_data["datasources"]["datasource"]
    if isinstance(datasources, dict):
        datasources = [datasources]

    logger.info(f"Found {len(datasources)} data source(s):")
    for datasource in datasources:
        name = datasource.get("name", "Unknown")
        ds_uuid = datasource.get("id", "Unknown")
        ds_type = datasource.get("type", "Unknown")
        project_name = (
            datasource.get("project", {}).get("name", "Unknown")
            if isinstance(datasource.get("project"), dict)
            else "Unknown"
        )
        logger.info(f"  {name} | {ds_uuid} | Type: {ds_type} | Project: {project_name}")
