import logging
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List, Optional

import requests

from paradime.core.scripts.utils import handle_http_error

logging.basicConfig(format="%(asctime)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)


def trigger_hex_runs(
    *,
    api_token: str,
    base_url: str,
    project_ids: List[str],
    input_params: Optional[Dict[str, Any]] = None,
    update_published: bool = True,
    wait_for_completion: bool = True,
    timeout_minutes: int = 1440,
) -> List[str]:
    """
    Trigger runs for multiple Hex projects.

    Args:
        api_token: Hex API token
        base_url: Hex base URL
        project_ids: List of Hex project IDs to trigger
        input_params: Optional input parameters to pass to projects
        update_published: Whether to update cached app state with run results
        wait_for_completion: Whether to wait for runs to complete
        timeout_minutes: Maximum time to wait for completion

    Returns:
        List of run result messages for each project
    """
    futures = []
    results = []

    # Add visual separator and header
    print(f"\n{'='*60}")
    print("🚀 TRIGGERING HEX PROJECTS")
    print(f"{'='*60}")

    with ThreadPoolExecutor() as executor:
        for i, project_id in enumerate(set(project_ids), 1):
            print(f"\n[{i}/{len(set(project_ids))}] 🔌 {project_id}")
            print(f"{'-'*40}")

            futures.append(
                (
                    project_id,
                    executor.submit(
                        trigger_single_run,
                        api_token=api_token,
                        base_url=base_url,
                        project_id=project_id,
                        input_params=input_params,
                        update_published=update_published,
                        wait_for_completion=wait_for_completion,
                        timeout_minutes=timeout_minutes,
                    ),
                )
            )

        # Add separator for live progress section
        print(f"\n{'='*60}")
        print("⚡ LIVE PROGRESS")
        print(f"{'='*60}")

        # Wait for completion and collect results
        project_results = []
        for project_id, future in futures:
            # Use longer timeout when waiting for completion
            future_timeout = (timeout_minutes * 60 + 120) if wait_for_completion else 120
            response_txt = future.result(timeout=future_timeout)
            project_results.append((project_id, response_txt))
            results.append(response_txt)

        # Display results as simple table
        print(f"\n{'='*80}")
        print("📊 RUN RESULTS")
        print(f"{'='*80}")
        print(f"{'PROJECT':<40} {'STATUS':<15} {'URL'}")
        print(f"{'-'*40} {'-'*15} {'-'*45}")

        for project_id, response_txt in project_results:
            # Format result with emoji
            if "COMPLETED" in response_txt:
                status = "✅ COMPLETED"
            elif "ERRORED" in response_txt:
                status = "❌ ERRORED"
            elif "KILLED" in response_txt:
                status = "🚫 KILLED"
            elif "UNABLE_TO_ALLOCATE_KERNEL" in response_txt:
                status = "⚠️ NO KERNEL"
            elif "PENDING" in response_txt or "RUNNING" in response_txt:
                status = "🔄 RUNNING"
            else:
                status = "ℹ️ TRIGGERED"

            # Extract URL from response if available
            run_url = ""
            if "run_url:" in response_txt:
                run_url = response_txt.split("run_url:")[1].strip().split()[0]

            print(f"{project_id:<40} {status:<15} {run_url}")

        print(f"{'='*80}\n")

    return results


def trigger_single_run(
    *,
    api_token: str,
    base_url: str,
    project_id: str,
    input_params: Optional[Dict[str, Any]] = None,
    update_published: bool = True,
    wait_for_completion: bool = True,
    timeout_minutes: int = 1440,
) -> str:
    """
    Trigger a run for a single Hex project.

    Args:
        api_token: Hex API token
        base_url: Hex base URL
        project_id: Hex project ID
        input_params: Optional input parameters to pass to project
        update_published: Whether to update cached app state with run results
        wait_for_completion: Whether to wait for run to complete
        timeout_minutes: Maximum time to wait for completion

    Returns:
        Status message indicating run result
    """
    headers = {
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/json",
    }

    import datetime

    timestamp = datetime.datetime.now().strftime("%H:%M:%S")

    # Prepare request payload
    payload: Dict[str, Any] = {
        "updatePublishedResults": update_published,
        "useCachedSqlResults": False,
    }
    if input_params:
        payload["inputParams"] = input_params

    # Trigger the run
    api_url = f"{base_url}/api/v1/projects/{project_id}/runs"

    print(f"{timestamp} 🚀 [{project_id}] Triggering project run...")

    run_response = requests.post(
        api_url,
        headers=headers,
        json=payload,
        timeout=30,
    )

    handle_http_error(
        run_response,
        f"Error triggering run for project '{project_id}':",
    )

    run_data = run_response.json()
    run_id = run_data.get("runId")
    run_url = run_data.get("runUrl", f"{base_url}/hex/{project_id}/app")
    run_status = run_data.get("status", "PENDING")

    print(f"{timestamp} ✅ [{project_id}] Run triggered (Run ID: {run_id})", flush=True)
    print(f"{timestamp} 🔗 [{project_id}] URL: {run_url}", flush=True)

    if not wait_for_completion:
        return f"Run triggered (Run ID: {run_id}, Status: {run_status}) run_url:{run_url}"

    print(f"{timestamp} ⏳ [{project_id}] Monitoring run progress...", flush=True)

    # Wait for run completion
    final_status = _wait_for_run_completion(
        api_token=api_token,
        base_url=base_url,
        project_id=project_id,
        run_id=run_id,
        timeout_minutes=timeout_minutes,
    )

    return f"Run completed. Final status: {final_status} run_url:{run_url}"


def _wait_for_run_completion(
    *,
    api_token: str,
    base_url: str,
    project_id: str,
    run_id: str,
    timeout_minutes: int,
) -> str:
    """
    Poll run status until completion or timeout.

    Args:
        api_token: Hex API token
        base_url: Hex base URL
        project_id: Hex project ID
        run_id: Hex run ID
        timeout_minutes: Maximum time to wait for completion

    Returns:
        Final run status
    """
    headers = {
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/json",
    }
    start_time = time.time()
    timeout_seconds = timeout_minutes * 60
    sleep_interval = 10  # Poll every 10 seconds (Hex rate limits)
    counter = 0

    api_url = f"{base_url}/api/v1/projects/{project_id}/runs/{run_id}"

    while True:
        elapsed = time.time() - start_time
        if elapsed > timeout_seconds:
            raise Exception(
                f"Timeout waiting for project '{project_id}' run to complete after {timeout_minutes} minutes"
            )

        try:
            # Get run status
            run_response = requests.get(
                api_url,
                headers=headers,
                timeout=30,
            )

            if run_response.status_code != 200:
                raise Exception(f"Run status check failed with HTTP {run_response.status_code}")

            run_data = run_response.json()
            run_status = run_data.get("status", "UNKNOWN")

            # Log progress every 6 checks (60 seconds)
            if counter == 0 or counter % 6 == 0:
                import datetime

                timestamp = datetime.datetime.now().strftime("%H:%M:%S")
                elapsed_min = int(elapsed // 60)
                elapsed_sec = int(elapsed % 60)

                if run_status in ["PENDING", "RUNNING"]:
                    print(
                        f"{timestamp} 🔄 [{project_id}] {run_status}... ({elapsed_min}m {elapsed_sec}s elapsed)",
                        flush=True,
                    )

            # Check if run is complete
            if run_status == "COMPLETED":
                import datetime

                timestamp = datetime.datetime.now().strftime("%H:%M:%S")
                elapsed_min = int(elapsed // 60)
                elapsed_sec = int(elapsed % 60)
                print(
                    f"{timestamp} ✅ [{project_id}] Completed successfully ({elapsed_min}m {elapsed_sec}s)",
                    flush=True,
                )
                return "COMPLETED"

            elif run_status == "ERRORED":
                import datetime

                timestamp = datetime.datetime.now().strftime("%H:%M:%S")
                print(f"{timestamp} ❌ [{project_id}] Run errored", flush=True)
                return "ERRORED"

            elif run_status == "KILLED":
                import datetime

                timestamp = datetime.datetime.now().strftime("%H:%M:%S")
                print(f"{timestamp} 🚫 [{project_id}] Run was killed", flush=True)
                return "KILLED"

            elif run_status == "UNABLE_TO_ALLOCATE_KERNEL":
                import datetime

                timestamp = datetime.datetime.now().strftime("%H:%M:%S")
                print(f"{timestamp} ⚠️ [{project_id}] Unable to allocate kernel", flush=True)
                return "UNABLE_TO_ALLOCATE_KERNEL"

            counter += 1
            time.sleep(sleep_interval)

        except requests.exceptions.RequestException as e:
            import datetime

            timestamp = datetime.datetime.now().strftime("%H:%M:%S")
            print(f"{timestamp} ⚠️  [{project_id}] Network error: {str(e)[:50]}... Retrying.", flush=True)
            time.sleep(sleep_interval)
            continue


def list_hex_projects(
    *,
    api_token: str,
    base_url: str,
    limit: int = 100,
    include_archived: bool = False,
    include_trashed: bool = False,
) -> None:
    """
    List all Hex projects in the workspace.

    Args:
        api_token: Hex API token
        base_url: Hex base URL
        limit: Number of projects to fetch
        include_archived: Whether to include archived projects
        include_trashed: Whether to include trashed projects
    """
    headers = {
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/json",
    }

    api_url = f"{base_url}/api/v1/projects"
    params: Dict[str, str] = {
        "limit": str(limit),
        "includeArchived": str(include_archived).lower(),
        "includeTrashed": str(include_trashed).lower(),
    }

    print("\n🔍 Listing Hex projects")

    projects_response = requests.get(
        api_url,
        headers=headers,
        params=params,
        timeout=30,
    )

    handle_http_error(projects_response, "Error getting projects:")

    response_data = projects_response.json()
    projects = response_data.get("values", [])

    if not projects:
        print("No projects found.")
        return

    print(f"\n{'='*80}")
    print(f"📋 FOUND {len(projects)} PROJECT(S)")
    print(f"{'='*80}")

    for i, project in enumerate(projects, 1):
        project_id = project.get("id", "Unknown")
        name = project.get("title", "Unnamed")
        owner_info = project.get("owner", {})
        owner = owner_info.get("email", "N/A") if isinstance(owner_info, dict) else "N/A"

        # Status is nested in a status object
        status_info = project.get("status", {})
        status = status_info.get("name", "Unknown") if isinstance(status_info, dict) else "Unknown"

        # Check if archived or trashed
        archived_at = project.get("archivedAt")
        trashed_at = project.get("trashedAt")

        # Format status with emoji
        if trashed_at:
            status_emoji = "🗑️"
            display_status = "TRASHED"
        elif archived_at:
            status_emoji = "📦"
            display_status = "ARCHIVED"
        else:
            status_emoji = "✅"
            display_status = status

        # Create project URL
        project_url = f"{base_url}/hex/{project_id}/app"

        print(f"\n[{i}/{len(projects)}] 🔌 {project_id}")
        print(f"{'-'*50}")
        print(f"   Name: {name}")
        print(f"   Owner: {owner}")
        print(f"   {status_emoji} Status: {display_status}")
        print(f"   🔗 URL: {project_url}")

    print(f"\n{'='*80}\n")
