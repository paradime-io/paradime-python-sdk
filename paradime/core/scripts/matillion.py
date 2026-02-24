import logging
import time
from concurrent.futures import ThreadPoolExecutor
from typing import List

import requests

from paradime.core.scripts.utils import handle_http_error

logging.basicConfig(format="%(asctime)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)


def _get_access_token(*, client_id: str, client_secret: str) -> str:
    """
    Get OAuth access token for Matillion DPC API.

    Args:
        client_id: OAuth client ID
        client_secret: OAuth client secret

    Returns:
        Access token string
    """
    # Matillion DPC OAuth token endpoint
    token_url = "https://id.core.matillion.com/oauth/dpc/token"

    # Use form-encoded data as per Matillion docs
    payload = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
        "audience": "https://api.matillion.com",
    }

    response = requests.post(
        token_url,
        data=payload,  # Use data instead of json for form-encoded
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )

    handle_http_error(response, "Error obtaining access token:")

    token_data = response.json()
    return token_data.get("access_token")


def _resolve_project_id(
    *,
    base_url: str,
    access_token: str,
    project_name: str,
) -> str:
    """
    Resolve a project name to its UUID by listing projects.

    Args:
        base_url: Matillion DPC API base URL
        access_token: OAuth access token
        project_name: Project name to look up

    Returns:
        Project UUID

    Raises:
        Exception: If project name is not found or matches multiple projects
    """
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }

    url = f"{base_url}/dpc/v1/projects"
    all_projects = []
    page = 0
    page_size = 25

    while True:
        params = {"page": page, "size": page_size}
        response = requests.get(url, headers=headers, params=params)
        handle_http_error(response, "Error listing projects:")

        data = response.json()
        if not data or not isinstance(data, dict):
            break

        projects = data.get("results", [])
        if not projects:
            break

        all_projects.extend(projects)

        total = data.get("total", 0)
        if len(all_projects) >= total:
            break

        page += 1

    matches = [p for p in all_projects if p.get("name") == project_name]

    if not matches:
        available = [p.get("name", "Unknown") for p in all_projects]
        raise Exception(f"Project '{project_name}' not found. Available projects: {available}")

    if len(matches) > 1:
        ids = [p.get("id") for p in matches]
        raise Exception(
            f"Multiple projects found with name '{project_name}': {ids}. "
            f"This is unexpected — please contact support."
        )

    project_id = matches[0]["id"]
    print(f"📁 Resolved project '{project_name}' -> {project_id}")
    return project_id


def trigger_matillion_pipeline(
    *,
    base_url: str,
    client_id: str,
    client_secret: str,
    project_name: str,
    pipeline_names: List[str],
    environment: str,
    wait_for_completion: bool = True,
    timeout_minutes: int = 1440,
) -> List[str]:
    """
    Trigger execution for multiple Matillion pipelines.

    Args:
        base_url: Matillion DPC API base URL (e.g., https://us1.api.matillion.com)
        client_id: OAuth client ID
        client_secret: OAuth client secret
        project_name: Matillion project name (will be resolved to project ID)
        pipeline_names: List of pipeline names to execute
        environment: Matillion environment name
        wait_for_completion: Whether to wait for executions to complete
        timeout_minutes: Maximum time to wait for completion

    Returns:
        List of execution result messages for each pipeline
    """
    futures = []
    results = []

    # Get OAuth access token
    print("🔐 Authenticating with Matillion DPC API...")
    access_token = _get_access_token(
        client_id=client_id,
        client_secret=client_secret,
    )

    # Resolve project name to ID
    base_url = base_url.rstrip("/")
    project_id = _resolve_project_id(
        base_url=base_url,
        access_token=access_token,
        project_name=project_name,
    )

    # Add visual separator and header
    print(f"\n{'='*60}")
    print("🚀 TRIGGERING MATILLION PIPELINES")
    print(f"{'='*60}")

    with ThreadPoolExecutor() as executor:
        for i, pipeline_name in enumerate(set(pipeline_names), 1):
            print(f"\n[{i}/{len(set(pipeline_names))}] 📊 {pipeline_name}")
            print(f"{'-'*40}")

            futures.append(
                (
                    pipeline_name,
                    executor.submit(
                        trigger_single_pipeline,
                        base_url=base_url,
                        access_token=access_token,
                        project_id=project_id,
                        pipeline_name=pipeline_name,
                        environment=environment,
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
        pipeline_results = []
        for pipeline_name, future in futures:
            # Use longer timeout when waiting for completion
            future_timeout = (timeout_minutes * 60 + 120) if wait_for_completion else 120
            response_txt = future.result(timeout=future_timeout)
            pipeline_results.append((pipeline_name, response_txt))
            results.append(response_txt)

        # Display results as simple table
        print(f"\n{'='*80}")
        print("📊 EXECUTION RESULTS")
        print(f"{'='*80}")
        print(f"{'PIPELINE':<30} {'STATUS':<10}")
        print(f"{'-'*30} {'-'*10}")

        for pipeline_name, response_txt in pipeline_results:
            # Format result with emoji
            if "SUCCESS" in response_txt:
                status = "✅ SUCCESS"
            elif "FAILED" in response_txt:
                status = "❌ FAILED"
            elif "RUNNING" in response_txt:
                status = "🔄 RUNNING"
            else:
                status = "ℹ️ COMPLETED"

            print(f"{pipeline_name:<30} {status:<10}")

        print(f"{'='*80}\n")

    return results


def trigger_single_pipeline(
    *,
    base_url: str,
    access_token: str,
    project_id: str,
    pipeline_name: str,
    environment: str,
    wait_for_completion: bool = True,
    timeout_minutes: int = 1440,
) -> str:
    """
    Trigger execution for a single Matillion pipeline.

    Args:
        base_url: Matillion DPC API base URL
        access_token: OAuth access token
        project_id: Matillion project ID
        pipeline_name: Pipeline name to execute
        environment: Matillion environment name
        wait_for_completion: Whether to wait for execution to complete
        timeout_minutes: Maximum time to wait for completion

    Returns:
        Status message indicating execution result
    """
    base_url = base_url.rstrip("/")

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }

    import datetime

    timestamp = datetime.datetime.now().strftime("%H:%M:%S")

    # Trigger the pipeline execution
    # Endpoint: POST /dpc/v1/projects/{projectId}/pipeline-executions
    execution_url = f"{base_url}/dpc/v1/projects/{project_id}/pipeline-executions"

    execution_payload = {
        "pipelineName": pipeline_name,
        "environmentName": environment,
    }

    print(f"{timestamp} 🚀 [{pipeline_name}] Triggering pipeline execution...")
    execution_response = requests.post(
        execution_url,
        json=execution_payload,
        headers=headers,
    )

    handle_http_error(
        execution_response,
        f"Error triggering execution for pipeline '{pipeline_name}':",
    )

    execution_data = execution_response.json()
    execution_id = execution_data.get("pipelineExecutionId")

    print(f"{timestamp} ✅ [{pipeline_name}] Pipeline triggered (Execution ID: {execution_id})")

    if not wait_for_completion:
        return f"Pipeline triggered. Execution ID: {execution_id}"

    print(f"{timestamp} ⏳ [{pipeline_name}] Monitoring execution progress...")

    # Wait for execution completion
    execution_status = _wait_for_execution_completion(
        base_url=base_url,
        access_token=access_token,
        project_id=project_id,
        execution_id=execution_id,
        pipeline_name=pipeline_name,
        timeout_minutes=timeout_minutes,
    )

    return f"Execution completed. Final status: {execution_status}"


def _wait_for_execution_completion(
    *,
    base_url: str,
    access_token: str,
    project_id: str,
    execution_id: str,
    pipeline_name: str,
    timeout_minutes: int,
) -> str:
    """
    Poll execution status until completion or timeout.

    Args:
        base_url: Matillion DPC API base URL
        access_token: OAuth access token
        project_id: Matillion project ID
        execution_id: Pipeline execution ID
        pipeline_name: Pipeline name for logging
        timeout_minutes: Maximum time to wait for completion

    Returns:
        Final execution status
    """
    base_url = base_url.rstrip("/")

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }
    start_time = time.time()
    timeout_seconds = timeout_minutes * 60
    sleep_interval = 5  # Poll every 5 seconds
    counter = 0

    while True:
        elapsed = time.time() - start_time
        if elapsed > timeout_seconds:
            raise Exception(
                f"Timeout waiting for pipeline '{pipeline_name}' execution to complete after {timeout_minutes} minutes"
            )

        try:
            # Get execution status
            # Endpoint: GET /dpc/v1/projects/{projectId}/pipeline-executions/{executionId}
            status_url = (
                f"{base_url}/dpc/v1/projects/{project_id}/pipeline-executions/{execution_id}"
            )
            execution_response = requests.get(
                status_url,
                headers=headers,
            )

            if execution_response.status_code != 200:
                raise Exception(
                    f"Execution status check failed with HTTP {execution_response.status_code}"
                )

            execution_data = execution_response.json()
            # Matillion DPC nests status under "result": {"status": "RUNNING", ...}
            result = execution_data.get("result", {})
            status = result.get("status", execution_data.get("status", "UNKNOWN")).upper()

            # Log progress every 6 checks (30 seconds)
            if counter == 0 or counter % 6 == 0:
                import datetime

                timestamp = datetime.datetime.now().strftime("%H:%M:%S")
                elapsed_min = int(elapsed // 60)
                elapsed_sec = int(elapsed % 60)
                if status in ["RUNNING", "QUEUED"]:
                    print(
                        f"{timestamp} 🔄 [{pipeline_name}] Running... ({elapsed_min}m {elapsed_sec}s elapsed)"
                    )

            # Check if execution is complete
            if status == "SUCCESS":
                import datetime

                timestamp = datetime.datetime.now().strftime("%H:%M:%S")
                elapsed_min = int(elapsed // 60)
                elapsed_sec = int(elapsed % 60)
                print(
                    f"{timestamp} ✅ [{pipeline_name}] Completed successfully ({elapsed_min}m {elapsed_sec}s)"
                )
                return "SUCCESS"

            elif status in ["FAILED", "ERROR"]:
                import datetime

                timestamp = datetime.datetime.now().strftime("%H:%M:%S")
                print(f"{timestamp} ❌ [{pipeline_name}] Execution failed")
                error_message = result.get(
                    "message", execution_data.get("message", "No error details available")
                )
                return f"FAILED: {error_message}"

            elif status in ["CANCELLED", "CANCELED"]:
                import datetime

                timestamp = datetime.datetime.now().strftime("%H:%M:%S")
                print(f"{timestamp} ⚠️  [{pipeline_name}] Execution was cancelled")
                return "CANCELLED"

            elif status in ["RUNNING", "QUEUED"]:
                # Still running, continue waiting
                pass

            else:
                # Unknown status, continue waiting
                pass

            counter += 1
            time.sleep(sleep_interval)

        except requests.exceptions.RequestException as e:
            import datetime

            timestamp = datetime.datetime.now().strftime("%H:%M:%S")
            print(f"{timestamp} ⚠️  [{pipeline_name}] Network error: {str(e)[:50]}... Retrying.")
            time.sleep(sleep_interval)
            continue


def list_matillion_projects(
    *,
    base_url: str,
    client_id: str,
    client_secret: str,
) -> None:
    """
    List all Matillion projects.

    Args:
        base_url: Matillion DPC API base URL (e.g., https://us1.api.matillion.com)
        client_id: OAuth client ID
        client_secret: OAuth client secret
    """
    base_url = base_url.rstrip("/")

    # Get OAuth access token
    print("🔐 Authenticating with Matillion DPC API...")
    access_token = _get_access_token(
        client_id=client_id,
        client_secret=client_secret,
    )

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }

    # List projects with pagination
    # Endpoint: GET /dpc/v1/projects
    url = f"{base_url}/dpc/v1/projects"

    print("\n🔍 Listing all projects")

    # Start with first page
    all_projects = []
    page = 0
    page_size = 25

    while True:
        params = {
            "page": page,
            "size": page_size,
        }

        projects_response = requests.get(
            url,
            headers=headers,
            params=params,
        )

        handle_http_error(projects_response, "Error getting projects:")

        # Handle empty response
        if not projects_response.text or projects_response.text.strip() == "":
            break

        try:
            projects_data = projects_response.json()
        except Exception as e:
            content_type = projects_response.headers.get("Content-Type", "")
            print(f"❌ Error parsing response: {str(e)}")
            print(f"Response status: {projects_response.status_code}")
            print(f"Response content-type: {content_type}")
            print(f"Response text (first 500 chars): {projects_response.text[:500]}")
            raise

        # Extract projects from paginated response
        if not projects_data or not isinstance(projects_data, dict):
            break

        projects = projects_data.get("results", [])
        if not projects:
            if page == 0:
                print("\n⚠️  No projects returned by API")
                print("   This could mean:")
                print("   1. The OAuth client doesn't have permission to list projects")
                print("   2. Projects exist but aren't visible to this API client")
                print(
                    "   3. You may need to assign the API client to projects in Matillion settings"
                )
            break

        all_projects.extend(projects)

        # Check if there are more pages
        total = projects_data.get("total", 0)
        if len(all_projects) >= total:
            break

        page += 1

    if not all_projects:
        print("No projects found.")
        return

    print(f"\n{'='*80}")
    print(f"📋 FOUND {len(all_projects)} PROJECT(S)")
    print(f"{'='*80}")

    for i, project in enumerate(all_projects, 1):
        project_id = project.get("id", "Unknown")
        project_name = project.get("name", "Unknown")
        description = project.get("description", "")

        print(f"\n[{i}/{len(all_projects)}] 📁 {project_name}")
        print(f"{'-'*50}")
        print(f"   Project ID: {project_id}")
        if description:
            print(f"   Description: {description}")

    print(f"\n{'='*80}\n")


def list_matillion_pipelines(
    *,
    base_url: str,
    client_id: str,
    client_secret: str,
    project_name: str,
    environment: str,
) -> None:
    """
    List all Matillion pipelines (published pipelines) with their status.

    Args:
        base_url: Matillion DPC API base URL (e.g., https://us1.api.matillion.com)
        client_id: OAuth client ID
        client_secret: OAuth client secret
        project_name: Matillion project name (will be resolved to project ID)
        environment: Matillion environment name to filter pipelines
    """
    base_url = base_url.rstrip("/")

    # Get OAuth access token
    print("🔐 Authenticating with Matillion DPC API...")
    access_token = _get_access_token(
        client_id=client_id,
        client_secret=client_secret,
    )

    # Resolve project name to ID
    project_id = _resolve_project_id(
        base_url=base_url,
        access_token=access_token,
        project_name=project_name,
    )

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }

    # List published pipelines
    # Endpoint: GET /dpc/v1/projects/{projectId}/published-pipelines
    url = f"{base_url}/dpc/v1/projects/{project_id}/published-pipelines"

    if environment:
        print(f"\n🔍 Listing pipelines for environment: {environment}")
    else:
        print("\n🔍 Listing all published pipelines")

    pipelines_response = requests.get(
        url,
        headers=headers,
        params={
            "environmentName": environment,
        },
    )

    handle_http_error(pipelines_response, "Error getting pipelines:")

    # Handle empty response
    if not pipelines_response.text or pipelines_response.text.strip() == "":
        print("No pipelines found (empty response).")
        return

    try:
        pipelines_data = pipelines_response.json()
    except Exception as e:
        content_type = pipelines_response.headers.get("Content-Type", "")
        print(f"❌ Error parsing response: {str(e)}")
        print(f"Response status: {pipelines_response.status_code}")
        print(f"Response content-type: {content_type}")
        print(f"Response text (first 500 chars): {pipelines_response.text[:500]}")
        raise

    # Handle different response structures
    if not pipelines_data:
        print("No pipelines found.")
        return

    # Extract pipelines array
    # Matillion DPC API returns paginated results with "results" field
    if isinstance(pipelines_data, dict):
        pipelines = pipelines_data.get("results", pipelines_data.get("pipelines", []))
    elif isinstance(pipelines_data, list):
        pipelines = pipelines_data
    else:
        print("No pipelines found (unexpected response format).")
        return

    if not pipelines:
        print("No pipelines found.")
        return

    print(f"\n{'='*80}")
    print(f"📋 FOUND {len(pipelines)} PIPELINE(S)")
    print(f"{'='*80}")

    for i, pipeline in enumerate(pipelines, 1):
        # Handle different field name formats
        pipeline_name = pipeline.get("pipelineName", pipeline.get("name", "Unknown"))
        environment_name = pipeline.get("environmentName", pipeline.get("environment", "Unknown"))
        pipeline_id = pipeline.get("id", "Unknown")
        published_time = pipeline.get("publishedTime", pipeline.get("publishedAt", "N/A"))

        print(f"\n[{i}/{len(pipelines)}] 📊 {pipeline_name}")
        print(f"{'-'*50}")
        print(f"   Environment: {environment_name}")
        if pipeline_id != "Unknown":
            print(f"   Pipeline ID: {pipeline_id}")
        if published_time != "N/A":
            print(f"   Published: {published_time}")

    print(f"\n{'='*80}\n")
