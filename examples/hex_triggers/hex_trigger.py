"""
Example: Hex Project Trigger Integration

This example demonstrates how to create a custom integration for Hex projects
that can be triggered based on dbt model completions or other events.

Prerequisites:
- Hex API token should be available in the environment (HEX_API_TOKEN)
- Hex workspace URL should be set in environment (HEX_BASE_URL, defaults to https://app.hex.tech)
"""

import os
import time
from datetime import datetime
from typing import Dict, List, Optional

import requests

from paradime import Paradime
from paradime.apis.custom_integration.types import (
    Lineage,
    LineageDependencyDbtObject,
    NodeColor,
    NodeTriggerLike,
    NodeTriggerLikeAttributes,
    NodeType,
)

# Create a Paradime client with your API credentials
paradime = Paradime(
    api_endpoint="API_ENDPOINT",
    api_key="API_KEY",
    api_secret="API_SECRET",
)

# Hex API configuration from environment
HEX_API_TOKEN = os.getenv("HEX_API_TOKEN")
HEX_BASE_URL = os.getenv("HEX_BASE_URL", "https://app.hex.tech")
HEX_API_BASE = f"{HEX_BASE_URL}/api/v1"


def get_hex_headers() -> Dict[str, str]:
    """
    Get headers for Hex API requests.

    Returns:
        dict: Headers with authorization token
    """
    if not HEX_API_TOKEN:
        raise ValueError("HEX_API_TOKEN environment variable is not set")
    return {
        "Authorization": f"Bearer {HEX_API_TOKEN}",
        "Content-Type": "application/json",
    }


def list_hex_projects(
    limit: int = 100,
    include_archived: bool = False,
    include_trashed: bool = False,
) -> List[Dict]:
    """
    Retrieve all Hex projects from workspace.

    Args:
        limit: Number of projects to fetch per page (default: 100)
        include_archived: Include archived projects (default: False)
        include_trashed: Include trashed projects (default: False)

    Returns:
        list: List of Hex project configurations
    """
    try:
        params = {
            "limit": limit,
            "includeArchived": str(include_archived).lower(),
            "includeTrashed": str(include_trashed).lower(),
        }
        response = requests.get(
            f"{HEX_API_BASE}/projects",
            headers=get_hex_headers(),
            params=params,
            timeout=30,
        )
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Error fetching Hex projects: {e}")
        return []


def trigger_hex_project(
    project_id: str,
    input_params: Optional[Dict] = None,
    update_published_results: bool = True,
    use_cached_sql_results: bool = True,
    wait_for_completion: bool = False,
    timeout_minutes: int = 60,
) -> Dict:
    """
    Trigger a Hex project run.

    Args:
        project_id: UUID of the Hex project to trigger
        input_params: Optional dictionary of input variable names and values
        update_published_results: Update cached app state with run results (default: True)
        use_cached_sql_results: Use cached SQL results (default: True)
        wait_for_completion: Wait for the run to complete (default: False)
        timeout_minutes: Maximum wait time in minutes (default: 60)

    Returns:
        dict: Response with run information and status
    """
    try:
        payload = {
            "updatePublishedResults": update_published_results,
            "useCachedSqlResults": use_cached_sql_results,
        }
        if input_params:
            payload["inputParams"] = input_params

        response = requests.post(
            f"{HEX_API_BASE}/projects/{project_id}/runs",
            headers=get_hex_headers(),
            json=payload,
            timeout=30,
        )
        response.raise_for_status()
        result = response.json()

        print(f"✓ Triggered Hex project run: {result.get('runId')}")
        print(f"  Status: {result.get('status')}")
        print(f"  Run URL: {result.get('runUrl')}")

        if wait_for_completion:
            return _wait_for_run_completion(
                project_id=project_id,
                run_id=result["runId"],
                timeout_minutes=timeout_minutes,
            )

        return {
            "status": "success",
            "project_id": result.get("projectId"),
            "run_id": result.get("runId"),
            "run_url": result.get("runUrl"),
            "run_status": result.get("status"),
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}


def _wait_for_run_completion(
    project_id: str,
    run_id: str,
    timeout_minutes: int,
) -> Dict:
    """
    Wait for a Hex project run to complete.

    Args:
        project_id: UUID of the Hex project
        run_id: UUID of the run
        timeout_minutes: Maximum wait time in minutes

    Returns:
        dict: Final status information
    """
    start_time = time.time()
    timeout_seconds = timeout_minutes * 60
    poll_interval = 10  # Poll every 10 seconds
    counter = 0

    print(f"Waiting for Hex project run {run_id} to complete...")

    while True:
        elapsed_time = time.time() - start_time

        # Check timeout
        if elapsed_time >= timeout_seconds:
            return {
                "status": "timeout",
                "message": f"Run did not complete within {timeout_minutes} minutes",
                "run_id": run_id,
            }

        try:
            response = requests.get(
                f"{HEX_API_BASE}/projects/{project_id}/runs/{run_id}",
                headers=get_hex_headers(),
                timeout=30,
            )
            response.raise_for_status()
            run_info = response.json()
            run_status = run_info.get("status")

            # Log progress every 60 seconds (every 6 polls)
            if counter % 6 == 0:
                elapsed_minutes = int(elapsed_time / 60)
                print(f"  {elapsed_minutes}m elapsed - Status: {run_status}")

            # Check if run is complete
            if run_status in ["COMPLETED", "ERRORED", "KILLED", "UNABLE_TO_ALLOCATE_KERNEL"]:
                print(f"✓ Hex project run completed with status: {run_status}")
                return {
                    "status": "completed",
                    "run_status": run_status,
                    "run_id": run_id,
                    "message": f"Run finished with status: {run_status}",
                }

            counter += 1
            time.sleep(poll_interval)

        except requests.RequestException as e:
            print(f"Network error checking run status: {e}. Retrying...")
            time.sleep(poll_interval)
            counter += 1


def get_run_status(project_id: str, run_id: str) -> Dict:
    """
    Get the status of a Hex project run.

    Args:
        project_id: UUID of the Hex project
        run_id: UUID of the run

    Returns:
        dict: Run status information
    """
    try:
        response = requests.get(
            f"{HEX_API_BASE}/projects/{project_id}/runs/{run_id}",
            headers=get_hex_headers(),
            timeout=30,
        )
        response.raise_for_status()
        return response.json()
    except Exception as e:
        return {"status": "error", "message": str(e)}


# Setup Hex custom integration
hex_integration = paradime.custom_integration.upsert(
    name="HexProjects",
    logo_url="https://hex.tech/logo.svg",
    node_types=[
        NodeType(
            node_type="HexProject",
            icon_name="chart-line",  # Chart icon for Hex projects
            color=NodeColor.PURPLE,
        ),
    ],
)

# Fetch Hex projects
hex_projects = list_hex_projects()

# Create trigger nodes for each Hex project
trigger_nodes = []
for project in hex_projects:
    project_id = project.get("projectId")
    project_name = project.get("name", "Unnamed Project")
    description = project.get("description", "")
    owner_email = project.get("ownerEmail", "")
    created_at = project.get("createdAt")
    last_edited_at = project.get("lastEditedAt")

    # Parse timestamps
    try:
        created_epoch = int(datetime.fromisoformat(created_at.replace("Z", "+00:00")).timestamp())
    except:
        created_epoch = 0

    try:
        last_edited_epoch = int(
            datetime.fromisoformat(last_edited_at.replace("Z", "+00:00")).timestamp()
        )
    except:
        last_edited_epoch = 0

    # Create a trigger node
    # This example shows a Hex project that should be triggered after a dbt model completes
    trigger_node = NodeTriggerLike(
        name=project_name,
        node_type="HexProject",
        attributes=NodeTriggerLikeAttributes(
            description=description or f"Hex project: {project_name}",
            url=f"{HEX_BASE_URL}/hex/projects/{project_id}",
            project_id=project_id,
            owner=owner_email,
            state="Active",
            created_at=created_epoch,
            last_modified_at=last_edited_epoch,
            tags=["hex", "analytics", "dashboard"],
        ),
        lineage=Lineage(
            # Example: This Hex project is triggered after the 'customer_analytics' dbt model completes
            upstream_dependencies=[
                LineageDependencyDbtObject(
                    database_name="analytics",
                    schema_name="public",
                    table_name="customer_analytics",
                ),
            ],
        ),
    )

    trigger_nodes.append(trigger_node)

# Add all Hex trigger nodes to the integration
if trigger_nodes:
    paradime.custom_integration.add_nodes(
        integration_uid=hex_integration.uid,
        nodes=trigger_nodes,
    )
    print(f"Successfully added {len(trigger_nodes)} Hex project trigger nodes to Paradime")
else:
    print("No Hex projects found in workspace")


# Example usage: Trigger a Hex project after dbt run
if __name__ == "__main__":
    # Replace with your Hex project ID
    example_project_id = "your-hex-project-id"

    # Trigger the Hex project with optional input parameters
    result = trigger_hex_project(
        project_id=example_project_id,
        input_params={
            "date": datetime.now().strftime("%Y-%m-%d"),
            "source": "paradime",
        },
        update_published_results=True,
        wait_for_completion=True,
        timeout_minutes=60,
    )

    print(f"\nHex project trigger result: {result}")
