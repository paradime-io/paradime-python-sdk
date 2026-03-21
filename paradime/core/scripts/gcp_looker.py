from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List

import requests

from paradime.cli import console
from paradime.core.scripts.utils import handle_http_error


def _login(base_url: str, client_id: str, client_secret: str) -> str:
    """Authenticate with the Looker API and return an access token."""
    response = requests.post(
        f"{base_url}/api/4.0/login",
        data={"client_id": client_id, "client_secret": client_secret},
    )
    handle_http_error(response, "Error authenticating with Looker:")
    return response.json()["access_token"]


def _headers(access_token: str) -> Dict[str, str]:
    return {"Authorization": f"token {access_token}"}


def trigger_looker_scheduled_plans(
    *,
    base_url: str,
    client_id: str,
    client_secret: str,
    plan_ids: List[str],
) -> List[str]:
    """
    Trigger Looker scheduled plans by ID.

    Args:
        base_url: Looker instance base URL
        client_id: Looker API client ID
        client_secret: Looker API client secret
        plan_ids: List of scheduled plan IDs to trigger

    Returns:
        List of status keywords (SUCCESS / FAILED) for each plan
    """
    base_url = base_url.rstrip("/")
    access_token = _login(base_url, client_id, client_secret)

    unique_ids = list(dict.fromkeys(plan_ids))
    futures = []
    results: List[str] = []

    with ThreadPoolExecutor() as executor:
        for i, plan_id in enumerate(unique_ids, 1):
            futures.append(
                (
                    plan_id,
                    executor.submit(
                        _trigger_single_plan,
                        base_url=base_url,
                        access_token=access_token,
                        plan_id=plan_id,
                    ),
                )
            )

        plan_results = []
        for plan_id, future in futures:
            response_txt = future.result(timeout=120)
            plan_results.append((plan_id, response_txt))
            results.append(response_txt)

        console.table(
            columns=["Plan ID", "Status"],
            rows=[(pid, status) for pid, status in plan_results],
            title="Scheduled Plan Results",
        )

    return results


def _trigger_single_plan(
    *,
    base_url: str,
    access_token: str,
    plan_id: str,
) -> str:
    """Trigger a single Looker scheduled plan via run_once."""
    try:
        response = requests.post(
            f"{base_url}/api/4.0/scheduled_plans/{plan_id}/run_once",
            headers=_headers(access_token),
        )
        handle_http_error(
            response,
            f"Error triggering scheduled plan '{plan_id}':",
        )
        console.debug(f"[{plan_id}] Triggered successfully")
        return "SUCCESS"
    except Exception as e:
        console.error(f"[{plan_id}] Failed: {e}")
        return "FAILED"


def list_looker_scheduled_plans(
    *,
    base_url: str,
    client_id: str,
    client_secret: str,
    json_output: bool = False,
) -> list | None:
    """List all Looker scheduled plans for the authenticated user."""
    base_url = base_url.rstrip("/")
    access_token = _login(base_url, client_id, client_secret)

    response = requests.get(
        f"{base_url}/api/4.0/scheduled_plans",
        headers=_headers(access_token),
    )
    handle_http_error(response, "Error listing scheduled plans:")

    plans: List[Dict[str, Any]] = response.json()

    if not plans:
        if not json_output:
            console.info("No scheduled plans found.")
        return [] if json_output else None

    rows = []
    data = []
    for plan in plans:
        plan_id = str(plan.get("id", ""))
        name = plan.get("name") or ""
        title = plan.get("title") or ""
        enabled = str(plan.get("enabled", ""))
        crontab = plan.get("crontab") or ""
        last_run = plan.get("last_run_at") or "N/A"
        next_run = plan.get("next_run_at") or "N/A"
        rows.append((plan_id, name, title, enabled, crontab, str(last_run), str(next_run)))
        data.append(
            {
                "plan_id": plan_id,
                "name": name,
                "title": title,
                "enabled": enabled,
                "crontab": crontab,
                "last_run_at": str(last_run),
                "next_run_at": str(next_run),
            }
        )

    if json_output:
        return data

    console.table(
        columns=["Plan ID", "Name", "Title", "Enabled", "Crontab", "Last Run", "Next Run"],
        rows=rows,
        title="Looker Scheduled Plans",
    )
    return None
