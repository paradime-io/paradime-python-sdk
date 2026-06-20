"""Internal-mode transport for ``paradime bolt verify``.

When the CLI runs *inside* the Paradime cluster (e.g. the in-IDE terminal of a
Theia editor pod) it cannot use the public GraphQL API: those pods carry a
``COMPANY_TOKEN`` but no API key/secret. They can, however, reach the backend's
unauthenticated, company-scoped internal REST endpoints over cluster-internal
DNS, which is not routable from outside the network perimeter.

This module mirrors the surface ``verify`` needs (existing schedule names,
cross-workspace references, slug minting, Slack-ID checks) but talks to those
internal REST endpoints instead of GraphQL. It is selected by ``verify`` when
``PARADIME_INTERNAL_MODE`` is truthy; the endpoint URLs are injected via Helm
(``LIST_SCHEDULE_NAMES_URL``, ``CREATE_SCHEDULE_SLUGS_URL``,
``SLACK_ID_EXISTS_URL``).

Setting ``PARADIME_INTERNAL_MODE`` outside the cluster confers nothing: the URLs
point at internal-only DNS, so the requests simply fail to connect. The network
perimeter — not this flag — is the security boundary.
"""

import json
import os
from typing import Dict, List, Optional, Set, Tuple

import requests


def is_internal_mode() -> bool:
    """Whether the CLI should use the internal REST transport."""
    return os.getenv("PARADIME_INTERNAL_MODE", "").strip().lower() in ("1", "true", "yes")


def _workspace_uid() -> str:
    # Theia historically read this under two different casings; honour both so we
    # stay compatible regardless of how the pod env is populated.
    return os.getenv("WORKSPACE_UID") or os.getenv("workspace_uid") or ""


def list_existing_schedules() -> List[Dict[str, str]]:
    """List schedule names across all workspaces in the company.

    Returns a list of ``{"name", "workspace_name", "workspace_uid"}`` dicts, or
    an empty list on any error so ``verify`` keeps working when the backend is
    unreachable.
    """
    try:
        url = os.getenv("LIST_SCHEDULE_NAMES_URL")
        if not url:
            return []

        r = requests.get(url)
        if r.status_code != 200:
            if os.getenv("VERBOSE_LOGS", False):
                print(r.text)
            return []

        r_json = r.json()
        if not r_json.get("ok", False):
            return []

        schedules = r_json.get("schedules")
        if not isinstance(schedules, list):
            return []
        return [s for s in schedules if isinstance(s, dict)]
    except Exception as e:
        if os.getenv("VERBOSE_LOGS", False):
            print(f"Problem listing existing schedules: {e}")
        return []


def current_workspace_names(schedules: List[Dict[str, str]]) -> Set[str]:
    """Names of schedules deployed in the current workspace (by ``WORKSPACE_UID``).

    When no workspace UID is available, every deployed name is treated as
    current-workspace (the historical Theia behaviour).
    """
    workspace_uid = _workspace_uid()
    return {
        s["name"]
        for s in schedules
        if s.get("name") and (not workspace_uid or s.get("workspace_uid") == workspace_uid)
    }


def all_workspace_refs(schedules: List[Dict[str, str]]) -> Set[Tuple[str, str]]:
    """``(workspace_name, schedule_name)`` pairs across all workspaces.

    Used to validate cross-workspace ``schedule_trigger`` references.
    """
    return {
        (s["workspace_name"], s["name"])
        for s in schedules
        if s.get("name") and s.get("workspace_name")
    }


def create_schedule_slugs(display_names: List[str]) -> List[str]:
    """Mint a slug for each display name via the backend.

    Returns slugs order-aligned with ``display_names``. Raises on any failure so
    the caller can fall back gracefully.
    """
    if not display_names:
        return []

    url = os.getenv("CREATE_SCHEDULE_SLUGS_URL")
    if not url:
        raise RuntimeError("CREATE_SCHEDULE_SLUGS_URL is not configured.")

    r = requests.post(url, data=json.dumps({"display_names": display_names}))
    if r.status_code != 200:
        raise RuntimeError(f"Failed to mint slugs (status {r.status_code}).")

    r_json = r.json()
    if not r_json.get("ok", False):
        raise RuntimeError("Backend returned ok=False while minting slugs.")

    slugs = r_json.get("slugs")
    if not isinstance(slugs, list) or len(slugs) != len(display_names):
        raise RuntimeError("Backend returned an unexpected number of slugs.")
    return slugs


def verify_slack_ids(slack_ids: List[str]) -> Tuple[bool, Optional[Dict[str, bool]]]:
    """Check whether each Slack ID is accessible to the Paradime Slack bot.

    Returns ``(ok, ids_with_status)`` where ``ids_with_status`` maps each Slack
    ID to whether it is reachable. ``ok`` is ``False`` (with ``None`` status) when
    Slack is not connected or the endpoint cannot be reached.
    """
    if not slack_ids:
        return True, None
    try:
        url = os.getenv("SLACK_ID_EXISTS_URL")
        if not url:
            print("Slack verification is not configured.")
            return False, None

        r = requests.post(
            url, data=json.dumps({"ids": slack_ids, "workspace_uid": _workspace_uid()})
        )
        if r.status_code != 200:
            print("Problem verifying slack IDs. Please try again later.")
            if os.getenv("VERBOSE_LOGS", False):
                print(r.text)
            return False, None

        r_json = r.json()
        if not r_json.get("ok", False):
            print(
                "Slack is not connected. Please connect at: "
                "https://app.paradime.io/account-settings/integrations"
            )
            return False, None
    except Exception as e:
        print(f"Problem querying slack: {e}")
        return False, None
    return True, r_json.get("ids_with_status")
