from typing import Any, Dict, Optional

from paradime.apis.catalog.types import (
    CatalogRefresh,
    CatalogRefreshError,
    CatalogRefreshProgress,
    CatalogRefreshRequest,
    CatalogRefreshStatus,
    CatalogRefreshStep,
    CatalogRefreshStepStatus,
)
from paradime.client.api_client import APIClient

_ERROR_FIELDS = "code message detail"

_STEP_FIELDS = f"""
    kind
    label
    status
    blocking
    startedAt
    finishedAt
    progress {{ stage stageLabel done total unit }}
    error {{ {_ERROR_FIELDS} }}
"""


class CatalogClient:
    def __init__(self, client: APIClient):
        self.client = client

    def refresh(self) -> CatalogRefreshRequest:
        """
        Requests a refresh of the Paradime catalog. If a refresh is already running, that
        refresh is returned instead of starting a new one.

        Returns:
            CatalogRefreshRequest: The refresh id (`token`), to follow the refresh with
            `get_refresh`, and whether an already running refresh was joined.
        """

        query = """
            mutation refreshCatalog {
                refreshCatalog {
                    ok
                    token
                    joined
                }
            }
        """

        response_json = self.client._call_gql(query)["refreshCatalog"]
        return CatalogRefreshRequest(
            token=response_json.get("token"),
            joined=bool(response_json.get("joined")),
        )

    def get_refresh(self, token: str) -> Optional[CatalogRefresh]:
        """
        Gets the state of a catalog refresh and of each of its steps.

        Args:
            token (str): The refresh id returned by `refresh`.

        Returns:
            Optional[CatalogRefresh]: The refresh, or None if no refresh has this id.
        """

        query = f"""
            query catalogRefresh($token: String!) {{
                catalogRefresh(token: $token) {{
                    token
                    status
                    startedAt
                    finishedAt
                    error {{ {_ERROR_FIELDS} }}
                    finished
                    steps {{ {_STEP_FIELDS} }}
                }}
            }}
        """

        response_json = self.client._call_gql(query=query, variables={"token": token})[
            "catalogRefresh"
        ]
        if response_json is None:
            return None

        return CatalogRefresh(
            token=response_json["token"],
            status=CatalogRefreshStatus(response_json["status"]),
            started_at=response_json.get("startedAt"),
            finished_at=response_json.get("finishedAt"),
            error=_error(response_json.get("error")),
            finished=bool(response_json.get("finished")),
            steps=[_step(step) for step in response_json.get("steps") or []],
        )


def _error(value: Optional[Dict[str, Any]]) -> Optional[CatalogRefreshError]:
    if not value:
        return None
    return CatalogRefreshError(
        code=value["code"], message=value["message"], detail=value.get("detail")
    )


def _step(value: Dict[str, Any]) -> CatalogRefreshStep:
    progress = value.get("progress")
    return CatalogRefreshStep(
        kind=value["kind"],
        label=value.get("label"),
        status=CatalogRefreshStepStatus(value["status"]),
        blocking=value["blocking"],
        started_at=value.get("startedAt"),
        finished_at=value.get("finishedAt"),
        progress=(
            CatalogRefreshProgress(
                stage=progress["stage"],
                stage_label=progress["stageLabel"],
                done=progress["done"],
                total=progress["total"],
                unit=progress["unit"],
            )
            if progress
            else None
        ),
        error=_error(value.get("error")),
    )
