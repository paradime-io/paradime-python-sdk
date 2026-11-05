from typing import Optional

from paradime.apis.admin.setup_status.types import (
    WORKSPACE_SETUP_STATUS_FIELDS,
    WorkspaceSetupStatus,
)
from paradime.apis.admin.transport import AdminTransport


class AdminSetupStatusClient:
    def __init__(self, transport: AdminTransport) -> None:
        self.transport = transport

    def get(self, *, workspace_uid: Optional[str] = None) -> Optional[WorkspaceSetupStatus]:
        """
        Reads a workspace's onboarding checklist: each step and where it is, and whether the
        Code IDE, Bolt and DinoAI agents can run. Reading it changes nothing. A step waiting
        for a person in a browser is `PENDING_USER_ACTION`, with its page in `install_url`.

        Args:
            workspace_uid (str, optional): The workspace to read. Leave it out for the key's
                workspace, or the one a company-level key targets.

        Returns:
            Optional[WorkspaceSetupStatus]: The checklist, or None when the server returned none.
        """

        query = f"""
            query adminWorkspaceSetupStatus($workspaceUid: String) {{
                workspaceSetupStatus(workspaceUid: $workspaceUid) {{
                    {WORKSPACE_SETUP_STATUS_FIELDS}
                }}
            }}
        """

        response = self.transport.execute(
            query, {"workspaceUid": workspace_uid}, workspace_uid=workspace_uid
        )
        status = response["workspaceSetupStatus"]
        return WorkspaceSetupStatus.parse_obj(status) if status is not None else None
