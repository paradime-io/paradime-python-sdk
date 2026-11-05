from typing import Any, Dict, Iterator, Optional

from paradime.apis.admin.transport import AdminTransport
from paradime.apis.admin.types import (
    CONNECTION_FIELDS,
    DEFAULT_PAGE_SIZE,
    UNSET,
    Page,
    Settable,
    check_page_size,
    given,
    iterate_pages,
    parse_page,
    serialize_input,
)
from paradime.apis.admin.workspaces.types import (
    WORKSPACE_FIELDS,
    AdminWorkspace,
    CreateWorkspaceInput,
    DeleteWorkspaceInput,
    UpdateWorkspaceInput,
)


class AdminWorkspacesClient:
    def __init__(self, transport: AdminTransport) -> None:
        self.transport = transport

    def get(self, uid: Optional[str] = None) -> AdminWorkspace:
        """
        Reads a workspace and its settings.

        Args:
            uid (str, optional): The workspace's uid. Leave it out for the key's workspace, or
                the one a company-level key targets.

        Returns:
            AdminWorkspace: The workspace.
        """

        query = f"""
            query adminWorkspace($uid: String) {{
                workspace(uid: $uid) {{
                    {WORKSPACE_FIELDS}
                }}
            }}
        """

        response = self.transport.execute(query, {"uid": uid})
        return AdminWorkspace.parse_obj(response["workspace"])

    def list(self, *, page_size: int = DEFAULT_PAGE_SIZE) -> Iterator[AdminWorkspace]:
        """
        Iterates over the workspaces the API key can reach, ordered by uid.

        Args:
            page_size (int): How many workspaces to read per request, at most 500.

        Returns:
            Iterator[AdminWorkspace]: The workspaces, read a page at a time.
        """

        check_page_size(page_size)
        query = f"""
            query adminWorkspaces($first: Int, $after: String) {{
                workspaces(first: $first, after: $after) {{
                    edges {{
                        node {{
                            {WORKSPACE_FIELDS}
                        }}
                    }}
                    {CONNECTION_FIELDS}
                }}
            }}
        """

        def fetch_page(after: Optional[str]) -> Page[AdminWorkspace]:
            response = self.transport.execute(query, {"first": page_size, "after": after})
            return parse_page(response["workspaces"], AdminWorkspace.parse_obj)

        return iterate_pages(fetch_page)

    def create(self, name: str, *, dbt_version: Optional[str] = None) -> AdminWorkspace:
        """
        Creates a workspace. Needs a company-level key. Sending the same name again returns
        the existing workspace.

        Args:
            name (str): The workspace's name, unique among the account's workspaces.
            dbt_version (str, optional): The dbt version to run; the latest by default.

        Returns:
            AdminWorkspace: The new workspace, or the existing one.
        """

        query = f"""
            mutation adminCreateWorkspace($input: CreateWorkspaceInput!) {{
                createWorkspace(input: $input) {{
                    workspace {{
                        {WORKSPACE_FIELDS}
                    }}
                }}
            }}
        """

        workspace_input = CreateWorkspaceInput(**given(name=name, dbt_version=dbt_version or UNSET))
        response = self.transport.execute(query, {"input": serialize_input(workspace_input)})
        return AdminWorkspace.parse_obj(response["createWorkspace"]["workspace"])

    def update(
        self,
        workspace_uid: str,
        *,
        name: Settable[str] = UNSET,
        dbt_version: Settable[str] = UNSET,
        dbt_partial_parsing: Settable[bool] = UNSET,
        default_branch: Settable[str] = UNSET,
        schedule_deployment_branch: Settable[str] = UNSET,
    ) -> AdminWorkspace:
        """
        Changes a workspace's settings. A setting left out keeps its value; None sends null,
        which clears `default_branch` and `schedule_deployment_branch`.

        Args:
            workspace_uid (str): The uid of the workspace to change.
            name (str, optional): A new name.
            dbt_version (str, optional): The dbt version to run, one of `supported_dbt_versions`.
            dbt_partial_parsing (bool, optional): Whether dbt uses partial parsing.
            default_branch (str, optional): The branch Paradime treats as the default.
            schedule_deployment_branch (str, optional): The branch Bolt schedules deploy from.

        Returns:
            AdminWorkspace: The workspace, with its new settings.
        """

        query = f"""
            mutation adminUpdateWorkspace($input: UpdateWorkspaceInput!) {{
                updateWorkspace(input: $input) {{
                    workspace {{
                        {WORKSPACE_FIELDS}
                    }}
                }}
            }}
        """

        settings: Dict[str, Any] = given(
            name=name,
            dbt_version=dbt_version,
            dbt_partial_parsing=dbt_partial_parsing,
            default_branch=default_branch,
            schedule_deployment_branch=schedule_deployment_branch,
        )
        workspace_input = UpdateWorkspaceInput(workspace_uid=workspace_uid, **settings)
        response = self.transport.execute(query, {"input": serialize_input(workspace_input)})
        return AdminWorkspace.parse_obj(response["updateWorkspace"]["workspace"])

    def delete(self, workspace_uid: str) -> AdminWorkspace:
        """
        Deletes a workspace. Deleting it again is not an error.

        Args:
            workspace_uid (str): The uid of the workspace to delete.

        Returns:
            AdminWorkspace: The deleted workspace; its settings read as None.
        """

        query = """
            mutation adminDeleteWorkspace($input: DeleteWorkspaceInput!) {
                deleteWorkspace(input: $input) {
                    workspace {
                        uid
                        name
                    }
                }
            }
        """

        workspace_input = DeleteWorkspaceInput(workspace_uid=workspace_uid)
        response = self.transport.execute(query, {"input": serialize_input(workspace_input)})
        return AdminWorkspace.parse_obj(response["deleteWorkspace"]["workspace"])
