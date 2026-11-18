from typing import Any, Dict, Iterator, List, Mapping, Optional

from paradime.apis.admin.environment_variables.types import (
    DeleteEnvironmentVariablesInput,
    EnvironmentVariable,
    EnvironmentVariableScope,
    EnvironmentVariableValueInput,
    RenameEnvironmentVariableInput,
    RevealEnvironmentVariableInput,
    SetEnvironmentVariablesInput,
    SetEnvironmentVariablesMode,
    SetEnvironmentVariablesResult,
)
from paradime.apis.admin.transport import AdminTransport
from paradime.apis.admin.types import (
    CONNECTION_FIELDS,
    DEFAULT_PAGE_SIZE,
    Page,
    check_page_size,
    iterate_pages,
    parse_page,
    serialize_input,
)

ENVIRONMENT_VARIABLE_FIELDS = """
    name
    isSet
    updatedAt
"""


def _scope(environment_slug: Optional[str]) -> EnvironmentVariableScope:
    if environment_slug is None:
        return EnvironmentVariableScope.WORKSPACE
    if not environment_slug.strip():
        raise ValueError(
            "environment_slug can't be empty; leave it out (None) for the workspace's variables."
        )
    return EnvironmentVariableScope.ENVIRONMENT


def _scope_fields(environment_slug: Optional[str]) -> Dict[str, Any]:
    scope = _scope(environment_slug)
    if environment_slug is None:
        return {"scope": scope}
    return {"scope": scope, "environment_slug": environment_slug}


class AdminEnvironmentVariablesClient:
    """
    Environment variables of a workspace, or of one of its environments when
    `environment_slug` is given. Values are write-only: only `reveal` reads one.
    """

    def __init__(self, transport: AdminTransport) -> None:
        self.transport = transport

    def list(
        self,
        *,
        environment_slug: Optional[str] = None,
        workspace_uid: Optional[str] = None,
        page_size: int = DEFAULT_PAGE_SIZE,
    ) -> Iterator[EnvironmentVariable]:
        """
        Iterates over the variables' names, ordered by name. Values are never returned.

        Args:
            environment_slug (str, optional): An environment's slug, for its variables. Leave it
                out for the workspace's own variables.
            workspace_uid (str, optional): The workspace to read, for a company-level key.
            page_size (int): How many variables to read per request, at most 500.

        Returns:
            Iterator[EnvironmentVariable]: The variables, read a page at a time.
        """

        check_page_size(page_size)
        scope = _scope(environment_slug)
        query = f"""
            query adminEnvironmentVariables(
                $workspaceUid: String
                $scope: EnvironmentVariableScope
                $environmentSlug: String
                $first: Int
                $after: String
            ) {{
                workspace(uid: $workspaceUid) {{
                    environmentVariables(
                        scope: $scope
                        environmentSlug: $environmentSlug
                        first: $first
                        after: $after
                    ) {{
                        edges {{
                            node {{
                                {ENVIRONMENT_VARIABLE_FIELDS}
                            }}
                        }}
                        {CONNECTION_FIELDS}
                    }}
                }}
            }}
        """

        def fetch_page(after: Optional[str]) -> Page[EnvironmentVariable]:
            response = self.transport.execute(
                query,
                {
                    "workspaceUid": workspace_uid,
                    "scope": scope.value,
                    "environmentSlug": environment_slug,
                    "first": page_size,
                    "after": after,
                },
                workspace_uid=workspace_uid,
            )
            workspace = response["workspace"] or {}
            return parse_page(workspace.get("environmentVariables"), EnvironmentVariable.parse_obj)

        return iterate_pages(fetch_page)

    def set(
        self,
        variables: Mapping[str, str],
        *,
        environment_slug: Optional[str] = None,
        replace: bool = False,
        workspace_uid: Optional[str] = None,
    ) -> SetEnvironmentVariablesResult:
        """
        Sets variables in one go. Sending the same variables again changes nothing.

        Args:
            variables (Mapping[str, str]): Names and values. A value can't be empty: delete a
                variable with `delete`.
            environment_slug (str, optional): An environment's slug, to set its variables.
            replace (bool): Make these the scope's only variables, deleting the others.
            workspace_uid (str, optional): The workspace to change, for a company-level key.

        Returns:
            SetEnvironmentVariablesResult: The names added, changed or deleted, and how many
                variables the scope holds now.
        """

        query = """
            mutation adminSetEnvironmentVariables($input: SetEnvironmentVariablesInput!) {
                setEnvironmentVariables(input: $input) {
                    changedNames
                    variables(first: 0) {
                        totalCount
                    }
                }
            }
        """

        variables_input = SetEnvironmentVariablesInput(
            **_scope_fields(environment_slug),
            variables=[
                EnvironmentVariableValueInput(name=name, value=value)
                for name, value in variables.items()
            ],
            mode=(
                SetEnvironmentVariablesMode.REPLACE
                if replace
                else SetEnvironmentVariablesMode.MERGE
            ),
        )
        response = self.transport.execute(
            query, {"input": serialize_input(variables_input)}, workspace_uid=workspace_uid
        )
        payload = response["setEnvironmentVariables"]
        return SetEnvironmentVariablesResult(
            changed_names=payload["changedNames"],
            total_count=payload["variables"]["totalCount"],
        )

    def delete(
        self,
        names: List[str],
        *,
        environment_slug: Optional[str] = None,
        workspace_uid: Optional[str] = None,
    ) -> List[str]:
        """
        Deletes variables. Names that don't exist are ignored.

        Args:
            names (List[str]): The names to delete.
            environment_slug (str, optional): An environment's slug, to delete its variables.
            workspace_uid (str, optional): The workspace to change, for a company-level key.

        Returns:
            List[str]: The names that existed and were deleted.
        """

        query = """
            mutation adminDeleteEnvironmentVariables($input: DeleteEnvironmentVariablesInput!) {
                deleteEnvironmentVariables(input: $input) {
                    deletedNames
                }
            }
        """

        delete_input = DeleteEnvironmentVariablesInput(
            **_scope_fields(environment_slug), names=list(names)
        )
        response = self.transport.execute(
            query, {"input": serialize_input(delete_input)}, workspace_uid=workspace_uid
        )
        return response["deleteEnvironmentVariables"]["deletedNames"]

    def rename(
        self,
        name: str,
        new_name: str,
        *,
        environment_slug: Optional[str] = None,
        workspace_uid: Optional[str] = None,
    ) -> EnvironmentVariable:
        """
        Renames a variable. A retry of a finished rename succeeds.

        Args:
            name (str): The variable's current name.
            new_name (str): Its new name, which no other variable may have.
            environment_slug (str, optional): An environment's slug, for its variables.
            workspace_uid (str, optional): The workspace to change, for a company-level key.

        Returns:
            EnvironmentVariable: The variable, under its new name.
        """

        query = f"""
            mutation adminRenameEnvironmentVariable($input: RenameEnvironmentVariableInput!) {{
                renameEnvironmentVariable(input: $input) {{
                    variable {{
                        {ENVIRONMENT_VARIABLE_FIELDS}
                    }}
                }}
            }}
        """

        rename_input = RenameEnvironmentVariableInput(
            **_scope_fields(environment_slug),
            name=name,
            new_name=new_name,
        )
        response = self.transport.execute(
            query, {"input": serialize_input(rename_input)}, workspace_uid=workspace_uid
        )
        return EnvironmentVariable.parse_obj(response["renameEnvironmentVariable"]["variable"])

    def reveal(
        self,
        name: str,
        *,
        environment_slug: Optional[str] = None,
        workspace_uid: Optional[str] = None,
    ) -> str:
        """
        Reads a variable's value. Needs the `env_vars:reveal` capability, and is audited.

        Args:
            name (str): The variable's name.
            environment_slug (str, optional): An environment's slug, for its variables.
            workspace_uid (str, optional): The workspace to read, for a company-level key.

        Returns:
            str: The value.
        """

        query = """
            mutation adminRevealEnvironmentVariable($input: RevealEnvironmentVariableInput!) {
                revealEnvironmentVariable(input: $input) {
                    name
                    value
                }
            }
        """

        reveal_input = RevealEnvironmentVariableInput(**_scope_fields(environment_slug), name=name)
        response = self.transport.execute(
            query, {"input": serialize_input(reveal_input)}, workspace_uid=workspace_uid
        )
        return response["revealEnvironmentVariable"]["value"]
