from typing import Iterator, Optional

from paradime.apis.admin.environments.types import (
    ENVIRONMENT_FIELDS,
    CreateEnvironmentInput,
    DeferTargetInput,
    DeleteEnvironmentInput,
    Environment,
    EnvironmentType,
    SetAgentEnvironmentDeferTargetInput,
    SetDefaultEnvironmentInput,
    UpdateEnvironmentInput,
)
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
from paradime.tools.pydantic import BaseModel


class AdminEnvironmentsClient:
    """
    A workspace's Bolt (`SCHEDULER`) and agent (`AGENT`) environments, named by slug.
    """

    def __init__(self, transport: AdminTransport) -> None:
        self.transport = transport

    def list(
        self,
        *,
        type: Optional[EnvironmentType] = None,
        workspace_uid: Optional[str] = None,
        page_size: int = DEFAULT_PAGE_SIZE,
    ) -> Iterator[Environment]:
        """
        Iterates over the workspace's environments, ordered by slug.

        Args:
            type (EnvironmentType, optional): Only environments of this type.
            workspace_uid (str, optional): The workspace to read, for a company-level key.
            page_size (int): How many environments to read per request, at most 500.

        Returns:
            Iterator[Environment]: The environments, read a page at a time.
        """

        check_page_size(page_size)
        query = f"""
            query adminEnvironments(
                $workspaceUid: String
                $type: EnvironmentType
                $first: Int
                $after: String
            ) {{
                workspace(uid: $workspaceUid) {{
                    environments(type: $type, first: $first, after: $after) {{
                        edges {{
                            node {{
                                {ENVIRONMENT_FIELDS}
                            }}
                        }}
                        {CONNECTION_FIELDS}
                    }}
                }}
            }}
        """

        def fetch_page(after: Optional[str]) -> Page[Environment]:
            response = self.transport.execute(
                query,
                {
                    "workspaceUid": workspace_uid,
                    "type": EnvironmentType(type).value if type else None,
                    "first": page_size,
                    "after": after,
                },
                workspace_uid=workspace_uid,
            )
            workspace = response["workspace"] or {}
            return parse_page(workspace.get("environments"), Environment.parse_obj)

        return iterate_pages(fetch_page)

    def get(self, slug: str, *, workspace_uid: Optional[str] = None) -> Optional[Environment]:
        """
        Finds an environment by slug.

        Args:
            slug (str): The environment's slug.
            workspace_uid (str, optional): The workspace to read, for a company-level key.

        Returns:
            Optional[Environment]: The environment, or None when the workspace has none with
                this slug.
        """

        return next(
            (
                environment
                for environment in self.list(workspace_uid=workspace_uid, page_size=500)
                if environment.slug == slug
            ),
            None,
        )

    def create(
        self,
        type: EnvironmentType,
        name: str,
        *,
        slug: Optional[str] = None,
        branch: Optional[str] = None,
        dbt_version: Optional[str] = None,
        workspace_uid: Optional[str] = None,
    ) -> Environment:
        """
        Creates an environment. With a `slug`, sending the same input again returns the same
        environment; without one, every call creates another.

        Args:
            type (EnvironmentType): `SCHEDULER` for Bolt, `AGENT` for DinoAI agents.
            name (str): The display name.
            slug (str, optional): The slug to give it; minted when left out.
            branch (str, optional): The git branch its runs use.
            dbt_version (str, optional): The dbt version its runs use.
            workspace_uid (str, optional): The workspace to change, for a company-level key.

        Returns:
            Environment: The new environment, or the existing one.
        """

        environment_input = CreateEnvironmentInput(
            type=type,
            name=name,
            **given(
                slug=slug or UNSET,
                branch=branch or UNSET,
                dbt_version=dbt_version or UNSET,
            ),
        )
        return self._mutate("createEnvironment", environment_input, workspace_uid)

    def update(
        self,
        slug: str,
        *,
        name: Settable[str] = UNSET,
        branch: Settable[str] = UNSET,
        dbt_version: Settable[str] = UNSET,
        workspace_uid: Optional[str] = None,
    ) -> Environment:
        """
        Changes an environment. A setting left out keeps its value; None sends null, which
        falls back to the workspace's branch or dbt version.

        Args:
            slug (str): The slug of the environment to change.
            name (str, optional): A new display name.
            branch (str, optional): The git branch its runs use.
            dbt_version (str, optional): The dbt version its runs use.
            workspace_uid (str, optional): The workspace to change, for a company-level key.

        Returns:
            Environment: The environment, with its new settings.
        """

        environment_input = UpdateEnvironmentInput(
            slug=slug, **given(name=name, branch=branch, dbt_version=dbt_version)
        )
        return self._mutate("updateEnvironment", environment_input, workspace_uid)

    def delete(self, slug: str, *, workspace_uid: Optional[str] = None) -> Environment:
        """
        Deletes an environment. Deleting it again is not an error.

        Args:
            slug (str): The slug of the environment to delete.
            workspace_uid (str, optional): The workspace to change, for a company-level key.

        Returns:
            Environment: The deleted environment.
        """

        return self._mutate("deleteEnvironment", DeleteEnvironmentInput(slug=slug), workspace_uid)

    def set_default(self, slug: str, *, workspace_uid: Optional[str] = None) -> Environment:
        """
        Makes an environment the default of its type.

        Args:
            slug (str): The slug of the environment.
            workspace_uid (str, optional): The workspace to change, for a company-level key.

        Returns:
            Environment: The environment, now the default.
        """

        return self._mutate(
            "setDefaultEnvironment", SetDefaultEnvironmentInput(slug=slug), workspace_uid
        )

    def set_agent_defer_target(
        self,
        slug: str,
        *,
        environment_slug: Optional[str] = None,
        schedule_slug: Optional[str] = None,
        workspace_uid: Optional[str] = None,
    ) -> Environment:
        """
        Sets what an agent environment's runs defer to: a Bolt environment's latest run or a
        Bolt schedule's runs. Give neither to clear it.

        Args:
            slug (str): The slug of the agent environment.
            environment_slug (str, optional): A Bolt environment to defer to.
            schedule_slug (str, optional): A Bolt schedule to defer to.
            workspace_uid (str, optional): The workspace to change, for a company-level key.

        Returns:
            Environment: The environment, with its new target.
        """

        if environment_slug and schedule_slug:
            raise ValueError("Give environment_slug or schedule_slug, not both.")
        defer_to = (
            DeferTargetInput(
                **given(
                    environment_slug=environment_slug or UNSET,
                    schedule_slug=schedule_slug or UNSET,
                )
            )
            if environment_slug or schedule_slug
            else None
        )
        return self._mutate(
            "setAgentEnvironmentDeferTarget",
            SetAgentEnvironmentDeferTargetInput(slug=slug, defer_to=defer_to),
            workspace_uid,
        )

    def _mutate(
        self, mutation: str, mutation_input: BaseModel, workspace_uid: Optional[str]
    ) -> Environment:
        input_type = mutation[0].upper() + mutation[1:] + "Input"
        query = f"""
            mutation admin{input_type[:-5]}($input: {input_type}!) {{
                {mutation}(input: $input) {{
                    environment {{
                        {ENVIRONMENT_FIELDS}
                    }}
                }}
            }}
        """

        response = self.transport.execute(
            query, {"input": serialize_input(mutation_input)}, workspace_uid=workspace_uid
        )
        return Environment.parse_obj(response[mutation]["environment"])
