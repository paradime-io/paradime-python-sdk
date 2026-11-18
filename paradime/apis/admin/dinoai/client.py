from typing import Iterator, List, Optional, Sequence, Tuple

from paradime.apis.admin.dinoai.types import (
    DINOAI_AGENT_DEFINITION_FIELDS,
    DinoaiAgentDefinition,
    DinoaiAgentEdgeInput,
    DinoaiAgentSpecInput,
    DinoaiWorkspaceSettings,
    SetDinoaiAgentDefinitionInput,
    SetDinoaiWorkspaceSettingsInput,
)
from paradime.apis.admin.jobs.types import ADMIN_JOB_FIELDS, AdminJob
from paradime.apis.admin.transport import AdminTransport
from paradime.apis.admin.types import (
    CONNECTION_FIELDS,
    DEFAULT_PAGE_SIZE,
    UNSET,
    Page,
    check_page_size,
    given,
    iterate_pages,
    parse_page,
    serialize_input,
)


class AdminDinoaiClient:
    """
    DinoAI agent definitions and workspace settings. Running agents stays on
    `paradime.dinoai_agents`.
    """

    def __init__(self, transport: AdminTransport) -> None:
        self.transport = transport

    def list_agents(
        self, *, workspace_uid: Optional[str] = None, page_size: int = DEFAULT_PAGE_SIZE
    ) -> Iterator[DinoaiAgentDefinition]:
        """
        Iterates over the workspace's DinoAI agent definitions, from YAML and from the UI.

        Args:
            workspace_uid (str, optional): The workspace to read, for a company-level key.
            page_size (int): How many agents to read per request, at most 500.

        Returns:
            Iterator[DinoaiAgentDefinition]: The agents, read a page at a time.
        """

        check_page_size(page_size)
        query = f"""
            query adminDinoaiAgentDefinitions($workspaceUid: String, $first: Int, $after: String) {{
                workspace(uid: $workspaceUid) {{
                    dinoaiAgentDefinitions(first: $first, after: $after) {{
                        edges {{
                            node {{
                                {DINOAI_AGENT_DEFINITION_FIELDS}
                            }}
                        }}
                        {CONNECTION_FIELDS}
                    }}
                }}
            }}
        """

        def fetch_page(after: Optional[str]) -> Page[DinoaiAgentDefinition]:
            response = self.transport.execute(
                query,
                {"workspaceUid": workspace_uid, "first": page_size, "after": after},
                workspace_uid=workspace_uid,
            )
            workspace = response["workspace"] or {}
            return parse_page(
                workspace.get("dinoaiAgentDefinitions"), DinoaiAgentDefinition.parse_obj
            )

        return iterate_pages(fetch_page)

    def get_agent(
        self, slug: str, *, workspace_uid: Optional[str] = None
    ) -> Optional[DinoaiAgentDefinition]:
        """
        Reads one DinoAI agent definition.

        Args:
            slug (str): The agent's slug.
            workspace_uid (str, optional): The workspace to read, for a company-level key.

        Returns:
            Optional[DinoaiAgentDefinition]: The agent, or None when there is none.
        """

        query = f"""
            query adminDinoaiAgentDefinition($workspaceUid: String, $slug: String!) {{
                workspace(uid: $workspaceUid) {{
                    dinoaiAgentDefinition(slug: $slug) {{
                        {DINOAI_AGENT_DEFINITION_FIELDS}
                    }}
                }}
            }}
        """

        response = self.transport.execute(
            query, {"workspaceUid": workspace_uid, "slug": slug}, workspace_uid=workspace_uid
        )
        agent = (response["workspace"] or {}).get("dinoaiAgentDefinition")
        return DinoaiAgentDefinition.parse_obj(agent) if agent is not None else None

    def set_agent(
        self,
        name: str,
        spec: DinoaiAgentSpecInput,
        *,
        slug: Optional[str] = None,
        slack_bot_name: Optional[str] = None,
        slack_bot_icon_url: Optional[str] = None,
        workspace_uid: Optional[str] = None,
    ) -> DinoaiAgentDefinition:
        """
        Creates or replaces a DinoAI agent definition. With a `slug`, sending the same input
        again changes nothing. A spec field left out is unset on the agent.

        Args:
            name (str): The agent's name.
            spec (DinoaiAgentSpecInput): What the agent is and may do.
            slug (str, optional): The agent's slug; minted when left out.
            slack_bot_name (str, optional): The name the agent posts under in Slack.
            slack_bot_icon_url (str, optional): The icon the agent posts with in Slack.
            workspace_uid (str, optional): The workspace to change, for a company-level key.

        Returns:
            DinoaiAgentDefinition: The agent, as now stored.
        """

        query = f"""
            mutation adminSetDinoaiAgentDefinition($input: SetDinoaiAgentDefinitionInput!) {{
                setDinoaiAgentDefinition(input: $input) {{
                    dinoaiAgentDefinition {{
                        {DINOAI_AGENT_DEFINITION_FIELDS}
                    }}
                }}
            }}
        """

        agent_input = SetDinoaiAgentDefinitionInput(
            name=name,
            spec=spec,
            **given(
                slug=slug or UNSET,
                slack_bot_name=slack_bot_name or UNSET,
                slack_bot_icon_url=slack_bot_icon_url or UNSET,
            ),
        )
        response = self.transport.execute(
            query, {"input": serialize_input(agent_input)}, workspace_uid=workspace_uid
        )
        return DinoaiAgentDefinition.parse_obj(
            response["setDinoaiAgentDefinition"]["dinoaiAgentDefinition"]
        )

    def delete_agent(self, slug: str, *, workspace_uid: Optional[str] = None) -> str:
        """
        Deletes a DinoAI agent definition.

        Args:
            slug (str): The agent's slug.
            workspace_uid (str, optional): The workspace to change, for a company-level key.

        Returns:
            str: The slug of the deleted agent.
        """

        query = """
            mutation adminDeleteDinoaiAgentDefinition($input: DeleteDinoaiAgentDefinitionInput!) {
                deleteDinoaiAgentDefinition(input: $input) {
                    deletedSlug
                }
            }
        """

        response = self.transport.execute(
            query, {"input": {"slug": slug}}, workspace_uid=workspace_uid
        )
        return response["deleteDinoaiAgentDefinition"]["deletedSlug"]

    def set_agent_graph(
        self, edges: Sequence[Tuple[str, str]], *, workspace_uid: Optional[str] = None
    ) -> None:
        """
        Replaces which agents may delegate to which.

        Args:
            edges (Sequence[Tuple[str, str]]): ``(from_slug, to_slug)`` pairs: the first agent
                may delegate to the second.
            workspace_uid (str, optional): The workspace to change, for a company-level key.
        """

        query = """
            mutation adminSetDinoaiAgentGraph($input: SetDinoaiAgentGraphInput!) {
                setDinoaiAgentGraph(input: $input) {
                    workspace {
                        uid
                    }
                }
            }
        """

        edge_inputs: List[DinoaiAgentEdgeInput] = [
            DinoaiAgentEdgeInput(from_slug=from_slug, to_slug=to_slug)
            for from_slug, to_slug in edges
        ]
        self.transport.execute(
            query,
            {"input": {"edges": [serialize_input(edge) for edge in edge_inputs]}},
            workspace_uid=workspace_uid,
        )

    def get_settings(
        self, *, workspace_uid: Optional[str] = None
    ) -> Optional[DinoaiWorkspaceSettings]:
        """
        Reads the workspace's DinoAI settings.

        Args:
            workspace_uid (str, optional): The workspace to read, for a company-level key.

        Returns:
            Optional[DinoaiWorkspaceSettings]: The settings.
        """

        query = """
            query adminDinoaiSettings($workspaceUid: String) {
                workspace(uid: $workspaceUid) {
                    dinoaiSettings {
                        sqlQueryToolEnabled
                    }
                }
            }
        """

        response = self.transport.execute(
            query, {"workspaceUid": workspace_uid}, workspace_uid=workspace_uid
        )
        settings = (response["workspace"] or {}).get("dinoaiSettings")
        return DinoaiWorkspaceSettings.parse_obj(settings) if settings is not None else None

    def set_settings(
        self,
        *,
        sql_query_tool_enabled: Optional[bool] = None,
        workspace_uid: Optional[str] = None,
    ) -> Optional[DinoaiWorkspaceSettings]:
        """
        Changes the workspace's DinoAI settings. A setting left out keeps its value.

        Args:
            sql_query_tool_enabled (bool, optional): Whether agents may run SQL queries.
            workspace_uid (str, optional): The workspace to change, for a company-level key.

        Returns:
            Optional[DinoaiWorkspaceSettings]: The settings now.
        """

        query = """
            mutation adminSetDinoaiWorkspaceSettings($input: SetDinoaiWorkspaceSettingsInput!) {
                setDinoaiWorkspaceSettings(input: $input) {
                    workspace {
                        dinoaiSettings {
                            sqlQueryToolEnabled
                        }
                    }
                }
            }
        """

        settings_input = SetDinoaiWorkspaceSettingsInput(
            **given(
                sql_query_tool_enabled=(
                    sql_query_tool_enabled if sql_query_tool_enabled is not None else UNSET
                )
            )
        )
        response = self.transport.execute(
            query, {"input": serialize_input(settings_input)}, workspace_uid=workspace_uid
        )
        settings = response["setDinoaiWorkspaceSettings"]["workspace"].get("dinoaiSettings")
        return DinoaiWorkspaceSettings.parse_obj(settings) if settings is not None else None

    def sync_agents_from_code(
        self, *, branch: Optional[str] = None, workspace_uid: Optional[str] = None
    ) -> AdminJob:
        """
        Starts a job that reads the agents defined in YAML in the repository.

        Args:
            branch (str, optional): The branch to read; the default branch when left out.
            workspace_uid (str, optional): The workspace to sync, for a company-level key.

        Returns:
            AdminJob: The job; wait for it with `admin.jobs.wait`.
        """

        query = f"""
            mutation adminSyncDinoaiAgentDefinitionsFromCode(
                $input: SyncDinoaiAgentDefinitionsFromCodeInput!
            ) {{
                syncDinoaiAgentDefinitionsFromCode(input: $input) {{
                    job {{
                        {ADMIN_JOB_FIELDS}
                    }}
                }}
            }}
        """

        response = self.transport.execute(
            query,
            {"input": {"branch": branch} if branch else {}},
            workspace_uid=workspace_uid,
        )
        return AdminJob.parse_obj(response["syncDinoaiAgentDefinitionsFromCode"]["job"])
