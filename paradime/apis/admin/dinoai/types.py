from datetime import datetime
from typing import Any, Dict, List, Optional

from paradime.apis.admin.types import (
    ENVIRONMENT_LINK_FIELDS,
    MAX_PAGE_SIZE,
    AdminInputModel,
    AdminModel,
    EnvironmentLink,
    OpenEnum,
)
from paradime.tools.pydantic import root_validator

DINOAI_AGENT_DEFINITION_FIELDS = f"""
    slug
    name
    source
    version
    role
    goal
    backstory
    model
    modelFamily
    environment {{
        {ENVIRONMENT_LINK_FIELDS}
    }}
    tools {{
        mode
        names
    }}
    mcpTools {{
        mode
        names
    }}
    slack {{
        channel
        answerIn
        updates
    }}
    msTeams {{
        team
        channel
        answerIn
        updates
    }}
    jira {{
        defaultWatchers
    }}
    gitWrites {{
        comment
        edit
        approve
        merge
    }}
    agentsSquad(first: {MAX_PAGE_SIZE}) {{
        totalCount
        edges {{
            node {{
                slug
            }}
        }}
    }}
    slackBotName
    slackBotIconUrl
    gitPath
    gitCommitSha
    syncedAt
    createdAt
    updatedAt
"""

DINOAI_AGENT_DEFINITION_LINKS = frozenset({"environment"})


class DinoAiAgentSource(OpenEnum):
    YAML = "YAML"
    UI = "UI"
    UNKNOWN = "UNKNOWN"


class DinoAiAgentModel(OpenEnum):
    HAIKU = "HAIKU"
    SONNET = "SONNET"
    OPUS = "OPUS"
    UNKNOWN = "UNKNOWN"


class DinoAiAgentToolGateMode(OpenEnum):
    ALLOWLIST = "ALLOWLIST"
    DENYLIST = "DENYLIST"
    UNKNOWN = "UNKNOWN"


class DinoAiAgentAnswerIn(OpenEnum):
    THREAD = "THREAD"
    CHANNEL = "CHANNEL"
    UNKNOWN = "UNKNOWN"


class DinoAiAgentUpdates(OpenEnum):
    ALL = "ALL"
    FINAL = "FINAL"
    UNKNOWN = "UNKNOWN"


class DinoAiAgentToolGate(AdminModel):
    mode: Optional[DinoAiAgentToolGateMode] = None
    names: Optional[List[str]] = None


class DinoAiAgentSlack(AdminModel):
    channel: Optional[str] = None
    answer_in: Optional[DinoAiAgentAnswerIn] = None
    updates: Optional[DinoAiAgentUpdates] = None


class DinoAiAgentMsTeams(AdminModel):
    team: Optional[str] = None
    channel: Optional[str] = None
    answer_in: Optional[DinoAiAgentAnswerIn] = None
    updates: Optional[DinoAiAgentUpdates] = None


class DinoAiAgentJira(AdminModel):
    default_watchers: Optional[List[str]] = None


class DinoAiAgentGitWrites(AdminModel):
    comment: Optional[bool] = None
    edit: Optional[bool] = None
    approve: Optional[bool] = None
    merge: Optional[bool] = None


class DinoAiAgentDefinition(AdminModel):
    slug: str
    name: str
    source: Optional[DinoAiAgentSource] = None
    version: Optional[int] = None
    role: Optional[str] = None
    goal: Optional[str] = None
    backstory: Optional[str] = None
    model: Optional[DinoAiAgentModel] = None
    model_family: Optional[str] = None
    environment: Optional[EnvironmentLink] = None
    tools: Optional[DinoAiAgentToolGate] = None
    mcp_tools: Optional[DinoAiAgentToolGate] = None
    slack: Optional[DinoAiAgentSlack] = None
    ms_teams: Optional[DinoAiAgentMsTeams] = None
    jira: Optional[DinoAiAgentJira] = None
    git_writes: Optional[DinoAiAgentGitWrites] = None
    agents_squad: Optional[List[str]] = None
    agents_squad_count: Optional[int] = None
    slack_bot_name: Optional[str] = None
    slack_bot_icon_url: Optional[str] = None
    git_path: Optional[str] = None
    git_commit_sha: Optional[str] = None
    synced_at: Optional[datetime] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    @root_validator(pre=True)
    def _parse_agents_squad(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        for key in ("agentsSquad", "agents_squad"):
            squad = values.get(key)
            if isinstance(squad, dict):
                edges = squad.get("edges") or []
                return {
                    **values,
                    key: [edge["node"]["slug"] for edge in edges],
                    "agentsSquadCount": squad.get("totalCount"),
                }
        return values


class DinoAiWorkspaceSettings(AdminModel):
    sql_query_tool_enabled: Optional[bool] = None


class DinoAiAgentToolGateInput(AdminInputModel):
    mode: DinoAiAgentToolGateMode
    names: Optional[List[str]] = None


class DinoAiAgentSlackSettingsInput(AdminInputModel):
    channel: str
    answer_in: Optional[DinoAiAgentAnswerIn] = None
    updates: Optional[DinoAiAgentUpdates] = None


class DinoAiAgentMsTeamsInput(AdminInputModel):
    team: str
    channel: str
    answer_in: Optional[DinoAiAgentAnswerIn] = None
    updates: Optional[DinoAiAgentUpdates] = None


class DinoAiAgentJiraInput(AdminInputModel):
    default_watchers: List[str]


class DinoAiAgentGitWritesInput(AdminInputModel):
    comment: Optional[bool] = None
    edit: Optional[bool] = None
    approve: Optional[bool] = None
    merge: Optional[bool] = None


class DinoAiAgentSpecInput(AdminInputModel):
    role: str
    goal: str
    backstory: str
    version: Optional[int] = None
    model: Optional[DinoAiAgentModel] = None
    model_family: Optional[str] = None
    environment: Optional[str] = None
    tools: Optional[DinoAiAgentToolGateInput] = None
    mcp_tools: Optional[DinoAiAgentToolGateInput] = None
    slack: Optional[DinoAiAgentSlackSettingsInput] = None
    ms_teams: Optional[DinoAiAgentMsTeamsInput] = None
    jira: Optional[DinoAiAgentJiraInput] = None
    git_writes: Optional[DinoAiAgentGitWritesInput] = None


class SetDinoAiAgentDefinitionInput(AdminInputModel):
    slug: Optional[str] = None
    name: str
    spec: DinoAiAgentSpecInput
    slack_bot_name: Optional[str] = None
    slack_bot_icon_url: Optional[str] = None


class DinoAiAgentEdgeInput(AdminInputModel):
    from_slug: str
    to_slug: str


class SetDinoAiWorkspaceSettingsInput(AdminInputModel):
    sql_query_tool_enabled: Optional[bool] = None
