from datetime import datetime
from typing import List, Optional

from paradime.apis.admin.types import AdminInputModel, AdminModel, OpenEnum

DINOAI_AGENT_DEFINITION_FIELDS = """
    slug
    name
    source
    version
    role
    goal
    backstory
    model
    modelFamily
    environment
    tools {
        mode
        names
    }
    mcpTools {
        mode
        names
    }
    slack {
        channel
        answerIn
        updates
    }
    msTeams {
        team
        channel
        answerIn
        updates
    }
    jira {
        defaultWatchers
    }
    gitWrites {
        comment
        edit
        approve
        merge
    }
    agentsSquad
    slackBotName
    slackBotIconUrl
    gitPath
    gitCommitSha
    syncedAt
    createdAt
    updatedAt
"""


class DinoaiAgentSource(OpenEnum):
    YAML = "YAML"
    UI = "UI"
    UNKNOWN = "UNKNOWN"


class DinoaiAgentModel(OpenEnum):
    HAIKU = "HAIKU"
    SONNET = "SONNET"
    OPUS = "OPUS"
    UNKNOWN = "UNKNOWN"


class DinoaiAgentToolGateMode(OpenEnum):
    ALLOWLIST = "ALLOWLIST"
    DENYLIST = "DENYLIST"
    UNKNOWN = "UNKNOWN"


class DinoaiAgentAnswerIn(OpenEnum):
    THREAD = "THREAD"
    CHANNEL = "CHANNEL"
    UNKNOWN = "UNKNOWN"


class DinoaiAgentUpdates(OpenEnum):
    ALL = "ALL"
    FINAL = "FINAL"
    UNKNOWN = "UNKNOWN"


class DinoaiAgentToolGate(AdminModel):
    mode: Optional[DinoaiAgentToolGateMode] = None
    names: Optional[List[str]] = None


class DinoaiAgentSlack(AdminModel):
    channel: Optional[str] = None
    answer_in: Optional[DinoaiAgentAnswerIn] = None
    updates: Optional[DinoaiAgentUpdates] = None


class DinoaiAgentMsTeams(AdminModel):
    team: Optional[str] = None
    channel: Optional[str] = None
    answer_in: Optional[DinoaiAgentAnswerIn] = None
    updates: Optional[DinoaiAgentUpdates] = None


class DinoaiAgentJira(AdminModel):
    default_watchers: Optional[List[str]] = None


class DinoaiAgentGitWrites(AdminModel):
    comment: Optional[bool] = None
    edit: Optional[bool] = None
    approve: Optional[bool] = None
    merge: Optional[bool] = None


class DinoaiAgentDefinition(AdminModel):
    slug: str
    name: str
    source: Optional[DinoaiAgentSource] = None
    version: Optional[int] = None
    role: Optional[str] = None
    goal: Optional[str] = None
    backstory: Optional[str] = None
    model: Optional[DinoaiAgentModel] = None
    model_family: Optional[str] = None
    environment: Optional[str] = None
    tools: Optional[DinoaiAgentToolGate] = None
    mcp_tools: Optional[DinoaiAgentToolGate] = None
    slack: Optional[DinoaiAgentSlack] = None
    ms_teams: Optional[DinoaiAgentMsTeams] = None
    jira: Optional[DinoaiAgentJira] = None
    git_writes: Optional[DinoaiAgentGitWrites] = None
    agents_squad: Optional[List[str]] = None
    slack_bot_name: Optional[str] = None
    slack_bot_icon_url: Optional[str] = None
    git_path: Optional[str] = None
    git_commit_sha: Optional[str] = None
    synced_at: Optional[datetime] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class DinoaiWorkspaceSettings(AdminModel):
    sql_query_tool_enabled: Optional[bool] = None


class DinoaiAgentToolGateInput(AdminInputModel):
    mode: DinoaiAgentToolGateMode
    names: Optional[List[str]] = None


class DinoaiAgentSlackInput(AdminInputModel):
    channel: str
    answer_in: Optional[DinoaiAgentAnswerIn] = None
    updates: Optional[DinoaiAgentUpdates] = None


class DinoaiAgentMsTeamsInput(AdminInputModel):
    team: str
    channel: str
    answer_in: Optional[DinoaiAgentAnswerIn] = None
    updates: Optional[DinoaiAgentUpdates] = None


class DinoaiAgentJiraInput(AdminInputModel):
    default_watchers: List[str]


class DinoaiAgentGitWritesInput(AdminInputModel):
    comment: Optional[bool] = None
    edit: Optional[bool] = None
    approve: Optional[bool] = None
    merge: Optional[bool] = None


class DinoaiAgentSpecInput(AdminInputModel):
    role: str
    goal: str
    backstory: str
    version: Optional[int] = None
    model: Optional[DinoaiAgentModel] = None
    model_family: Optional[str] = None
    environment: Optional[str] = None
    tools: Optional[DinoaiAgentToolGateInput] = None
    mcp_tools: Optional[DinoaiAgentToolGateInput] = None
    slack: Optional[DinoaiAgentSlackInput] = None
    ms_teams: Optional[DinoaiAgentMsTeamsInput] = None
    jira: Optional[DinoaiAgentJiraInput] = None
    git_writes: Optional[DinoaiAgentGitWritesInput] = None


class SetDinoaiAgentDefinitionInput(AdminInputModel):
    slug: Optional[str] = None
    name: str
    spec: DinoaiAgentSpecInput
    slack_bot_name: Optional[str] = None
    slack_bot_icon_url: Optional[str] = None


class DinoaiAgentEdgeInput(AdminInputModel):
    from_slug: str
    to_slug: str


class SetDinoaiWorkspaceSettingsInput(AdminInputModel):
    sql_query_tool_enabled: Optional[bool] = None
