from datetime import datetime
from typing import Any, Dict, List, Optional, Type, Union

from paradime.apis.admin.exception import ErrorCode, parse_error_code
from paradime.apis.admin.types import AdminModel, GitProvider, OpenEnum, parse_page
from paradime.tools.pydantic import validator

DEPLOY_BOLT_SCHEDULES_RESULT_FIELDS = """
    ... on DeployBoltSchedulesResult {
        scheduleParseId
        gitHash
        schedules {
            totalCount
            pageInfo {
                hasNextPage
                endCursor
            }
            edges {
                cursor
                node {
                    slug
                    source
                    failureReason
                }
            }
        }
    }
"""

GIT_REPOSITORY_CONNECT_RESULT_FIELDS = """
    ... on GitRepositoryConnectResult {
        uri
        provider
        dbtProjectDir
    }
"""

GIT_REPOSITORY_CHECK_RESULT_FIELDS = """
    ... on GitRepositoryCheckResult {
        uri
    }
"""

DINOAI_AGENT_SYNC_RESULT_FIELDS = """
    ... on DinoAiAgentSyncResult {
        added
        updated
        skipped
        branch
        commitSha
        isFetched
    }
"""


def admin_job_fields(*result_fields: str) -> str:
    return f"""
    id
    kind
    status
    failure {{
        code
        reason
    }}
    result {{
        __typename
        {"".join(result_fields)}
    }}
    createdAt
    updatedAt
"""


ADMIN_JOB_FIELDS = admin_job_fields(
    DEPLOY_BOLT_SCHEDULES_RESULT_FIELDS,
    GIT_REPOSITORY_CONNECT_RESULT_FIELDS,
    GIT_REPOSITORY_CHECK_RESULT_FIELDS,
    DINOAI_AGENT_SYNC_RESULT_FIELDS,
)


class AdminJobStatus(OpenEnum):
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    UNKNOWN = "UNKNOWN"


class AdminJobKind(OpenEnum):
    DEPLOY_BOLT_SCHEDULES = "DEPLOY_BOLT_SCHEDULES"
    CONNECT_GIT_REPOSITORY = "CONNECT_GIT_REPOSITORY"
    CHECK_GIT_REPOSITORY = "CHECK_GIT_REPOSITORY"
    SYNC_DINOAI_AGENT_DEFINITIONS_FROM_CODE = "SYNC_DINOAI_AGENT_DEFINITIONS_FROM_CODE"
    UNKNOWN = "UNKNOWN"


class BoltScheduleSource(OpenEnum):
    YAML = "YAML"
    UI = "UI"
    UNKNOWN = "UNKNOWN"


class AdminJobFailure(AdminModel):
    code: str
    reason: str

    @property
    def error_code(self) -> Optional[ErrorCode]:
        return parse_error_code(self.code)


class DeployedBoltSchedule(AdminModel):
    slug: str
    source: Optional[BoltScheduleSource] = None
    failure_reason: Optional[str] = None


class DeployedBoltSchedulePage(AdminModel):
    nodes: List[DeployedBoltSchedule] = []
    total_count: int = 0
    end_cursor: Optional[str] = None
    has_next_page: bool = False


class DeployBoltSchedulesResult(AdminModel):
    schedule_parse_id: Optional[int] = None
    git_hash: Optional[str] = None
    schedules: DeployedBoltSchedulePage = DeployedBoltSchedulePage()

    @validator("schedules", pre=True)
    def _parse_schedules(cls, value: Any) -> Any:
        if value is None:
            return DeployedBoltSchedulePage()
        if isinstance(value, dict) and "edges" in value:
            page = parse_page(value, DeployedBoltSchedule.parse_obj)
            return DeployedBoltSchedulePage(
                nodes=page.nodes,
                total_count=page.total_count,
                end_cursor=page.end_cursor,
                has_next_page=page.has_next_page,
            )
        return value


class GitRepositoryConnectResult(AdminModel):
    uri: str
    provider: Optional[GitProvider] = None
    dbt_project_dir: Optional[str] = None


class GitRepositoryCheckResult(AdminModel):
    uri: str


class DinoAiAgentSyncResult(AdminModel):
    added: Optional[int] = None
    updated: Optional[int] = None
    skipped: Optional[int] = None
    branch: Optional[str] = None
    commit_sha: Optional[str] = None
    is_fetched: Optional[bool] = None


AdminJobResult = Union[
    DeployBoltSchedulesResult,
    GitRepositoryConnectResult,
    GitRepositoryCheckResult,
    DinoAiAgentSyncResult,
]

RESULT_BY_TYPENAME: Dict[str, Type[AdminModel]] = {
    result.__name__: result
    for result in (
        DeployBoltSchedulesResult,
        GitRepositoryConnectResult,
        GitRepositoryCheckResult,
        DinoAiAgentSyncResult,
    )
}


class AdminJob(AdminModel):
    id: str
    kind: AdminJobKind
    status: AdminJobStatus
    failure: Optional[AdminJobFailure] = None
    result: Optional[Any] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    @validator("result", pre=True)
    def _parse_result(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        result_class = RESULT_BY_TYPENAME.get(value.get("__typename", ""))
        if result_class is None:
            return None
        return result_class.parse_obj(value)

    @property
    def is_finished(self) -> bool:
        return self.status in (AdminJobStatus.SUCCEEDED, AdminJobStatus.FAILED)
