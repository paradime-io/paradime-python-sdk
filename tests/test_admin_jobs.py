from typing import Any, Callable, Dict, List, Optional

import pytest
from test_admin_clients import REPOSITORY, _admin

from paradime.apis.admin.exception import (
    AdminApiException,
    AdminConnectionTestFailedException,
    AdminJobTimeoutException,
    AdminNotFoundException,
    AdminValidationFailedException,
    ErrorCode,
)
from paradime.apis.admin.git.types import GitHubAppConnectionInput
from paradime.apis.admin.jobs import client as jobs_client
from paradime.apis.admin.jobs.client import AdminJobsClient
from paradime.apis.admin.jobs.types import (
    RESULT_BY_TYPENAME,
    AdminJob,
    AdminJobKind,
    AdminJobStatus,
    BoltScheduleSource,
    DeployBoltSchedulesResult,
    DinoAiAgentSyncResult,
    GitRepositoryCheckResult,
    GitRepositoryConnectResult,
)
from paradime.apis.admin.types import GitProvider


def _job(status: str, **fields: Any) -> Dict[str, Any]:
    return {
        "id": "job-1",
        "kind": "CHECK_GIT_REPOSITORY",
        "status": status,
        "failure": None,
        "result": None,
        "createdAt": "2026-11-05T10:00:00Z",
        "updatedAt": "2026-11-05T10:00:05Z",
        **fields,
    }


class JobTransport:
    """Answers every adminJob query with the next job, repeating the last one."""

    def __init__(self, *jobs: Optional[Dict[str, Any]]) -> None:
        self.jobs = list(jobs)
        self.calls: List[Dict[str, Any]] = []
        self.queries: List[str] = []

    def execute(
        self,
        query: str,
        variables: Optional[Dict[str, Any]] = None,
        *,
        workspace_uid: Optional[str] = None,
    ) -> Dict[str, Any]:
        assert "adminJob(jobId: $jobId)" in query
        self.calls.append({"variables": variables, "workspace_uid": workspace_uid})
        self.queries.append(query)
        job = self.jobs[min(len(self.calls), len(self.jobs)) - 1]
        return {"adminJob": job}


def _jobs(*jobs: Optional[Dict[str, Any]]) -> "tuple[AdminJobsClient, JobTransport, List[float]]":
    transport = JobTransport(*jobs)
    sleeps: List[float] = []
    return AdminJobsClient(transport, sleep=sleeps.append), transport, sleeps


def test_wait_polls_until_the_job_succeeds_and_returns_its_typed_result() -> None:
    result = {"__typename": "GitRepositoryCheckResult", "uri": "git@github.com:acme/analytics.git"}
    jobs, transport, sleeps = _jobs(
        _job("QUEUED"), _job("RUNNING"), _job("SUCCEEDED", result=result)
    )

    job = jobs.wait("job-1", poll_interval=3, workspace_uid="w1")

    assert job.status == AdminJobStatus.SUCCEEDED
    assert job.kind == AdminJobKind.CHECK_GIT_REPOSITORY
    assert isinstance(job.result, GitRepositoryCheckResult)
    assert job.result.uri == "git@github.com:acme/analytics.git"
    assert sleeps == [3, 3]
    assert transport.calls[0] == {"variables": {"jobId": "job-1"}, "workspace_uid": "w1"}


def test_the_poll_selects_every_result_type() -> None:
    jobs, transport, _ = _jobs(_job("SUCCEEDED"))

    jobs.wait("job-1")

    for typename in RESULT_BY_TYPENAME:
        assert f"... on {typename} {{" in transport.queries[0]
    assert "__typename" in transport.queries[0]


@pytest.mark.parametrize(
    "start, field, typename",
    [
        (
            lambda admin: admin.bolt.deploy_schedules(),
            "deployBoltSchedules",
            "DeployBoltSchedulesResult",
        ),
        (
            lambda admin: admin.git.check_repository("git@x:y.git"),
            "checkGitRepository",
            "GitRepositoryCheckResult",
        ),
        (
            lambda admin: admin.dinoai.sync_agents_from_code(),
            "syncDinoAiAgentDefinitionsFromCode",
            "DinoAiAgentSyncResult",
        ),
    ],
)
def test_a_starting_operation_selects_only_its_own_result(
    start: Callable[[Any], AdminJob], field: str, typename: str
) -> None:
    admin, transport = _admin({field: {"job": _job("QUEUED")}})

    assert start(admin).status == AdminJobStatus.QUEUED
    query = transport.calls[0].query
    assert [name for name in RESULT_BY_TYPENAME if f"... on {name} {{" in query] == [typename]


def test_connect_selects_only_the_connect_result() -> None:
    admin, transport = _admin(
        {"connectGitRepository": {"gitRepository": REPOSITORY, "job": _job("QUEUED")}}
    )

    admin.git.connect(GitHubAppConnectionInput(installation_id="1", repository_id="2"))

    query = transport.calls[0].query
    selected = [name for name in RESULT_BY_TYPENAME if f"... on {name} {{" in query]
    assert selected == ["GitRepositoryConnectResult"]


def test_a_deploy_result_parses_its_schedules_page() -> None:
    job = AdminJob.parse_obj(
        _job(
            "SUCCEEDED",
            kind="DEPLOY_BOLT_SCHEDULES",
            result={
                "__typename": "DeployBoltSchedulesResult",
                "scheduleParseId": 7,
                "gitHash": "0f3c2a1",
                "schedules": {
                    "totalCount": 3,
                    "pageInfo": {"hasNextPage": True, "endCursor": "c2"},
                    "edges": [
                        {
                            "cursor": "c1",
                            "node": {"slug": "daily", "source": "YAML", "failureReason": None},
                        },
                        {
                            "cursor": "c2",
                            "node": {"slug": "adhoc", "source": "NEW", "failureReason": "bad"},
                        },
                    ],
                },
            },
        )
    )

    result = job.result
    assert job.kind == AdminJobKind.DEPLOY_BOLT_SCHEDULES
    assert isinstance(result, DeployBoltSchedulesResult)
    assert result.schedule_parse_id == 7 and result.git_hash == "0f3c2a1"
    assert [schedule.slug for schedule in result.schedules.nodes] == ["daily", "adhoc"]
    assert result.schedules.nodes[0].source == BoltScheduleSource.YAML
    assert result.schedules.nodes[1].source == BoltScheduleSource.UNKNOWN
    assert result.schedules.nodes[1].failure_reason == "bad"
    assert result.schedules.total_count == 3
    assert result.schedules.has_next_page is True and result.schedules.end_cursor == "c2"


def test_a_deploy_result_without_schedules_has_an_empty_page() -> None:
    job = AdminJob.parse_obj(
        _job(
            "SUCCEEDED",
            kind="DEPLOY_BOLT_SCHEDULES",
            result={"__typename": "DeployBoltSchedulesResult", "scheduleParseId": None},
        )
    )

    assert isinstance(job.result, DeployBoltSchedulesResult)
    assert job.result.schedules.nodes == [] and job.result.schedules.total_count == 0


def test_a_connect_result_parses_its_provider() -> None:
    job = AdminJob.parse_obj(
        _job(
            "SUCCEEDED",
            kind="CONNECT_GIT_REPOSITORY",
            result={
                "__typename": "GitRepositoryConnectResult",
                "uri": "git@gitlab.com:acme/analytics.git",
                "provider": "GITLAB",
                "dbtProjectDir": "dbt",
            },
        )
    )

    assert job.kind == AdminJobKind.CONNECT_GIT_REPOSITORY
    assert isinstance(job.result, GitRepositoryConnectResult)
    assert job.result.provider == GitProvider.GITLAB
    assert job.result.dbt_project_dir == "dbt"


def test_a_sync_result_parses_its_counts() -> None:
    job = AdminJob.parse_obj(
        _job(
            "SUCCEEDED",
            kind="SYNC_DINOAI_AGENT_DEFINITIONS_FROM_CODE",
            result={
                "__typename": "DinoAiAgentSyncResult",
                "added": 2,
                "updated": 1,
                "skipped": 0,
                "branch": "main",
                "commitSha": "abc123",
                "isFetched": True,
            },
        )
    )

    assert job.kind == AdminJobKind.SYNC_DINOAI_AGENT_DEFINITIONS_FROM_CODE
    assert job.result == DinoAiAgentSyncResult(
        added=2, updated=1, skipped=0, branch="main", commit_sha="abc123", is_fetched=True
    )


def test_an_unknown_kind_is_unknown_and_an_unknown_result_type_is_none() -> None:
    job = AdminJob.parse_obj(
        _job("SUCCEEDED", kind="EXPORT_EVERYTHING", result={"__typename": "ExportResult"})
    )

    assert job.kind == AdminJobKind.UNKNOWN
    assert job.result is None


@pytest.mark.parametrize(
    "code, exception_class",
    [
        ("CONNECTION_TEST_FAILED", AdminConnectionTestFailedException),
        ("VALIDATION_FAILED", AdminValidationFailedException),
    ],
)
def test_a_failed_job_raises_the_typed_error_of_its_failure(
    code: str, exception_class: type
) -> None:
    jobs, _, _ = _jobs(
        _job("RUNNING"),
        _job("FAILED", failure={"code": code, "reason": "The deploy key can't push."}),
    )

    with pytest.raises(exception_class) as exc_info:
        jobs.wait("job-1", poll_interval=0)

    assert exc_info.value.code == ErrorCode(code)
    assert exc_info.value.message == "The deploy key can't push."
    assert exc_info.value.job.id == "job-1"


def test_a_failure_with_an_unknown_code_raises_the_base_exception() -> None:
    jobs, _, _ = _jobs(_job("FAILED", failure={"code": "NEW_CODE", "reason": "nope"}))

    with pytest.raises(AdminApiException) as exc_info:
        jobs.wait("job-1")

    assert type(exc_info.value) is AdminApiException
    assert exc_info.value.extensions == {"code": "NEW_CODE"}


def test_wait_times_out_with_the_last_state(monkeypatch: pytest.MonkeyPatch) -> None:
    clock = iter([0.0, 5.0, 11.0])
    monkeypatch.setattr(jobs_client.time, "monotonic", lambda: next(clock))
    jobs, transport, sleeps = _jobs(_job("RUNNING"))

    with pytest.raises(AdminJobTimeoutException) as exc_info:
        jobs.wait("job-1", timeout=10, poll_interval=1)

    assert isinstance(exc_info.value, TimeoutError)
    assert exc_info.value.job.status == AdminJobStatus.RUNNING
    assert len(transport.calls) == 2 and sleeps == [1]


def test_an_unknown_status_stops_the_wait_with_a_clear_error() -> None:
    jobs, transport, sleeps = _jobs(_job("PAUSED"), _job("SUCCEEDED"))

    with pytest.raises(AdminApiException, match="does not know") as exc_info:
        jobs.wait("job-1", poll_interval=0)

    assert exc_info.value.job.status == AdminJobStatus.UNKNOWN
    assert len(transport.calls) == 1 and sleeps == []


def test_the_last_sleep_ends_at_the_deadline(monkeypatch: pytest.MonkeyPatch) -> None:
    clock = iter([0.0, 4.0, 10.0])
    monkeypatch.setattr(jobs_client.time, "monotonic", lambda: next(clock))
    jobs, _, sleeps = _jobs(_job("RUNNING"))

    with pytest.raises(AdminJobTimeoutException):
        jobs.wait("job-1", timeout=10, poll_interval=60)

    assert sleeps == [6.0]


def test_a_job_the_key_cannot_see_is_not_found() -> None:
    jobs, _, _ = _jobs(None)

    assert jobs.get("job-1") is None
    with pytest.raises(AdminNotFoundException):
        jobs.wait("job-1")
