import json
from typing import Any, Dict, List, Optional

import pytest

from paradime.apis.admin.exception import (
    AdminApiException,
    AdminConnectionTestFailedException,
    AdminJobTimeoutException,
    AdminNotFoundException,
    AdminValidationFailedException,
    ErrorCode,
)
from paradime.apis.admin.jobs import client as jobs_client
from paradime.apis.admin.jobs.client import AdminJobsClient
from paradime.apis.admin.jobs.types import AdminJobStatus


def _job(status: str, **fields: Any) -> Dict[str, Any]:
    return {
        "id": "job-1",
        "kind": "checkGitRepository",
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

    def execute(
        self,
        query: str,
        variables: Optional[Dict[str, Any]] = None,
        *,
        workspace_uid: Optional[str] = None,
    ) -> Dict[str, Any]:
        assert "adminJob(jobId: $jobId)" in query
        self.calls.append({"variables": variables, "workspace_uid": workspace_uid})
        job = self.jobs[min(len(self.calls), len(self.jobs)) - 1]
        return {"adminJob": job}


def _jobs(*jobs: Optional[Dict[str, Any]]) -> "tuple[AdminJobsClient, JobTransport, List[float]]":
    transport = JobTransport(*jobs)
    sleeps: List[float] = []
    return AdminJobsClient(transport, sleep=sleeps.append), transport, sleeps


def test_wait_polls_until_the_job_succeeds_and_parses_its_result() -> None:
    result = {"uri": "git@github.com:acme/analytics.git"}
    jobs, transport, sleeps = _jobs(
        _job("QUEUED"), _job("RUNNING"), _job("SUCCEEDED", result=json.dumps(result))
    )

    job = jobs.wait("job-1", poll_interval=3, workspace_uid="w1")

    assert job.status == AdminJobStatus.SUCCEEDED
    assert job.result == result
    assert sleeps == [3, 3]
    assert transport.calls[0] == {"variables": {"jobId": "job-1"}, "workspace_uid": "w1"}


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
