import logging
import time
from typing import Callable, Iterator, Optional

from paradime.apis.admin.exception import AdminJobTimeoutException, ErrorCode, admin_exception_for
from paradime.apis.admin.jobs.types import (
    ADMIN_JOB_FIELDS,
    DEPLOYED_BOLT_SCHEDULE_FIELDS,
    AdminJob,
    AdminJobStatus,
    DeployedBoltSchedule,
)
from paradime.apis.admin.transport import AdminTransport
from paradime.apis.admin.types import (
    CONNECTION_FIELDS,
    DEFAULT_PAGE_SIZE,
    Page,
    check_page_size,
    iterate_pages,
    parse_page,
)

logger = logging.getLogger(__name__)


class AdminJobsClient:
    def __init__(
        self, transport: AdminTransport, *, sleep: Callable[[float], None] = time.sleep
    ) -> None:
        self.transport = transport
        self.sleep = sleep

    def get(self, job_id: str, *, workspace_uid: Optional[str] = None) -> Optional[AdminJob]:
        """
        Reads an async Admin operation's job.

        Args:
            job_id (str): The job's id, as the operation that started it returned.
            workspace_uid (str, optional): The workspace to target, for a company API key.

        Returns:
            Optional[AdminJob]: The job, or None when the key can't see a job with this id.
        """

        query = f"""
            query adminJob($jobId: String!) {{
                adminJob(jobId: $jobId) {{
                    {ADMIN_JOB_FIELDS}
                }}
            }}
        """

        response = self.transport.execute(query, {"jobId": job_id}, workspace_uid=workspace_uid)
        job = response["adminJob"]
        return AdminJob.parse_obj(job) if job is not None else None

    def deployed_schedules(
        self,
        job_id: str,
        *,
        workspace_uid: Optional[str] = None,
        page_size: int = DEFAULT_PAGE_SIZE,
    ) -> Iterator[DeployedBoltSchedule]:
        """
        Iterates over every schedule a deploy job read, YAML and UI, in the order the deploy
        stored them. The job's `result` holds only their first page.

        Args:
            job_id (str): The id of the job `admin.bolt.deploy_schedules` started.
            workspace_uid (str, optional): The workspace to target, for a company API key.
            page_size (int): How many schedules to read per request, at most 500.

        Returns:
            Iterator[DeployedBoltSchedule]: The schedules, read a page at a time; none unless the
                job is a deploy that succeeded.

        Raises:
            AdminNotFoundException: When the key can't see the job.
        """

        check_page_size(page_size)
        query = f"""
            query adminJobDeployedSchedules($jobId: String!, $first: Int, $after: String) {{
                adminJob(jobId: $jobId) {{
                    result {{
                        __typename
                        ... on DeployBoltSchedulesResult {{
                            schedules(first: $first, after: $after) {{
                                edges {{
                                    node {{
                                        {DEPLOYED_BOLT_SCHEDULE_FIELDS}
                                    }}
                                }}
                                {CONNECTION_FIELDS}
                            }}
                        }}
                    }}
                }}
            }}
        """

        def fetch_page(after: Optional[str]) -> Page[DeployedBoltSchedule]:
            response = self.transport.execute(
                query,
                {"jobId": job_id, "first": page_size, "after": after},
                workspace_uid=workspace_uid,
            )
            job = response["adminJob"]
            if job is None:
                raise admin_exception_for(ErrorCode.NOT_FOUND, f"No admin job with id {job_id!r}.")
            result = job.get("result") or {}
            return parse_page(result.get("schedules"), DeployedBoltSchedule.parse_obj)

        return iterate_pages(fetch_page)

    def wait(
        self,
        job_id: str,
        *,
        timeout: float = 600,
        poll_interval: float = 2,
        workspace_uid: Optional[str] = None,
    ) -> AdminJob:
        """
        Polls a job until it is `SUCCEEDED` or `FAILED`.

        Args:
            job_id (str): The job's id.
            timeout (float): How many seconds to wait at most. Defaults to 600.
            poll_interval (float): Seconds between polls. Defaults to 2.
            workspace_uid (str, optional): The workspace to target, for a company API key.

        Returns:
            AdminJob: The succeeded job; its `result` is the typed result of its `kind`, such as
                a `DeployBoltSchedulesResult`.

        Raises:
            AdminApiException: The typed exception of the failure's code, when the job fails.
                Its `job` attribute is the failed job.
            AdminNotFoundException: When the key can't see the job.
            AdminJobTimeoutException: When the job is not finished after `timeout` seconds.
            AdminApiException: When the job reports a status this SDK version does not know.
        """

        deadline = time.monotonic() + timeout
        while True:
            job = self.get(job_id, workspace_uid=workspace_uid)
            if job is None:
                raise admin_exception_for(ErrorCode.NOT_FOUND, f"No admin job with id {job_id!r}.")
            if job.status == AdminJobStatus.SUCCEEDED:
                return job
            if job.status == AdminJobStatus.FAILED:
                raise_for_failed_job(job)
            if job.status == AdminJobStatus.UNKNOWN:
                exception = admin_exception_for(
                    None,
                    f"Admin job {job_id!r} reports a status this SDK version does not know. "
                    "Upgrade paradime-io to follow it.",
                )
                exception.job = job
                raise exception
            now = time.monotonic()
            if now >= deadline:
                raise AdminJobTimeoutException(
                    f"Admin job {job_id!r} is still {job.status.value} after {timeout:g}s.",
                    job=job,
                )
            logger.info(f"Admin job {job_id} is {job.status.value}.")
            self.sleep(min(poll_interval, deadline - now))


def raise_for_failed_job(job: AdminJob) -> None:
    if job.status != AdminJobStatus.FAILED:
        return
    failure = job.failure
    exception = admin_exception_for(
        failure.error_code if failure else None,
        failure.reason if failure else f"Admin job {job.id!r} failed.",
        extensions={"code": failure.code} if failure else None,
    )
    exception.job = job
    raise exception
