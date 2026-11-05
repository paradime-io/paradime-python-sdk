import logging
import time
from typing import Callable, Optional

from paradime.apis.admin.exception import AdminJobTimeoutException, ErrorCode, admin_exception_for
from paradime.apis.admin.jobs.types import ADMIN_JOB_FIELDS, AdminJob, AdminJobStatus
from paradime.apis.admin.transport import AdminTransport

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
            workspace_uid (str, optional): The workspace to target, for a company-level key.

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
            workspace_uid (str, optional): The workspace to target, for a company-level key.

        Returns:
            AdminJob: The succeeded job; its `result` holds what it produced.

        Raises:
            AdminApiException: The typed exception of the failure's code, when the job fails.
                Its `job` attribute is the failed job.
            AdminNotFoundException: When the key can't see the job.
            AdminJobTimeoutException: When the job is not finished after `timeout` seconds.
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
            if time.monotonic() >= deadline:
                raise AdminJobTimeoutException(
                    f"Admin job {job_id!r} is still {job.status.value} after {timeout:g}s.",
                    job=job,
                )
            logger.info(f"Admin job {job_id} is {job.status.value}.")
            self.sleep(poll_interval)


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
