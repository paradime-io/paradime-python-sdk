import logging
from typing import List

from paradime.apis.lineage_diff.exception import LineageDiffReportFailedException
from paradime.apis.lineage_diff.types import Report, ReportStatus
from paradime.client.api_client import APIClient
from paradime.graphql import load_operation
from paradime.tools.polling import poll_until
from paradime.tools.pydantic import parse_obj_as

logger = logging.getLogger(__name__)


class LineageDiffClient:
    def __init__(self, client: APIClient) -> None:
        self.client = client

    def trigger_report(
        self,
        *,
        bolt_run_id: int,
        pull_request_number: int,
        user_email: str,
        changed_file_paths: List[str],
    ) -> str:
        """
        Triggers a lineage diff report for the specified parameters.

        Args:
            bolt_run_id (int): The ID of the completed Turbo CI bolt run.
            pull_request_number (int): The number of the pull request.
            user_email (str): The email of the user triggering the report (pull request author).
            changed_file_paths (List[str]): A list of file paths that have changed in the pull request.

        Returns:
            str: The UUID of the triggered lineage diff report.
        """
        query = load_operation("lineage_diff", "trigger_report")

        variables = {
            "boltRunId": bolt_run_id,
            "pullRequestNumber": pull_request_number,
            "userEmail": user_email,
            "changedFilePaths": changed_file_paths,
        }

        response = self.client._call_gql(query, variables)

        return response["triggerLineageDiffReport"]["uuid"]

    def fetch_report(self, *, uuid: str) -> Report:
        """
        Fetches a lineage diff report by UUID.

        Args:
            uuid (str): The UUID of the lineage diff report.

        Returns:
            Report: The lineage diff report.
        """
        query = load_operation("lineage_diff", "fetch_report")

        variables = {"uuid": uuid}

        response = self.client._call_gql(query, variables)

        report = response["fetchLineageDiffReport"]["report"]

        return parse_obj_as(Report, report)

    def trigger_report_and_wait(
        self,
        *,
        bolt_run_id: int,
        pull_request_number: int,
        user_email: str,
        changed_file_paths: List[str],
        timeout: int = 3600,
        poll_interval: float = 20.0,
    ) -> Report:
        """
        Triggers a lineage diff report for the specified parameters and waits for the report to be available.

        Args:
            bolt_run_id (int): The ID of the completed Turbo CI bolt run.
            pull_request_number (int): The number of the pull request.
            user_email (str): The email of the user triggering the report (pull request author).
            changed_file_paths (List[str]): A list of file paths that have changed in the pull request.
            timeout (int): Maximum seconds to wait before raising ``TimeoutError``. Defaults to 3600.
            poll_interval (float): Seconds between status polls. Defaults to 20.

        Returns:
            Report: The lineage diff report.

        Raises:
            LineageDiffReportFailedException: If the report finishes with status ``FAILED``.
            TimeoutError: If the report is not available within ``timeout`` seconds.
        """
        uuid = self.trigger_report(
            bolt_run_id=bolt_run_id,
            pull_request_number=pull_request_number,
            user_email=user_email,
            changed_file_paths=changed_file_paths,
        )

        logger.info(
            f"[STARTED] Lineage diff report triggered. UUID: {uuid}. Waiting for report to be available..."
        )

        def is_terminal(report: Report) -> bool:
            if report.status == ReportStatus.FAILED:
                error_message = (
                    f"[ERROR] Failed to generate lineage diff report. Message: {report.message}"
                )
                logger.info(error_message)
                raise LineageDiffReportFailedException(error_message)
            return report.status == ReportStatus.AVAILABLE

        report = poll_until(
            lambda: self.fetch_report(uuid=uuid),
            is_terminal,
            timeout=timeout,
            interval=poll_interval,
            on_poll=lambda r: logger.info(
                f"[IN PROGRESS] Lineage diff report is in progress. Message: {r.message}. URL: {r.url}"
            ),
            timeout_message=lambda r: (
                "[TIMEOUT] Timed out waiting for lineage diff report to be available. "
                f"Last status: {r.status}. Last message: {r.message}"
            ),
        )

        logger.info("[AVAILABLE] Lineage diff report is now available!")
        return report
