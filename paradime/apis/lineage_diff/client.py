import logging
import time
from datetime import datetime, timedelta
from typing import List

from paradime.apis.lineage_diff.exception import LineageDiffReportFailedException
from paradime.apis.lineage_diff.types import Report, ReportStatus
from paradime.client.api_client import APIClient

logging.basicConfig(format="%(asctime)s - %(message)s", level=logging.INFO)
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
        query = """
            mutation TriggerLineageDiffReport(
                $boltRunId: Int!
                $pullRequestNumber: Int!
                $userEmail: String!
                $changedFilePaths: [String!]!
            ) {
                triggerLineageDiffReport(
                    boltRunId: $boltRunId
                    pullRequestNumber: $pullRequestNumber
                    userEmail: $userEmail
                    changedFilePaths: $changedFilePaths
                ) {
                    ok
                    uuid
                }
            }
        """

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
        query = """
            query FetchLineageDiffReport($uuid: String!) {
                fetchLineageDiffReport(uuid: $uuid) {
                    ok
                    report {
                        message
                        resultJson
                        resultMarkdown
                        status
                        url
                        uuid
                    }
                }
            }
        """

        variables = {"uuid": uuid}

        response = self.client._call_gql(query, variables)

        report = response["fetchLineageDiffReport"]["report"]

        return Report(
            message=report["message"],
            status=report["status"],
            url=report["url"],
            uuid=report["uuid"],
            result_json=report["resultJson"],
            result_markdown=report["resultMarkdown"],
        )

    def trigger_report_and_wait(
        self,
        *,
        bolt_run_id: int,
        pull_request_number: int,
        user_email: str,
        changed_file_paths: List[str],
        timeout: int = 3600,
    ) -> Report:
        """
        Triggers a lineage diff report for the specified parameters and waits for the report to be available.

        Args:
            bolt_run_id (int): The ID of the completed Turbo CI bolt run.
            pull_request_number (int): The number of the pull request.
            user_email (str): The email of the user triggering the report (pull request author).
            changed_file_paths (List[str]): A list of file paths that have changed in the pull request.

        Returns:
            Report: The lineage diff report.
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

        start_time = datetime.now()
        while True:
            report = self.fetch_report(uuid=uuid)
            if report.status == ReportStatus.AVAILABLE:
                logger.info("[AVAILABLE] Lineage diff report is now available!")
                return report
            elif report.status == ReportStatus.FAILED:
                error_message = (
                    f"[ERROR] Failed to generate lineage diff report. Message: {report.message}"
                )
                logger.info(error_message)
                raise LineageDiffReportFailedException(error_message)
            elif datetime.now() - start_time > timedelta(seconds=timeout):
                timeout_message = f"[TIMEOUT] Timed out waiting for lineage diff report to be available. Last status: {report.status}. Last message: {report.message}"
                raise TimeoutError(timeout_message)

            logger.info(
                f"[IN PROGRESS] Lineage diff report is in progress. Message: {report.message}. URL: {report.url}"
            )

            time.sleep(20)
