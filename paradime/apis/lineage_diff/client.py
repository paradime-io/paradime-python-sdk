import time
from datetime import datetime, timedelta
from typing import List

from paradime.apis.lineage_diff.exception import LineageDiffReportFailedException
from paradime.apis.lineage_diff.types import Report, ReportStatus
from paradime.client.api_client import APIClient


class LineageDiffClient:
    def __init__(self, client: APIClient) -> None:
        self.client = client

    def trigger_report(
        self,
        *,
        user_email: str,
        pull_request_number: int,
        repository_name: str,
        base_commit_sha: str,
        head_commit_sha: str,
        changed_file_paths: List[str],
    ) -> str:
        """
        Triggers a lineage diff report for the specified parameters.

        Args:
            user_email (str): The email of the user triggering the report (pull request author).
            pull_request_number (int): The number of the pull request.
            repository_name (str): The full name of the repository. E.g. "paradime-io/jaffle-shop".
            base_commit_sha (str): The SHA of the base commit.
            head_commit_sha (str): The SHA of the head commit.
            changed_file_paths (List[str]): A list of file paths that have changed in the pull request.

        Returns:
            str: The UUID of the triggered lineage diff report.
        """
        query = """
            mutation TriggerLineageDiffReport(
                $baseCommitSha: String!
                $changedFilePaths: [String!]!
                $headCommitSha: String!
                $pullRequestNumber: Int!
                $repositoryName: String!
                $userEmail: String!
            ) {
                triggerLineageDiffReport(
                    baseCommitSha: $baseCommitSha
                    changedFilePaths: $changedFilePaths
                    headCommitSha: $headCommitSha
                    pullRequestNumber: $pullRequestNumber
                    repositoryName: $repositoryName
                    userEmail: $userEmail
                ) {
                    ok
                    uuid
                }
            }
        """

        variables = {
            "baseCommitSha": base_commit_sha,
            "changedFilePaths": changed_file_paths,
            "headCommitSha": head_commit_sha,
            "pullRequestNumber": pull_request_number,
            "repositoryName": repository_name,
            "userEmail": user_email,
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
        user_email: str,
        pull_request_number: int,
        repository_name: str,
        base_commit_sha: str,
        head_commit_sha: str,
        changed_file_paths: List[str],
        timeout: int = 3600,
    ) -> Report:
        """
        Triggers a lineage diff report for the specified parameters and waits for the report to be available.

        Args:
            user_email (str): The email of the user triggering the report (pull request author).
            pull_request_number (int): The number of the pull request.
            repository_name (str): The full name of the repository. E.g. "paradime-io/jaffle-shop".
            base_commit_sha (str): The SHA of the base commit.
            head_commit_sha (str): The SHA of the head commit.
            changed_file_paths (List[str]): A list of file paths that have changed in the pull request.

        Returns:
            Report: The lineage diff report.
        """
        uuid = self.trigger_report(
            user_email=user_email,
            pull_request_number=pull_request_number,
            repository_name=repository_name,
            base_commit_sha=base_commit_sha,
            head_commit_sha=head_commit_sha,
            changed_file_paths=changed_file_paths,
        )

        print(
            f"[STARTED] Lineage diff report triggered. UUID: {uuid}. Waiting for report to be available..."
        )

        start_time = datetime.now()
        while True:
            report = self.fetch_report(uuid=uuid)
            if report.status == ReportStatus.AVAILABLE:
                print("[AVAILABLE] Lineage diff report is now available!")
                return report
            elif report.status == ReportStatus.FAILED:
                error_message = (
                    f"[ERROR] Failed to generate lineage diff report. Message: {report.message}"
                )
                print(error_message)
                raise LineageDiffReportFailedException(error_message)
            elif datetime.now() - start_time > timedelta(seconds=timeout):
                timeout_message = f"[TIMEOUT] Timed out waiting for lineage diff report to be available. Last status: {report.status}. Last message: {report.message}"
                raise TimeoutError(timeout_message)

            print(
                f"[IN PROGRESS] Lineage diff report is in progress. Message: {report.message}. URL: {report.url}"
            )

            time.sleep(20)
