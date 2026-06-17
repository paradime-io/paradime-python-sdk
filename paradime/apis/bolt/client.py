import time
import warnings
from typing import Iterator, List, Optional

import requests

from paradime.apis.bolt.exception import (
    BoltScheduleArtifactNotFoundException,
    BoltScheduleLatestRunNotFoundException,
)
from paradime.apis.bolt.types import (
    BoltCommand,
    BoltCommandArtifact,
    BoltCommandLogs,
    BoltDeferredSchedule,
    BoltLogLine,
    BoltLogStream,
    BoltNotificationItem,
    BoltNotifications,
    BoltRun,
    BoltRunGitInfo,
    BoltRunState,
    BoltSchedule,
    BoltScheduleInfo,
    BoltScheduleRuns,
    BoltSchedules,
)
from paradime.client.api_client import APIClient


def _resolve_slug_or_schedule_name(
    *,
    slug: Optional[str],
    schedule_name: Optional[str],
    method: str,
) -> str:
    """Resolve the schedule slug from the dual `slug` / `schedule_name` SDK input.

    Both arguments accept a slug — ``schedule_name`` is the deprecated alias
    kept for backwards compatibility with existing callers. Exactly one of the
    two must be provided; ``ValueError`` is raised otherwise. When the caller
    uses the deprecated ``schedule_name`` kwarg, a ``DeprecationWarning`` is
    emitted pointing at their call site.

    Mirrors the GraphQL XOR check on the public API. The resolved value is sent
    on the wire as the new ``slug`` field, which both new and old backends
    accept.
    """
    if bool(slug) == bool(schedule_name):
        raise ValueError(
            f"`{method}` requires exactly one of `slug` or `schedule_name` (deprecated). "
            "Both fields accept a schedule slug."
        )
    if schedule_name is not None:
        warnings.warn(
            f"Passing `schedule_name=` to `{method}` is deprecated; use `slug=` instead. "
            "Both kwargs accept a schedule slug.",
            DeprecationWarning,
            stacklevel=3,
        )
    return slug or schedule_name  # type: ignore[return-value]


def _parse_notification_items(
    items: Optional[list],
) -> Optional[List[BoltNotificationItem]]:
    if items is None:
        return None
    return [
        BoltNotificationItem(
            channel=item.get("channel"),
            events=item.get("events"),
            template_slug=item.get("templateSlug"),
            template_name=item.get("templateName"),
        )
        for item in items
    ]


def _parse_notifications(
    notifications_json: Optional[dict],
) -> Optional[BoltNotifications]:
    if notifications_json is None:
        return None
    return BoltNotifications(
        email_notifications=_parse_notification_items(notifications_json.get("emailNotifications")),
        slack_notifications=_parse_notification_items(notifications_json.get("slackNotifications")),
        ms_teams_notifications=_parse_notification_items(
            notifications_json.get("msTeamsNotifications")
        ),
    )


class BoltClient:
    def __init__(self, client: APIClient):
        self.client = client

    def trigger_run(
        self,
        schedule_name: Optional[str] = None,
        commands: Optional[List[str]] = None,
        branch: Optional[str] = None,
        pr_number: Optional[int] = None,
        *,
        slug: Optional[str] = None,
    ) -> int:
        """
        Triggers a run for a given schedule.

        Args:
            schedule_name (Optional[str]): Deprecated alias for ``slug`` — carries a schedule slug. Kept for backwards compatibility with older SDK callers. Exactly one of ``slug`` or ``schedule_name`` must be provided.
            commands (Optional[List[str]], optional): The list of commands to execute in the run. This will override the commands defined in the schedule. Defaults to None.
            branch (Optional[str], optional): The branch or commit hash to run the commands on. Defaults to None.
            pr_number (Optional[int], optional): The pull request number to associate with the run. Defaults to None.
            slug (Optional[str]): The schedule slug returned by ``createBoltSchedule``. Preferred over ``schedule_name``.

        Returns:
            int: The ID of the triggered run.
        """

        resolved_slug = _resolve_slug_or_schedule_name(
            slug=slug, schedule_name=schedule_name, method="trigger_run"
        )

        query = """
            mutation triggerBoltRun($slug: String, $commands: [String!], $branch: String, $prNumber: Int) {
                triggerBoltRun(slug: $slug, commands: $commands, branch: $branch, prNumber: $prNumber){
                    runId
                }
            }
        """

        response_json = self.client._call_gql(
            query=query,
            variables={
                "slug": resolved_slug,
                "commands": commands,
                "branch": branch,
                "prNumber": pr_number,
            },
        )["triggerBoltRun"]

        return response_json["runId"]

    def suspend_schedule(
        self,
        *,
        suspend: bool,
        slug: Optional[str] = None,
        schedule_name: Optional[str] = None,
    ) -> None:
        """
        Suspends or resumes a UI/API-created schedule.

        Args:
            suspend (bool): True to suspend the schedule, False to unsuspend.
            slug (Optional[str]): The schedule slug returned by ``createBoltSchedule``. Preferred over ``schedule_name``.
            schedule_name (Optional[str]): Deprecated alias for ``slug`` — carries a slug. Kept for backwards compatibility with older SDK callers. Exactly one of ``slug`` or ``schedule_name`` must be provided.

        Note: This only works for schedules created via the UI or API, not via YAML.
        """

        resolved_slug = _resolve_slug_or_schedule_name(
            slug=slug, schedule_name=schedule_name, method="suspend_schedule"
        )

        query = """
            mutation SuspendBoltSchedule($slug: String, $suspend: Boolean!) {
                suspendBoltSchedule(slug: $slug, suspend: $suspend) {
                    ok
                }
            }
        """

        self.client._call_gql(query=query, variables={"slug": resolved_slug, "suspend": suspend})[
            "suspendBoltSchedule"
        ]

    def create_schedule(
        self,
        *,
        display_name: str,
        schedule: str,
        environment: str,
        commands: List[str],
        git_branch: Optional[str] = None,
        description: Optional[str] = None,
        timezone: Optional[str] = None,
        owner_email: Optional[str] = None,
        suspended: Optional[bool] = None,
        sla_seconds: Optional[int] = None,
        trigger_on_merge: Optional[bool] = None,
    ) -> str:
        """
        Create a new Bolt schedule.

        Args:
            display_name (str): Human-readable schedule name shown in the Bolt UI.
            schedule (str): Cron expression (e.g. ``"0 1 * * *"``) or the literal ``"OFF"`` for manual-only runs.
            environment (str): Name of the environment to run in (e.g. ``"production"``).
            commands (List[str]): Commands the schedule should run, in order (e.g. ``["dbt run", "dbt test"]``).
            git_branch (Optional[str]): Git branch the run should check out. Defaults to the environment's branch.
            description (Optional[str]): Free-text description shown in the UI.
            timezone (Optional[str]): IANA timezone for the cron expression (e.g. ``"UTC"``, ``"Europe/London"``).
            owner_email (Optional[str]): Email of the workspace member who should own the schedule.
            suspended (Optional[bool]): Create the schedule already suspended. Defaults to active.
            sla_seconds (Optional[int]): Soft SLA window in seconds; runs exceeding this are surfaced as overdue.
            trigger_on_merge (Optional[bool]): If True, run on every merge to ``git_branch``.

        Returns:
            str: The slug assigned by the backend. This is the identifier to pass as ``slug=`` to every
            other Bolt method (``trigger_run``, ``get_schedule``, ``delete_schedule``, etc.).

        Note:
            There is a short consistency window (~10s) between schedule creation and the trigger path
            accepting the new slug. Callers that immediately invoke ``trigger_run`` may need to retry.
        """

        schedule_input: dict = {
            "displayName": display_name,
            "schedule": schedule,
            "environment": environment,
            "commands": commands,
        }
        optional_fields = {
            "gitBranch": git_branch,
            "description": description,
            "timezone": timezone,
            "ownerEmail": owner_email,
            "suspended": suspended,
            "slaSeconds": sla_seconds,
            "triggerOnMerge": trigger_on_merge,
        }
        for key, value in optional_fields.items():
            if value is not None:
                schedule_input[key] = value

        query = """
            mutation CreateBoltSchedule($schedule: BoltScheduleInput!) {
                createBoltSchedule(schedule: $schedule) {
                    slug
                }
            }
        """

        result = self.client._call_gql(query=query, variables={"schedule": schedule_input})
        return result["createBoltSchedule"]["slug"]

    def delete_schedule(self, slug: str) -> None:
        """
        Delete a Bolt schedule.

        Args:
            slug (str): The schedule slug returned by ``create_schedule``.

        Returns:
            None
        """

        query = """
            mutation DeleteBoltSchedule($slug: String!) {
                deleteBoltSchedule(slug: $slug) {
                    ok
                }
            }
        """

        self.client._call_gql(query=query, variables={"slug": slug})

    def list_schedules(
        self,
        *,
        offset: int = 0,
        limit: int = 100,
        show_inactive: bool = False,
    ) -> BoltSchedules:
        """
        Get a list of Bolt schedules. The list is paginated. The total count of schedules is also returned.

        Args:
            offset (int): The offset value for pagination. Default is 0.
            limit (int): The limit value for pagination. Default is 100.
            show_inactive (bool): Flag to indicate whether to return inactive schedules instead of active schedules. Default is False.

        Returns:
            BoltSchedules: An object containing the list of Bolt schedules and the total count of schedules.
        """

        query = """
            query listBoltSchedules($offset: Int!, $limit: Int!, $showInactive: Boolean!) {
                listBoltSchedules(offset: $offset, limit: $limit, showInactive: $showInactive) {
                    schedules {
                        name
                        schedule
                        owner
                        lastRunAt
                        lastRunState
                        nextRunAt
                        id
                        uuid
                        source
                        turboCi {
                            enabled
                            deferredScheduleName
                            successfulRunOnly
                        }
                        deferredSchedule {
                            enabled
                            deferredScheduleName
                            successfulRunOnly
                        }
                        commands
                        gitBranch
                        slackOn
                        slackNotify
                        emailOn
                        emailNotify
                        notifications {
                            emailNotifications {
                                channel
                                events
                                templateSlug
                                templateName
                            }
                            slackNotifications {
                                channel
                                events
                                templateSlug
                                templateName
                            }
                            msTeamsNotifications {
                                channel
                                events
                                templateSlug
                                templateName
                            }
                        }
                    }
                    totalCount
                }
            }
        """

        response_json = self.client._call_gql(
            query=query,
            variables={"offset": offset, "limit": limit, "showInactive": show_inactive},
        )["listBoltSchedules"]

        schedules: List[BoltSchedule] = []
        for schedule_json in response_json["schedules"]:
            schedules.append(
                BoltSchedule(
                    name=schedule_json["name"],
                    schedule=schedule_json["schedule"],
                    owner=schedule_json["owner"],
                    last_run_at=schedule_json["lastRunAt"],
                    last_run_state=schedule_json["lastRunState"],
                    next_run_at=schedule_json["nextRunAt"],
                    id=schedule_json["id"],
                    uuid=schedule_json["uuid"],
                    source=schedule_json["source"],
                    deferred_schedule=(
                        BoltDeferredSchedule(
                            enabled=schedule_json["deferredSchedule"]["enabled"],
                            deferred_schedule_name=schedule_json["deferredSchedule"][
                                "deferredScheduleName"
                            ],
                            successful_run_only=schedule_json["deferredSchedule"][
                                "successfulRunOnly"
                            ],
                        )
                        if schedule_json["deferredSchedule"]
                        else None
                    ),
                    turbo_ci=(
                        BoltDeferredSchedule(
                            enabled=schedule_json["turboCi"]["enabled"],
                            deferred_schedule_name=schedule_json["turboCi"]["deferredScheduleName"],
                            successful_run_only=schedule_json["turboCi"]["successfulRunOnly"],
                        )
                        if schedule_json["turboCi"]
                        else None
                    ),
                    commands=schedule_json["commands"],
                    git_branch=schedule_json["gitBranch"],
                    slack_on=schedule_json["slackOn"],
                    slack_notify=schedule_json["slackNotify"],
                    email_on=schedule_json["emailOn"],
                    email_notify=schedule_json["emailNotify"],
                    notifications=_parse_notifications(schedule_json.get("notifications")),
                )
            )

        return BoltSchedules(
            schedules=schedules,
            total_count=response_json["totalCount"],
        )

    def list_runs(
        self,
        *,
        slug: Optional[str] = None,
        schedule_name: Optional[str] = None,
        offset: int = 0,
        limit: int = 50,
    ) -> BoltScheduleRuns:
        """
        Get a list of Bolt runs for a specific schedule. The list is paginated.

        Args:
            slug (Optional[str]): The schedule slug returned by ``createBoltSchedule``. Preferred over ``schedule_name``.
            schedule_name (Optional[str]): Deprecated alias for ``slug`` — carries a slug. Kept for backwards compatibility with older SDK callers. Exactly one of ``slug`` or ``schedule_name`` must be provided.
            offset (int): The offset value for pagination. Default is 0. Must be >= 0.
            limit (int): The limit value for pagination. Default is 50. Must be between 1 and 1000.

        Returns:
            BoltScheduleRuns: An object containing the list of Bolt runs.

        Raises:
            ValueError: If offset < 0, limit is not between 1 and 1000, or neither/both of slug/schedule_name are provided.
        """

        # Validate inputs
        if offset < 0:
            raise ValueError(f"offset must be >= 0, got {offset}")
        if limit < 1 or limit > 1000:
            raise ValueError(f"limit must be between 1 and 1000, got {limit}")

        resolved_slug = _resolve_slug_or_schedule_name(
            slug=slug, schedule_name=schedule_name, method="list_runs"
        )

        query = """
            query listBoltRuns($slug: String, $offset: Int!, $limit: Int!) {
                listBoltRuns(slug: $slug, offset: $offset, limit: $limit) {
                    ok
                    runs {
                        id
                        state
                        actor
                        actorEmail
                        startDttm
                        endDttm
                        parentScheduleRunId
                        gitInfo {
                            branch
                            commitHash
                            pullRequestId
                        }
                    }
                }
            }
        """

        response_json = self.client._call_gql(
            query=query,
            variables={"slug": resolved_slug, "offset": offset, "limit": limit},
        )["listBoltRuns"]

        runs: List[BoltRun] = []
        for run_json in response_json["runs"]:
            runs.append(
                BoltRun(
                    id=run_json["id"],
                    state=run_json["state"],
                    actor=run_json["actor"],
                    actor_email=run_json.get("actorEmail"),
                    parent_schedule_run_id=run_json.get("parentScheduleRunId"),
                    start_dttm=run_json["startDttm"],
                    end_dttm=run_json.get("endDttm"),
                    git_info=BoltRunGitInfo(
                        branch=run_json["gitInfo"].get("branch"),
                        commit_hash=run_json["gitInfo"].get("commitHash"),
                        pull_request_id=run_json["gitInfo"].get("pullRequestId"),
                    ),
                )
            )

        return BoltScheduleRuns(
            ok=response_json["ok"],
            runs=runs,
        )

    def get_schedule(
        self,
        schedule_name: Optional[str] = None,
        *,
        slug: Optional[str] = None,
    ) -> BoltScheduleInfo:
        """
        Retrieves information about a specific schedule.

        Args:
            schedule_name (Optional[str]): Deprecated alias for ``slug`` — carries a slug. Kept for backwards compatibility with older SDK callers. Exactly one of ``slug`` or ``schedule_name`` must be provided.
            slug (Optional[str]): The schedule slug returned by ``createBoltSchedule``. Preferred over ``schedule_name``.

        Returns:
            BoltScheduleInfo: An object containing information about the schedule.
        """

        resolved_slug = _resolve_slug_or_schedule_name(
            slug=slug, schedule_name=schedule_name, method="get_schedule"
        )

        query = """
            query boltScheduleName($slug: String) {
                boltScheduleName(slug: $slug) {
                    ok
                    latestRunId
                    commands
                    owner
                    schedule
                    uuid
                    source
                }
            }
        """

        response_json = self.client._call_gql(query=query, variables={"slug": resolved_slug})[
            "boltScheduleName"
        ]

        return BoltScheduleInfo(
            name=resolved_slug,
            commands=response_json["commands"],
            schedule=response_json["schedule"],
            uuid=response_json["uuid"],
            source=response_json["source"],
            owner=response_json["owner"],
            latest_run_id=response_json["latestRunId"],
        )

    def get_run_status(self, run_id: int) -> Optional[BoltRunState]:
        """
        Retrieves the status of a run based on the provided run ID.

        Args:
            run_id (int): The ID of the run.

        Returns:
            str: The state of the run.
        """

        query = """
            query boltRunStatus($runId: Int!) {
                boltRunStatus(runId: $runId) {
                    state
                }
            }
        """

        response_json = self.client._call_gql(query=query, variables={"runId": int(run_id)})[
            "boltRunStatus"
        ]

        return BoltRunState.from_str(response_json["state"])

    def list_run_commands(self, run_id: int) -> List[BoltCommand]:
        """
        Retrieves the list of command level details for a given run.

        Args:
            run_id (int): The ID of the run.

        Returns:
            List[BoltCommand]: The list of Bolt commands for the run, sorted by command ID.
        """

        query = """
            query boltRunStatus($runId: Int!) {
                boltRunStatus(runId: $runId) {
                    commands {
                        id
                        command
                        startDttm
                        endDttm
                        stdout
                        stderr
                        returnCode
                    }
                }
            }
        """

        response_json = self.client._call_gql(query=query, variables={"runId": int(run_id)})[
            "boltRunStatus"
        ]

        commands: List[BoltCommand] = []
        for command_json in response_json["commands"]:
            commands.append(
                BoltCommand(
                    id=command_json["id"],
                    command=command_json["command"],
                    start_dttm=command_json["startDttm"],
                    end_dttm=command_json["endDttm"],
                    stdout=command_json["stdout"],
                    stderr=command_json["stderr"],
                    return_code=command_json["returnCode"],
                )
            )

        return sorted(commands, key=lambda command: command.id)

    def get_command_logs(self, command_id: int, cursor: str = "0:0") -> BoltCommandLogs:
        """
        Fetches a single batch of stdout/stderr log lines for a Bolt command.

        Use this for one-shot polling. For automatic looping until the command
        finishes, prefer `stream_command_logs`.

        Args:
            command_id (int): The ID of the command.
            cursor (str): Opaque cursor returned by the previous call. Use the
                default `"0:0"` for the first call to fetch from the beginning.

        Returns:
            BoltCommandLogs: New lines, the cursor to pass to the next call,
            and a `finished` flag that flips to True once the command exits.
        """

        query = """
            query boltCommandLogs($commandId: Int!, $cursor: String) {
                boltCommandLogs(commandId: $commandId, cursor: $cursor) {
                    lines { stream line }
                    cursor
                    finished
                }
            }
        """

        response_json = self.client._call_gql(
            query=query, variables={"commandId": int(command_id), "cursor": cursor}
        )["boltCommandLogs"]

        return BoltCommandLogs(
            lines=[
                BoltLogLine(stream=BoltLogStream(item["stream"]), line=item["line"])
                for item in response_json["lines"]
            ],
            cursor=response_json["cursor"],
            finished=response_json["finished"],
        )

    def stream_command_logs(
        self, command_id: int, poll_interval: float = 2.0
    ) -> Iterator[BoltLogLine]:
        """
        Yields log lines for a Bolt command as they arrive, stopping when the
        command finishes.

        Args:
            command_id (int): The ID of the command.
            poll_interval (float): Seconds to wait between empty polls. Default 2.0.

        Yields:
            BoltLogLine: Each log line, in arrival order within a poll batch.
                stdout lines for the batch precede stderr lines (approximate
                interleaving — true cross-stream ordering is not recorded).
        """

        cursor = "0:0"
        while True:
            batch = self.get_command_logs(command_id, cursor=cursor)
            for line in batch.lines:
                yield line
            if batch.finished:
                return
            cursor = batch.cursor
            if not batch.lines:
                time.sleep(poll_interval)

    def list_command_artifacts(self, command_id: int) -> List[BoltCommandArtifact]:
        """
        Retrieves the artifacts associated with a given command.

        Args:
            command_id (int): The ID of the command.

        Returns:
            List[BoltResource]: A list of BoltResource objects representing the artifacts.
        """

        query = """
            query boltCommand($commandId: Int!) {
                boltCommand(commandId: $commandId) {
                    resources {
                        id
                        path
                    }
                }
            }
        """

        response_json = self.client._call_gql(
            query=query, variables={"commandId": int(command_id)}
        )["boltCommand"]

        artifacts: List[BoltCommandArtifact] = []
        for artifact_json in response_json["resources"]:
            artifacts.append(
                BoltCommandArtifact(
                    id=artifact_json["id"],
                    path=artifact_json["path"],
                )
            )

        return artifacts

    def get_artifact_url(self, artifact_id: int) -> str:
        """
        Retrieves the URL of an artifact based on its ID.

        Args:
            artifact_id (int): The ID of the artifact.

        Returns:
            str: The URL of the artifact.
        """

        query = """
            query boltResourceUrl($resourceId: Int!) {
                boltResourceUrl(resourceId: $resourceId) {
                    url
                }
            }
        """

        response_json = self.client._call_gql(
            query=query, variables={"resourceId": int(artifact_id)}
        )["boltResourceUrl"]

        return response_json["url"]

    def cancel_run(self, run_id: int) -> None:
        """
        Cancels a Bolt run.

        Args:
            run_id (int): The ID of the run to cancel.

        Returns:
            None
        """

        query = """
            mutation CancelBoltRun($runId: Int!) {
                cancelBoltRun(scheduleRunId: $runId) {
                    ok
                }
            }
        """

        self.client._call_gql(query=query, variables={"runId": int(run_id)})

    def retry_run(self, run_id: int) -> int:
        """
        Retries a failed Bolt run by re-running only the failed commands.

        The first failed dbt command is substituted with `dbt retry` when supported,
        re-running just the failed models. Infrastructure commands (git clone, dbt deps)
        are skipped.

        Args:
            run_id (int): The ID of the failed run to retry.

        Returns:
            int: The ID of the newly created retry run.
        """

        query = """
            mutation RetryBoltRun($scheduleRunId: Int!) {
                retryBoltRun(scheduleRunId: $scheduleRunId) {
                    runId
                }
            }
        """

        response_json = self.client._call_gql(
            query=query, variables={"scheduleRunId": int(run_id)}
        )["retryBoltRun"]

        return response_json["runId"]

    def retry_run_all(self, run_id: int) -> int:
        """
        Retries a Bolt run by re-running ALL original commands.

        Every command from the original run is re-executed verbatim, except
        infrastructure commands (git clone, dbt deps).

        Args:
            run_id (int): The ID of the run to retry.

        Returns:
            int: The ID of the newly created retry run.
        """

        query = """
            mutation RetryAllBoltRun($scheduleRunId: Int!) {
                retryAllBoltRun(scheduleRunId: $scheduleRunId) {
                    runId
                }
            }
        """

        response_json = self.client._call_gql(
            query=query, variables={"scheduleRunId": int(run_id)}
        )["retryAllBoltRun"]

        return response_json["runId"]

    def retry_schedule_from_failure(
        self,
        schedule_name: Optional[str] = None,
        *,
        slug: Optional[str] = None,
    ) -> int:
        """
        Retries the latest failed run of a Bolt schedule, by slug.

        Resumes from the failed command of the most recent run of the given schedule,
        without needing to know its run ID.

        Args:
            schedule_name (Optional[str]): Deprecated alias for ``slug`` — carries a slug. Kept for backwards compatibility with older SDK callers. Exactly one of ``slug`` or ``schedule_name`` must be provided.
            slug (Optional[str]): The schedule slug returned by ``createBoltSchedule``. Preferred over ``schedule_name``.

        Returns:
            int: The ID of the newly created retry run.
        """

        resolved_slug = _resolve_slug_or_schedule_name(
            slug=slug, schedule_name=schedule_name, method="retry_schedule_from_failure"
        )

        query = """
            mutation RetryBoltRunFromFailure($slug: String) {
                retryBoltRunFromFailure(slug: $slug) {
                    runId
                }
            }
        """

        response_json = self.client._call_gql(query=query, variables={"slug": resolved_slug})[
            "retryBoltRunFromFailure"
        ]

        return response_json["runId"]

    def get_latest_artifact_url(
        self,
        *,
        artifact_path: str,
        slug: Optional[str] = None,
        schedule_name: Optional[str] = None,
        command_index: Optional[int] = None,
        max_runs: int = 50,
    ) -> str:
        """
        Retrieves the URL of the latest artifact for a given schedule.

        Args:
            artifact_path (str): The path of the artifact.
            slug (Optional[str]): The schedule slug returned by ``createBoltSchedule``. Preferred over ``schedule_name``.
            schedule_name (Optional[str]): Deprecated alias for ``slug`` — carries a slug. Kept for backwards compatibility with older SDK callers. Exactly one of ``slug`` or ``schedule_name`` must be provided.
            command_index (Optional[int]): The index of the command in the schedule. Defaults to searching through all commands from the last command to the first.
            max_runs (int): The maximum number of latest runs to search through. Defaults to 50.

        Returns:
            str: The URL of the latest artifact.
        """

        resolved_slug = _resolve_slug_or_schedule_name(
            slug=slug, schedule_name=schedule_name, method="get_latest_artifact_url"
        )

        # Get the latest runs for the schedule
        latest_runs = self.list_runs(slug=resolved_slug, offset=0, limit=max_runs).runs

        if not latest_runs:
            raise BoltScheduleLatestRunNotFoundException(
                f"No runs found for schedule {resolved_slug!r}."
            )

        # Search through runs until we find the artifact
        artifact_id = None

        for run in latest_runs:
            # Get all the commands for this run
            all_commands = self.list_run_commands(run.id)
            commands_to_look = all_commands[::-1]
            if command_index is not None:
                commands_to_look = [all_commands[command_index]]

            # Find the artifact in this run
            for command in commands_to_look:
                # Skip commands that did not complete successfully
                if command.return_code != 0:
                    continue

                # Find the artifact in this command
                artifacts = self.list_command_artifacts(command.id)
                for artifact in artifacts:
                    if artifact.path == artifact_path:
                        artifact_id = artifact.id
                        break
                if artifact_id is not None:
                    break

            # If we found the artifact, stop searching
            if artifact_id is not None:
                break

        if artifact_id is None:
            raise BoltScheduleArtifactNotFoundException(
                f"No artifact found for schedule {resolved_slug!r} in the latest {max_runs} runs."
            )

        # Get the URL of the artifact
        artifact_url = self.get_artifact_url(artifact_id)

        return artifact_url

    def get_latest_manifest_json(
        self,
        schedule_name: Optional[str] = None,
        command_index: Optional[int] = None,
        max_runs: int = 50,
        *,
        slug: Optional[str] = None,
    ) -> dict:
        """
        Retrieves the latest manifest JSON for a given schedule.

        Args:
            schedule_name (Optional[str]): Deprecated alias for ``slug`` — carries a slug. Kept for backwards compatibility with older SDK callers. Exactly one of ``slug`` or ``schedule_name`` must be provided.
            command_index (Optional[int]): The index of the command in the schedule. Defaults to None.
            max_runs (int): The maximum number of latest runs to search through. Defaults to 50.
            slug (Optional[str]): The schedule slug returned by ``createBoltSchedule``. Preferred over ``schedule_name``.

        Returns:
            dict: The content of the latest manifest JSON.
        """

        resolved_slug = _resolve_slug_or_schedule_name(
            slug=slug, schedule_name=schedule_name, method="get_latest_manifest_json"
        )

        manifest_url = self.get_latest_artifact_url(
            slug=resolved_slug,
            artifact_path="target/manifest.json",
            command_index=command_index,
            max_runs=max_runs,
        )

        return requests.get(manifest_url).json()

    def _get_latest_run_results_json(
        self,
        schedule_name: str,
        command_index: Optional[int] = None,
        merge: bool = False,
    ) -> dict:
        """
        Retrieves the latest run_results JSON for a given schedule.
        By default returns the first run_results.json found, consistent with other get_latest_* functions.
        When merge=True, collects run_results.json from multiple commands and merges them.

        Args:
            schedule_name (str): The name of the schedule.
            command_index (Optional[int]): The index of a specific command. If provided, only gets results from that command.
            merge (bool): If True, merge run_results from multiple commands. If False, return first found. Defaults to False.

        Returns:
            dict: The content of the latest run_results JSON file(s).
        """
        if command_index is not None:
            # Get run results from specific command
            run_results_url = self.get_latest_artifact_url(
                slug=schedule_name,
                artifact_path="target/run_results.json",
                command_index=command_index,
                max_runs=1,
            )
            return requests.get(run_results_url).json()

        # Get the latest runs for the schedule
        latest_runs = self.list_runs(slug=schedule_name, offset=0, limit=1).runs

        if not latest_runs:
            raise BoltScheduleLatestRunNotFoundException(
                f"No runs found for schedule {schedule_name!r}."
            )

        all_run_results = []

        # Search through runs until we find run_results.json files
        for run in latest_runs:
            # Get all commands for this run
            all_commands = self.list_run_commands(run.id)

            # Check each command for run_results.json (newest first)
            for command in reversed(all_commands):
                # Skip commands that did not complete successfully
                if command.return_code != 0:
                    continue

                # Skip commands that can't produce run_results.json
                if not self._is_relevant_dbt_command(command.command):
                    continue

                # Find run_results.json in this command
                artifacts = self.list_command_artifacts(command.id)
                for artifact in artifacts:
                    if artifact.path == "target/run_results.json":
                        artifact_url = self.get_artifact_url(artifact.id)
                        run_results = requests.get(artifact_url).json()
                        all_run_results.append(run_results)
                        if not merge:
                            # Return first found when merge=False (consistent behavior)
                            return run_results
                        break

            # If we found at least one run_results.json and not merging, we're done
            if all_run_results and not merge:
                break

        if not all_run_results:
            raise BoltScheduleArtifactNotFoundException(
                f"No run_results.json found for schedule {schedule_name!r} in the latest run."
            )

        # If only one run_results.json or merge=False, return it as-is
        if len(all_run_results) == 1 or not merge:
            return all_run_results[0]

        # Merge multiple run_results.json files when merge=True
        from paradime.apis.metadata.utils import merge_run_results

        return merge_run_results(all_run_results)

    def _get_latest_sources_json(
        self,
        schedule_name: str,
        command_index: Optional[int] = None,
        merge: bool = False,
    ) -> dict:
        """
        Retrieves the latest sources JSON for a given schedule.

        Args:
            schedule_name (str): The name of the schedule.
            command_index (Optional[int]): The index of a specific command. If provided, only gets results from that command.
            merge (bool): If True, merge sources from multiple commands. If False, return first found. Defaults to False.

        Returns:
            dict: The content of the latest sources JSON.
        """
        if command_index is not None:
            sources_url = self.get_latest_artifact_url(
                slug=schedule_name,
                artifact_path="target/sources.json",
                command_index=command_index,
                max_runs=1,
            )
            return requests.get(sources_url).json()

        latest_runs = self.list_runs(slug=schedule_name, offset=0, limit=1).runs

        if not latest_runs:
            raise BoltScheduleLatestRunNotFoundException(
                f"No runs found for schedule {schedule_name!r}."
            )

        all_sources = []

        for run in latest_runs:
            all_commands = self.list_run_commands(run.id)

            for command in reversed(all_commands):
                if command.return_code != 0:
                    continue

                if not self._is_relevant_dbt_command(command.command):
                    continue

                artifacts = self.list_command_artifacts(command.id)
                for artifact in artifacts:
                    if artifact.path == "target/sources.json":
                        artifact_url = self.get_artifact_url(artifact.id)
                        sources = requests.get(artifact_url).json()
                        all_sources.append(sources)
                        if not merge:
                            return sources
                        break

            if all_sources and not merge:
                break

        if not all_sources:
            raise BoltScheduleArtifactNotFoundException(
                f"No sources.json found for schedule {schedule_name!r} in the latest run."
            )

        if len(all_sources) == 1 or not merge:
            return all_sources[0]

        from paradime.apis.metadata.utils import merge_sources

        return merge_sources(all_sources)

    def _get_all_latest_artifacts(self, schedule_name: str, max_runs: int = 1) -> dict:
        """
        Retrieves all available artifacts (manifest, run_results, sources) for a given schedule.

        Args:
            schedule_name (str): The name of the schedule.
            max_runs (int): The maximum number of latest runs to search through. Defaults to 1.

        Returns:
            dict: Dictionary containing all available artifacts with keys: 'manifest', 'run_results', 'sources'.
        """
        artifacts = {}

        try:
            artifacts["manifest"] = self.get_latest_manifest_json(
                slug=schedule_name, max_runs=max_runs
            )
        except BoltScheduleArtifactNotFoundException:
            pass

        try:
            artifacts["run_results"] = self._get_latest_run_results_json(schedule_name, merge=True)
        except BoltScheduleArtifactNotFoundException:
            pass

        try:
            artifacts["sources"] = self._get_latest_sources_json(schedule_name, merge=True)
        except BoltScheduleArtifactNotFoundException:
            pass

        if not artifacts:
            raise BoltScheduleArtifactNotFoundException(
                f"No artifacts found for schedule {schedule_name!r}."
            )

        return artifacts

    def _is_relevant_dbt_command(self, command: str) -> bool:
        """
        Check if a command is a relevant dbt command that produces artifacts we care about.

        Args:
            command: The command string to check

        Returns:
            bool: True if the command is a relevant dbt command
        """
        command_lower = command.lower().strip()

        # Commands to include - these produce useful artifacts
        relevant_patterns = [
            "dbt run",  # Model execution
            "dbt test",  # Test execution
            "dbt build",  # Combined run + test
            "dbt source",  # Source freshness
            "dbt snapshot",  # Snapshot execution
            "dbt compile",  # Compilation (produces manifest)
            "dbt parse",  # Parsing (produces manifest)
        ]

        # Commands to exclude - these don't produce useful metadata
        exclude_patterns = [
            "git clone",  # Git operations
            "git checkout",
            "git pull",
            "dbt deps",  # Dependency installation
            "dbt clean",  # Cleanup operations
            "dbt debug",  # Debug info
            "pip install",  # Package installation
            "poetry install",
            "npm install",
        ]

        # Check exclusions first
        for exclude_pattern in exclude_patterns:
            if exclude_pattern in command_lower:
                return False

        # Check if it's a relevant dbt command
        for relevant_pattern in relevant_patterns:
            if relevant_pattern in command_lower:
                return True

        # Default to False for unknown commands
        return False
