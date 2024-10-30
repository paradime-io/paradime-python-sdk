from typing import List, Optional

import requests

from paradime.apis.bolt.exception import (
    BoltScheduleArtifactNotFoundException,
    BoltScheduleLatestRunNotFoundException,
)
from paradime.apis.bolt.types import (
    BoltCommand,
    BoltCommandArtifact,
    BoltDeferredSchedule,
    BoltRunState,
    BoltSchedule,
    BoltScheduleInfo,
    BoltSchedules,
)
from paradime.client.api_client import APIClient


class BoltClient:
    def __init__(self, client: APIClient):
        self.client = client

    def trigger_run(
        self, schedule_name: str, commands: Optional[List[str]] = None, branch: Optional[str] = None
    ) -> int:
        """
        Triggers a run for a given schedule.

        Args:
            schedule_name (str): The name of the schedule to trigger the run for.
            commands (Optional[List[str]], optional): The list of commands to execute in the run. This will override the commands defined in the schedule. Defaults to None.
            branch (Optional[str], optional): The branch or commit hash to run the commands on. Defaults to None.

        Returns:
            int: The ID of the triggered run.
        """

        query = """
            mutation triggerBoltRun($scheduleName: String!, $commands: [String!], $branch: String) {
                triggerBoltRun(scheduleName: $scheduleName, commands: $commands, branch: $branch){
                    runId
                }
            }
        """

        response_json = self.client._call_gql(
            query=query,
            variables={"scheduleName": schedule_name, "commands": commands, "branch": branch},
        )["triggerBoltRun"]

        return response_json["runId"]

    def suspend_schedule(self, *, schedule_name: str, suspend: bool) -> None:
        """
        Suspends a UI based schedule name.
        Args:
            schedule_name (str): The name of the schedule to suspend
            suspend (bool): True to suspend the schedule, False to unsuspend the schedule

        Note: This only works with schedule names created via the UI and not via YAML.
        """

        query = """
            mutation SuspendBoltSchedule($scheduleName: String!, $suspend: Boolean!) {
                suspendBoltSchedule(scheduleName: $scheduleName, suspend: $suspend) {
                    ok
                }
            }
        """

        self.client._call_gql(
            query=query, variables={"scheduleName": schedule_name, "suspend": suspend}
        )["suspendBoltSchedule"]

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
                )
            )

        return BoltSchedules(
            schedules=schedules,
            total_count=response_json["totalCount"],
        )

    def get_schedule(self, schedule_name: str) -> BoltScheduleInfo:
        """
        Retrieves information about a specific schedule.

        Args:
            schedule_name (str): The name of the schedule.

        Returns:
            BoltScheduleInfo: An object containing information about the schedule.
        """

        query = """
            query boltScheduleName($scheduleName: String!) {
                boltScheduleName(scheduleName: $scheduleName) {
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

        response_json = self.client._call_gql(
            query=query, variables={"scheduleName": schedule_name}
        )["boltScheduleName"]

        return BoltScheduleInfo(
            name=schedule_name,
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

    def get_latest_artifact_url(
        self, *, schedule_name: str, artifact_path: str, command_index: Optional[int] = None
    ) -> str:
        """
        Retrieves the URL of the latest artifact for a given schedule.

        Args:
            schedule_name (str): The name of the schedule.
            artifact_path (str): The path of the artifact.
            command_index (Optional[int]): The index of the command in the schedule. Defaults to searching through all commands from the last command to the first.

        Returns:
            str: The URL of the latest artifact.
        """

        # Get the latest run ID for the schedule
        schedule = self.get_schedule(schedule_name)
        latest_run_id = schedule.latest_run_id
        if not latest_run_id:
            raise BoltScheduleLatestRunNotFoundException(
                f"No latest run ID found for schedule {schedule_name!r}."
            )

        # Get all the commands for the schedule
        all_commands = self.list_run_commands(latest_run_id)
        commands_to_look = all_commands[::-1]
        if command_index is not None:
            commands_to_look = [all_commands[command_index]]

        # Find the artifact
        artifact_id = None
        for command in commands_to_look:
            artifacts = self.list_command_artifacts(command.id)
            for artifact in artifacts:
                if artifact.path == artifact_path:
                    artifact_id = artifact.id
                    break
        if artifact_id is None:
            raise BoltScheduleArtifactNotFoundException(
                f"No artifact found for schedule {schedule_name!r} and run id {latest_run_id}."
            )

        # Get the URL of the artifact
        artifact_url = self.get_artifact_url(artifact_id)

        return artifact_url

    def get_latest_manifest_json(
        self, schedule_name: str, command_index: Optional[int] = None
    ) -> dict:
        """
        Retrieves the latest manifest JSON for a given schedule.

        Args:
            schedule_name (str): The name of the schedule.
            command_index (Optional[int]): The index of the command in the schedule. Defaults to None.

        Returns:
            dict: The content of the latest manifest JSON.
        """

        manifest_url = self.get_latest_artifact_url(
            schedule_name=schedule_name,
            artifact_path="target/manifest.json",
            command_index=command_index,
        )

        return requests.get(manifest_url).json()
