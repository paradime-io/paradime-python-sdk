from typing import Any, List, Optional

import requests

from paradime.apis.bolt.exception import (
    BoltScheduleArtifactNotFoundException,
    BoltScheduleLatestRunNotFoundException,
)
from paradime.apis.bolt.types import (
    BoltCommand,
    BoltCommandArtifact,
    BoltDeferredSchedule,
    BoltRun,
    BoltRunGitInfo,
    BoltRunState,
    BoltSchedule,
    BoltScheduleInfo,
    BoltScheduleRuns,
    BoltSchedules,
)
from paradime.client.api_client import APIClient


class BoltClient:
    def __init__(self, client: APIClient):
        self.client = client

    def trigger_run(
        self,
        schedule_name: str,
        commands: Optional[List[str]] = None,
        branch: Optional[str] = None,
        pr_number: Optional[int] = None,
    ) -> int:
        """
        Triggers a run for a given schedule.

        Args:
            schedule_name (str): The name of the schedule to trigger the run for.
            commands (Optional[List[str]], optional): The list of commands to execute in the run. This will override the commands defined in the schedule. Defaults to None.
            branch (Optional[str], optional): The branch or commit hash to run the commands on. Defaults to None.
            pr_number (Optional[int], optional): The pull request number to associate with the run. Defaults to None.

        Returns:
            int: The ID of the triggered run.
        """

        query = """
            mutation triggerBoltRun($scheduleName: String!, $commands: [String!], $branch: String, $prNumber: Int) {
                triggerBoltRun(scheduleName: $scheduleName, commands: $commands, branch: $branch, prNumber: $prNumber){
                    runId
                }
            }
        """

        response_json = self.client._call_gql(
            query=query,
            variables={
                "scheduleName": schedule_name,
                "commands": commands,
                "branch": branch,
                "prNumber": pr_number,
            },
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

    def list_runs(
        self,
        *,
        schedule_name: str,
        offset: int = 0,
        limit: int = 50,
    ) -> BoltScheduleRuns:
        """
        Get a list of Bolt runs for a specific schedule. The list is paginated.

        Args:
            schedule_name (str): The name of the Bolt schedule. Must be exact schedule name.
            offset (int): The offset value for pagination. Default is 0. Must be >= 0.
            limit (int): The limit value for pagination. Default is 50. Must be between 1 and 1000.

        Returns:
            BoltScheduleRuns: An object containing the list of Bolt runs.

        Raises:
            ValueError: If offset < 0 or limit is not between 1 and 1000.
        """

        # Validate inputs
        if offset < 0:
            raise ValueError(f"offset must be >= 0, got {offset}")
        if limit < 1 or limit > 1000:
            raise ValueError(f"limit must be between 1 and 1000, got {limit}")

        query = """
            query listBoltRuns($scheduleName: String!, $offset: Int!, $limit: Int!) {
                listBoltRuns(scheduleName: $scheduleName, offset: $offset, limit: $limit) {
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
            variables={"scheduleName": schedule_name, "offset": offset, "limit": limit},
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
        self,
        *,
        schedule_name: str,
        artifact_path: str,
        command_index: Optional[int] = None,
        max_runs: int = 50,
    ) -> str:
        """
        Retrieves the URL of the latest artifact for a given schedule.

        Args:
            schedule_name (str): The name of the schedule.
            artifact_path (str): The path of the artifact.
            command_index (Optional[int]): The index of the command in the schedule. Defaults to searching through all commands from the last command to the first.
            max_runs (int): The maximum number of latest runs to search through. Defaults to 50.

        Returns:
            str: The URL of the latest artifact.
        """

        # Get the latest runs for the schedule
        latest_runs = self.list_runs(schedule_name=schedule_name, offset=0, limit=max_runs).runs

        if not latest_runs:
            raise BoltScheduleLatestRunNotFoundException(
                f"No runs found for schedule {schedule_name!r}."
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
                f"No artifact found for schedule {schedule_name!r} in the latest {max_runs} runs."
            )

        # Get the URL of the artifact
        artifact_url = self.get_artifact_url(artifact_id)

        return artifact_url

    def get_latest_manifest_json(
        self, schedule_name: str, command_index: Optional[int] = None, max_runs: int = 50
    ) -> dict:
        """
        Retrieves the latest manifest JSON for a given schedule.

        Args:
            schedule_name (str): The name of the schedule.
            command_index (Optional[int]): The index of the command in the schedule. Defaults to None.
            max_runs (int): The maximum number of latest runs to search through. Defaults to 50.

        Returns:
            dict: The content of the latest manifest JSON.
        """

        manifest_url = self.get_latest_artifact_url(
            schedule_name=schedule_name,
            artifact_path="target/manifest.json",
            command_index=command_index,
            max_runs=max_runs,
        )

        return requests.get(manifest_url).json()

    def get_latest_run_results_json(
        self, schedule_name: str, command_index: Optional[int] = None, max_runs: int = 1
    ) -> dict:
        """
        Retrieves and merges the latest run_results JSON from all relevant commands for a given schedule.
        This method will collect run_results.json from multiple commands (e.g., dbt run + dbt test)
        and merge them into a single comprehensive result.

        Args:
            schedule_name (str): The name of the schedule.
            command_index (Optional[int]): The index of a specific command. If provided, only gets results from that command.
            max_runs (int): The maximum number of latest runs to search through. Defaults to 50.

        Returns:
            dict: The merged content of the latest run_results JSON files.
        """
        if command_index is not None:
            # Get run results from specific command
            run_results_url = self.get_latest_artifact_url(
                schedule_name=schedule_name,
                artifact_path="target/run_results.json",
                command_index=command_index,
                max_runs=max_runs,
            )
            return requests.get(run_results_url).json()

        # Get the latest runs for the schedule
        latest_runs = self.list_runs(schedule_name=schedule_name, offset=0, limit=max_runs).runs

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

                # Find run_results.json in this command
                artifacts = self.list_command_artifacts(command.id)
                for artifact in artifacts:
                    if artifact.path == "target/run_results.json":
                        artifact_url = self.get_artifact_url(artifact.id)
                        run_results = requests.get(artifact_url).json()
                        all_run_results.append(run_results)
                        break

            # If we found at least one run_results.json, process them
            if all_run_results:
                break

        if not all_run_results:
            raise BoltScheduleArtifactNotFoundException(
                f"No run_results.json found for schedule {schedule_name!r} in the latest {max_runs} runs."
            )

        # If only one run_results.json, return it as-is
        if len(all_run_results) == 1:
            return all_run_results[0]

        # Merge multiple run_results.json files
        return self._merge_run_results(all_run_results)

    def get_latest_sources_json(
        self, schedule_name: str, command_index: Optional[int] = None, max_runs: int = 1
    ) -> dict:
        """
        Retrieves the latest sources JSON for a given schedule.

        Args:
            schedule_name (str): The name of the schedule.
            command_index (Optional[int]): The index of the command in the schedule. Defaults to None.
            max_runs (int): The maximum number of latest runs to search through. Defaults to 50.

        Returns:
            dict: The content of the latest sources JSON.
        """
        sources_url = self.get_latest_artifact_url(
            schedule_name=schedule_name,
            artifact_path="target/sources.json",
            command_index=command_index,
            max_runs=max_runs,
        )

        return requests.get(sources_url).json()

    def get_all_latest_artifacts_fast(self, schedule_name: str, max_runs: int = 1) -> dict:
        """
        Fast version: Retrieves artifacts by directly accessing them from the latest successful run.
        Uses concurrent downloads and connection pooling for better performance.

        Args:
            schedule_name (str): The name of the schedule.
            max_runs (int): The maximum number of latest runs to search through. Defaults to 1.

        Returns:
            dict: Dictionary containing all available artifacts with keys: 'manifest', 'run_results', 'sources'.
        """
        import concurrent.futures
        from typing import Any, Optional, Tuple

        import requests

        artifacts: dict[str, dict[str, Any]] = {}

        # Create a session for connection pooling
        session = requests.Session()

        def download_artifact(artifact_id: int, artifact_type: str) -> Tuple[str, Optional[dict]]:
            """Download a single artifact with retry logic"""
            import time

            for attempt in range(3):  # 3 retry attempts
                try:
                    url = self.get_artifact_url(artifact_id)
                    response = session.get(url, timeout=30)
                    if response.status_code == 200:
                        return artifact_type, response.json()
                    elif response.status_code >= 500 and attempt < 2:  # Retry on server errors
                        time.sleep(1 * (attempt + 1))  # Exponential backoff
                        continue
                    return artifact_type, None
                except Exception:
                    if attempt < 2:
                        time.sleep(1 * (attempt + 1))
                        continue
                    return artifact_type, None

            return artifact_type, None

        # Get latest run (success or failure) for complete monitoring
        runs_result = self.list_runs(schedule_name=schedule_name, limit=max_runs)

        for run in runs_result.runs:
            if run.state in ["SUCCESS", "ERROR", "FAILED"]:  # Exclude SKIPPED runs
                commands = self.list_run_commands(run.id)

                # Collect all artifact download tasks
                download_tasks = []

                for cmd in commands:
                    # Filter for dbt commands only (exclude git clone, deps, etc.)
                    if cmd.return_code is not None and self._is_relevant_dbt_command(cmd.command):
                        try:
                            cmd_artifacts = self.list_command_artifacts(cmd.id)

                            for artifact in cmd_artifacts:
                                if (
                                    artifact.path.endswith("manifest.json")
                                    and "manifest" not in artifacts
                                ):
                                    download_tasks.append((artifact.id, "manifest"))
                                elif artifact.path.endswith("run_results.json"):
                                    download_tasks.append((artifact.id, "run_results"))
                                elif (
                                    artifact.path.endswith("sources.json")
                                    and "sources" not in artifacts
                                ):
                                    download_tasks.append((artifact.id, "sources"))
                        except Exception:
                            continue

                # Download artifacts concurrently
                if download_tasks:
                    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
                        futures = [
                            executor.submit(download_artifact, artifact_id, artifact_type)
                            for artifact_id, artifact_type in download_tasks
                        ]

                        for future in concurrent.futures.as_completed(futures):
                            artifact_type, content = future.result()
                            if content:
                                if artifact_type == "run_results" and "run_results" in artifacts:
                                    # Merge run results
                                    artifacts["run_results"] = self._merge_run_results(
                                        [artifacts["run_results"], content]
                                    )
                                else:
                                    artifacts[artifact_type] = content

                # If we found artifacts, we're done
                if artifacts:
                    break

        session.close()

        if not artifacts:
            raise BoltScheduleArtifactNotFoundException(
                f"No artifacts found for schedule {schedule_name!r} in the latest {max_runs} runs."
            )

        return artifacts

    def get_all_latest_artifacts(self, schedule_name: str, max_runs: int = 1) -> dict:
        """
        Retrieves all available artifacts (manifest, run_results, sources) for a given schedule.
        This method intelligently collects artifacts from all relevant commands in the latest run.

        Args:
            schedule_name (str): The name of the schedule.
            max_runs (int): The maximum number of latest runs to search through. Defaults to 1.

        Returns:
            dict: Dictionary containing all available artifacts with keys: 'manifest', 'run_results', 'sources'.
        """
        # Use the fast version by default
        return self.get_all_latest_artifacts_fast(schedule_name, max_runs)

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

    def _merge_run_results(self, run_results_list: List[dict]) -> dict:
        """
        Merges multiple run_results.json files into a single comprehensive result.
        This is useful when a schedule has multiple commands that generate run_results.json
        (e.g., dbt run and dbt test).

        Args:
            run_results_list (List[dict]): List of run_results.json dictionaries to merge.

        Returns:
            dict: Merged run_results.json with all results combined.
        """
        if not run_results_list:
            return {}

        if len(run_results_list) == 1:
            return run_results_list[0]

        # Use the metadata from the most recent run_results
        merged = {
            "metadata": run_results_list[0].get("metadata", {}),
            "results": [],
            "elapsed_time": sum(r.get("elapsed_time", 0) for r in run_results_list),
            "args": run_results_list[0].get("args", {}),
        }

        # Collect all results
        seen_results: dict[str, dict[str, Any]] = {}
        for run_results in run_results_list:
            for result in run_results.get("results", []):
                unique_id = result.get("unique_id")
                if unique_id:
                    # If we've seen this result before, keep the one with latest timing
                    if unique_id in seen_results:
                        existing_result = seen_results[unique_id]
                        existing_timing = existing_result.get("timing", [])
                        new_timing = result.get("timing", [])

                        # Compare completed_at timestamps if available
                        if (
                            new_timing
                            and existing_timing
                            and len(new_timing) > 0
                            and len(existing_timing) > 0
                            and new_timing[-1].get("completed_at", "")
                            > existing_timing[-1].get("completed_at", "")
                        ):
                            seen_results[unique_id] = result
                    else:
                        seen_results[unique_id] = result

        merged["results"] = list(seen_results.values())
        return merged
