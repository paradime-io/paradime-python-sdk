import logging
import time
from datetime import datetime, timedelta
from typing import Any, Dict, Optional

from paradime.apis.dinoai_agents.exception import DinoaiAgentRunFailedException
from paradime.apis.dinoai_agents.types import (
    DinoaiAgentMessage,
    DinoaiAgentRun,
    DinoaiAgentRunStatus,
    DinoaiAgentStartupStep,
    DinoaiAgentStep,
    DinoaiAgentTriggerResult,
)
from paradime.client.api_client import APIClient

logging.basicConfig(format="%(asctime)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)


class DinoaiAgentsClient:
    def __init__(self, client: APIClient) -> None:
        self.client = client

    def trigger_run(
        self,
        *,
        agent: Optional[str] = None,
        message: Optional[str] = None,
        slack_channel: Optional[str] = None,
        slack_thread: Optional[str] = None,
        base_branch: Optional[str] = None,
        model_family: Optional[str] = None,
    ) -> DinoaiAgentTriggerResult:
        """
        Triggers a DinoAI programmable agent run.

        At least one of ``agent`` or ``message`` must be provided.

        Args:
            agent (str, optional): Name of the YAML-defined agent to load (matches the file
                name under ``.dinoai/agents/`` without the ``.yml`` extension).
            message (str, optional): Custom prompt appended to the agent's context. When
                only ``agent`` is provided the run starts with the agent's role/goal/backstory.
            slack_channel (str, optional): Override the Slack channel for this run
                (e.g. ``"#alerts"``).
            slack_thread (str, optional): Override the Slack thread timestamp for this run.
            base_branch (str, optional): Git branch, tag or commit SHA the agent checks out before creating its
                working branch. Defaults to the repository's default branch.
            model_family (str, optional): Slug of a model family (a model preset of the workspace)
                for this run. An unknown slug does not fail the run: it starts on the workspace's
                default family, and ``warning`` in the result says so.

        Returns:
            DinoaiAgentTriggerResult: Contains ``ok``, ``agent_session_id``, ``status`` and, when
                ``model_family`` is set, ``warning``.
        """
        if agent is None and message is None:
            raise ValueError("At least one of 'agent' or 'message' must be provided.")

        # modelFamily and warning are only in the request when a model family is set,
        # so that a call without one sends the same query as before.
        model_variable = "\n                $modelFamily: String" if model_family else ""
        model_argument = "\n                    modelFamily: $modelFamily" if model_family else ""
        warning_field = "\n                    warning" if model_family else ""
        query = f"""
            mutation TriggerDinoaiAgentRun(
                $agent: String
                $message: String
                $slack: DinoAiAgentSlackInput
                $baseBranch: String{model_variable}
            ) {{
                triggerDinoaiAgentRun(
                    agent: $agent
                    message: $message
                    slack: $slack
                    baseBranch: $baseBranch{model_argument}
                ) {{
                    ok
                    agentSessionId
                    status{warning_field}
                }}
            }}
        """

        slack: Optional[dict] = None
        if slack_channel is not None or slack_thread is not None:
            if slack_channel is None or slack_thread is None:
                raise ValueError("slack_channel and slack_thread must be provided together")
            slack = {"channel": slack_channel, "threadTs": slack_thread}

        variables = {
            "agent": agent,
            "message": message,
            "slack": slack,
            "baseBranch": base_branch,
        }
        if model_family:
            variables["modelFamily"] = model_family

        response = self.client._call_gql(query, variables)["triggerDinoaiAgentRun"]

        return DinoaiAgentTriggerResult(
            ok=response["ok"],
            agent_session_id=response["agentSessionId"],
            status=response["status"],
            warning=response.get("warning"),
        )

    def get_run(
        self,
        *,
        agent_session_id: str,
        include_steps: bool = False,
        include_tool_io: bool = False,
        steps_after: Optional[int] = None,
        max_chars: Optional[int] = None,
    ) -> DinoaiAgentRun:
        """
        Fetches the current state of a DinoAI agent run.

        ``messages`` holds only what the agent delivered. To show progress while
        the agent works, pass ``include_steps=True``: the run then also carries
        ``steps``, every step so far with the tool calls, as the session panel in
        the app shows them, and ``startup_steps``, the checklist while the agent
        pod boots. A backend without these fields rejects the query with a
        ``ParadimeAPIException`` that names the unknown field.

        Args:
            agent_session_id (str): The session ID returned by :meth:`trigger_run` or
                :meth:`send_message`.
            include_steps (bool): Also return ``steps`` and ``startup_steps``.
            include_tool_io (bool): With ``include_steps``, also return each tool's input
                and output. They can hold raw query results and file contents.
            steps_after (int, optional): With ``include_steps``, return only the steps with
                a greater ``index``.
            max_chars (int, optional): With ``include_steps``, cut each step's text at this
                length. Defaults to the backend's limit.

        Returns:
            DinoaiAgentRun: Contains ``ok``, ``status``, ``messages``, ``child_session_ids``,
                ``workspace_uid`` and, with ``include_steps``, ``steps`` and ``startup_steps``.
        """
        variables: Dict[str, Any] = {"id": agent_session_id}
        steps_query = ""
        variable_definitions = "$id: String!"
        if include_steps:
            variable_definitions += ", $after: Int, $includeToolIo: Boolean, $maxChars: Int"
            steps_query = """
                    steps(after: $after, includeToolIo: $includeToolIo, maxChars: $maxChars) {
                        index
                        role
                        toolName
                        toolInput
                        content
                        truncated
                    }
                    startupSteps {
                        label
                        done
                    }
            """
            variables["includeToolIo"] = include_tool_io
            # Left out when unset, so the backend applies its own default.
            if steps_after is not None:
                variables["after"] = steps_after
            if max_chars is not None:
                variables["maxChars"] = max_chars

        query = f"""
            query DinoaiAgentRun({variable_definitions}) {{
                dinoaiAgentRun(agentSessionId: $id) {{
                    ok
                    status
                    messages {{
                        ts
                        role
                        content
                    }}
                    childSessionIds
                    workspaceUid
                    {steps_query}
                }}
            }}
        """

        response = self.client._call_gql(query, variables)["dinoaiAgentRun"]

        steps = response.get("steps")
        startup_steps = response.get("startupSteps")
        return DinoaiAgentRun(
            ok=response["ok"],
            status=DinoaiAgentRunStatus(response["status"]),
            messages=[
                DinoaiAgentMessage(ts=m["ts"], role=m["role"], content=m["content"])
                for m in response["messages"]
            ],
            child_session_ids=response["childSessionIds"],
            workspace_uid=response.get("workspaceUid"),
            steps=(
                [
                    DinoaiAgentStep(
                        index=s["index"],
                        role=s["role"],
                        tool_name=s.get("toolName"),
                        tool_input=s.get("toolInput"),
                        content=s.get("content"),
                        truncated=s.get("truncated", False),
                    )
                    for s in steps
                ]
                if steps is not None
                else None
            ),
            startup_steps=(
                [DinoaiAgentStartupStep(label=s["label"], done=s["done"]) for s in startup_steps]
                if startup_steps is not None
                else None
            ),
        )

    def send_message(self, *, agent_session_id: str, message: str) -> DinoaiAgentTriggerResult:
        """
        Sends a follow-up message to an active DinoAI agent session.

        The agent pod stays alive for up to 24 hours since the last message. Follow-ups
        resume the same conversation with full context.

        Args:
            agent_session_id (str): The session ID of the running agent.
            message (str): The follow-up message to send.

        Returns:
            DinoaiAgentTriggerResult: Contains ``ok``, ``agent_session_id``, and ``status``.
        """
        query = """
            mutation SendDinoaiAgentMessage($id: String!, $message: String!) {
                sendDinoaiAgentMessage(agentSessionId: $id, message: $message) {
                    ok
                    agentSessionId
                    status
                }
            }
        """

        response = self.client._call_gql(query, {"id": agent_session_id, "message": message})[
            "sendDinoaiAgentMessage"
        ]

        return DinoaiAgentTriggerResult(
            ok=response["ok"],
            agent_session_id=response["agentSessionId"],
            status=response["status"],
        )

    def trigger_run_and_wait(
        self,
        *,
        agent: Optional[str] = None,
        message: Optional[str] = None,
        slack_channel: Optional[str] = None,
        slack_thread: Optional[str] = None,
        base_branch: Optional[str] = None,
        model_family: Optional[str] = None,
        timeout: int = 3600,
        poll_interval: int = 10,
    ) -> DinoaiAgentRun:
        """
        Triggers a DinoAI agent run and blocks until it completes or fails.

        Args:
            agent (str, optional): Name of the YAML-defined agent to load.
            message (str, optional): Custom prompt appended to the agent's context.
            slack_channel (str, optional): Override the Slack channel for this run.
            slack_thread (str, optional): Override the Slack thread timestamp for this run.
            base_branch (str, optional): Git branch, tag or commit SHA the agent checks out before creating its
                working branch. Defaults to the repository's default branch.
            model_family (str, optional): Slug of a model family for this run. See
                :meth:`trigger_run`.
            timeout (int): Maximum seconds to wait before raising ``TimeoutError``. Defaults to 3600.
            poll_interval (int): Seconds between status polls. Defaults to 10.

        Returns:
            DinoaiAgentRun: The final run state with all messages.

        Raises:
            DinoaiAgentRunFailedException: If the agent run ends without completing, i.e. with
                status ``FAILED``, ``STOPPED`` or ``EXPIRED``.
            TimeoutError: If the run does not complete within ``timeout`` seconds.
        """
        result = self.trigger_run(
            agent=agent,
            message=message,
            slack_channel=slack_channel,
            slack_thread=slack_thread,
            base_branch=base_branch,
            model_family=model_family,
        )

        logger.info(
            f"[STARTED] DinoAI agent run triggered. Session ID: {result.agent_session_id}."
            " Waiting for completion..."
        )

        start_time = datetime.now()
        while True:
            run = self.get_run(agent_session_id=result.agent_session_id)

            if run.status == DinoaiAgentRunStatus.COMPLETED:
                logger.info("[COMPLETED] DinoAI agent run finished successfully.")
                return run

            if run.status in (
                DinoaiAgentRunStatus.FAILED,
                DinoaiAgentRunStatus.STOPPED,
                DinoaiAgentRunStatus.EXPIRED,
            ):
                last_content = run.messages[-1].content if run.messages else "no messages"
                error_message = (
                    f"[ERROR] DinoAI agent run ended with status {run.status.value}."
                    f" Last message: {last_content}"
                )
                logger.info(error_message)
                raise DinoaiAgentRunFailedException(error_message)

            if datetime.now() - start_time > timedelta(seconds=timeout):
                raise TimeoutError(
                    f"[TIMEOUT] Timed out waiting for DinoAI agent run to complete."
                    f" Last status: {run.status}. Session ID: {result.agent_session_id}"
                )

            logger.info(f"[IN PROGRESS] DinoAI agent run status: {run.status}.")
            time.sleep(poll_interval)
