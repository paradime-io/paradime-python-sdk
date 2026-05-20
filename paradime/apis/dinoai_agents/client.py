import logging
import time
from datetime import datetime, timedelta
from typing import Optional

from paradime.apis.dinoai_agents.exception import DinoaiAgentRunFailedException
from paradime.apis.dinoai_agents.types import (
    DinoaiAgentMessage,
    DinoaiAgentRun,
    DinoaiAgentRunStatus,
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

        Returns:
            DinoaiAgentTriggerResult: Contains ``ok``, ``agent_session_id``, and ``status``.
        """
        if agent is None and message is None:
            raise ValueError("At least one of 'agent' or 'message' must be provided.")

        query = """
            mutation TriggerDinoaiAgentRun(
                $agent: String
                $message: String
                $slack: DinoAiAgentSlackInput
            ) {
                triggerDinoaiAgentRun(agent: $agent, message: $message, slack: $slack) {
                    ok
                    agentSessionId
                    status
                }
            }
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
        }

        response = self.client._call_gql(query, variables)["triggerDinoaiAgentRun"]

        return DinoaiAgentTriggerResult(
            ok=response["ok"],
            agent_session_id=response["agentSessionId"],
            status=response["status"],
        )

    def get_run(self, *, agent_session_id: str) -> DinoaiAgentRun:
        """
        Fetches the current state of a DinoAI agent run.

        Args:
            agent_session_id (str): The session ID returned by :meth:`trigger_run` or
                :meth:`send_message`.

        Returns:
            DinoaiAgentRun: Contains ``ok``, ``status``, ``messages``, ``child_session_ids``,
                and ``workspace_uid``.
        """
        query = """
            query DinoaiAgentRun($id: String!) {
                dinoaiAgentRun(agentSessionId: $id) {
                    ok
                    status
                    messages {
                        ts
                        role
                        content
                    }
                    childSessionIds
                    workspaceUid
                }
            }
        """

        response = self.client._call_gql(query, {"id": agent_session_id})["dinoaiAgentRun"]

        return DinoaiAgentRun(
            ok=response["ok"],
            status=DinoaiAgentRunStatus(response["status"]),
            messages=[
                DinoaiAgentMessage(ts=m["ts"], role=m["role"], content=m["content"])
                for m in response["messages"]
            ],
            child_session_ids=response["childSessionIds"],
            workspace_uid=response.get("workspaceUid"),
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
            timeout (int): Maximum seconds to wait before raising ``TimeoutError``. Defaults to 3600.
            poll_interval (int): Seconds between status polls. Defaults to 10.

        Returns:
            DinoaiAgentRun: The final run state with all messages.

        Raises:
            DinoaiAgentRunFailedException: If the agent run finishes with status ``FAILED``.
            TimeoutError: If the run does not complete within ``timeout`` seconds.
        """
        result = self.trigger_run(
            agent=agent,
            message=message,
            slack_channel=slack_channel,
            slack_thread=slack_thread,
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

            if run.status == DinoaiAgentRunStatus.FAILED:
                last_content = run.messages[-1].content if run.messages else "no messages"
                error_message = f"[ERROR] DinoAI agent run failed. Last message: {last_content}"
                logger.info(error_message)
                raise DinoaiAgentRunFailedException(error_message)

            if datetime.now() - start_time > timedelta(seconds=timeout):
                raise TimeoutError(
                    f"[TIMEOUT] Timed out waiting for DinoAI agent run to complete."
                    f" Last status: {run.status}. Session ID: {result.agent_session_id}"
                )

            logger.info(f"[IN PROGRESS] DinoAI agent run status: {run.status}.")
            time.sleep(poll_interval)
