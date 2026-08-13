from typing import Any, Dict, List

import pytest

from paradime.apis.dinoai_agents.client import DinoaiAgentsClient
from paradime.apis.dinoai_agents.exception import DinoaiAgentRunFailedException
from paradime.apis.dinoai_agents.types import DinoaiAgentRunStatus


class FakeAPIClient:
    """Replays a canned run status for every ``dinoaiAgentRun`` query."""

    def __init__(self, statuses: List[str]) -> None:
        self.statuses = statuses
        self.calls = 0

    def _call_gql(self, query: str, variables: Dict[str, Any] = {}) -> Dict[str, Any]:
        if "triggerDinoaiAgentRun" in query:
            return {
                "triggerDinoaiAgentRun": {
                    "ok": True,
                    "agentSessionId": "session-1",
                    "status": "QUEUED",
                }
            }

        status = self.statuses[min(self.calls, len(self.statuses) - 1)]
        self.calls += 1
        return {
            "dinoaiAgentRun": {
                "ok": True,
                "status": status,
                "messages": [{"ts": "1", "role": "assistant", "content": "hello"}],
                "childSessionIds": [],
                "workspaceUid": "workspace-1",
            }
        }


def _dinoai(*statuses: str) -> DinoaiAgentsClient:
    return DinoaiAgentsClient(FakeAPIClient(list(statuses)))  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "status",
    ["QUEUED", "RUNNING", "COMPLETED", "FAILED", "EXPIRED", "STOPPED"],
)
def test_get_run_parses_known_statuses(status: str) -> None:
    run = _dinoai(status).get_run(agent_session_id="session-1")

    assert run.status == DinoaiAgentRunStatus(status)
    assert run.workspace_uid == "workspace-1"


def test_get_run_maps_unknown_status_to_unknown() -> None:
    """A status added by a newer backend must not blow up an older SDK."""
    run = _dinoai("SOMETHING_NEW").get_run(agent_session_id="session-1")

    assert run.status == DinoaiAgentRunStatus.UNKNOWN


def test_from_str() -> None:
    assert DinoaiAgentRunStatus.from_str("STOPPED") is DinoaiAgentRunStatus.STOPPED
    assert DinoaiAgentRunStatus.from_str("UNKNOWN") is DinoaiAgentRunStatus.UNKNOWN
    assert DinoaiAgentRunStatus.from_str("SOMETHING_NEW") is None


def test_trigger_run_and_wait_returns_completed_run() -> None:
    run = _dinoai("RUNNING", "COMPLETED").trigger_run_and_wait(
        message="hi", timeout=5, poll_interval=0
    )

    assert run.status == DinoaiAgentRunStatus.COMPLETED


@pytest.mark.parametrize("status", ["FAILED", "STOPPED", "EXPIRED"])
def test_trigger_run_and_wait_raises_on_unsuccessful_end(status: str) -> None:
    with pytest.raises(DinoaiAgentRunFailedException) as exc_info:
        _dinoai(status).trigger_run_and_wait(message="hi", timeout=5, poll_interval=0)

    assert status in str(exc_info.value)


def test_trigger_run_and_wait_times_out_on_unknown_status() -> None:
    with pytest.raises(TimeoutError):
        _dinoai("SOMETHING_NEW").trigger_run_and_wait(message="hi", timeout=0, poll_interval=0)


def test_trigger_run_requires_agent_or_message() -> None:
    with pytest.raises(ValueError):
        _dinoai("QUEUED").trigger_run()
