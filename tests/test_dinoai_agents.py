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


class RecordingAPIClient:
    """Records the run query and answers it with ``run``."""

    def __init__(self, run: Dict[str, Any]) -> None:
        self.run = run
        self.query = ""
        self.variables: Dict[str, Any] = {}

    def _call_gql(self, query: str, variables: Dict[str, Any] = {}) -> Dict[str, Any]:
        self.query, self.variables = query, variables
        return {"dinoaiAgentRun": self.run}


RUN_WITH_STEPS = {
    "ok": True,
    "status": "RUNNING",
    "messages": [],
    "childSessionIds": [],
    "workspaceUid": "workspace-1",
    "steps": [
        {
            "index": 0,
            "role": "USER",
            "toolName": None,
            "toolInput": None,
            "content": "q",
            "truncated": False,
        },
        {
            "index": 1,
            "role": "TOOL",
            "toolName": "run_sql_query",
            "toolInput": '{"query": "select 1"}',
            "content": None,
            "truncated": False,
        },
    ],
    "startupSteps": [{"label": "Starting the agent", "done": True}],
}


def test_get_run_without_steps_sends_the_original_query() -> None:
    """Older backends reject unknown fields, so steps are only asked for on request."""
    api = RecordingAPIClient({**RUN_WITH_STEPS, "steps": None, "startupSteps": None})
    run = DinoaiAgentsClient(api).get_run(agent_session_id="session-1")  # type: ignore[arg-type]

    assert "steps" not in api.query and "startupSteps" not in api.query
    assert api.variables == {"id": "session-1"}
    assert run.steps is None and run.startup_steps is None


def test_get_run_with_steps_parses_steps_and_the_startup_checklist() -> None:
    api = RecordingAPIClient(RUN_WITH_STEPS)
    run = DinoaiAgentsClient(api).get_run(  # type: ignore[arg-type]
        agent_session_id="session-1", include_steps=True
    )

    assert "steps(after: $after, includeToolIo: $includeToolIo, maxChars: $maxChars)" in api.query
    # Unset options are left out, so the backend applies its own defaults.
    assert api.variables == {"id": "session-1", "includeToolIo": False}
    assert run.steps is not None and run.startup_steps is not None
    assert [(s.index, s.role, s.tool_name) for s in run.steps] == [
        (0, "USER", None),
        (1, "TOOL", "run_sql_query"),
    ]
    assert run.steps[1].tool_input == '{"query": "select 1"}'
    assert run.startup_steps[0].label == "Starting the agent" and run.startup_steps[0].done


def test_get_run_passes_the_step_options() -> None:
    api = RecordingAPIClient(RUN_WITH_STEPS)
    DinoaiAgentsClient(api).get_run(  # type: ignore[arg-type]
        agent_session_id="session-1",
        include_steps=True,
        include_tool_io=True,
        steps_after=4,
        max_chars=300,
    )

    assert api.variables == {"id": "session-1", "includeToolIo": True, "after": 4, "maxChars": 300}


def test_get_run_keeps_the_run_when_the_backend_could_not_read_steps() -> None:
    """The backend nulls only the field it failed on; status and messages still arrive."""
    api = RecordingAPIClient({**RUN_WITH_STEPS, "steps": None})
    run = DinoaiAgentsClient(api).get_run(  # type: ignore[arg-type]
        agent_session_id="session-1", include_steps=True
    )

    assert run.status == DinoaiAgentRunStatus.RUNNING
    assert run.steps is None
    assert run.startup_steps is not None


class TriggerRecorder:
    """Records the trigger mutation and answers it, with a warning if one was asked for."""

    def __init__(self, warning: Any = None) -> None:
        self.warning = warning
        self.query = ""
        self.variables: Dict[str, Any] = {}

    def _call_gql(self, query: str, variables: Dict[str, Any] = {}) -> Dict[str, Any]:
        if "triggerDinoaiAgentRun" not in query:
            return FakeAPIClient(["COMPLETED"])._call_gql(query, variables)
        self.query, self.variables = query, variables
        result = {"ok": True, "agentSessionId": "session-1", "status": "queued"}
        if "warning" in query:
            result["warning"] = self.warning
        return {"triggerDinoaiAgentRun": result}


def test_trigger_run_sends_the_model_family_and_returns_the_warning() -> None:
    api = TriggerRecorder(warning="Unknown model family 'fast'; the run uses the default.")

    result = DinoaiAgentsClient(api).trigger_run(  # type: ignore[arg-type]
        agent="analyst", message="hi", model_family="fast"
    )

    assert api.variables["modelFamily"] == "fast"
    assert "modelFamily: $modelFamily" in api.query
    assert result.warning == "Unknown model family 'fast'; the run uses the default."


def test_trigger_run_without_a_model_family_sends_the_same_query_as_before() -> None:
    """Older APIs have no modelFamily argument and no warning field."""
    api = TriggerRecorder()

    result = DinoaiAgentsClient(api).trigger_run(agent="analyst", message="hi")  # type: ignore[arg-type]

    assert "modelFamily" not in api.query and "modelFamily" not in api.variables
    assert "warning" not in api.query
    assert result.warning is None


def test_trigger_run_and_wait_passes_the_model_family_on() -> None:
    api = TriggerRecorder()

    DinoaiAgentsClient(api).trigger_run_and_wait(  # type: ignore[arg-type]
        message="hi", model_family="fast", timeout=5, poll_interval=0
    )

    assert api.variables["modelFamily"] == "fast"
