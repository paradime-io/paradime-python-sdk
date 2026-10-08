from contextlib import contextmanager
from typing import Any, Dict, Iterator, List

import pytest
from rich.console import Console

from paradime.apis.dinoai_agents.types import (
    DinoaiAgentMessage,
    DinoaiAgentRun,
    DinoaiAgentRunStatus,
    DinoaiAgentStartupStep,
    DinoaiAgentStep,
)
from paradime.cli import console as cli_console, dinoai as dinoai_cli
from paradime.client.api_exception import ParadimeAPIException

OLD_BACKEND = ParadimeAPIException(
    "Error: 400 - Cannot query field 'steps' on type 'DinoAiAgentRun'."
)


def _run(
    status: str,
    *,
    steps: Any = None,
    startup: Any = None,
    messages: Any = None,
) -> DinoaiAgentRun:
    return DinoaiAgentRun(
        ok=True,
        status=DinoaiAgentRunStatus(status),
        messages=messages or [],
        child_session_ids=[],
        workspace_uid="workspace-1",
        steps=steps,
        startup_steps=startup,
    )


def _tool(index: int, name: str, tool_input: Any = None) -> DinoaiAgentStep:
    return DinoaiAgentStep(index=index, role="TOOL", tool_name=name, tool_input=tool_input)


class FakeAgents:
    """Answers get_run from a script of runs, or raises for the steps query."""

    def __init__(self, runs: List[DinoaiAgentRun], steps_error: Any = None) -> None:
        self.runs = runs
        self.steps_error = steps_error
        self.calls: List[Dict[str, Any]] = []

    def get_run(self, **kwargs: Any) -> DinoaiAgentRun:
        self.calls.append(kwargs)
        if kwargs.get("include_steps") and self.steps_error:
            raise self.steps_error
        return self.runs[min(len(self.calls), len(self.runs)) - 1]


class FakeClient:
    def __init__(self, agents: FakeAgents) -> None:
        self.dinoai_agents = agents


class TestDescribeStep:
    def test_a_sql_call_shows_its_query_even_when_the_input_was_cut(self) -> None:
        cut = '{\n  "query": "SELECT race_year,\\n       COUNT(*) AS wins FROM `proj`.`dbt_prod`.`fct_f1__race_result` WHERE position'
        line = dinoai_cli._describe_step(_tool(1, "run_sql_query", cut))

        assert line is not None
        assert line.startswith("Ran a SQL query: SELECT race_year, COUNT(*) AS wins FROM")
        assert line.endswith(" …") and "\\n" not in line

    def test_known_unknown_and_hidden_tools(self) -> None:
        describe = dinoai_cli._describe_step

        assert describe(_tool(1, "search_catalog", '{"query": "race results"}')) == (
            "Searched the data catalog: race results"
        )
        assert describe(_tool(1, "get_mart_models")) == "Listed the mart models"
        assert describe(_tool(1, "list_all_bigquery_projects_and_datasets")) == (
            "List all bigquery projects and datasets"
        )
        assert describe(_tool(1, "send_api_message", '{"message": "hi"}')) is None

    def test_text_steps_are_left_to_the_messages(self) -> None:
        assert (
            dinoai_cli._describe_step(DinoaiAgentStep(index=0, role="AGENT", content="hi")) is None
        )
        assert dinoai_cli._describe_step(DinoaiAgentStep(index=0, role="USER", content="q")) is None


class TestStepFeed:
    def test_reads_steps_after_the_last_one_shown(self) -> None:
        agents = FakeAgents(
            [_run("RUNNING", steps=[_tool(0, "get_mart_models"), _tool(2, "run_sql_query")])]
        )
        feed = dinoai_cli._StepFeed()

        lines = feed.new_lines(feed.read(FakeClient(agents), "s1"))  # type: ignore[arg-type]
        feed.read(FakeClient(agents), "s1")  # type: ignore[arg-type]

        assert lines == ["Listed the mart models", "Ran a SQL query"]
        assert agents.calls[0]["steps_after"] is None
        assert agents.calls[1]["steps_after"] == 2
        assert feed.new_lines(agents.runs[0]) == []  # already shown

    def test_an_older_backend_falls_back_to_the_plain_run_once(self) -> None:
        agents = FakeAgents([_run("RUNNING")], steps_error=OLD_BACKEND)
        feed = dinoai_cli._StepFeed()

        feed.read(FakeClient(agents), "s1")  # type: ignore[arg-type]
        feed.read(FakeClient(agents), "s1")  # type: ignore[arg-type]

        assert [call.get("include_steps", False) for call in agents.calls] == [True, False, False]
        assert feed.supported is False

    def test_other_api_errors_still_raise(self) -> None:
        agents = FakeAgents([_run("RUNNING")], steps_error=ParadimeAPIException("Error: 401"))

        with pytest.raises(ParadimeAPIException):
            dinoai_cli._StepFeed().read(FakeClient(agents), "s1")  # type: ignore[arg-type]


class FakeStatus:
    def __init__(self, label: str) -> None:
        self.labels = [label]

    def update(self, label: str) -> None:
        self.labels.append(label)


def test_poll_prints_tool_calls_and_the_startup_step(monkeypatch: pytest.MonkeyPatch) -> None:
    recorded = Console(record=True, width=200)
    statuses: List[FakeStatus] = []

    @contextmanager
    def fake_spinner(label: str) -> Iterator[FakeStatus]:
        statuses.append(FakeStatus(label))
        yield statuses[-1]

    monkeypatch.setattr(cli_console, "console", recorded)
    monkeypatch.setattr(cli_console, "spinner", fake_spinner)
    monkeypatch.setattr(dinoai_cli.time, "sleep", lambda _: None)

    booting = [
        DinoaiAgentStartupStep(label="Starting the agent", done=True),
        DinoaiAgentStartupStep(label="Cloning your repository", done=False),
    ]
    agents = FakeAgents(
        [
            _run("QUEUED", steps=[], startup=booting),
            _run(
                "RUNNING",
                steps=[
                    DinoaiAgentStep(index=0, role="USER", content="q"),
                    _tool(1, "run_sql_query", '{"query": "select 1"}'),
                ],
            ),
            _run(
                "COMPLETED",
                steps=[],
                messages=[DinoaiAgentMessage(ts="10", role="agent", content="Mercedes won.")],
            ),
        ]
    )

    status = dinoai_cli._poll(
        FakeClient(agents),  # type: ignore[arg-type]
        session_id="s1",
        rendered=set(),
        steps=dinoai_cli._StepFeed(),
    )

    output = recorded.export_text()
    assert status == DinoaiAgentRunStatus.COMPLETED
    assert "↳ Ran a SQL query: select 1" in output
    assert output.index("Ran a SQL query") < output.index("Mercedes won.")
    assert any("Cloning your repository" in label for label in statuses[0].labels)


class TriggeringAgents(FakeAgents):
    """FakeAgents that also starts runs, and records how."""

    def __init__(self, runs: List[DinoaiAgentRun], warning: Any = None) -> None:
        super().__init__(runs)
        self.warning = warning
        self.triggered: List[Dict[str, Any]] = []

    def trigger_run(self, **kwargs: Any) -> Any:
        from paradime.apis.dinoai_agents.types import DinoaiAgentTriggerResult

        self.triggered.append(kwargs)
        return DinoaiAgentTriggerResult(
            ok=True, agent_session_id="s1", status="queued", warning=self.warning
        )


def test_a_new_session_gets_the_model_family_and_shows_its_warning(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    recorded = Console(record=True, width=200)
    monkeypatch.setattr(cli_console, "console", recorded)
    monkeypatch.setattr(dinoai_cli, "_poll", lambda *args, **kwargs: DinoaiAgentRunStatus.COMPLETED)
    agents = TriggeringAgents([_run("COMPLETED")], warning="Unknown model family 'fast'.")

    dinoai_cli._send(
        FakeClient(agents),  # type: ignore[arg-type]
        agent="analyst",
        message="hi",
        session_id=None,
        rendered=set(),
        steps=dinoai_cli._StepFeed(),
        model_family="fast",
    )

    assert agents.triggered == [{"agent": "analyst", "message": "hi", "model_family": "fast"}]
    assert "Unknown model family 'fast'." in recorded.export_text()


def test_the_model_family_cannot_change_an_existing_session() -> None:
    from click.testing import CliRunner

    result = CliRunner().invoke(
        dinoai_cli.dinoai, ["--session", "s1", "--model-family", "fast", "--message", "hi"]
    )

    assert result.exit_code == 2
    assert "--model-family applies when a new session starts" in result.output
