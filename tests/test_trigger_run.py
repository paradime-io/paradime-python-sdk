from typing import Any, Dict, Optional

import pytest

from paradime.apis.bolt.client import BoltClient
from paradime.apis.bolt.types import BoltCommandConfigInput


class FakeAPIClient:
    """Captures the GraphQL document and variables sent by the client."""

    def __init__(self) -> None:
        self.query: Optional[str] = None
        self.variables: Optional[Dict[str, Any]] = None

    def _call_gql(self, *, query: str, variables: Dict[str, Any]) -> Dict[str, Any]:
        self.query = query
        self.variables = variables
        return {"triggerBoltRun": {"runId": 42}}


def _bolt() -> BoltClient:
    return BoltClient(FakeAPIClient())  # type: ignore[arg-type]


def test_plain_trigger_request() -> None:
    bolt = _bolt()
    run_id = bolt.trigger_run(slug="my-schedule", commands=["dbt run"], branch="main")

    assert run_id == 42
    api = bolt.client
    assert api.variables == {  # type: ignore[union-attr]
        "slug": "my-schedule",
        "commands": ["dbt run"],
        "branch": "main",
        "prNumber": None,
        "reason": None,
        "commandConfigs": None,
        "continueOnError": None,
    }


def test_command_configs_serialized_to_camel_case() -> None:
    bolt = _bolt()
    bolt.trigger_run(
        slug="my-schedule",
        command_configs=[
            BoltCommandConfigInput(command="dbt seed"),
            BoltCommandConfigInput(command="dbt run", continue_on_error=True),
        ],
    )

    api = bolt.client
    assert api.variables["commandConfigs"] == [  # type: ignore[index]
        {"command": "dbt seed"},
        {"command": "dbt run", "continueOnError": True},
    ]


def test_command_configs_dict_escape_hatch() -> None:
    bolt = _bolt()
    bolt.trigger_run(
        slug="my-schedule",
        command_configs=[{"command": "dbt run", "continueOnError": False}],
    )

    api = bolt.client
    assert api.variables["commandConfigs"] == [  # type: ignore[index]
        {"command": "dbt run", "continueOnError": False}
    ]


def test_run_level_continue_on_error() -> None:
    bolt = _bolt()
    bolt.trigger_run(slug="my-schedule", continue_on_error=False)

    api = bolt.client
    assert api.variables["continueOnError"] is False  # type: ignore[index]


def test_commands_and_command_configs_are_mutually_exclusive() -> None:
    bolt = _bolt()
    with pytest.raises(ValueError, match="at most one of"):
        bolt.trigger_run(
            slug="my-schedule",
            commands=["dbt run"],
            command_configs=[BoltCommandConfigInput(command="dbt run")],
        )
