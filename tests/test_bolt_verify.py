"""Unit tests for the consolidated ``bolt verify`` logic.

These cover the parity additions made when the Theia editor's
``paradime schedule verify`` was consolidated onto the SDK:
- the command allow-list,
- Slack-ID verification threading,
- ``self_healing`` / ``env_overrides`` schedule fields,
- internal-mode transport selection.
"""

from typing import Dict, List, Optional, Tuple

import pytest

from paradime.core.bolt import internal_transport
from paradime.core.bolt.schedule import (
    ParadimeSchedules,
    get_allowed_commands,
    verify_single_schedule,
)


def _schedule(**overrides):
    base = {
        "name": "my-schedule",
        "schedule": "0 1 * * *",
        "environment": "production",
        "commands": ["dbt run"],
    }
    base.update(overrides)
    return ParadimeSchedules.parse_obj({"schedules": [base]}).schedules[0]


def test_allowed_command_passes():
    assert verify_single_schedule(_schedule(commands=["dbt run"])) is None


def test_disallowed_command_rejected():
    error = verify_single_schedule(_schedule(commands=["rm -rf /"]))
    assert error is not None
    assert "not an allowed command" in error


def test_allowed_commands_env_override(monkeypatch):
    monkeypatch.setenv("BOLT_ALLOWED_COMMANDS", '["dbt", "custom_tool"]')
    assert "custom_tool" in get_allowed_commands()
    assert verify_single_schedule(_schedule(commands=["custom_tool sync"])) is None


def test_allowed_commands_env_override_invalid_falls_back(monkeypatch):
    monkeypatch.setenv("BOLT_ALLOWED_COMMANDS", "not-json")
    assert verify_single_schedule(_schedule(commands=["python foo.py"])) is None


def test_slack_verifier_flags_unreachable_id():
    def verifier(ids: List[str]) -> Tuple[bool, Optional[Dict[str, bool]]]:
        return True, {ids[0]: False}

    error = verify_single_schedule(
        _schedule(slack_notify=["U123"]), slack_id_verifier=verifier
    )
    assert error is not None
    assert "not accessible" in error


def test_slack_verifier_passes_when_reachable():
    def verifier(ids: List[str]) -> Tuple[bool, Optional[Dict[str, bool]]]:
        return True, {ids[0]: True}

    assert (
        verify_single_schedule(_schedule(slack_notify=["U123"]), slack_id_verifier=verifier)
        is None
    )


def test_slack_check_skipped_without_verifier():
    # External mode passes no verifier; Slack IDs must not be validated.
    assert verify_single_schedule(_schedule(slack_notify=["U123"])) is None


def test_self_healing_requires_channel_in_notifications():
    with pytest.raises(Exception):
        _schedule(
            self_healing={"enabled": True, "slack_channel": "#alerts"},
            slack_notify=["#other"],
        )


def test_env_overrides_parse():
    schedule = _schedule(env_overrides=[{"key": "FOO", "value": "bar"}])
    assert schedule.env_overrides is not None
    assert schedule.env_overrides[0].key == "FOO"


@pytest.mark.parametrize(
    "value,expected",
    [("true", True), ("1", True), ("yes", True), ("", False), ("false", False)],
)
def test_is_internal_mode(monkeypatch, value, expected):
    monkeypatch.setenv("PARADIME_INTERNAL_MODE", value)
    assert internal_transport.is_internal_mode() is expected
