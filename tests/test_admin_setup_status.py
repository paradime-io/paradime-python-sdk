import json
from typing import Any, List

import pytest
from click.testing import CliRunner
from test_admin_clients import _admin

from paradime.apis.admin.setup_status.types import SetupStep, SetupStepStatus
from paradime.cli.admin import admin as admin_cli, common

READINESS = {"status": "DONE", "missing": [], "missingSuggested": ["AGENT_CONNECTION"]}
SETUP_STATUS = {
    "isComplete": False,
    "steps": [
        {"step": "GIT_REPOSITORY", "status": "DONE", "required": True},
        {
            "step": "GITHUB_APP",
            "status": "PENDING_USER_ACTION",
            "required": False,
            "installUrl": "https://app.example.test/install-github",
            "detail": "Install the GitHub App.",
        },
        {
            "step": "SCHEDULER_CONNECTION",
            "status": "MISSING",
            "required": True,
            "environmentSlug": "production",
            "detail": "No warehouse connection.",
        },
        {"step": "SOMETHING_NEW", "status": "DONE", "required": False},
    ],
    "products": {
        "codeIde": READINESS,
        "bolt": {"status": "MISSING", "missing": ["SCHEDULER_CONNECTION"], "missingSuggested": []},
        "agent": READINESS,
    },
}


def test_get_reads_the_checklist_for_one_workspace() -> None:
    admin, transport = _admin({"workspaceSetupStatus": SETUP_STATUS})

    status = admin.setup_status.get(workspace_uid="w1")

    assert status is not None and status.is_complete is False
    assert status.steps is not None
    assert [(s.step, s.status) for s in status.steps[:3]] == [
        (SetupStep.GIT_REPOSITORY, SetupStepStatus.DONE),
        (SetupStep.GITHUB_APP, SetupStepStatus.PENDING_USER_ACTION),
        (SetupStep.SCHEDULER_CONNECTION, SetupStepStatus.MISSING),
    ]
    assert status.steps[1].install_url == "https://app.example.test/install-github"
    assert status.steps[2].environment_slug == "production"
    assert status.steps[3].step == SetupStep.UNKNOWN
    assert status.products.bolt.missing == [SetupStep.SCHEDULER_CONNECTION]
    assert status.products.code_ide.missing_suggested == [SetupStep.AGENT_CONNECTION]
    call = transport.calls[0]
    assert "workspaceSetupStatus(workspaceUid: $workspaceUid)" in call.query
    assert call.variables == {"workspaceUid": "w1"} and call.workspace_uid == "w1"


def test_get_returns_none_when_the_server_returns_none() -> None:
    admin, _ = _admin({"workspaceSetupStatus": None})

    assert admin.setup_status.get() is None


@pytest.fixture
def setup_admin(monkeypatch: pytest.MonkeyPatch) -> Any:
    admin, _ = _admin({"workspaceSetupStatus": SETUP_STATUS})
    monkeypatch.setattr(common, "get_admin", lambda: admin)
    return admin


def test_cli_prints_every_row_with_the_install_url(
    setup_admin: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    tables: List[Any] = []
    monkeypatch.setattr(
        common.console, "table", lambda columns, rows, **_: tables.append((columns, list(rows)))
    )

    result = CliRunner().invoke(admin_cli, ["setup-status", "--workspace", "w1"])

    assert result.exit_code == 0, result.output
    columns, rows = tables[0]
    assert columns[:4] == ["Step", "Environment", "Status", "Required"]
    assert rows[1][:4] == ["GITHUB_APP", "", "PENDING_USER_ACTION", "no"]
    assert "https://app.example.test/install-github" in rows[1][4]
    assert rows[2][:3] == ["SCHEDULER_CONNECTION", "production", "MISSING"]
    assert "No warehouse connection." in rows[2][4]


def test_cli_json_prints_the_checklist(setup_admin: Any) -> None:
    result = CliRunner().invoke(admin_cli, ["setup-status", "--json"])

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["isComplete"] is False
    assert payload["steps"][1]["installUrl"] == "https://app.example.test/install-github"
