import json
from pathlib import Path
from typing import Any, Dict, List

import pytest
from click.testing import CliRunner

from paradime import Paradime
from paradime.apis.admin import transport as transport_module
from paradime.cli import cli as cli_module, login as login_module
from paradime.client import api_client as api_client_module
from paradime.client.api_client import WORKSPACE_SELECTION_HEADER, APIClient
from paradime.client.api_exception import ParadimeAPIException, ParadimeWorkspaceRequiredException

ENDPOINT = "https://api.example.test/api/v1/company-token/graphql"
COMPANY_SECRET = "prdm_cmp_secret"


class FakeResponse:
    def __init__(self, body: Dict[str, Any]) -> None:
        self.body = body
        self.status_code = 200
        self.ok = True
        self.headers: Dict[str, str] = {}
        self.text = json.dumps(body)

    def raise_for_status(self) -> None:
        pass

    def json(self) -> Any:
        return self.body


class FakePost:
    def __init__(self, data: Dict[str, Any]) -> None:
        self.data = data
        self.requests: List[Dict[str, Any]] = []

    def __call__(self, **kwargs: Any) -> FakeResponse:
        self.requests.append(kwargs)
        return FakeResponse({"data": self.data})


@pytest.fixture(autouse=True)
def no_version_check(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PARADIME_DISABLE_VERSION_CHECK", "1")


def test_a_company_token_builds_a_client_without_a_workspace() -> None:
    paradime = Paradime(api_secret=COMPANY_SECRET, api_endpoint=ENDPOINT)

    assert paradime.workspace_uid is None


def test_a_legacy_secret_still_needs_an_api_key() -> None:
    with pytest.raises(ValueError, match="api_key is required"):
        APIClient(api_secret="legacy-secret", api_endpoint=ENDPOINT)


def test_a_workspace_level_call_without_a_workspace_raises_before_sending(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    post = FakePost({})
    monkeypatch.setattr(api_client_module.requests, "post", post)
    paradime = Paradime(api_secret=COMPANY_SECRET, api_endpoint=ENDPOINT)

    with pytest.raises(ParadimeWorkspaceRequiredException) as exc_info:
        paradime.bolt.trigger_run(slug="daily")

    assert isinstance(exc_info.value, ParadimeAPIException)
    assert "workspace_uid" in str(exc_info.value)
    assert post.requests == []


def test_listing_workspaces_needs_no_workspace(monkeypatch: pytest.MonkeyPatch) -> None:
    post = FakePost({"listWorkspaces": {"workspaces": [{"uid": "w1", "name": "Analytics"}]}})
    monkeypatch.setattr(api_client_module.requests, "post", post)
    paradime = Paradime(api_secret=COMPANY_SECRET, api_endpoint=ENDPOINT)

    workspaces = paradime.workspaces.list_all()

    assert [workspace.uid for workspace in workspaces] == ["w1"]
    assert WORKSPACE_SELECTION_HEADER not in post.requests[0]["headers"]


def test_audit_logs_need_no_workspace(monkeypatch: pytest.MonkeyPatch) -> None:
    post = FakePost({"getAuditLogs": {"auditLogs": []}})
    monkeypatch.setattr(api_client_module.requests, "post", post)
    paradime = Paradime(api_secret=COMPANY_SECRET, api_endpoint=ENDPOINT)

    assert paradime.audit_log.get_all() == []
    assert WORKSPACE_SELECTION_HEADER not in post.requests[0]["headers"]


def test_listing_schedule_names_across_workspaces_needs_no_workspace(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    post = FakePost(
        {
            "listAllScheduleNames": {
                "ok": True,
                "schedules": [{"name": "daily", "workspaceName": "Analytics"}],
            }
        }
    )
    monkeypatch.setattr(api_client_module.requests, "post", post)
    paradime = Paradime(api_secret=COMPANY_SECRET, api_endpoint=ENDPOINT)

    assert paradime.bolt.list_all_schedule_names() == [("Analytics", "daily")]
    assert WORKSPACE_SELECTION_HEADER not in post.requests[0]["headers"]


def test_a_workspace_level_call_with_a_workspace_sends_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    post = FakePost({"triggerBoltRun": {"runId": 7}})
    monkeypatch.setattr(api_client_module.requests, "post", post)
    paradime = Paradime(api_secret=COMPANY_SECRET, api_endpoint=ENDPOINT, workspace_uid="w1")

    assert paradime.bolt.trigger_run(slug="daily") == 7
    assert post.requests[0]["headers"][WORKSPACE_SELECTION_HEADER] == "w1"


def test_admin_calls_take_their_workspace_per_call(monkeypatch: pytest.MonkeyPatch) -> None:
    post = FakePost({"workspace": {"uid": "w2", "name": "Analytics"}})
    monkeypatch.setattr(transport_module.requests, "post", post)
    paradime = Paradime(api_secret=COMPANY_SECRET, api_endpoint=ENDPOINT)

    paradime.admin.transport.execute("query q { x }", workspace_uid="w2")

    assert post.requests[0]["headers"][WORKSPACE_SELECTION_HEADER] == "w2"


def test_a_cli_command_without_a_workspace_explains_what_is_missing(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    post = FakePost({})
    monkeypatch.setattr(api_client_module.requests, "post", post)
    monkeypatch.setattr(cli_module, "get_credentials_path", lambda: tmp_path / "credentials")
    monkeypatch.setattr(cli_module, "check_for_new_version", lambda: None)
    monkeypatch.setenv("PARADIME_API_ENDPOINT", ENDPOINT)
    monkeypatch.setenv("PARADIME_API_SECRET", COMPANY_SECRET)
    monkeypatch.delenv("PARADIME_WORKSPACE_UID", raising=False)
    monkeypatch.delenv("WORKSPACE_UID", raising=False)

    result = CliRunner().invoke(cli_module.cli, ["bolt", "run", "daily", "--json"])

    assert result.exit_code == 1
    assert "PARADIME_WORKSPACE_UID" in json.loads(result.stdout.splitlines()[-1])["error"]
    assert post.requests == []


@pytest.mark.parametrize(
    "args",
    [
        ["catalog", "refresh"],
        ["bolt", "schedule", "unsuspend", "daily"],
        ["bolt", "schedule", "suspend", "daily"],
    ],
)
def test_workspace_commands_without_a_workspace_print_the_error_and_exit_1(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, args: List[str]
) -> None:
    post = _company_cli_env(monkeypatch, tmp_path)

    result = CliRunner().invoke(cli_module.cli, args)

    assert result.exit_code == 1
    assert isinstance(result.exception, SystemExit)
    assert "PARADIME_WORKSPACE_UID" in result.stderr
    assert "Traceback" not in result.output
    assert post.requests == []


def test_bolt_verify_without_a_workspace_fails_instead_of_skipping_checks(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    post = _company_cli_env(monkeypatch, tmp_path)
    schedules = tmp_path / "paradime_schedules.yml"
    schedules.write_text("schedules: []\n")

    result = CliRunner().invoke(cli_module.cli, ["bolt", "verify", "--path", str(schedules)])

    assert result.exit_code == 1
    assert "PARADIME_WORKSPACE_UID" in result.output
    assert "All schedules verified" not in result.output
    assert post.requests == []


def _company_cli_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> FakePost:
    post = FakePost({})
    monkeypatch.setattr(api_client_module.requests, "post", post)
    monkeypatch.setattr(cli_module, "get_credentials_path", lambda: tmp_path / "credentials")
    monkeypatch.setattr(cli_module, "check_for_new_version", lambda: None)
    monkeypatch.setenv("PARADIME_API_ENDPOINT", ENDPOINT)
    monkeypatch.setenv("PARADIME_API_SECRET", COMPANY_SECRET)
    monkeypatch.delenv("PARADIME_WORKSPACE_UID", raising=False)
    monkeypatch.delenv("WORKSPACE_UID", raising=False)
    return post


def test_login_lets_a_company_token_skip_the_workspace(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    credentials = tmp_path / "credentials"
    monkeypatch.setattr(login_module, "get_credentials_path", lambda: credentials)

    result = CliRunner().invoke(login_module.login, input=f"{COMPANY_SECRET}\n\n{ENDPOINT}\n")

    assert result.exit_code == 0, result.output
    assert credentials.read_text() == f"API_ENDPOINT={ENDPOINT}\nAPI_SECRET={COMPANY_SECRET}\n"
