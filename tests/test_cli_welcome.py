import json
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner

from paradime.cli import cli as cli_module
from paradime.client import api_client as api_client_module


class FakeResponse:
    status_code = 200
    ok = True
    headers: dict = {}
    text = ""

    def raise_for_status(self) -> None:
        pass

    def json(self) -> Any:
        return {"data": {"triggerBoltRun": {"runId": 7}}}


@pytest.fixture(autouse=True)
def cli_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(cli_module, "get_credentials_path", lambda: tmp_path / "credentials")
    monkeypatch.setattr(cli_module, "check_for_new_version", lambda: None)
    monkeypatch.setenv("PARADIME_DISABLE_VERSION_CHECK", "1")
    monkeypatch.setenv("PARADIME_API_ENDPOINT", "https://api.example.test/api/v1/t/graphql")
    monkeypatch.setenv("PARADIME_API_SECRET", "prdm_wsp_secret")
    monkeypatch.setattr(api_client_module.requests, "post", lambda **kwargs: FakeResponse())


def test_the_welcome_panel_goes_to_stderr() -> None:
    result = CliRunner().invoke(cli_module.cli, ["bolt", "run", "daily", "--json"])

    assert result.exit_code == 0, result.output
    assert "Paradime CLI" in result.stderr
    assert "Paradime CLI" not in result.stdout


def test_json_output_on_stdout_is_only_json() -> None:
    result = CliRunner().invoke(cli_module.cli, ["bolt", "run", "daily", "--json"])

    assert json.loads(result.stdout) == {
        "run_id": 7,
        "url": "https://app.paradime.io/bolt/run_id/7",
    }
