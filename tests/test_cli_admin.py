from typing import Any, Dict, List, Optional

import pytest
from click.testing import CliRunner

from paradime.apis.admin.connections.types import WarehouseConnection
from paradime.apis.admin.environment_variables.types import SetEnvironmentVariablesResult
from paradime.apis.admin.exception import AdminValidationFailedException
from paradime.cli.admin import admin, common


class FakeEnvironmentVariables:
    def __init__(self, error: Optional[Exception] = None) -> None:
        self.error = error
        self.set_calls: List[Dict[str, Any]] = []
        self.revealed: List[str] = []

    def set(self, variables: Dict[str, str], **kwargs: Any) -> SetEnvironmentVariablesResult:
        if self.error:
            raise self.error
        self.set_calls.append({"variables": dict(variables), **kwargs})
        return SetEnvironmentVariablesResult(changed_names=sorted(variables), total_count=9)

    def reveal(self, name: str, **kwargs: Any) -> str:
        self.revealed.append(name)
        return "s3cret value"


class FakeConnections:
    def __init__(self) -> None:
        self.set_calls: List[Dict[str, Any]] = []

    def set(self, adapter: Any, **kwargs: Any) -> WarehouseConnection:
        self.set_calls.append({"adapter": adapter, **kwargs})
        return WarehouseConnection(
            scope="SCHEDULER", connection_id="default", type="SNOWFLAKE", environment_slug="prod"
        )


class FakeAdmin:
    def __init__(self, error: Optional[Exception] = None) -> None:
        self.environment_variables = FakeEnvironmentVariables(error)
        self.connections = FakeConnections()


@pytest.fixture
def fake_admin(monkeypatch: pytest.MonkeyPatch) -> FakeAdmin:
    fake = FakeAdmin()
    monkeypatch.setattr(common, "get_admin", lambda: fake)
    return fake


def _run(*args: str, input: Optional[str] = None) -> Any:
    return CliRunner().invoke(admin, list(args), input=input)


def test_set_reads_one_value_from_stdin(fake_admin: FakeAdmin) -> None:
    result = _run("env-vars", "set", "API_TOKEN", "--environment", "prod", input="s3cret\n")

    assert result.exit_code == 0, result.output
    assert fake_admin.environment_variables.set_calls == [
        {
            "variables": {"API_TOKEN": "s3cret"},
            "environment_slug": "prod",
            "replace": False,
            "workspace_uid": None,
        }
    ]
    assert "s3cret" not in result.output


def test_set_reads_one_value_from_a_file(fake_admin: FakeAdmin, tmp_path: Any) -> None:
    value_file = tmp_path / "token.txt"
    value_file.write_text("line one\nline two\n")

    result = _run("env-vars", "set", "API_TOKEN", "--file", str(value_file), "--workspace", "w2")

    assert result.exit_code == 0, result.output
    call = fake_admin.environment_variables.set_calls[0]
    assert call["variables"] == {"API_TOKEN": "line one\nline two"}
    assert call["workspace_uid"] == "w2"


def test_set_reads_name_value_lines_from_a_file(fake_admin: FakeAdmin, tmp_path: Any) -> None:
    env_file = tmp_path / "vars.env"
    env_file.write_text('A=1\nexport B="two words"\n# a comment line in the file\nC=\n')

    result = _run("env-vars", "set", "--file", str(env_file), "--replace", "--json")

    assert result.exit_code == 0, result.output
    call = fake_admin.environment_variables.set_calls[0]
    assert call["variables"] == {"A": "1", "B": "two words", "C": ""}
    assert call["replace"] is True
    assert '"changedNames": ["A", "B", "C"]' in result.output


def test_set_reads_name_value_lines_from_stdin(fake_admin: FakeAdmin) -> None:
    result = _run("env-vars", "set", input="A=1\nB=2\n")

    assert result.exit_code == 0, result.output
    assert fake_admin.environment_variables.set_calls[0]["variables"] == {"A": "1", "B": "2"}


def test_set_refuses_a_value_as_an_argument(fake_admin: FakeAdmin) -> None:
    result = _run("env-vars", "set", "API_TOKEN=s3cret", input="")

    assert result.exit_code == 2
    assert "never taken from the command line" in result.output
    assert fake_admin.environment_variables.set_calls == []


def test_set_has_no_value_option(fake_admin: FakeAdmin) -> None:
    result = _run("env-vars", "set", "API_TOKEN", "--value", "s3cret")

    assert result.exit_code == 2
    assert "No such option" in result.output
    assert fake_admin.environment_variables.set_calls == []


def test_set_refuses_an_extra_argument(fake_admin: FakeAdmin) -> None:
    result = _run("env-vars", "set", "API_TOKEN", "s3cret")

    assert result.exit_code == 2
    assert fake_admin.environment_variables.set_calls == []


def test_set_refuses_a_name_line_without_a_value(fake_admin: FakeAdmin) -> None:
    result = _run("env-vars", "set", input="A=1\nB\n")

    assert result.exit_code == 2
    assert "No value for B" in result.output


def test_set_refuses_an_invalid_name(fake_admin: FakeAdmin) -> None:
    result = _run("env-vars", "set", "1BAD", input="x")

    assert result.exit_code == 2
    assert "is not a variable name" in result.output


def test_set_refuses_an_interactive_stdin(
    fake_admin: FakeAdmin, monkeypatch: pytest.MonkeyPatch
) -> None:
    class Terminal:
        def isatty(self) -> bool:
            return True

    monkeypatch.setattr("click.get_text_stream", lambda name: Terminal())

    result = _run("env-vars", "set", "API_TOKEN")

    assert result.exit_code == 2
    assert "Pipe the value on stdin or pass --file" in result.output


def test_reveal_prints_the_value(fake_admin: FakeAdmin) -> None:
    result = _run("env-vars", "reveal", "API_TOKEN")

    assert result.exit_code == 0
    assert result.output == "s3cret value\n"


def test_an_admin_error_exits_non_zero_with_its_code_and_field(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    error = AdminValidationFailedException(
        "A value can't be empty.", field=["input", "variables", "0", "value"]
    )
    fake = FakeAdmin(error)
    monkeypatch.setattr(common, "get_admin", lambda: fake)
    printed: List[str] = []
    monkeypatch.setattr(common.console, "error", lambda message, **_: printed.append(message))

    result = _run("env-vars", "set", "A", input="\n")

    assert result.exit_code == 1
    assert printed == [
        "VALIDATION_FAILED: A value can't be empty. (field: input.variables.0.value)"
    ]


def test_connection_settings_come_from_the_config_file(
    fake_admin: FakeAdmin, tmp_path: Any
) -> None:
    config = tmp_path / "snowflake.yml"
    config.write_text("snowflake:\n  account: acme\n  schema: dbt\n  password: hunter2\n")

    result = _run(
        "connections",
        "set",
        "--scope",
        "scheduler",
        "--environment",
        "prod",
        "--config",
        str(config),
        "--threads",
        "8",
        "--json",
    )

    assert result.exit_code == 0, result.output
    call = fake_admin.connections.set_calls[0]
    assert call["adapter"].snowflake.password == "hunter2"
    assert call["adapter"].snowflake.schema_name == "dbt"
    assert call["threads"] == 8 and call["scope"].value == "SCHEDULER"
    assert "hunter2" not in result.output


def test_connection_config_rejects_unknown_settings(fake_admin: FakeAdmin) -> None:
    result = _run(
        "connections",
        "set",
        "--scope",
        "AGENT",
        "--environment",
        "agent",
        "--config",
        "-",
        input='{"snowflake": {"acount": "typo"}}',
    )

    assert result.exit_code == 2
    assert "acount" in result.output
    assert fake_admin.connections.set_calls == []


def test_connections_have_no_secret_options() -> None:
    result = _run("connections", "set", "--help")

    assert "--password" not in result.output
    assert "--config" in result.output


def test_set_sends_dollar_references_verbatim(
    fake_admin: FakeAdmin, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("PARADIME_API_SECRET", "prdm_cmp_leaked")

    result = _run(
        "env-vars",
        "set",
        input="A=${PARADIME_API_SECRET}\nB='${PARADIME_API_SECRET}'\nC=\"x${HOME}y\"\n",
    )

    assert result.exit_code == 0, result.output
    assert fake_admin.environment_variables.set_calls[0]["variables"] == {
        "A": "${PARADIME_API_SECRET}",
        "B": "${PARADIME_API_SECRET}",
        "C": "x${HOME}y",
    }


def test_set_refuses_a_line_it_cannot_parse(fake_admin: FakeAdmin) -> None:
    result = _run("env-vars", "set", input="A=1\nB = 'unclosed\nC=3\n")

    assert result.exit_code == 2
    assert "Line 2" in result.output
    assert fake_admin.environment_variables.set_calls == []
