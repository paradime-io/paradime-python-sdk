import json
import sys
from typing import Any, Dict, List, Optional

import pytest
from click.testing import CliRunner

from paradime.apis.admin.connections.types import (
    WarehouseConnection,
    WarehouseConnectionTestResult,
    WarehouseType,
)
from paradime.apis.admin.environment_variables.types import SetEnvironmentVariablesResult
from paradime.apis.admin.exception import (
    AdminConflictException,
    AdminNotFoundException,
    AdminValidationFailedException,
)
from paradime.apis.admin.users.types import AdminUser, UserAccountType
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
        self.create_calls: List[Dict[str, Any]] = []
        self.update_calls: List[Dict[str, Any]] = []
        self.test_calls: List[Dict[str, Any]] = []
        self.error: Optional[Exception] = None

    def test(self, adapter: Any, **kwargs: Any) -> WarehouseConnectionTestResult:
        self.test_calls.append({"adapter": adapter, **kwargs})
        return WarehouseConnectionTestResult(type="SNOWFLAKE", tested_at="2026-11-05T10:00:00Z")

    def create(self, adapter: Any, **kwargs: Any) -> WarehouseConnection:
        self.create_calls.append({"adapter": adapter, **kwargs})
        return self._answer()

    def update(self, adapter: Any, **kwargs: Any) -> WarehouseConnection:
        self.update_calls.append({"adapter": adapter, **kwargs})
        return self._answer()

    def _answer(self) -> WarehouseConnection:
        if self.error:
            raise self.error
        return WarehouseConnection(
            scope="SCHEDULER",
            connection_id="default",
            type="SNOWFLAKE",
            environment={"slug": "prod", "name": "Production"},
        )

    def list(self, **kwargs: Any) -> List[WarehouseConnection]:
        return [
            self._answer(),
            WarehouseConnection(scope="COST", connection_id="cost", type="BIGQUERY"),
        ]


class FakeUsers:
    def __init__(self) -> None:
        self.error: Optional[Exception] = None
        self.calls: List[Dict[str, Any]] = []

    def _answer(self, method: str, user: Dict[str, Any], **kwargs: Any) -> AdminUser:
        self.calls.append({"method": method, **kwargs})
        if self.error:
            raise self.error
        return AdminUser.parse_obj(user)

    def create(self, email: str, account_type: Any, **kwargs: Any) -> AdminUser:
        invitee = {
            "email": email,
            "accountType": account_type.value,
            "status": "INVITED",
            "inviteStatus": "PENDING",
        }
        return self._answer("create", invitee, email=email, account_type=account_type, **kwargs)

    def update(self, uid: str, **kwargs: Any) -> AdminUser:
        member = {
            "email": "member@example.com",
            "uid": uid,
            "accountType": kwargs["account_type"].value,
            "status": "ACTIVE",
        }
        return self._answer("update", member, uid=uid, **kwargs)

    def deactivate(self, uid: str, **kwargs: Any) -> AdminUser:
        former = {
            "email": "member@example.com",
            "uid": uid,
            "accountType": "DEVELOPER",
            "status": "DEACTIVATED",
        }
        return self._answer("deactivate", former, uid=uid, **kwargs)


class FakeAdmin:
    def __init__(self, error: Optional[Exception] = None) -> None:
        self.environment_variables = FakeEnvironmentVariables(error)
        self.connections = FakeConnections()
        self.users = FakeUsers()


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

    result = _run("env-vars", "set", "--file", str(env_file), "--replace", "--yes", "--json")

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
        "create",
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
    call = fake_admin.connections.create_calls[0]
    assert call["adapter"].snowflake.password == "hunter2"
    assert call["adapter"].snowflake.schema_name == "dbt"
    assert call["threads"] == 8 and call["scope"].value == "SCHEDULER"
    assert "hunter2" not in result.output


def test_connection_config_rejects_unknown_settings(fake_admin: FakeAdmin) -> None:
    result = _run(
        "connections",
        "create",
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
    assert fake_admin.connections.create_calls == []


def test_a_profile_connection_config_takes_its_warehouse_type(fake_admin: FakeAdmin) -> None:
    result = _run(
        "connections",
        "create",
        "--scope",
        "SCHEDULER",
        "--environment",
        "prod",
        "--config",
        "-",
        input="profileConnection:\n  warehouseType: TRINO\n  schema: analytics\n  profile: 'x'\n",
    )

    assert result.exit_code == 0, result.output
    settings = fake_admin.connections.create_calls[0]["adapter"].profile_connection
    assert settings.warehouse_type == WarehouseType.TRINO
    assert settings.schema_name == "analytics"


def test_an_extension_connection_config_is_refused(fake_admin: FakeAdmin) -> None:
    result = _run(
        "connections",
        "create",
        "--scope",
        "SCHEDULER",
        "--environment",
        "prod",
        "--config",
        "-",
        input="extension:\n  dbtType: TRINO\n  schema: analytics\n",
    )

    assert result.exit_code == 2
    assert "extension" in result.output
    assert fake_admin.connections.create_calls == []


@pytest.mark.parametrize("command", ["create", "update", "test"])
def test_connections_have_no_secret_options(command: str) -> None:
    result = _run("connections", command, "--help")

    assert "--password" not in result.output
    assert "--config" in result.output


def test_connections_update_sends_only_what_was_given(fake_admin: FakeAdmin) -> None:
    result = _run(
        "connections",
        "update",
        "--scope",
        "AGENT",
        "--environment",
        "agent",
        "--connection-id",
        "second",
        "--config",
        "-",
        "--json",
        input="snowflake:\n  account: acme\n  database: analytics\n",
    )

    assert result.exit_code == 0, result.output
    assert fake_admin.connections.create_calls == []
    call = fake_admin.connections.update_calls[0]
    assert call["scope"].value == "AGENT" and call["environment_slug"] == "agent"
    assert call["connection_id"] == "second"
    assert call["name"] is None and call["target_name"] is None and call["threads"] is None
    assert call["adapter"].snowflake.database == "analytics"
    assert json.loads(result.output)["connectionId"] == "default"


def test_connections_set_is_gone() -> None:
    result = _run("connections", "set", "--scope", "COST", "--config", "-", input="snowflake: {}\n")

    assert result.exit_code == 2
    assert "No such command 'set'" in result.output


@pytest.mark.parametrize(
    "command, error",
    [
        (
            "create",
            AdminConflictException(
                "Connection default exists with other settings.",
                field=["input", "connectionId"],
            ),
        ),
        (
            "update",
            AdminNotFoundException("No connection default.", field=["input", "connectionId"]),
        ),
    ],
)
def test_connection_create_conflict_and_update_not_found_exit_with_the_error(
    fake_admin: FakeAdmin, command: str, error: Exception
) -> None:
    fake_admin.connections.error = error

    result = _run(
        "connections", command, "--scope", "COST", "--config", "-", input="snowflake: {}\n"
    )

    assert result.exit_code == 1
    assert " ".join(str(error).split()) in " ".join(result.output.split())


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


@pytest.mark.parametrize("scalar", ["00123456", "1e3", "0x1F", "5439"])
def test_connection_config_refuses_an_unquoted_number_for_a_text_setting(
    fake_admin: FakeAdmin, scalar: str
) -> None:
    result = _run(
        "connections",
        "create",
        "--scope",
        "SCHEDULER",
        "--environment",
        "prod",
        "--config",
        "-",
        input=f"redshift:\n  port: {scalar}\n  database: analytics\n",
    )

    assert result.exit_code == 2, result.output
    assert "redshift.port" in result.output and "Quote the value" in result.output
    assert fake_admin.connections.create_calls == []


def test_connection_config_keeps_a_quoted_number_as_written(fake_admin: FakeAdmin) -> None:
    result = _run(
        "connections",
        "create",
        "--scope",
        "SCHEDULER",
        "--environment",
        "prod",
        "--config",
        "-",
        input="snowflake:\n  account: '00123456'\n",
    )

    assert result.exit_code == 0, result.output
    assert fake_admin.connections.create_calls[0]["adapter"].snowflake.account == "00123456"


def test_set_refuses_an_empty_environment(fake_admin: FakeAdmin) -> None:
    result = _run("env-vars", "set", "--environment", "", "--replace", input="A=1\n")

    assert result.exit_code == 2
    assert "--environment" in result.output
    assert fake_admin.environment_variables.set_calls == []


def test_set_refuses_replace_with_a_single_name(fake_admin: FakeAdmin) -> None:
    result = _run("env-vars", "set", "API_TOKEN", "--replace", "--yes", input="s3cret\n")

    assert result.exit_code == 2
    assert "--replace" in result.output
    assert fake_admin.environment_variables.set_calls == []


def test_replace_from_stdin_needs_yes(fake_admin: FakeAdmin) -> None:
    refused = _run("env-vars", "set", "--replace", input="A=1\n")
    confirmed = _run("env-vars", "set", "--replace", "--yes", input="A=1\n")

    assert refused.exit_code == 2
    assert "--yes" in refused.output
    assert confirmed.exit_code == 0, confirmed.output
    assert [call["replace"] for call in fake_admin.environment_variables.set_calls] == [True]


def test_replace_from_a_file_asks_on_a_terminal(
    fake_admin: FakeAdmin, monkeypatch: pytest.MonkeyPatch, tmp_path: Any
) -> None:
    env_vars_cli = sys.modules["paradime.cli.admin.env_vars"]
    env_file = tmp_path / "vars.env"
    env_file.write_text("A=1\n")
    monkeypatch.setattr(env_vars_cli, "_stdin_is_terminal", lambda: True)

    declined = _run("env-vars", "set", "--file", str(env_file), "--replace", input="n\n")
    accepted = _run("env-vars", "set", "--file", str(env_file), "--replace", input="y\n")

    assert declined.exit_code == 1
    assert "Delete every variable" in declined.output
    assert accepted.exit_code == 0, accepted.output
    assert len(fake_admin.environment_variables.set_calls) == 1


def test_workspaces_get_exits_non_zero_when_there_is_no_workspace(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Workspaces:
        def get(self, uid: Optional[str]) -> None:
            return None

    class Admin:
        workspaces = Workspaces()

    monkeypatch.setattr(common, "get_admin", lambda: Admin())

    result = _run("workspaces", "get", "w-gone")

    assert result.exit_code == 1


@pytest.mark.parametrize("wait", [False, True])
def test_connect_deploy_key_json_prints_the_repository_and_the_job_together(
    monkeypatch: pytest.MonkeyPatch, wait: bool
) -> None:
    import json

    from paradime.apis.admin.git.types import ConnectGitRepositoryResult
    from paradime.apis.admin.jobs.types import AdminJob

    queued = AdminJob(id="job-1", kind="CONNECT_GIT_REPOSITORY", status="QUEUED")
    done = AdminJob.parse_obj(
        {
            "id": "job-1",
            "kind": "CONNECT_GIT_REPOSITORY",
            "status": "SUCCEEDED",
            "result": {
                "__typename": "GitRepositoryConnectResult",
                "uri": "git@x:y.git",
                "provider": "GITHUB",
            },
        }
    )

    class Git:
        def connect(self, connection: Any, **kwargs: Any) -> ConnectGitRepositoryResult:
            return ConnectGitRepositoryResult.parse_obj(
                {
                    "gitRepository": {"mode": "SSH_DEPLOY_KEY", "state": "CONNECTED"},
                    "job": queued.dict(by_alias=True),
                }
            )

    class Jobs:
        def wait(self, job_id: str, **kwargs: Any) -> AdminJob:
            return done

    class Admin:
        git = Git()
        jobs = Jobs()

    monkeypatch.setattr(common, "get_admin", lambda: Admin())
    args = ["git", "connect-deploy-key", "--provider", "github", "--uri", "git@x:y.git", "--json"]

    result = _run(*args, *(["--wait"] if wait else []))

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["gitRepository"]["state"] == "CONNECTED"
    assert payload["job"]["status"] == ("SUCCEEDED" if wait else "QUEUED")
    connected = {"uri": "git@x:y.git", "provider": "GITHUB", "dbtProjectDir": None}
    assert payload["job"]["result"] == (connected if wait else None)


def test_a_connection_error_prints_without_the_company_token(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import requests

    from paradime.apis.admin import transport as transport_module
    from paradime.apis.admin.client import AdminClient
    from paradime.client.api_client import APIClient

    token = "FAKECOMPANYTOKEN123"

    def unreachable(**kwargs: Any) -> None:
        raise requests.exceptions.ConnectionError(
            f"Max retries exceeded with url: /api/v1/{token}/graphql"
        )

    client = APIClient(
        api_secret="prdm_wsp_secret",
        api_endpoint=f"https://api.example.test/api/v1/{token}/graphql",
    )
    monkeypatch.setattr(transport_module.requests, "post", unreachable)
    monkeypatch.setattr(common, "get_admin", lambda: AdminClient(client))
    printed: List[str] = []
    monkeypatch.setattr(common.console, "error", lambda message, **_: printed.append(message))

    result = _run("workspaces", "list")

    assert result.exit_code == 1
    assert printed and "/api/v1/***/graphql" in printed[0]
    assert token not in printed[0] and token not in result.output
    assert result.exception is None or token not in repr(result.exception)


@pytest.mark.parametrize("scope", ["DEVELOPMENT_SHARED", "cost"])
def test_workspace_level_scopes_take_no_environment(fake_admin: FakeAdmin, scope: str) -> None:
    config = "snowflake:\n  account: acme\n"
    with_environment = _run(
        "connections",
        "create",
        "--scope",
        scope,
        "--environment",
        "prod",
        "--config",
        "-",
        input=config,
    )
    without = _run("connections", "create", "--scope", scope, "--config", "-", input=config)

    assert with_environment.exit_code == 2
    assert without.exit_code == 0, without.output
    call = fake_admin.connections.create_calls[0]
    assert call["scope"].value == scope.upper() and call["environment_slug"] is None


def test_environment_scopes_need_an_environment(fake_admin: FakeAdmin) -> None:
    result = _run(
        "connections", "create", "--scope", "SCHEDULER", "--config", "-", input="snowflake: {}\n"
    )

    assert result.exit_code == 2
    assert "--environment" in result.output
    assert fake_admin.connections.create_calls == []


@pytest.mark.parametrize("command", ["create", "update", "test"])
def test_connection_name_option_is_sent_as_name(fake_admin: FakeAdmin, command: str) -> None:
    result = _run(
        "connections",
        command,
        "--scope",
        "SCHEDULER",
        "--environment",
        "prod",
        "--name",
        "Dev",
        "--config",
        "-",
        input="snowflake:\n  account: acme\n",
    )

    assert result.exit_code == 0, result.output
    calls = getattr(fake_admin.connections, f"{command}_calls")
    assert len(calls) == 1 and calls[0]["name"] == "Dev"
    assert "connection_name" not in calls[0]


class FakeBolt:
    def __init__(self) -> None:
        self.calls: List[Dict[str, Any]] = []

    def _stored(self, slug: str, **settings: Any) -> Any:
        from paradime.apis.admin.bolt.types import BoltScheduleDefinition
        from paradime.apis.admin.types import EnvironmentLink

        return BoltScheduleDefinition(
            slug=slug,
            name=settings["name"],
            schedule=settings["schedule"],
            environment=EnvironmentLink(slug=settings["environment"]),
            commands=settings.get("commands"),
            is_suspended=bool(settings.get("suspended")),
        )

    def create_schedule(self, **kwargs: Any) -> Any:
        self.calls.append({"method": "create", **kwargs})
        settings = {key: value for key, value in kwargs.items() if key != "slug"}
        return self._stored(kwargs["slug"] or "minted", **settings)

    def update_schedule(self, slug: str, **kwargs: Any) -> Any:
        self.calls.append({"method": "update", "slug": slug, **kwargs})
        return self._stored(slug, **kwargs)

    def delete_schedule(self, slug: str, **kwargs: Any) -> str:
        self.calls.append({"method": "delete", "slug": slug, **kwargs})
        return slug

    def list_schedules(self, **kwargs: Any) -> Any:
        self.calls.append({"method": "list", **kwargs})
        return iter(
            [
                self._stored("nightly", name="Nightly", schedule="0 1 * * *", environment="prod"),
                self._stored("hourly", name="Hourly", schedule="0 * * * *", environment="prod"),
            ]
        )


@pytest.fixture
def fake_bolt(monkeypatch: pytest.MonkeyPatch) -> FakeBolt:
    bolt = FakeBolt()

    class Admin:
        pass

    admin_client = Admin()
    admin_client.bolt = bolt
    monkeypatch.setattr(common, "get_admin", lambda: admin_client)
    return bolt


def test_bolt_schedules_list_prints_every_schedule(fake_bolt: FakeBolt) -> None:
    import json

    result = _run("bolt", "schedules", "list", "--workspace", "w2", "--json")

    assert result.exit_code == 0, result.output
    listed = json.loads(result.output)
    assert [schedule["slug"] for schedule in listed] == ["nightly", "hourly"]
    assert listed[0]["environment"] == {"slug": "prod", "name": None}
    assert fake_bolt.calls == [{"method": "list", "workspace_uid": "w2"}]


def test_bolt_schedules_list_shows_the_environment_slug(fake_bolt: FakeBolt) -> None:
    result = _run("bolt", "schedules", "list")

    assert result.exit_code == 0, result.output
    assert "prod" in result.output and "slug=" not in result.output


def test_bolt_schedules_create_merges_the_config_with_the_options(
    fake_bolt: FakeBolt,
) -> None:
    from paradime.apis.bolt.types import BoltNotificationsInput

    config = """
name: Nightly
schedule: "0 1 * * *"
environment: production
commands: [dbt build]
continueOnError: true
notifications:
  slackNotifications:
    - channel: "#data"
      events: [FAILED]
"""

    result = _run(
        "bolt",
        "schedules",
        "create",
        "--slug",
        "nightly",
        "--config",
        "-",
        "--name",
        "Nightly build",
        "--suspended",
        input=config,
    )

    assert result.exit_code == 0, result.output
    call = fake_bolt.calls[0]
    notifications = call.pop("notifications")
    assert call == {
        "method": "create",
        "slug": "nightly",
        "workspace_uid": None,
        "name": "Nightly build",
        "schedule": "0 1 * * *",
        "environment": "production",
        "commands": ["dbt build"],
        "continue_on_error": True,
        "suspended": True,
    }
    assert isinstance(notifications, BoltNotificationsInput)
    assert notifications.slack_notifications[0].channel == "#data"


def test_bolt_schedules_update_sends_the_options(fake_bolt: FakeBolt) -> None:
    result = _run(
        "bolt",
        "schedules",
        "update",
        "nightly",
        "--name",
        "Nightly",
        "--schedule",
        "OFF",
        "--environment",
        "production",
        "--command",
        "dbt run",
        "--command",
        "dbt test",
        "--trigger-on-webhook",
        "--active",
    )

    assert result.exit_code == 0, result.output
    assert fake_bolt.calls == [
        {
            "method": "update",
            "slug": "nightly",
            "workspace_uid": None,
            "name": "Nightly",
            "schedule": "OFF",
            "environment": "production",
            "commands": ["dbt run", "dbt test"],
            "trigger_on_webhook": True,
            "suspended": False,
        }
    ]


def test_bolt_schedules_update_needs_the_required_settings(fake_bolt: FakeBolt) -> None:
    result = _run("bolt", "schedules", "update", "nightly", "--name", "Nightly")

    assert result.exit_code == 2
    assert "schedule, environment" in result.output
    assert fake_bolt.calls == []


def test_bolt_schedules_config_refuses_a_setting_the_api_does_not_take(
    fake_bolt: FakeBolt,
) -> None:
    config = "name: Nightly\nschedule: OFF\nenvironment: production\ncron: '0 1 * * *'\n"

    result = _run("bolt", "schedules", "create", "--config", "-", input=config)

    assert result.exit_code == 2
    assert "cron" in result.output
    assert fake_bolt.calls == []


@pytest.mark.parametrize("as_json", [False, True])
def test_bolt_schedules_delete_reports_the_deleted_slug(fake_bolt: FakeBolt, as_json: bool) -> None:
    import json

    args = ["bolt", "schedules", "delete", "nightly", "--yes"]
    result = _run(*args, *(["--json"] if as_json else []))

    assert result.exit_code == 0, result.output
    if as_json:
        assert json.loads(result.output) == {"deletedSlug": "nightly"}
    else:
        assert "Deleted schedule nightly." in result.output
    assert fake_bolt.calls == [{"method": "delete", "slug": "nightly", "workspace_uid": None}]


def test_bolt_schedules_config_refuses_a_nested_setting_the_api_does_not_take(
    fake_bolt: FakeBolt,
) -> None:
    config = """
name: Nightly
schedule: "OFF"
environment: production
commands: [dbt run]
notifications:
  teamsNotifications:
    - channel: alerts
      events: [FAILED]
      teamName: Data
self_healing:
  enabled: true
  msTeamsChanel: alerts
"""

    result = _run("bolt", "schedules", "create", "--config", "-", input=config)

    assert result.exit_code == 2
    output = " ".join(result.output.split())
    assert "notifications.teamsNotifications[0].teamName" in output
    assert "self_healing.msTeamsChanel" in output
    assert fake_bolt.calls == []


def test_bolt_schedules_config_sends_teams_settings_in_the_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from test_admin_clients import FakeTransport

    from paradime.apis.admin.client import AdminClient

    transport = FakeTransport(
        {"createBoltScheduleDefinition": {"schedule": {"slug": "nightly", "name": "Nightly"}}}
    )
    admin_client = AdminClient(client=None, transport=transport)
    monkeypatch.setattr(common, "get_admin", lambda: admin_client)
    config = """
name: Nightly
schedule: "OFF"
environment: production
commands: [dbt run]
notifications:
  teamsNotifications:
    - channel: alerts
      events: [FAILED]
      team: Data
selfHealing:
  enabled: true
  msTeamsTeam: Data
  msTeamsChannel: healing
"""

    result = _run("bolt", "schedules", "create", "--config", "-", input=config)

    assert result.exit_code == 0, result.output
    schedule = transport.calls[0].variables["input"]["schedule"]
    assert schedule["notifications"] == {
        "teamsNotifications": [{"channel": "alerts", "events": ["FAILED"], "team": "Data"}]
    }
    assert schedule["selfHealing"] == {
        "enabled": True,
        "msTeamsTeam": "Data",
        "msTeamsChannel": "healing",
    }


def test_bolt_schedules_command_option_replaces_the_config_command_configs(
    fake_bolt: FakeBolt,
) -> None:
    config = """
name: Nightly
schedule: "OFF"
environment: production
command_configs:
  - command: dbt build
    continue_on_error: true
"""

    result = _run(
        "bolt",
        "schedules",
        "update",
        "nightly",
        "--config",
        "-",
        "--command",
        "dbt run",
        input=config,
    )

    assert result.exit_code == 0, result.output
    call = fake_bolt.calls[0]
    assert call["commands"] == ["dbt run"]
    assert "command_configs" not in call


def test_bolt_schedules_config_command_configs_stay_without_a_command_option(
    fake_bolt: FakeBolt,
) -> None:
    from paradime.apis.bolt.types import BoltCommandConfigInput

    config = """
name: Nightly
schedule: "OFF"
environment: production
commandConfigs:
  - command: dbt build
    continueOnError: true
"""

    result = _run("bolt", "schedules", "create", "--config", "-", input=config)

    assert result.exit_code == 0, result.output
    call = fake_bolt.calls[0]
    assert call["command_configs"] == [
        BoltCommandConfigInput(command="dbt build", continue_on_error=True)
    ]
    assert "commands" not in call


def test_declining_the_schedule_delete_prompt_deletes_nothing(fake_bolt: FakeBolt) -> None:
    declined = _run("bolt", "schedules", "delete", "nightly", input="n\n")

    assert declined.exit_code == 1
    assert "Delete this schedule?" in declined.output
    assert fake_bolt.calls == []


def test_bolt_schedules_list_reads_past_the_first_page(monkeypatch: pytest.MonkeyPatch) -> None:
    import json

    from test_admin_clients import FakeTransport, _connection

    from paradime.apis.admin.client import AdminClient

    transport = FakeTransport(
        {"workspace": {"boltSchedules": _connection([{"slug": "a", "name": "A"}], "c1", True)}},
        {"workspace": {"boltSchedules": _connection([{"slug": "b", "name": "B"}], None, False)}},
    )
    admin_client = AdminClient(client=None, transport=transport)
    monkeypatch.setattr(common, "get_admin", lambda: admin_client)

    result = _run("bolt", "schedules", "list", "--json")

    assert result.exit_code == 0, result.output
    assert [schedule["slug"] for schedule in json.loads(result.output)] == ["a", "b"]
    assert [call.variables["after"] for call in transport.calls] == [None, "c1"]


def test_connections_list_shows_the_environment_slug(fake_admin: FakeAdmin) -> None:
    table = _run("connections", "list")
    as_json = _run("connections", "list", "--json")

    assert table.exit_code == 0, table.output
    assert "prod" in table.output and "Production" not in table.output
    assert as_json.exit_code == 0, as_json.output
    rows = json.loads(as_json.output)
    assert rows[0]["environment"] == {"slug": "prod", "name": "Production"}
    assert rows[1]["environment"] is None


@pytest.mark.parametrize("as_json", [False, True])
def test_jobs_wait_prints_the_typed_deploy_result(
    monkeypatch: pytest.MonkeyPatch, as_json: bool
) -> None:
    import json

    from paradime.apis.admin.jobs.types import AdminJob

    done = AdminJob.parse_obj(
        {
            "id": "job-1",
            "kind": "DEPLOY_BOLT_SCHEDULES",
            "status": "SUCCEEDED",
            "result": {
                "__typename": "DeployBoltSchedulesResult",
                "scheduleParseId": 7,
                "gitHash": "0f3c2a1",
                "schedules": {
                    "totalCount": 1,
                    "pageInfo": {"hasNextPage": False, "endCursor": "c1"},
                    "edges": [
                        {
                            "cursor": "c1",
                            "node": {
                                "slug": "daily_run",
                                "source": "YAML",
                                "failureReason": "no command",
                            },
                        }
                    ],
                },
            },
        }
    )

    class Jobs:
        def wait(self, job_id: str, **kwargs: Any) -> AdminJob:
            return done

    class Admin:
        jobs = Jobs()

    monkeypatch.setattr(common, "get_admin", lambda: Admin())

    result = _run("jobs", "wait", "job-1", *(["--json"] if as_json else []))

    assert result.exit_code == 0, result.output
    if as_json:
        payload = json.loads(result.output)
        assert payload["kind"] == "DEPLOY_BOLT_SCHEDULES"
        assert payload["result"]["gitHash"] == "0f3c2a1"
        assert payload["result"]["schedules"]["nodes"] == [
            {"slug": "daily_run", "source": "YAML", "failureReason": "no command"}
        ]
    else:
        assert "DEPLOY_BOLT_SCHEDULES" in result.output
        assert "result.git_hash: 0f3c2a1" in result.output
        assert "result.schedules: 1 of 1" in result.output
        assert "daily_run" in result.output and "no command" in result.output


def test_jobs_wait_says_a_truncated_deploy_result_shows_only_its_first_page(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from paradime.apis.admin.jobs.types import AdminJob

    done = AdminJob.parse_obj(
        {
            "id": "job-1",
            "kind": "DEPLOY_BOLT_SCHEDULES",
            "status": "SUCCEEDED",
            "result": {
                "__typename": "DeployBoltSchedulesResult",
                "schedules": {
                    "totalCount": 250,
                    "pageInfo": {"hasNextPage": True, "endCursor": "c1"},
                    "edges": [{"cursor": "c1", "node": {"slug": "daily_run", "source": "YAML"}}],
                },
            },
        }
    )

    class Jobs:
        def wait(self, job_id: str, **kwargs: Any) -> AdminJob:
            return done

    class Admin:
        jobs = Jobs()

    monkeypatch.setattr(common, "get_admin", lambda: Admin())

    result = _run("jobs", "wait", "job-1")

    assert result.exit_code == 0, result.output
    output = " ".join(result.output.split())
    assert "result.schedules: 1 of 250" in output
    assert "Showing the first 1 of 250 schedules." in output
    assert "paradime admin jobs deployed-schedules job-1" in output


def test_jobs_deployed_schedules_lists_every_schedule(monkeypatch: pytest.MonkeyPatch) -> None:
    import json

    from paradime.apis.admin.jobs.types import DeployedBoltSchedule

    calls: List[Dict[str, Any]] = []

    class Jobs:
        def deployed_schedules(self, job_id: str, **kwargs: Any) -> Any:
            calls.append({"job_id": job_id, **kwargs})
            return iter(
                [
                    DeployedBoltSchedule(slug="daily_run", source="YAML"),
                    DeployedBoltSchedule(slug="nightly", source="UI", failure_reason="bad cron"),
                ]
            )

    class Admin:
        jobs = Jobs()

    monkeypatch.setattr(common, "get_admin", lambda: Admin())

    result = _run("jobs", "deployed-schedules", "job-1", "--workspace", "w1", "--json")

    assert result.exit_code == 0, result.output
    assert json.loads(result.output) == [
        {"slug": "daily_run", "source": "YAML", "failureReason": None},
        {"slug": "nightly", "source": "UI", "failureReason": "bad cron"},
    ]
    assert calls == [{"job_id": "job-1", "workspace_uid": "w1"}]


def test_users_create_invites_the_email_with_the_role(fake_admin: FakeAdmin) -> None:
    result = _run(
        "users", "create", "new@example.com", "--account-type", "developer", "--workspace", "w2"
    )

    assert result.exit_code == 0, result.output
    assert fake_admin.users.calls == [
        {
            "method": "create",
            "email": "new@example.com",
            "account_type": UserAccountType.DEVELOPER,
            "workspace_uid": "w2",
        }
    ]
    assert "INVITED" in result.output and "PENDING" in result.output


def test_users_create_json_prints_the_invitee(fake_admin: FakeAdmin) -> None:
    result = _run("users", "create", "new@example.com", "--account-type", "ADMIN", "--json")

    assert result.exit_code == 0, result.output
    assert '"email": "new@example.com"' in result.output
    assert '"accountType": "ADMIN"' in result.output
    assert '"status": "INVITED"' in result.output


@pytest.mark.parametrize("command", ["create", "update"])
@pytest.mark.parametrize("account_type", ["UNKNOWN", "SANDBOX_USER", "sandbox_user"])
def test_users_refuse_a_role_the_api_never_gives(
    fake_admin: FakeAdmin, command: str, account_type: str
) -> None:
    target = "new@example.com" if command == "create" else "u1"
    result = _run("users", command, target, "--account-type", account_type)

    assert result.exit_code == 2
    assert fake_admin.users.calls == []


def test_users_update_sends_the_new_role(fake_admin: FakeAdmin) -> None:
    result = _run("users", "update", "u1", "--account-type", "ANALYST", "--json")

    assert result.exit_code == 0, result.output
    assert fake_admin.users.calls == [
        {
            "method": "update",
            "uid": "u1",
            "account_type": UserAccountType.ANALYST,
            "workspace_uid": None,
        }
    ]
    assert '"accountType": "ANALYST"' in result.output


def test_users_deactivate_asks_first(fake_admin: FakeAdmin) -> None:
    result = _run("users", "deactivate", "u1", input="n\n")

    assert result.exit_code == 1
    assert fake_admin.users.calls == []


def test_users_deactivate_with_yes(fake_admin: FakeAdmin) -> None:
    result = _run("users", "deactivate", "u1", "--yes", "--workspace", "w2")

    assert result.exit_code == 0, result.output
    assert fake_admin.users.calls == [{"method": "deactivate", "uid": "u1", "workspace_uid": "w2"}]
    assert "Deactivated user member@example.com (u1)." in result.output


def test_users_deactivate_json_prints_the_former_member(fake_admin: FakeAdmin) -> None:
    result = _run("users", "deactivate", "u1", "--yes", "--json")

    assert result.exit_code == 0, result.output
    assert '"status": "DEACTIVATED"' in result.output


@pytest.mark.parametrize(
    "args, error, printed",
    [
        (
            ["create", "new@example.com", "--account-type", "ADMIN"],
            AdminConflictException("new@example.com is already invited."),
            "CONFLICT: new@example.com is already invited.",
        ),
        (
            ["update", "u-missing", "--account-type", "ADMIN"],
            AdminNotFoundException("No user u-missing."),
            "NOT_FOUND: No user u-missing.",
        ),
        (
            ["deactivate", "u-missing", "--yes"],
            AdminNotFoundException("No user u-missing."),
            "NOT_FOUND: No user u-missing.",
        ),
    ],
)
def test_users_errors_exit_non_zero_with_their_code(
    fake_admin: FakeAdmin,
    monkeypatch: pytest.MonkeyPatch,
    args: List[str],
    error: Exception,
    printed: str,
) -> None:
    fake_admin.users.error = error
    messages: List[str] = []
    monkeypatch.setattr(common.console, "error", lambda message, **_: messages.append(message))

    result = _run("users", *args)

    assert result.exit_code == 1
    assert messages == [printed]
