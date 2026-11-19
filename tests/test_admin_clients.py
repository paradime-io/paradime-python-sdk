from datetime import datetime, timezone
from typing import Any, Collection, Dict, List, Optional

import pytest
from test_admin_transport import FakeResponse, _error, _transport

from paradime.apis.admin.client import AdminClient
from paradime.apis.admin.connections.types import (
    WAREHOUSE_CONNECTION_FIELDS,
    BigQueryConnectionSettings,
    ConnectionScope,
    DatabricksConnectionSettings,
    DuckDbConnectionSettings,
    FireboltConnectionSettings,
    ProfileConnectionInput,
    ProfileConnectionSettings,
    RedshiftConnectionSettings,
    SnowflakeConnectionInput,
    SnowflakeConnectionSettings,
    WarehouseAdapterInput,
    WarehouseType,
)
from paradime.apis.admin.environments.types import EnvironmentType
from paradime.apis.admin.exception import AdminConflictException, AdminNotFoundException, ErrorCode
from paradime.apis.admin.git.types import (
    DeployKeyConnectionInput,
    GitHubAppConnectionInput,
    GitProvider,
    GitRepository,
    GitRepositoryState,
)
from paradime.apis.admin.integrations.types import (
    AzureKeyVaultSettings,
    DatadogSettings,
    FivetranSettings,
    GcpSecretManagerSettings,
)
from paradime.apis.admin.jobs.types import AdminJobKind, AdminJobStatus
from paradime.apis.admin.notifications.types import SlackInstallation
from paradime.apis.admin.types import UNSET


class Call:
    def __init__(
        self,
        query: str,
        variables: Dict[str, Any],
        workspace_uid: Optional[str],
        linked_fields: Collection[str] = (),
    ):
        self.query = query
        self.variables = variables
        self.workspace_uid = workspace_uid
        self.linked_fields = frozenset(linked_fields)


class FakeTransport:
    """Answers each request with the next canned `data`, and records the requests."""

    def __init__(self, *responses: Dict[str, Any]) -> None:
        self.responses = list(responses)
        self.calls: List[Call] = []

    def execute(
        self,
        query: str,
        variables: Optional[Dict[str, Any]] = None,
        *,
        workspace_uid: Optional[str] = None,
        linked_fields: Collection[str] = (),
    ) -> Dict[str, Any]:
        self.calls.append(Call(query, variables or {}, workspace_uid, linked_fields))
        return self.responses.pop(0)


def _admin(*responses: Dict[str, Any]) -> "tuple[AdminClient, FakeTransport]":
    transport = FakeTransport(*responses)
    return AdminClient(client=None, transport=transport), transport


def _connection(nodes: List[Dict[str, Any]], end_cursor: Optional[str], has_next: bool) -> Any:
    return {
        "edges": [{"cursor": f"c-{i}", "node": node} for i, node in enumerate(nodes)],
        "pageInfo": {"endCursor": end_cursor, "hasNextPage": has_next},
        "totalCount": 3,
    }


WORKSPACE = {
    "uid": "w1",
    "name": "Analytics",
    "createdAt": "2026-10-01T12:00:00+00:00",
    "dbtVersion": "1.10",
    "supportedDbtVersions": ["1.9", "1.10"],
    "dbtPartialParsing": True,
    "defaultBranch": "main",
    "scheduleDeploymentBranch": None,
}


class TestWorkspaces:
    def test_list_pages_with_first_and_after(self) -> None:
        admin, transport = _admin(
            {"workspaces": _connection([WORKSPACE, {**WORKSPACE, "uid": "w2"}], "cur-2", True)},
            {"workspaces": _connection([{**WORKSPACE, "uid": "w3"}], "cur-3", False)},
        )

        workspaces = list(admin.workspaces.list(page_size=2))

        assert [w.uid for w in workspaces] == ["w1", "w2", "w3"]
        assert [call.variables for call in transport.calls] == [
            {"first": 2, "after": None},
            {"first": 2, "after": "cur-2"},
        ]
        assert "workspaces(first: $first, after: $after)" in transport.calls[0].query
        assert workspaces[0].created_at == datetime(2026, 10, 1, 12, tzinfo=timezone.utc)
        assert workspaces[0].supported_dbt_versions == ["1.9", "1.10"]

    def test_list_is_lazy(self) -> None:
        admin, transport = _admin()

        admin.workspaces.list()

        assert transport.calls == []

    def test_list_rejects_a_page_size_over_the_server_cap(self) -> None:
        admin, _ = _admin()

        with pytest.raises(ValueError):
            admin.workspaces.list(page_size=501)

    def test_update_sends_only_the_settings_given_and_null_for_none(self) -> None:
        admin, transport = _admin({"updateWorkspace": {"workspace": WORKSPACE}})

        admin.workspaces.update("w1", dbt_version="1.10", default_branch=None)

        assert transport.calls[0].variables == {
            "input": {"workspaceUid": "w1", "dbtVersion": "1.10", "defaultBranch": None}
        }
        assert "updateWorkspace(input: $input)" in transport.calls[0].query

    def test_create_leaves_out_an_unset_dbt_version(self) -> None:
        admin, transport = _admin({"createWorkspace": {"workspace": WORKSPACE}})

        workspace = admin.workspaces.create("Analytics")

        assert transport.calls[0].variables == {"input": {"name": "Analytics"}}
        assert workspace.name == "Analytics"

    def test_get_returns_none_when_the_server_returns_no_workspace(self) -> None:
        admin, _ = _admin({"workspace": None})

        assert admin.workspaces.get("w-gone") is None

    def test_get_and_delete(self) -> None:
        admin, transport = _admin(
            {"workspace": WORKSPACE},
            {"deleteWorkspace": {"workspace": {"uid": "w1", "name": "Analytics"}}},
        )

        workspace = admin.workspaces.get()
        assert workspace is not None and workspace.uid == "w1"
        assert admin.workspaces.delete("w1").dbt_version is None
        assert transport.calls[0].variables == {"workspaceUid": None}
        assert transport.calls[1].variables == {"input": {"workspaceUid": "w1"}}


class TestEnvironmentVariables:
    def test_list_reads_the_workspace_scope_through_the_workspace(self) -> None:
        admin, transport = _admin(
            {
                "workspace": {
                    "environmentVariables": _connection(
                        [{"name": "A", "isSet": True, "updatedAt": None}], None, False
                    )
                }
            }
        )

        variables = list(admin.environment_variables.list(workspace_uid="w9"))

        assert [v.name for v in variables] == ["A"]
        call = transport.calls[0]
        assert call.workspace_uid == "w9"
        assert call.variables["scope"] == "WORKSPACE"
        assert call.variables["workspaceUid"] == "w9"
        assert "environmentVariables(" in call.query

    def test_an_environment_slug_selects_the_environment_scope(self) -> None:
        admin, transport = _admin(
            {"setEnvironmentVariables": {"changedNames": ["A"], "variables": {"totalCount": 4}}}
        )

        result = admin.environment_variables.set(
            {"A": "1", "B": "2"}, environment_slug="production", replace=True
        )

        assert transport.calls[0].variables == {
            "input": {
                "scope": "ENVIRONMENT",
                "environmentSlug": "production",
                "variables": [{"name": "A", "value": "1"}, {"name": "B", "value": "2"}],
                "mode": "REPLACE",
            }
        }
        assert result.changed_names == ["A"] and result.total_count == 4

    def test_values_stay_out_of_repr(self) -> None:
        from paradime.apis.admin.environment_variables.types import EnvironmentVariableValueInput

        assert "s3cret" not in repr(EnvironmentVariableValueInput(name="A", value="s3cret"))

    def test_delete_rename_and_reveal(self) -> None:
        admin, transport = _admin(
            {"deleteEnvironmentVariables": {"deletedNames": ["A"]}},
            {"renameEnvironmentVariable": {"variable": {"name": "C", "isSet": True}}},
            {"revealEnvironmentVariable": {"name": "C", "value": "v"}},
        )

        assert admin.environment_variables.delete(["A", "Z"]) == ["A"]
        assert admin.environment_variables.rename("B", "C").name == "C"
        assert admin.environment_variables.reveal("C", workspace_uid="w2") == "v"
        assert transport.calls[0].variables == {
            "input": {"scope": "WORKSPACE", "names": ["A", "Z"]}
        }
        assert transport.calls[1].variables["input"]["newName"] == "C"
        assert transport.calls[2].workspace_uid == "w2"


ENVIRONMENT = {
    "slug": "production",
    "type": "SCHEDULER",
    "name": "Production",
    "isDefault": True,
    "branch": None,
    "dbtVersion": None,
    "createdAt": "2026-10-01T12:00:00Z",
    "deferEnvironment": None,
    "deferSchedule": None,
    "hasWarehouseConnection": True,
    "variableNames": ["A"],
}


class TestEnvironments:
    def test_list_filters_by_type(self) -> None:
        admin, transport = _admin(
            {"workspace": {"environments": _connection([ENVIRONMENT], None, False)}}
        )

        environments = list(admin.environments.list(type=EnvironmentType.SCHEDULER))

        assert environments[0].type == EnvironmentType.SCHEDULER
        assert transport.calls[0].variables["type"] == "SCHEDULER"

    def test_an_unknown_type_reads_as_unknown(self) -> None:
        admin, _ = _admin(
            {
                "workspace": {
                    "environments": _connection([{**ENVIRONMENT, "type": "NEW"}], None, False)
                }
            }
        )

        assert next(admin.environments.list()).type == EnvironmentType.UNKNOWN

    def test_mutations_send_one_input_and_read_the_environment(self) -> None:
        admin, transport = _admin(
            {"createEnvironment": {"environment": ENVIRONMENT}},
            {"updateEnvironment": {"environment": ENVIRONMENT}},
            {"setAgentEnvironmentDeferTarget": {"environment": ENVIRONMENT}},
            {"setAgentEnvironmentDeferTarget": {"environment": ENVIRONMENT}},
        )

        admin.environments.create(EnvironmentType.AGENT, "Agent", slug="agent")
        admin.environments.update("agent", branch=None, name=UNSET)
        admin.environments.set_agent_defer_target("agent", schedule_slug="nightly")
        admin.environments.set_agent_defer_target("agent")

        assert [call.variables["input"] for call in transport.calls] == [
            {"type": "AGENT", "name": "Agent", "slug": "agent"},
            {"slug": "agent", "branch": None},
            {"slug": "agent", "deferTo": {"scheduleSlug": "nightly"}},
            {"slug": "agent", "deferTo": None},
        ]
        assert "mutation adminCreateEnvironment($input: CreateEnvironmentInput!)" in (
            transport.calls[0].query
        )

    def test_defer_targets_read_as_linked_objects(self) -> None:
        agent = {
            **ENVIRONMENT,
            "slug": "agent",
            "type": "AGENT",
            "deferEnvironment": {"slug": "production", "name": "Production"},
            "deferSchedule": {"slug": "nightly", "name": "Nightly"},
        }
        admin, transport = _admin(
            {"workspace": {"environments": _connection([agent, ENVIRONMENT], None, False)}},
            {"setAgentEnvironmentDeferTarget": {"environment": agent}},
        )

        linked, unlinked = admin.environments.list()
        changed = admin.environments.set_agent_defer_target("agent", schedule_slug="nightly")

        assert linked.defer_environment is not None
        assert linked.defer_environment.name == "Production"
        assert linked.defer_environment_slug == "production"
        assert linked.defer_schedule_slug == "nightly"
        assert unlinked.defer_environment_slug is None and unlinked.defer_schedule_slug is None
        assert changed.defer_schedule_slug == "nightly"
        for call in transport.calls:
            assert "deferEnvironment {" in call.query and "deferSchedule {" in call.query
            assert call.linked_fields == {"deferEnvironment", "deferSchedule"}

    def test_defer_target_takes_one_target(self) -> None:
        admin, _ = _admin()

        with pytest.raises(ValueError):
            admin.environments.set_agent_defer_target("a", environment_slug="e", schedule_slug="s")


JOB = {
    "id": "job-1",
    "kind": "CONNECT_GIT_REPOSITORY",
    "status": "QUEUED",
    "failure": None,
    "result": None,
    "createdAt": "2026-11-05T10:00:00Z",
    "updatedAt": "2026-11-05T10:00:00Z",
}

REPOSITORY = {
    "mode": "SSH_DEPLOY_KEY",
    "state": "PENDING_VALIDATION",
    "provider": "GITHUB",
    "uri": "git@github.com:acme/analytics.git",
    "dbtProjectDir": None,
    "isVerified": False,
    "deployKey": {"publicKey": "ssh-ed25519 AAAA"},
    "githubApp": None,
    "gitlabApp": None,
    "githubAppInstallUrl": None,
}


class TestGit:
    def test_connect_wraps_the_connection_in_its_variant(self) -> None:
        admin, transport = _admin(
            {"connectGitRepository": {"gitRepository": REPOSITORY, "job": JOB}},
            {"connectGitRepository": {"gitRepository": REPOSITORY, "job": None}},
        )

        result = admin.git.connect(
            DeployKeyConnectionInput(
                provider=GitProvider.GITHUB,
                uri="git@github.com:acme/analytics.git",
                validate_repository=False,
            ),
            workspace_uid="w1",
        )
        admin.git.connect(GitHubAppConnectionInput(installation_id="1", repository_id="2"))

        assert transport.calls[0].variables == {
            "input": {
                "connection": {
                    "deployKey": {
                        "provider": "GITHUB",
                        "uri": "git@github.com:acme/analytics.git",
                        "validate": False,
                    }
                }
            }
        }
        assert transport.calls[1].variables == {
            "input": {"connection": {"githubApp": {"installationId": "1", "repositoryId": "2"}}}
        }
        assert result.git_repository.state == GitRepositoryState.PENDING_VALIDATION
        assert result.job is not None and result.job.status == AdminJobStatus.QUEUED

    def test_check_and_rotate(self) -> None:
        admin, transport = _admin(
            {"checkGitRepository": {"job": {**JOB, "kind": "CHECK_GIT_REPOSITORY"}}},
            {"rotateGitDeployKey": {"deployKey": {"publicKey": "ssh-ed25519 BBBB"}}},
            {"rotateGitDeployKey": {"deployKey": {"publicKey": "ssh-ed25519 CCCC"}}},
        )

        assert admin.git.check_repository("git@x:y.git").kind == AdminJobKind.CHECK_GIT_REPOSITORY
        admin.git.rotate_deploy_key()
        assert admin.git.rotate_deploy_key(force=True).public_key == "ssh-ed25519 CCCC"
        assert transport.calls[1].variables == {"input": {}}
        assert transport.calls[2].variables == {"input": {"force": True}}

    def test_get_repository_reads_it_through_the_workspace(self) -> None:
        admin, transport = _admin({"workspace": {"gitRepository": REPOSITORY}})

        repository = admin.git.get_repository()

        assert repository is not None and repository.deploy_key is not None
        assert repository.deploy_key.public_key == "ssh-ed25519 AAAA"
        assert "gitRepository {" in transport.calls[0].query


class TestConnections:
    def test_an_adapter_input_is_wrapped_and_unset_fields_left_out(self) -> None:
        admin, transport = _admin(
            {"testWarehouseConnection": {"type": "SNOWFLAKE", "testedAt": "2026-11-05T10:00:00Z"}}
        )

        result = admin.connections.test(
            SnowflakeConnectionInput(account="acme", password="hunter2", schema_name="dbt"),
            scope=ConnectionScope.SCHEDULER,
            environment_slug="production",
            threads=8,
        )

        assert transport.calls[0].variables == {
            "input": {
                "scope": "SCHEDULER",
                "environmentSlug": "production",
                "threads": 8,
                "adapter": {
                    "snowflake": {"account": "acme", "schema": "dbt", "password": "hunter2"}
                },
            }
        }
        assert result.type == WarehouseType.SNOWFLAKE

    def test_secrets_stay_out_of_repr(self) -> None:
        settings = SnowflakeConnectionInput(account="acme", password="hunter2", private_key="pk")

        assert "hunter2" not in repr(settings) and "pk'" not in repr(settings)

    def test_a_whole_adapter_block_is_sent_as_given(self) -> None:
        admin, transport = _admin(
            {"createWarehouseConnection": {"warehouseConnection": CONNECTION}},
        )

        admin.connections.create(
            WarehouseAdapterInput.parse_obj({"duckdb": {"connectionType": "MOTHERDUCK"}}),
            scope=ConnectionScope.AGENT,
            environment_slug="agent",
        )

        assert transport.calls[0].variables["input"]["adapter"] == {
            "duckdb": {"connectionType": "MOTHERDUCK"}
        }

    def test_list_parses_each_adapter_settings_type(self) -> None:
        admin, transport = _admin(
            {
                "workspace": {
                    "warehouseConnections": _connection(
                        [
                            CONNECTION,
                            {
                                **CONNECTION,
                                "type": "BIGQUERY",
                                "settings": {
                                    "__typename": "BigQueryConnectionSettings",
                                    "bigqueryConnectionType": "SERVICE_ACCOUNT",
                                    "hasServiceAccountJson": True,
                                },
                            },
                            {
                                **CONNECTION,
                                "type": "DUCKDB",
                                "settings": {
                                    "__typename": "DuckDbConnectionSettings",
                                    "duckdbConnectionType": "MOTHERDUCK",
                                    "schema": "main",
                                },
                            },
                        ],
                        None,
                        False,
                    )
                }
            }
        )

        snowflake, bigquery, duckdb = list(admin.connections.list(scope=ConnectionScope.SCHEDULER))

        assert isinstance(snowflake.settings, SnowflakeConnectionSettings)
        assert snowflake.settings.has_password is True
        assert snowflake.settings.schema_name == "dbt"
        assert isinstance(bigquery.settings, BigQueryConnectionSettings)
        assert bigquery.settings.connection_type is not None
        assert bigquery.settings.connection_type.value == "SERVICE_ACCOUNT"
        assert isinstance(duckdb.settings, DuckDbConnectionSettings)
        assert duckdb.settings.schema_name == "main"
        assert "... on SnowflakeConnectionSettings" in transport.calls[0].query
        assert transport.calls[0].variables["scope"] == "SCHEDULER"

    def test_a_profile_connection_is_sent_with_its_warehouse_type(self) -> None:
        admin, transport = _admin(
            {"createWarehouseConnection": {"warehouseConnection": CONNECTION}},
        )

        settings = ProfileConnectionInput(
            warehouse_type=WarehouseType.SQL_SERVER, schema_name="dbo", profile="dbt: {}"
        )
        admin.connections.create(settings, scope=ConnectionScope.SCHEDULER, environment_slug="prod")

        assert transport.calls[0].variables["input"]["adapter"] == {
            "profileConnection": {
                "warehouseType": "SQL_SERVER",
                "schema": "dbo",
                "profile": "dbt: {}",
            }
        }
        assert "dbt: {}" not in repr(settings)

    def test_list_reads_a_profile_connection_and_its_warehouse_type(self) -> None:
        profile_connection = {
            **CONNECTION,
            "type": "TRINO",
            "settings": {
                "__typename": "ProfileConnectionSettings",
                "warehouseType": "TRINO",
                "schema": "analytics",
                "hasProfile": True,
            },
        }
        admin, transport = _admin(
            {
                "workspace": {
                    "warehouseConnections": _connection(
                        [
                            profile_connection,
                            {
                                **profile_connection,
                                "settings": {
                                    **profile_connection["settings"],
                                    "warehouseType": "SOMETHING_NEW",
                                },
                            },
                        ],
                        None,
                        False,
                    )
                }
            }
        )

        trino, unknown = list(admin.connections.list(scope=ConnectionScope.SCHEDULER))

        assert isinstance(trino.settings, ProfileConnectionSettings)
        assert trino.settings.warehouse_type == WarehouseType.TRINO
        assert trino.settings.schema_name == "analytics"
        assert trino.settings.has_profile is True
        assert isinstance(unknown.settings, ProfileConnectionSettings)
        assert unknown.settings.warehouse_type == WarehouseType.UNKNOWN
        assert "... on ProfileConnectionSettings" in transport.calls[0].query
        assert "warehouseType" in transport.calls[0].query

    def test_the_environment_reads_as_a_linked_object(self) -> None:
        admin, transport = _admin(
            {
                "workspace": {
                    "warehouseConnections": _connection(
                        [CONNECTION, {**CONNECTION, "environment": None}], None, False
                    )
                }
            },
            {"createWarehouseConnection": {"warehouseConnection": CONNECTION}},
            {"updateWarehouseConnection": {"warehouseConnection": CONNECTION}},
            {"setDefaultWarehouseConnection": {"warehouseConnection": CONNECTION}},
        )

        linked, unreadable = admin.connections.list()
        admin.connections.create(
            SnowflakeConnectionInput(account="acme"),
            scope=ConnectionScope.SCHEDULER,
            environment_slug="production",
        )
        admin.connections.update(
            SnowflakeConnectionInput(account="acme"),
            scope=ConnectionScope.SCHEDULER,
            environment_slug="production",
        )
        admin.connections.set_default(
            "default", scope=ConnectionScope.SCHEDULER, environment_slug="production"
        )

        assert linked.environment is not None and linked.environment.name == "Production"
        assert linked.environment_slug == "production"
        assert unreadable.environment is None and unreadable.environment_slug is None
        for call in transport.calls:
            assert "environment {" in call.query
            assert call.linked_fields == {"environment"}

    @pytest.mark.parametrize("method", ["create", "update"])
    def test_create_and_update_send_their_own_document(self, method: str) -> None:
        mutation = f"{method}WarehouseConnection"
        admin, transport = _admin({mutation: {"warehouseConnection": CONNECTION}})

        saved = getattr(admin.connections, method)(
            SnowflakeConnectionInput(account="acme", password="hunter2"),
            scope=ConnectionScope.SCHEDULER,
            environment_slug="production",
            connection_id="second",
            name="Snowflake",
            target_name="prod",
            threads=8,
            workspace_uid="w2",
        )

        input_type = mutation[0].upper() + mutation[1:] + "Input"
        assert " ".join(transport.calls[0].query.split()) == " ".join(
            f"""
            mutation admin{input_type[:-5]}($input: {input_type}!) {{
                {mutation}(input: $input) {{
                    warehouseConnection {{
                        {WAREHOUSE_CONNECTION_FIELDS}
                    }}
                }}
            }}
            """.split()
        )
        assert transport.calls[0].variables == {
            "input": {
                "scope": "SCHEDULER",
                "environmentSlug": "production",
                "connectionId": "second",
                "name": "Snowflake",
                "targetName": "prod",
                "threads": 8,
                "adapter": {"snowflake": {"account": "acme", "password": "hunter2"}},
            }
        }
        assert transport.calls[0].workspace_uid == "w2"
        assert transport.calls[0].linked_fields == {"environment"}
        assert saved.connection_id == "default" and saved.name == "Snowflake"

    def test_update_leaves_out_what_keeps_its_stored_value(self) -> None:
        admin, transport = _admin(
            {"updateWarehouseConnection": {"warehouseConnection": CONNECTION}}
        )

        admin.connections.update(
            SnowflakeConnectionInput(account="acme", database="analytics"),
            scope=ConnectionScope.SCHEDULER,
            environment_slug="production",
        )

        assert transport.calls[0].variables == {
            "input": {
                "scope": "SCHEDULER",
                "environmentSlug": "production",
                "adapter": {"snowflake": {"account": "acme", "database": "analytics"}},
            }
        }

    def test_create_over_other_settings_raises_conflict(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        transport, post = _transport(
            monkeypatch,
            [],
            FakeResponse(
                _error(
                    "CONFLICT",
                    "Connection default exists with other settings.",
                    field=["input", "connectionId"],
                )
            ),
        )
        admin = AdminClient(client=None, transport=transport)

        with pytest.raises(AdminConflictException) as exc_info:
            admin.connections.create(
                SnowflakeConnectionInput(account="other"),
                scope=ConnectionScope.SCHEDULER,
                environment_slug="production",
            )

        assert exc_info.value.code == ErrorCode.CONFLICT
        assert exc_info.value.field == ["input", "connectionId"]
        assert len(post.requests) == 1

    def test_update_of_a_missing_connection_raises_not_found(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        transport, post = _transport(
            monkeypatch,
            [],
            FakeResponse(
                _error("NOT_FOUND", "No connection ghost.", field=["input", "connectionId"])
            ),
        )
        admin = AdminClient(client=None, transport=transport)

        with pytest.raises(AdminNotFoundException) as exc_info:
            admin.connections.update(
                SnowflakeConnectionInput(account="acme"),
                scope=ConnectionScope.SCHEDULER,
                environment_slug="production",
                connection_id="ghost",
            )

        assert exc_info.value.code == ErrorCode.NOT_FOUND
        assert exc_info.value.field == ["input", "connectionId"]
        assert len(post.requests) == 1

    def test_delete_and_set_default(self) -> None:
        admin, transport = _admin(
            {"deleteWarehouseConnection": {"deletedConnectionId": "default"}},
            {"setDefaultWarehouseConnection": {"warehouseConnection": CONNECTION}},
        )

        assert (
            admin.connections.delete(scope=ConnectionScope.AGENT, environment_slug="agent")
            == "default"
        )
        admin.connections.set_default(
            "second", scope=ConnectionScope.SCHEDULER, environment_slug="production"
        )

        assert transport.calls[0].variables == {
            "input": {"scope": "AGENT", "environmentSlug": "agent"}
        }
        assert transport.calls[1].variables == {
            "input": {
                "scope": "SCHEDULER",
                "environmentSlug": "production",
                "connectionId": "second",
            }
        }


CONNECTION = {
    "scope": "SCHEDULER",
    "environment": {"slug": "production", "name": "Production"},
    "connectionId": "default",
    "type": "SNOWFLAKE",
    "isDefault": True,
    "name": "Snowflake",
    "targetName": "prod",
    "threads": 8,
    "updatedAt": "2026-11-05T10:00:00Z",
    "settings": {
        "__typename": "SnowflakeConnectionSettings",
        "account": "acme",
        "schema": "dbt",
        "hasPassword": True,
    },
}


class TestUsage:
    def test_get_sends_the_period(self) -> None:
        admin, transport = _admin(
            {
                "usage": {
                    "from": "2026-10-01T00:00:00Z",
                    "to": "2026-11-01T00:00:00Z",
                    "dinoAi": {"credits": 120, "backgroundAgentCredits": 20},
                    "bolt": {"billedSeconds": 3600},
                }
            }
        )

        usage = admin.usage.get(datetime(2026, 10, 1), datetime(2026, 11, 1), workspace_uid="w1")

        assert usage is not None
        assert usage.dino_ai.credits == 120 and usage.bolt.billed_seconds == 3600
        assert usage.period_from == datetime(2026, 10, 1, tzinfo=timezone.utc)
        assert transport.calls[0].variables == {
            "period": {"from": "2026-10-01T00:00:00", "to": "2026-11-01T00:00:00"},
            "workspaceUid": "w1",
        }

    def test_list_workspaces_pages_the_nested_connection(self) -> None:
        row = {
            "workspace": {"uid": "w1", "name": "A"},
            "dinoAi": {"credits": 1, "backgroundAgentCredits": 0},
            "bolt": {"billedSeconds": 60},
        }
        admin, transport = _admin(
            {"usage": {"workspaces": _connection([row], "c1", True)}},
            {"usage": {"workspaces": _connection([row], "c2", False)}},
        )

        rows = list(admin.usage.list_workspaces(datetime(2026, 10, 1), datetime(2026, 11, 1)))

        assert len(rows) == 2
        assert transport.calls[1].variables["after"] == "c1"


@pytest.mark.parametrize(
    "call",
    [
        lambda admin: admin.environment_variables.set(
            {"A": "1"}, environment_slug="", replace=True
        ),
        lambda admin: admin.environment_variables.delete(["A"], environment_slug=""),
        lambda admin: admin.environment_variables.rename("A", "B", environment_slug=""),
        lambda admin: admin.environment_variables.reveal("A", environment_slug=""),
        lambda admin: next(admin.environment_variables.list(environment_slug="")),
    ],
)
def test_an_empty_environment_slug_is_refused_not_read_as_the_workspace(call: Any) -> None:
    admin, transport = _admin()

    with pytest.raises(ValueError, match="environment_slug"):
        call(admin)

    assert transport.calls == []


class TestDevelopmentAndCostConnections:
    def test_a_cost_connection_is_sent_without_an_environment(self) -> None:
        from paradime.apis.admin.connections.types import BigQueryConnectionInput

        cost = {
            **CONNECTION,
            "scope": "COST",
            "environment": None,
            "type": "BIGQUERY",
            "settings": {
                "__typename": "BigQueryConnectionSettings",
                "costGcsBucketName": "audit-logs",
                "costProjectIds": ["p1", "p2"],
                "hasServiceAccountJson": True,
            },
        }
        admin, transport = _admin({"createWarehouseConnection": {"warehouseConnection": cost}})

        saved = admin.connections.create(
            BigQueryConnectionInput(
                service_account_json="{}",
                cost_gcs_bucket_name="audit-logs",
                cost_project_ids=["p1", "p2"],
            ),
            scope=ConnectionScope.COST,
        )

        assert transport.calls[0].variables == {
            "input": {
                "scope": "COST",
                "adapter": {
                    "bigquery": {
                        "serviceAccountJson": "{}",
                        "costGcsBucketName": "audit-logs",
                        "costProjectIds": ["p1", "p2"],
                    }
                },
            }
        }
        assert saved.scope == ConnectionScope.COST and saved.environment_slug is None
        assert isinstance(saved.settings, BigQueryConnectionSettings)
        assert saved.settings.cost_project_ids == ["p1", "p2"]

    def test_development_shared_fields_are_sent_and_read(self) -> None:
        from paradime.apis.admin.connections.types import BigQueryConnectionInput

        dev = {
            **CONNECTION,
            "scope": "DEVELOPMENT_SHARED",
            "environment": None,
            "settings": {
                "__typename": "SnowflakeConnectionSettings",
                "account": "acme",
                "useSso": True,
                "clientId": "cid",
                "hasClientSecret": True,
            },
        }
        admin, transport = _admin(
            {"createWarehouseConnection": {"warehouseConnection": dev}},
            {"createWarehouseConnection": {"warehouseConnection": dev}},
        )

        saved = admin.connections.create(
            SnowflakeConnectionInput(
                account="acme", use_sso=True, client_id="cid", client_secret="cs"
            ),
            scope=ConnectionScope.DEVELOPMENT_SHARED,
        )
        admin.connections.create(
            BigQueryConnectionInput(project_id="proj", client_id="cid", client_secret="cs"),
            scope=ConnectionScope.DEVELOPMENT_SHARED,
        )

        assert transport.calls[0].variables["input"]["adapter"]["snowflake"] == {
            "account": "acme",
            "useSso": True,
            "clientId": "cid",
            "clientSecret": "cs",
        }
        assert transport.calls[1].variables["input"]["adapter"]["bigquery"] == {
            "projectId": "proj",
            "clientId": "cid",
            "clientSecret": "cs",
        }
        assert isinstance(saved.settings, SnowflakeConnectionSettings)
        assert saved.settings.use_sso is True and saved.scope == ConnectionScope.DEVELOPMENT_SHARED
        assert "... on BigQueryConnectionSettings" in transport.calls[0].query
        assert "costProjectIds" in transport.calls[0].query and "useSso" in transport.calls[0].query


@pytest.mark.parametrize(
    "model, data, attributes",
    [
        (DatabricksConnectionSettings, {"hasToken": True}, ["has_token"]),
        (DatabricksConnectionSettings, {"hasProfile": True}, ["has_profile"]),
        (RedshiftConnectionSettings, {"hasPassword": True}, ["has_password"]),
        (RedshiftConnectionSettings, {"hasProfile": True}, ["has_profile"]),
        (DuckDbConnectionSettings, {"hasProfile": True}, ["has_profile"]),
        (FireboltConnectionSettings, {"hasPassword": True}, ["has_password"]),
        (FireboltConnectionSettings, {"hasProfile": True}, ["has_profile"]),
        (FivetranSettings, {"hasApiSecret": True}, ["has_api_secret"]),
        (
            DatadogSettings,
            {"hasApiKey": True, "hasApplicationKey": True},
            ["has_api_key", "has_application_key"],
        ),
        (AzureKeyVaultSettings, {"hasClientSecret": True}, ["has_client_secret"]),
        (GcpSecretManagerSettings, {"hasServiceAccountJson": True}, ["has_service_account_json"]),
        (
            SlackInstallation,
            {"status": "CONNECTED", "isReconnectRequired": True},
            ["is_reconnect_required"],
        ),
        (
            GitRepository,
            {"mode": "SSH_DEPLOY_KEY", "state": "CONNECTED", "isVerified": True},
            ["is_verified"],
        ),
    ],
)
def test_has_and_is_fields_parse_from_their_api_keys(
    model: Any, data: Dict[str, Any], attributes: List[str]
) -> None:
    parsed = model.parse_obj(data)

    for attribute in attributes:
        assert getattr(parsed, attribute) is True
