import json
from typing import List

import pytest
from test_admin_clients import _admin, _connection
from test_admin_transport import FakeResponse, _error, _transport

from paradime.apis.admin.client import AdminClient
from paradime.apis.admin.dinoai.types import (
    DINOAI_AGENT_DEFINITION_FIELDS,
    DinoAiAgentDefinition,
    DinoAiAgentModel,
    DinoAiAgentSpecInput,
    DinoAiAgentToolGateInput,
)
from paradime.apis.admin.exception import AdminConflictException, AdminNotFoundException, ErrorCode
from paradime.apis.admin.integrations.types import (
    AwsSecretsManagerConfigInput,
    AwsSecretsManagerSettings,
    DatadogIntegrationInput,
    DatadogSite,
    ExternalSecretsProviderName,
    IntegrationName,
    LookerIntegrationInput,
    LookerSettings,
)
from paradime.apis.admin.notifications.types import AlertCategory, SetAlertNotificationsInput
from paradime.apis.admin.users.types import (
    USER_FIELDS,
    AdminUser,
    UserAccountType,
    UserInviteStatus,
    UserStatus,
)
from paradime.tools.pydantic import ValidationError


class TestUsers:
    def test_list_filters_by_status_and_pages(self) -> None:
        user = {"email": "a@example.com", "uid": "u1", "status": "ACTIVE", "accountType": "ADMIN"}
        admin, transport = _admin(
            {"users": _connection([user], "c1", True)},
            {"users": _connection([{**user, "uid": None, "status": "INVITED"}], None, False)},
        )

        users = list(admin.users.list(statuses=[UserStatus.ACTIVE, UserStatus.INVITED]))

        assert [u.status for u in users] == [UserStatus.ACTIVE, UserStatus.INVITED]
        assert transport.calls[0].variables == {
            "status": ["ACTIVE", "INVITED"],
            "first": 100,
            "after": None,
        }
        assert transport.calls[1].variables["after"] == "c1"

    def test_reactivate_and_auto_join_domains(self) -> None:
        admin, transport = _admin(
            {"reactivateUser": {"user": {"uid": "u1", "status": "ACTIVE"}}},
            {"setAutoJoinDomains": {"workspace": {"autoJoinDomains": ["example.com"]}}},
        )

        assert admin.users.reactivate("u1", workspace_uid="w2").status == UserStatus.ACTIVE
        assert admin.users.set_auto_join_domains(["example.com"]) == ["example.com"]
        query = transport.calls[0].query
        assert "mutation adminReactivateUser($input: ReactivateUserInput!)" in query
        assert "reactivateUser(input: $input)" in query
        assert transport.calls[0].variables == {"input": {"uid": "u1"}}
        assert transport.calls[0].workspace_uid == "w2"
        assert transport.calls[1].variables == {"input": {"domains": ["example.com"]}}

    def test_create_invites_with_a_role_and_returns_the_invitee(self) -> None:
        invitee = {
            "email": "new@example.com",
            "uid": None,
            "accountType": "DEVELOPER",
            "status": "INVITED",
            "inviteStatus": "PENDING",
            "invitedAt": "2026-11-18T10:00:00Z",
        }
        admin, transport = _admin({"createUser": {"user": invitee}})

        user = admin.users.create("new@example.com", UserAccountType.DEVELOPER, workspace_uid="w2")

        assert " ".join(transport.calls[0].query.split()) == " ".join(
            f"""
            mutation adminCreateUser($input: CreateUserInput!) {{
                createUser(input: $input) {{
                    user {{
                        {USER_FIELDS}
                    }}
                }}
            }}
            """.split()
        )
        assert transport.calls[0].variables == {
            "input": {"email": "new@example.com", "accountType": "DEVELOPER"}
        }
        assert transport.calls[0].workspace_uid == "w2"
        assert user.uid is None
        assert user.account_type == UserAccountType.DEVELOPER
        assert user.status == UserStatus.INVITED
        assert user.invite_status == UserInviteStatus.PENDING

    def test_update_sends_the_uid_and_the_new_role(self) -> None:
        admin, transport = _admin(
            {"updateUser": {"user": {"uid": "u1", "accountType": "ADMIN", "status": "ACTIVE"}}}
        )

        user = admin.users.update("u1", account_type=UserAccountType.ADMIN)

        assert "mutation adminUpdateUser($input: UpdateUserInput!)" in transport.calls[0].query
        assert "updateUser(input: $input)" in transport.calls[0].query
        assert transport.calls[0].variables == {"input": {"uid": "u1", "accountType": "ADMIN"}}
        assert transport.calls[0].workspace_uid is None
        assert user.account_type == UserAccountType.ADMIN

    def test_deactivate_sends_the_uid_and_returns_the_former_member(self) -> None:
        admin, transport = _admin(
            {
                "deactivateUser": {
                    "user": {"uid": "u1", "accountType": "ANALYST", "status": "DEACTIVATED"}
                }
            }
        )

        user = admin.users.deactivate("u1", workspace_uid="w2")

        query = transport.calls[0].query
        assert "mutation adminDeactivateUser($input: DeactivateUserInput!)" in query
        assert "deactivateUser(input: $input)" in query
        assert transport.calls[0].variables == {"input": {"uid": "u1"}}
        assert transport.calls[0].workspace_uid == "w2"
        assert user.status == UserStatus.DEACTIVATED
        assert user.account_type == UserAccountType.ANALYST

    def test_a_role_the_sdk_does_not_know_reads_as_unknown(self) -> None:
        user = AdminUser.parse_obj({"uid": "u1", "accountType": "NEW_ROLE", "status": "ACTIVE"})

        assert user.account_type == UserAccountType.UNKNOWN

    def test_create_with_an_email_already_taken_raises_conflict(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        sleeps: List[float] = []
        transport, _ = _transport(
            monkeypatch,
            sleeps,
            FakeResponse(_error("CONFLICT", "new@example.com is already invited.", field="email")),
        )
        admin = AdminClient(client=None, transport=transport)

        with pytest.raises(AdminConflictException) as exc_info:
            admin.users.create("new@example.com", UserAccountType.DEVELOPER)

        assert exc_info.value.code == ErrorCode.CONFLICT
        assert exc_info.value.field == ["email"]
        assert sleeps == []

    @pytest.mark.parametrize("method", ["update", "deactivate"])
    def test_an_unknown_uid_raises_not_found(
        self, monkeypatch: pytest.MonkeyPatch, method: str
    ) -> None:
        transport, _ = _transport(
            monkeypatch, [], FakeResponse(_error("NOT_FOUND", "No user u-missing."))
        )
        admin = AdminClient(client=None, transport=transport)

        with pytest.raises(AdminNotFoundException) as exc_info:
            if method == "update":
                admin.users.update("u-missing", account_type=UserAccountType.ADMIN)
            else:
                admin.users.deactivate("u-missing")

        assert exc_info.value.code == ErrorCode.NOT_FOUND
        assert exc_info.value.message == "No user u-missing."


class TestNotifications:
    def test_set_alert_notifications_sends_only_the_destinations_given(self) -> None:
        notifications = {
            "category": "BOLT_SYSTEM",
            "slack": {"channelIds": ["C1"], "userIds": []},
            "email": None,
        }
        admin, transport = _admin({"setAlertNotifications": {"alertNotifications": notifications}})

        result = admin.notifications.set_alert_notifications(slack_channel_ids=["C1"])

        assert transport.calls[0].variables == {
            "input": {
                "category": "BOLT_SYSTEM",
                "slack": {"channelIds": ["C1"], "userIds": []},
            }
        }
        assert result.category == AlertCategory.BOLT_SYSTEM
        assert result.slack is not None and result.slack.channel_ids == ["C1"]

    def test_alerts_have_no_microsoft_teams_destination(self) -> None:
        admin, transport = _admin(
            {"workspace": {"alertNotifications": {"category": "BOLT_SYSTEM", "email": None}}}
        )

        admin.notifications.get_alert_notifications()

        assert "msTeams" not in transport.calls[0].query
        assert not hasattr(admin.notifications, "set_ms_teams_webhook")
        with pytest.raises(ValidationError):
            SetAlertNotificationsInput.parse_obj(
                {"category": "BOLT_SYSTEM", "msTeams": {"webhookNames": ["alerts"]}}
            )

    def test_get_slack(self) -> None:
        admin, transport = _admin(
            {"slack": {"status": "PENDING_USER_ACTION", "installUrl": "https://app/slack"}},
        )

        slack = admin.notifications.get_slack()
        assert slack is not None and slack.install_url == "https://app/slack"
        assert transport.calls[0].variables == {}


TRIGGER_URL = {
    "publicId": "p1",
    "name": "ci",
    "isEnabled": True,
    "url": "https://hooks/p1",
    "schedule": {"slug": "nightly", "name": "Nightly"},
    "hasSecret": True,
}


class TestBolt:
    def test_update_schedule_sends_the_whole_schedule(self) -> None:
        admin, transport = _admin(
            {
                "updateBoltSchedule": {
                    "schedule": {"slug": "nightly", "name": "Nightly", "isSuspended": True}
                }
            }
        )

        definition = admin.bolt.update_schedule(
            "nightly",
            name="Nightly",
            schedule="0 1 * * *",
            environment="production",
            commands=["dbt build"],
            trigger_on_webhook=True,
        )

        assert transport.calls[0].variables == {
            "input": {
                "slug": "nightly",
                "schedule": {
                    "name": "Nightly",
                    "schedule": "0 1 * * *",
                    "environment": "production",
                    "commands": ["dbt build"],
                    "triggerOnWebhook": True,
                },
            }
        }
        assert definition.slug == "nightly" and definition.is_suspended is True

    def test_trigger_url_secrets_are_returned_but_kept_out_of_repr(self) -> None:
        admin, transport = _admin(
            {"createTriggerUrl": {"triggerUrl": TRIGGER_URL, "secret": "tok-123"}}
        )

        created = admin.bolt.create_trigger_url("nightly", "ci")

        assert created.secret == "tok-123"
        assert created.trigger_url.is_enabled is True and created.trigger_url.has_secret is True
        assert "tok-123" not in repr(created)
        assert "https://hooks/p1" in repr(created)
        assert transport.calls[0].linked_fields == {"schedule"}

    def test_rotate_trigger_url_secret_returns_the_new_secret(self) -> None:
        admin, transport = _admin(
            {"rotateTriggerUrlSecret": {"triggerUrl": TRIGGER_URL, "secret": "tok-456"}}
        )

        rotated = admin.bolt.rotate_trigger_url_secret("p1")

        assert rotated.secret == "tok-456"
        assert rotated.trigger_url.schedule is not None
        assert rotated.trigger_url.schedule.slug == "nightly"
        assert transport.calls[0].variables == {"input": {"publicId": "p1"}}
        assert "schedule {" in transport.calls[0].query
        assert transport.calls[0].linked_fields == {"schedule"}

    def test_linked_schedules_and_environments_read_as_objects(self) -> None:
        schedule = {
            "slug": "nightly",
            "name": "Nightly",
            "environment": {"slug": "production", "name": "Production"},
        }
        admin, transport = _admin(
            {
                "workspace": {
                    "triggerUrls": _connection(
                        [TRIGGER_URL, {**TRIGGER_URL, "publicId": "p2", "schedule": None}],
                        None,
                        False,
                    )
                }
            },
            {"setTriggerUrlEnabled": {"triggerUrl": TRIGGER_URL}},
            {"updateBoltSchedule": {"schedule": schedule}},
        )

        linked, missing = admin.bolt.list_trigger_urls("nightly")
        admin.bolt.set_trigger_url_enabled("p1", False)
        definition = admin.bolt.update_schedule(
            "nightly", name="Nightly", schedule="OFF", environment="production", commands=["dbt"]
        )

        assert linked.schedule is not None and linked.schedule.slug == "nightly"
        assert missing.schedule is None
        assert definition.environment is not None
        assert definition.environment.slug == "production"
        assert [call.linked_fields for call in transport.calls] == [
            {"schedule"},
            {"schedule"},
            {"environment"},
        ]
        assert "scheduleFound" not in transport.calls[0].query
        assert "schedule {" in transport.calls[0].query
        assert "environment {" in transport.calls[2].query

    def test_deploy_returns_a_job(self) -> None:
        job = {"id": "j1", "kind": "deployBoltSchedules", "status": "QUEUED"}
        admin, transport = _admin({"deployBoltSchedules": {"job": job}})

        assert admin.bolt.deploy_schedules(branch="main").id == "j1"
        assert transport.calls[0].variables == {"input": {"branch": "main"}}


class TestIntegrations:
    def test_set_wraps_one_integration_and_parses_its_settings(self) -> None:
        integration = {
            "name": "LOOKER",
            "status": "CONNECTED",
            "settings": {
                "__typename": "LookerSettings",
                "baseUrl": "https://looker.example.test",
                "hasClientSecret": True,
            },
        }
        admin, transport = _admin({"setIntegration": {"integration": integration}})

        result = admin.integrations.set(
            LookerIntegrationInput(
                base_url="https://looker.example.test", client_id="id", client_secret="s"
            )
        )

        assert transport.calls[0].variables == {
            "input": {
                "config": {
                    "looker": {
                        "baseUrl": "https://looker.example.test",
                        "clientId": "id",
                        "clientSecret": "s",
                    }
                }
            }
        }
        assert result.name == IntegrationName.LOOKER
        assert isinstance(result.settings, LookerSettings)
        assert result.settings.has_client_secret is True

    def test_test_and_disconnect(self) -> None:
        admin, transport = _admin(
            {"testIntegration": {"name": "DATADOG", "testedAt": "2026-11-05T10:00:00Z"}},
            {
                "disconnectIntegration": {
                    "integration": {"name": "DATADOG", "status": "NOT_CONNECTED"}
                }
            },
        )

        admin.integrations.test(DatadogIntegrationInput(site=DatadogSite.EU, api_key="k"))
        admin.integrations.disconnect(IntegrationName.DATADOG)

        assert transport.calls[0].variables["input"]["config"] == {
            "datadog": {"site": "EU", "apiKey": "k"}
        }
        assert transport.calls[1].variables == {"input": {"name": "DATADOG"}}

    def test_external_secrets_providers(self) -> None:
        provider = {
            "name": "AWS",
            "settings": {
                "__typename": "AwsSecretsManagerSettings",
                "accessKeyIdHint": "AKIA…WXYZ",
                "hasSecretAccessKey": True,
            },
        }
        admin, transport = _admin(
            {"setExternalSecretsProvider": {"provider": provider}},
            {"deleteExternalSecretsProvider": {"name": "AWS"}},
        )

        saved = admin.integrations.set_external_secrets_provider(
            AwsSecretsManagerConfigInput(
                access_key_id="AKIA", secret_access_key="sk", role_arn="arn", region="eu-west-1"
            ),
            test_reference="aws:prod/db#password",
        )
        deleted = admin.integrations.delete_external_secrets_provider(
            ExternalSecretsProviderName.AWS
        )

        assert transport.calls[0].variables == {
            "input": {
                "provider": {
                    "aws": {
                        "accessKeyId": "AKIA",
                        "secretAccessKey": "sk",
                        "roleArn": "arn",
                        "region": "eu-west-1",
                    }
                },
                "testReference": "aws:prod/db#password",
            }
        }
        assert isinstance(saved.settings, AwsSecretsManagerSettings)
        assert deleted == ExternalSecretsProviderName.AWS

    def test_an_unsupported_config_is_refused(self) -> None:
        admin, _ = _admin()

        with pytest.raises(TypeError):
            admin.integrations.set(
                AwsSecretsManagerConfigInput(access_key_id="a", role_arn="r", region="x")
            )


AGENT = {"slug": "analyst", "name": "Analyst", "model": "SONNET", "source": "UI"}


class TestDinoAi:
    def test_create_agent_sends_the_spec(self) -> None:
        admin, transport = _admin({"createDinoAiAgentDefinition": {"dinoAiAgentDefinition": AGENT}})

        agent = admin.dinoai.create_agent(
            "Analyst",
            DinoAiAgentSpecInput(
                role="r",
                goal="g",
                backstory="b",
                model=DinoAiAgentModel.SONNET,
                tools=DinoAiAgentToolGateInput(mode="ALLOWLIST", names=["run_sql_query"]),
            ),
            slug="analyst",
            workspace_uid="w2",
        )

        assert " ".join(transport.calls[0].query.split()) == " ".join(
            f"""
            mutation adminCreateDinoAiAgentDefinition($input: CreateDinoAiAgentDefinitionInput!) {{
                createDinoAiAgentDefinition(input: $input) {{
                    dinoAiAgentDefinition {{
                        {DINOAI_AGENT_DEFINITION_FIELDS}
                    }}
                }}
            }}
            """.split()
        )
        assert transport.calls[0].variables == {
            "input": {
                "name": "Analyst",
                "spec": {
                    "role": "r",
                    "goal": "g",
                    "backstory": "b",
                    "model": "SONNET",
                    "tools": {"mode": "ALLOWLIST", "names": ["run_sql_query"]},
                },
                "slug": "analyst",
            }
        }
        assert transport.calls[0].workspace_uid == "w2"
        assert agent.model == DinoAiAgentModel.SONNET
        assert transport.calls[0].linked_fields == {"environment"}

    def test_create_agent_without_a_slug_leaves_it_out(self) -> None:
        admin, transport = _admin({"createDinoAiAgentDefinition": {"dinoAiAgentDefinition": AGENT}})

        admin.dinoai.create_agent(
            "Analyst", DinoAiAgentSpecInput(role="r", goal="g", backstory="b")
        )

        assert transport.calls[0].variables == {
            "input": {"name": "Analyst", "spec": {"role": "r", "goal": "g", "backstory": "b"}}
        }

    def test_update_agent_replaces_the_definition_by_slug(self) -> None:
        admin, transport = _admin({"updateDinoAiAgentDefinition": {"dinoAiAgentDefinition": AGENT}})

        agent = admin.dinoai.update_agent(
            "analyst",
            "Analyst",
            DinoAiAgentSpecInput(role="r", goal="g", backstory="b"),
            slack_bot_name="Analyst bot",
        )

        assert " ".join(transport.calls[0].query.split()) == " ".join(
            f"""
            mutation adminUpdateDinoAiAgentDefinition($input: UpdateDinoAiAgentDefinitionInput!) {{
                updateDinoAiAgentDefinition(input: $input) {{
                    dinoAiAgentDefinition {{
                        {DINOAI_AGENT_DEFINITION_FIELDS}
                    }}
                }}
            }}
            """.split()
        )
        assert transport.calls[0].variables == {
            "input": {
                "slug": "analyst",
                "name": "Analyst",
                "spec": {"role": "r", "goal": "g", "backstory": "b"},
                "slackBotName": "Analyst bot",
            }
        }
        assert agent.slug == "analyst"
        assert transport.calls[0].linked_fields == {"environment"}

    def test_create_agent_over_other_settings_raises_conflict(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        transport, _ = _transport(
            monkeypatch,
            [],
            FakeResponse(
                _error(
                    "CONFLICT",
                    "Agent analyst exists with other settings.",
                    field=["input", "slug"],
                )
            ),
        )
        admin = AdminClient(client=None, transport=transport)

        with pytest.raises(AdminConflictException) as exc_info:
            admin.dinoai.create_agent(
                "Other", DinoAiAgentSpecInput(role="r", goal="g", backstory="b"), slug="analyst"
            )

        assert exc_info.value.code == ErrorCode.CONFLICT
        assert exc_info.value.field == ["input", "slug"]

    def test_update_agent_of_an_unknown_slug_raises_not_found(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        transport, _ = _transport(
            monkeypatch,
            [],
            FakeResponse(_error("NOT_FOUND", "No agent ghost.", field=["input", "slug"])),
        )
        admin = AdminClient(client=None, transport=transport)

        with pytest.raises(AdminNotFoundException) as exc_info:
            admin.dinoai.update_agent(
                "ghost", "Ghost", DinoAiAgentSpecInput(role="r", goal="g", backstory="b")
            )

        assert exc_info.value.code == ErrorCode.NOT_FOUND
        assert exc_info.value.field == ["input", "slug"]

    def test_the_environment_and_squad_read_from_their_links(self) -> None:
        lead = {
            **AGENT,
            "slug": "lead",
            "environment": {"slug": "agent", "name": "Agent"},
            "agentsSquad": {
                "totalCount": 2,
                "edges": [{"node": {"slug": "analyst"}}, {"node": {"slug": "writer"}}],
            },
        }
        admin, transport = _admin(
            {"workspace": {"dinoAiAgentDefinitions": _connection([lead, AGENT], None, False)}},
            {"workspace": {"dinoAiAgentDefinition": lead}},
        )

        listed, alone = admin.dinoai.list_agents()
        read = admin.dinoai.get_agent("lead")

        assert listed.environment is not None and listed.environment.slug == "agent"
        assert listed.agents_squad == ["analyst", "writer"]
        assert listed.agents_squad_count == 2
        assert alone.environment is None and alone.agents_squad is None
        assert read is not None and read.agents_squad == ["analyst", "writer"]
        assert DinoAiAgentDefinition.parse_obj(json.loads(read.json(by_alias=True))) == read
        for call in transport.calls:
            assert "agentsSquad(first: 500) {" in call.query
            assert call.linked_fields == {"environment"}

    def test_graph_settings_and_sync(self) -> None:
        job = {"id": "j1", "kind": "syncDinoAiAgentDefinitionsFromCode", "status": "RUNNING"}
        admin, transport = _admin(
            {"setDinoAiAgentGraph": {"workspace": {"uid": "w1"}}},
            {
                "setDinoAiWorkspaceSettings": {
                    "workspace": {"dinoAiSettings": {"sqlQueryToolEnabled": False}}
                }
            },
            {"syncDinoAiAgentDefinitionsFromCode": {"job": job}},
            {"workspace": {"dinoAiAgentDefinition": None}},
        )

        admin.dinoai.set_agent_graph([("lead", "analyst")])
        settings = admin.dinoai.set_settings(sql_query_tool_enabled=False)
        job_started = admin.dinoai.sync_agents_from_code()

        assert transport.calls[0].variables == {
            "input": {"edges": [{"fromSlug": "lead", "toSlug": "analyst"}]}
        }
        assert transport.calls[1].variables == {"input": {"sqlQueryToolEnabled": False}}
        assert transport.calls[2].variables == {"input": {}}
        assert settings is not None and settings.sql_query_tool_enabled is False
        assert job_started.id == "j1"
        assert admin.dinoai.get_agent("missing") is None

    def test_reads_use_the_dino_ai_keys(self) -> None:
        admin, transport = _admin(
            {"workspace": {"dinoAiAgentDefinitions": _connection([AGENT], None, False)}},
            {"workspace": {"dinoAiAgentDefinition": AGENT}},
            {"workspace": {"dinoAiSettings": {"sqlQueryToolEnabled": True}}},
        )

        agents = list(admin.dinoai.list_agents())
        agent = admin.dinoai.get_agent("analyst")
        settings = admin.dinoai.get_settings()

        assert [a.slug for a in agents] == ["analyst"]
        assert agent is not None and agent.slug == "analyst"
        assert settings is not None and settings.sql_query_tool_enabled is True
        assert "dinoAiAgentDefinitions(first: $first" in transport.calls[0].query
        assert "dinoAiAgentDefinition(slug: $slug)" in transport.calls[1].query
        assert "dinoAiSettings {" in transport.calls[2].query


def test_every_domain_is_on_the_admin_client() -> None:
    admin, _ = _admin()

    for domain in (
        "workspaces",
        "git",
        "connections",
        "environments",
        "environment_variables",
        "usage",
        "jobs",
        "users",
        "notifications",
        "bolt",
        "integrations",
        "dinoai",
        "setup_status",
    ):
        assert getattr(admin, domain).transport is admin.transport
