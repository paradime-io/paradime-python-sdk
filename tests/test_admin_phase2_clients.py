import pytest
from test_admin_clients import _admin, _connection

from paradime.apis.admin.dinoai.types import (
    DinoaiAgentModel,
    DinoaiAgentSpecInput,
    DinoaiAgentToolGateInput,
)
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
from paradime.apis.admin.users.types import UserStatus
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
        assert transport.calls[0].variables == {"input": {"uid": "u1"}}
        assert transport.calls[0].workspace_uid == "w2"
        assert transport.calls[1].variables == {"input": {"domains": ["example.com"]}}


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


TRIGGER_URL = {"publicId": "p1", "name": "ci", "enabled": True, "url": "https://hooks/p1"}


class TestBolt:
    def test_update_schedule_sends_the_whole_schedule(self) -> None:
        admin, transport = _admin(
            {"updateBoltSchedule": {"schedule": {"slug": "nightly", "name": "Nightly"}}}
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
        assert definition.slug == "nightly"

    def test_trigger_url_secrets_are_returned_but_kept_out_of_repr(self) -> None:
        admin, _ = _admin({"createTriggerUrl": {"triggerUrl": TRIGGER_URL, "secret": "tok-123"}})

        created = admin.bolt.create_trigger_url("nightly", "ci")

        assert created.secret == "tok-123"
        assert "tok-123" not in repr(created)
        assert "https://hooks/p1" in repr(created)

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
                "clientSecretIsSet": True,
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
        assert result.settings.client_secret_is_set is True

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
                "secretAccessKeyIsSet": True,
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


class TestDinoai:
    def test_set_agent_sends_the_spec(self) -> None:
        admin, transport = _admin({"setDinoaiAgentDefinition": {"dinoaiAgentDefinition": AGENT}})

        agent = admin.dinoai.set_agent(
            "Analyst",
            DinoaiAgentSpecInput(
                role="r",
                goal="g",
                backstory="b",
                model=DinoaiAgentModel.SONNET,
                tools=DinoaiAgentToolGateInput(mode="ALLOWLIST", names=["run_sql_query"]),
            ),
            slug="analyst",
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
        assert agent.model == DinoaiAgentModel.SONNET

    def test_graph_settings_and_sync(self) -> None:
        job = {"id": "j1", "kind": "syncDinoaiAgentDefinitionsFromCode", "status": "RUNNING"}
        admin, transport = _admin(
            {"setDinoaiAgentGraph": {"workspace": {"uid": "w1"}}},
            {
                "setDinoaiWorkspaceSettings": {
                    "workspace": {"dinoaiSettings": {"sqlQueryToolEnabled": False}}
                }
            },
            {"syncDinoaiAgentDefinitionsFromCode": {"job": job}},
            {"workspace": {"dinoaiAgentDefinition": None}},
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
