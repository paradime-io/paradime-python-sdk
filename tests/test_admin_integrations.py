import json
from typing import Any, Dict, List, Type

import pytest
from click.testing import CliRunner
from test_admin_clients import _admin, _connection
from test_admin_transport import FakeResponse, _transport

from paradime.apis.admin.client import AdminClient
from paradime.apis.admin.exception import AdminMissingCapabilityException
from paradime.apis.admin.integrations.types import (
    AGENT_INTEGRATION_LINKS,
    AgentIntegrationName,
    AgentIntegrationStatus,
    AirbyteIntegrationInput,
    AirbyteSettings,
    DataHubIntegrationInput,
    DataHubSettings,
    HightouchIntegrationInput,
    HightouchSettings,
    IncidentIoIntegrationInput,
    IncidentIoSettings,
    IntegrationConfigInput,
    IntegrationName,
    LightdashIntegrationInput,
    LightdashSettings,
    MetabaseIntegrationInput,
    MetabaseSettings,
    NewRelicIntegrationInput,
    NewRelicSettings,
    OmniIntegrationInput,
    OmniSettings,
    PagerDutyIntegrationInput,
    PagerDutyRegion,
    PagerDutySettings,
    PowerBiIntegrationInput,
    PowerBiSettings,
    RootlyIntegrationInput,
    RootlySettings,
    SigmaIntegrationInput,
    SigmaSettings,
    TableauIntegrationInput,
    TableauSettings,
    ThoughtSpotIntegrationInput,
    ThoughtSpotSettings,
)
from paradime.apis.admin.setup_status.types import SetupStep, SetupStepStatus
from paradime.cli.admin import admin as admin_cli, common
from paradime.tools.pydantic import BaseModel, ValidationError

CONFIGS: List[Any] = [
    (
        TableauIntegrationInput(
            url="https://tableau.example.test/#/site/acme",
            pat_name="paradime",
            pat_secret="tableau-secret",
            project_ids=["p1"],
        ),
        "tableau",
        {
            "url": "https://tableau.example.test/#/site/acme",
            "patName": "paradime",
            "patSecret": "tableau-secret",
            "projectIds": ["p1"],
        },
        "tableau-secret",
    ),
    (
        PowerBiIntegrationInput(
            tenant_id="t1", client_id="c1", client_secret="powerbi-secret", workspace_ids=["w1"]
        ),
        "powerBi",
        {
            "tenantId": "t1",
            "clientId": "c1",
            "clientSecret": "powerbi-secret",
            "workspaceIds": ["w1"],
        },
        "powerbi-secret",
    ),
    (
        DataHubIntegrationInput(
            gms_url="https://datahub.example.test/gms",
            pat_token="datahub-token",
            domain="urn:li:domain:analytics",
        ),
        "datahub",
        {
            "gmsUrl": "https://datahub.example.test/gms",
            "patToken": "datahub-token",
            "domain": "urn:li:domain:analytics",
        },
        "datahub-token",
    ),
    (
        SigmaIntegrationInput(
            host="api.sigma.example.test", client_id="c1", client_secret="sigma-secret"
        ),
        "sigma",
        {"host": "api.sigma.example.test", "clientId": "c1", "clientSecret": "sigma-secret"},
        "sigma-secret",
    ),
    (
        ThoughtSpotIntegrationInput(
            host="ts.example.test", username="paradime", password="thoughtspot-password"
        ),
        "thoughtspot",
        {"host": "ts.example.test", "username": "paradime", "password": "thoughtspot-password"},
        "thoughtspot-password",
    ),
    (
        LightdashIntegrationInput(
            instance_url="https://lightdash.example.test", api_token="lightdash-token"
        ),
        "lightdash",
        {"instanceUrl": "https://lightdash.example.test", "apiToken": "lightdash-token"},
        "lightdash-token",
    ),
    (
        OmniIntegrationInput(host="omni.example.test", api_key="omni-key"),
        "omni",
        {"host": "omni.example.test", "apiKey": "omni-key"},
        "omni-key",
    ),
    (
        MetabaseIntegrationInput(host="metabase.example.test", api_key="metabase-key"),
        "metabase",
        {"host": "metabase.example.test", "apiKey": "metabase-key"},
        "metabase-key",
    ),
    (
        AirbyteIntegrationInput(client_id="c1", client_secret="airbyte-secret"),
        "airbyte",
        {"clientId": "c1", "clientSecret": "airbyte-secret"},
        "airbyte-secret",
    ),
    (
        HightouchIntegrationInput(api_key="hightouch-key"),
        "hightouch",
        {"apiKey": "hightouch-key"},
        "hightouch-key",
    ),
    (
        NewRelicIntegrationInput(account_id="1234567", api_key="newrelic-key"),
        "newRelic",
        {"accountId": "1234567", "apiKey": "newrelic-key"},
        "newrelic-key",
    ),
    (
        PagerDutyIntegrationInput(api_region=PagerDutyRegion.EU, api_key="pagerduty-key"),
        "pagerduty",
        {"apiRegion": "EU", "apiKey": "pagerduty-key"},
        "pagerduty-key",
    ),
    (
        IncidentIoIntegrationInput(api_key="incident-key"),
        "incidentIo",
        {"apiKey": "incident-key"},
        "incident-key",
    ),
    (
        RootlyIntegrationInput(api_key="rootly-key"),
        "rootly",
        {"apiKey": "rootly-key"},
        "rootly-key",
    ),
]


@pytest.mark.parametrize("config, key, sent, secret", CONFIGS)
def test_set_wraps_each_integration_under_its_key(
    config: Any, key: str, sent: Dict[str, Any], secret: str
) -> None:
    admin, transport = _admin({"setIntegration": {"integration": {"name": "OMNI", "status": "X"}}})

    admin.integrations.set(config)

    assert "mutation adminSetIntegration($input: SetIntegrationInput!)" in transport.calls[0].query
    assert transport.calls[0].variables == {"input": {"config": {key: sent}}}


@pytest.mark.parametrize("config, key, sent, secret", CONFIGS)
def test_test_sends_each_integration_and_saves_nothing(
    config: Any, key: str, sent: Dict[str, Any], secret: str
) -> None:
    admin, transport = _admin(
        {"testIntegration": {"name": "METABASE", "testedAt": "2026-11-05T10:00:00Z"}}
    )

    result = admin.integrations.test(config)

    assert "testIntegration(input: $input)" in transport.calls[0].query
    assert transport.calls[0].variables == {"input": {"config": {key: sent}}}
    assert result.name == IntegrationName.METABASE


@pytest.mark.parametrize("config, key, sent, secret", CONFIGS)
def test_each_secret_is_kept_out_of_repr(
    config: Any, key: str, sent: Dict[str, Any], secret: str
) -> None:
    assert secret not in repr(config)
    assert secret not in repr(IntegrationConfigInput.parse_obj({key: config}))
    assert "**********" in repr(config)


@pytest.mark.parametrize("config, key, sent, secret", CONFIGS)
def test_a_secret_left_out_is_not_sent(
    config: Any, key: str, sent: Dict[str, Any], secret: str
) -> None:
    admin, transport = _admin({"setIntegration": {"integration": {"name": "OMNI", "status": "X"}}})
    secret_field = next(name for name, value in sent.items() if value == secret)
    without_secret = {name: value for name, value in sent.items() if name != secret_field}

    admin.integrations.set(type(config).parse_obj(without_secret))

    assert transport.calls[0].variables == {"input": {"config": {key: without_secret}}}


SETTINGS: List[Any] = [
    (
        "TABLEAU",
        TableauSettings,
        {
            "url": "https://tableau.example.test",
            "patName": "paradime",
            "hasPatSecret": True,
            "projectIds": ["p1"],
        },
        {"url": "https://tableau.example.test", "has_pat_secret": True, "project_ids": ["p1"]},
    ),
    (
        "POWER_BI",
        PowerBiSettings,
        {"tenantId": "t1", "clientId": "c1", "hasClientSecret": True, "workspaceIds": ["w1"]},
        {"tenant_id": "t1", "has_client_secret": True, "workspace_ids": ["w1"]},
    ),
    (
        "DATAHUB",
        DataHubSettings,
        {"gmsUrl": "https://datahub.example.test/gms", "domain": "d1", "hasPatToken": True},
        {"gms_url": "https://datahub.example.test/gms", "domain": "d1", "has_pat_token": True},
    ),
    (
        "SIGMA",
        SigmaSettings,
        {"host": "api.sigma.example.test", "clientId": "c1", "hasClientSecret": False},
        {"host": "api.sigma.example.test", "client_id": "c1", "has_client_secret": False},
    ),
    (
        "THOUGHTSPOT",
        ThoughtSpotSettings,
        {"host": "ts.example.test", "username": "paradime", "hasPassword": True},
        {"username": "paradime", "has_password": True},
    ),
    (
        "LIGHTDASH",
        LightdashSettings,
        {"instanceUrl": "https://lightdash.example.test", "hasApiToken": True},
        {"instance_url": "https://lightdash.example.test", "has_api_token": True},
    ),
    (
        "OMNI",
        OmniSettings,
        {"host": "omni.example.test", "hasApiKey": True},
        {"host": "omni.example.test", "has_api_key": True},
    ),
    (
        "METABASE",
        MetabaseSettings,
        {"host": "metabase.example.test", "hasApiKey": True},
        {"host": "metabase.example.test", "has_api_key": True},
    ),
    (
        "AIRBYTE",
        AirbyteSettings,
        {"clientId": "c1", "hasClientSecret": True},
        {"client_id": "c1", "has_client_secret": True},
    ),
    ("HIGHTOUCH", HightouchSettings, {"hasApiKey": True}, {"has_api_key": True}),
    (
        "NEW_RELIC",
        NewRelicSettings,
        {"accountId": "1234567", "hasApiKey": True},
        {"account_id": "1234567", "has_api_key": True},
    ),
    (
        "PAGERDUTY",
        PagerDutySettings,
        {"apiRegion": "EU", "hasApiKey": True},
        {"api_region": PagerDutyRegion.EU, "has_api_key": True},
    ),
    ("INCIDENT_IO", IncidentIoSettings, {"hasApiKey": True}, {"has_api_key": True}),
    ("ROOTLY", RootlySettings, {"hasApiKey": False}, {"has_api_key": False}),
]


@pytest.mark.parametrize("name, settings_class, settings, expected", SETTINGS)
def test_list_parses_each_settings_type_by_its_typename(
    name: str,
    settings_class: Type[BaseModel],
    settings: Dict[str, Any],
    expected: Dict[str, Any],
) -> None:
    node = {
        "name": name,
        "status": "CONNECTED",
        "settings": {"__typename": settings_class.__name__, **settings},
    }
    admin, transport = _admin({"workspace": {"integrations": _connection([node], None, False)}})

    [integration] = admin.integrations.list(workspace_uid="w1")

    assert integration.name == IntegrationName(name)
    assert isinstance(integration.settings, settings_class)
    for field, value in expected.items():
        assert getattr(integration.settings, field) == value
    assert f"... on {settings_class.__name__} {{" in transport.calls[0].query


def test_settings_of_a_type_the_sdk_does_not_know_read_as_none() -> None:
    node = {"name": "SOMETHING_NEW", "status": "CONNECTED", "settings": {"__typename": "NewOne"}}
    admin, _ = _admin({"workspace": {"integrations": _connection([node], None, False)}})

    [integration] = admin.integrations.list()

    assert integration.name == IntegrationName.UNKNOWN
    assert integration.settings is None


def test_a_region_the_sdk_does_not_know_reads_as_unknown() -> None:
    settings = PagerDutySettings.parse_obj({"apiRegion": "APAC", "hasApiKey": True})

    assert settings.api_region == PagerDutyRegion.UNKNOWN


def test_power_bi_needs_its_workspace_ids() -> None:
    with pytest.raises(ValidationError):
        PowerBiIntegrationInput(tenant_id="t1", client_id="c1")


def test_disconnect_sends_a_new_name() -> None:
    admin, transport = _admin(
        {"disconnectIntegration": {"integration": {"name": "PAGERDUTY", "status": "NOT_CONNECTED"}}}
    )

    integration = admin.integrations.disconnect(IntegrationName.PAGERDUTY, workspace_uid="w1")

    assert transport.calls[0].variables == {"input": {"name": "PAGERDUTY"}}
    assert transport.calls[0].workspace_uid == "w1"
    assert integration.name == IntegrationName.PAGERDUTY


AGENT_NODES = [
    {
        "name": "GOOGLE_DRIVE",
        "displayName": "Google Drive",
        "status": "CONNECTED",
        "reconnectRequired": False,
    },
    {
        "name": "NOTION",
        "displayName": "Notion",
        "status": "PENDING_USER_ACTION",
        "reconnectRequired": True,
    },
]


def test_list_agent_integrations_leaves_install_urls_out_by_default() -> None:
    admin, transport = _admin(
        {"workspace": {"agentIntegrations": _connection(AGENT_NODES, "c-1", True)}},
        {"workspace": {"agentIntegrations": _connection([AGENT_NODES[0]], "c-2", False)}},
    )

    listed = list(admin.integrations.list_agent_integrations(workspace_uid="w1", page_size=2))

    assert [i.name for i in listed] == [
        AgentIntegrationName.GOOGLE_DRIVE,
        AgentIntegrationName.NOTION,
        AgentIntegrationName.GOOGLE_DRIVE,
    ]
    assert listed[1].status == AgentIntegrationStatus.PENDING_USER_ACTION
    assert listed[1].reconnect_required is True
    assert listed[1].install_url is None
    call = transport.calls[0]
    assert "agentIntegrations(first: $first, after: $after)" in call.query
    assert "installUrl" not in call.query
    assert call.linked_fields == frozenset()
    assert call.variables == {"workspaceUid": "w1", "first": 2, "after": None}
    assert transport.calls[1].variables["after"] == "c-1"


def test_list_agent_integrations_reads_install_urls_when_asked() -> None:
    node = {
        **AGENT_NODES[1],
        "installUrl": "https://connect.example.test/session",
        "installUrlExpiresAt": "2026-11-23T10:30:00+00:00",
    }
    admin, transport = _admin(
        {"workspace": {"agentIntegrations": _connection([node], None, False)}}
    )

    [integration] = admin.integrations.list_agent_integrations(include_install_urls=True)

    assert integration.install_url == "https://connect.example.test/session"
    assert integration.install_url_expires_at is not None
    assert "installUrl" in transport.calls[0].query
    assert "installUrlExpiresAt" in transport.calls[0].query
    assert transport.calls[0].linked_fields == AGENT_INTEGRATION_LINKS


def test_list_agent_integrations_of_a_workspace_without_them_is_empty() -> None:
    admin, _ = _admin({"workspace": {"agentIntegrations": None}})

    assert list(admin.integrations.list_agent_integrations()) == []


def test_a_name_and_status_the_sdk_does_not_know_read_as_unknown() -> None:
    node = {"name": "LINEAR", "status": "PAUSED", "reconnectRequired": None}
    admin, _ = _admin({"workspace": {"agentIntegrations": _connection([node], None, False)}})

    [integration] = admin.integrations.list_agent_integrations()

    assert integration.name == AgentIntegrationName.UNKNOWN
    assert integration.status == AgentIntegrationStatus.UNKNOWN


def _agent_page(nodes: List[Dict[str, Any]], errors: List[Dict[str, Any]]) -> Dict[str, Any]:
    connection = {
        "edges": [{"node": node} for node in nodes],
        "totalCount": len(nodes),
        "pageInfo": {"endCursor": None, "hasNextPage": False},
    }
    return {"data": {"workspace": {"agentIntegrations": connection}}, "errors": errors}


def _missing_capability(index: int, field: str) -> Dict[str, Any]:
    return {
        "message": "Needs integration_settings:write.",
        "path": ["workspace", "agentIntegrations", "edges", index, "node", field],
        "extensions": {"code": "MISSING_CAPABILITY"},
    }


def test_a_read_only_key_lists_with_install_urls_as_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    nodes = [{**node, "installUrl": None, "installUrlExpiresAt": None} for node in AGENT_NODES]
    errors = [
        _missing_capability(index, field)
        for index in range(len(nodes))
        for field in ("installUrl", "installUrlExpiresAt")
    ]
    transport, _ = _transport(monkeypatch, [], FakeResponse(_agent_page(nodes, errors)))
    admin = AdminClient(transport.client, transport=transport)

    listed = list(admin.integrations.list_agent_integrations(include_install_urls=True))

    assert [i.status for i in listed] == [
        AgentIntegrationStatus.CONNECTED,
        AgentIntegrationStatus.PENDING_USER_ACTION,
    ]
    assert all(i.install_url is None and i.install_url_expires_at is None for i in listed)


def test_a_key_without_access_to_the_list_still_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    body = {
        "data": {"workspace": {"agentIntegrations": None}},
        "errors": [
            {
                "message": "Needs integration_settings:read.",
                "path": ["workspace", "agentIntegrations"],
                "extensions": {"code": "MISSING_CAPABILITY"},
            }
        ],
    }
    transport, _ = _transport(monkeypatch, [], FakeResponse(body))
    admin = AdminClient(transport.client, transport=transport)

    with pytest.raises(AdminMissingCapabilityException):
        list(admin.integrations.list_agent_integrations(include_install_urls=True))


def test_disconnect_agent_integration_sends_the_name() -> None:
    disconnected = {"name": "NOTION", "status": "PENDING_USER_ACTION", "reconnectRequired": False}
    admin, transport = _admin({"disconnectAgentIntegration": {"agentIntegration": disconnected}})

    integration = admin.integrations.disconnect_agent_integration(
        AgentIntegrationName.NOTION, workspace_uid="w1"
    )

    call = transport.calls[0]
    assert "disconnectAgentIntegration(input: $input)" in call.query
    assert "installUrl" not in call.query
    assert call.variables == {"input": {"name": "NOTION"}} and call.workspace_uid == "w1"
    assert integration.name == AgentIntegrationName.NOTION
    assert integration.status == AgentIntegrationStatus.PENDING_USER_ACTION


READINESS = {"status": "DONE", "missing": [], "missingSuggested": []}
SETUP_STATUS = {
    "isComplete": True,
    "steps": [
        {"step": "AGENT_CONNECTION", "status": "DONE", "isRequired": False},
        {
            "step": "AGENT_INTEGRATION",
            "status": "DONE",
            "isRequired": False,
            "agentIntegration": "GOOGLE_DRIVE",
        },
        {
            "step": "AGENT_INTEGRATION",
            "status": "PENDING_USER_ACTION",
            "isRequired": False,
            "agentIntegration": "NOTION",
            "installUrl": "https://app.example.test/settings/integrations",
            "detail": "Reconnect Notion.",
        },
        {
            "step": "AGENT_INTEGRATION",
            "status": "DONE",
            "isRequired": False,
            "agentIntegration": "SOMETHING_NEW",
        },
    ],
    "products": {"codeIde": READINESS, "bolt": READINESS, "agent": READINESS},
}


def test_setup_status_names_the_agent_integration_of_each_row() -> None:
    admin, transport = _admin({"workspace": {"setupStatus": SETUP_STATUS}})

    status = admin.setup_status.get()

    assert status is not None and status.steps is not None
    assert [(s.step, s.status, s.agent_integration) for s in status.steps] == [
        (SetupStep.AGENT_CONNECTION, SetupStepStatus.DONE, None),
        (SetupStep.AGENT_INTEGRATION, SetupStepStatus.DONE, AgentIntegrationName.GOOGLE_DRIVE),
        (
            SetupStep.AGENT_INTEGRATION,
            SetupStepStatus.PENDING_USER_ACTION,
            AgentIntegrationName.NOTION,
        ),
        (SetupStep.AGENT_INTEGRATION, SetupStepStatus.DONE, AgentIntegrationName.UNKNOWN),
    ]
    assert "agentIntegration" in transport.calls[0].query


def test_setup_status_cli_names_the_agent_integration(monkeypatch: pytest.MonkeyPatch) -> None:
    admin, _ = _admin({"workspace": {"setupStatus": SETUP_STATUS}})
    monkeypatch.setattr(common, "get_admin", lambda: admin)
    tables: List[Any] = []
    monkeypatch.setattr(
        common.console, "table", lambda columns, rows, **_: tables.append(list(rows))
    )

    result = CliRunner().invoke(admin_cli, ["setup-status"])

    assert result.exit_code == 0, result.output
    rows = tables[0]
    assert [row[0] for row in rows] == [
        "AGENT_CONNECTION",
        "AGENT_INTEGRATION GOOGLE_DRIVE",
        "AGENT_INTEGRATION NOTION",
        "AGENT_INTEGRATION UNKNOWN",
    ]
    assert "https://app.example.test/settings/integrations" in rows[2][4]


def _cli(
    monkeypatch: pytest.MonkeyPatch, *responses: Dict[str, Any]
) -> "tuple[Any, List[Any], Any]":
    admin, transport = _admin(*responses)
    monkeypatch.setattr(common, "get_admin", lambda: admin)
    tables: List[Any] = []
    monkeypatch.setattr(
        common.console, "table", lambda columns, rows, **_: tables.append((columns, list(rows)))
    )
    return transport, tables, CliRunner()


def test_cli_integrations_set_reads_the_config_file(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Any
) -> None:
    saved = {
        "name": "METABASE",
        "status": "SYNCING",
        "settings": {
            "__typename": "MetabaseSettings",
            "host": "mb.example.test",
            "hasApiKey": True,
        },
    }
    transport, _, runner = _cli(monkeypatch, {"setIntegration": {"integration": saved}})
    config = tmp_path / "metabase.yml"
    config.write_text("metabase:\n  host: mb.example.test\n  apiKey: metabase-key\n")

    result = runner.invoke(
        admin_cli, ["integrations", "set", "--config", str(config), "--workspace", "w1", "--json"]
    )

    assert result.exit_code == 0, result.output
    assert transport.calls[0].variables == {
        "input": {"config": {"metabase": {"host": "mb.example.test", "apiKey": "metabase-key"}}}
    }
    assert transport.calls[0].workspace_uid == "w1"
    assert json.loads(result.output)["settings"]["hasApiKey"] is True
    assert "metabase-key" not in result.output


def test_cli_integrations_test_reads_stdin(monkeypatch: pytest.MonkeyPatch) -> None:
    transport, _, runner = _cli(
        monkeypatch,
        {"testIntegration": {"name": "PAGERDUTY", "testedAt": "2026-11-05T10:00:00Z"}},
    )

    result = runner.invoke(
        admin_cli,
        ["integrations", "test", "--config", "-"],
        input='{"pagerduty": {"api_region": "EU", "api_key": "pagerduty-key"}}',
    )

    assert result.exit_code == 0, result.output
    assert transport.calls[0].variables == {
        "input": {"config": {"pagerduty": {"apiRegion": "EU", "apiKey": "pagerduty-key"}}}
    }
    assert "PAGERDUTY" in result.output


@pytest.mark.parametrize(
    "config, wrong",
    [
        ("metabase:\n  hots: mb.example.test\n", "hots"),
        ("lookr:\n  baseUrl: https://l.example.test\n", "lookr"),
        ("- metabase\n", "one integration block"),
    ],
)
def test_cli_integrations_config_refuses_what_the_api_does_not_take(
    monkeypatch: pytest.MonkeyPatch, config: str, wrong: str
) -> None:
    transport, _, runner = _cli(monkeypatch)

    result = runner.invoke(admin_cli, ["integrations", "set", "--config", "-"], input=config)

    assert result.exit_code == 2
    assert wrong in result.output
    assert transport.calls == []


@pytest.mark.parametrize("command", ["set", "test"])
def test_cli_integrations_have_no_secret_options(command: str) -> None:
    result = CliRunner().invoke(admin_cli, ["integrations", command, "--help"])

    assert result.exit_code == 0
    assert "--api-key" not in result.output and "--password" not in result.output


def test_cli_integrations_list_shows_the_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    node = {
        "name": "NEW_RELIC",
        "status": "CONNECTED",
        "settings": {"__typename": "NewRelicSettings", "accountId": "1234567", "hasApiKey": True},
    }
    _, tables, runner = _cli(
        monkeypatch, {"workspace": {"integrations": _connection([node], None, False)}}
    )

    result = runner.invoke(admin_cli, ["integrations", "list"])

    assert result.exit_code == 0, result.output
    columns, rows = tables[0]
    assert columns == ["Name", "Status", "Sync progress", "Updated", "Settings"]
    assert rows == [["NEW_RELIC", "CONNECTED", "", "", "account_id=1234567, has_api_key=yes"]]


def test_cli_integrations_disconnect_takes_a_new_name(monkeypatch: pytest.MonkeyPatch) -> None:
    transport, _, runner = _cli(
        monkeypatch,
        {
            "disconnectIntegration": {
                "integration": {"name": "INCIDENT_IO", "status": "NOT_CONNECTED"}
            }
        },
    )

    result = runner.invoke(admin_cli, ["integrations", "disconnect", "incident_io", "--yes"])

    assert result.exit_code == 0, result.output
    assert transport.calls[0].variables == {"input": {"name": "INCIDENT_IO"}}
    assert "Disconnected INCIDENT_IO." in result.output


@pytest.mark.parametrize(
    "group, name", [("integrations", "OMNI"), ("agent-integrations", "NOTION")]
)
def test_cli_disconnect_asks_first_and_refuses_unknown(group: str, name: str) -> None:
    declined = CliRunner().invoke(admin_cli, [group, "disconnect", name], input="n\n")
    unknown = CliRunner().invoke(admin_cli, [group, "disconnect", "UNKNOWN", "--yes"])

    assert declined.exit_code == 1 and "Aborted" in declined.output
    assert unknown.exit_code == 2


def test_cli_agent_integrations_list_without_install_urls(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transport, tables, runner = _cli(
        monkeypatch, {"workspace": {"agentIntegrations": _connection(AGENT_NODES, None, False)}}
    )

    result = runner.invoke(admin_cli, ["agent-integrations", "list"])

    assert result.exit_code == 0, result.output
    columns, rows = tables[0]
    assert columns == ["Name", "Display name", "Status", "Reconnect required"]
    assert rows[1] == ["NOTION", "Notion", "PENDING_USER_ACTION", "yes"]
    assert "installUrl" not in transport.calls[0].query


def test_cli_agent_integrations_list_with_install_urls_as_json(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    node = {**AGENT_NODES[1], "installUrl": "https://connect.example.test/session"}
    transport, _, runner = _cli(
        monkeypatch, {"workspace": {"agentIntegrations": _connection([node], None, False)}}
    )

    result = runner.invoke(admin_cli, ["agent-integrations", "list", "--install-urls", "--json"])

    assert result.exit_code == 0, result.output
    [listed] = json.loads(result.output)
    assert listed["installUrl"] == "https://connect.example.test/session"
    assert transport.calls[0].linked_fields == AGENT_INTEGRATION_LINKS


def test_cli_agent_integrations_disconnect(monkeypatch: pytest.MonkeyPatch) -> None:
    disconnected = {"name": "JIRA_CLOUD", "status": "PENDING_USER_ACTION"}
    transport, _, runner = _cli(
        monkeypatch, {"disconnectAgentIntegration": {"agentIntegration": disconnected}}
    )

    result = runner.invoke(
        admin_cli, ["agent-integrations", "disconnect", "jira_cloud", "--yes", "--workspace", "w1"]
    )

    assert result.exit_code == 0, result.output
    assert transport.calls[0].variables == {"input": {"name": "JIRA_CLOUD"}}
    assert transport.calls[0].workspace_uid == "w1"
    assert "Disconnected JIRA_CLOUD (PENDING_USER_ACTION)." in result.output
