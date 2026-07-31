"""Behaviour tests for the smaller sub-clients.

Same contract as the bolt tests: assert the request that goes on the wire and the
model that comes back, so the mapping refactor is verifiable.
"""

import json

import pytest
import responses
from tests.unit.constants import API_ENDPOINT, WORKSPACE_TOKEN

from paradime.apis.users.types import UserAccountType
from paradime.client.paradime_client import Paradime


@pytest.fixture
def paradime(monkeypatch):
    monkeypatch.setenv("PARADIME_DISABLE_VERSION_CHECK", "1")
    return Paradime(api_secret=WORKSPACE_TOKEN, api_endpoint=API_ENDPOINT)


def _gql(data):
    responses.add(responses.POST, API_ENDPOINT, json={"data": data}, status=200)


def _sent(call_index=0):
    return json.loads(responses.calls[call_index].request.body)


ACTIVE_USER = {
    "uid": "u-1",
    "email": "someone@paradime.io",
    "name": "Someone",
    "accountType": "ADMIN",
}


class TestUsers:
    @responses.activate
    def test_list_active_parses_users(self, paradime):
        _gql({"listUsers": {"activeUsers": [ACTIVE_USER]}})

        users = paradime.users.list_active()

        assert len(users) == 1
        assert users[0].uid == "u-1"
        assert users[0].account_type == "ADMIN"

    @responses.activate
    def test_list_invited_parses_invite_status(self, paradime):
        _gql(
            {
                "listUsers": {
                    "invitedUsers": [
                        {
                            "email": "new@paradime.io",
                            "accountType": "DEVELOPER",
                            "inviteStatus": "SENT",
                        }
                    ]
                }
            }
        )

        invited = paradime.users.list_invited()

        assert invited[0].email == "new@paradime.io"
        assert invited[0].invite_status == "SENT"

    @responses.activate
    def test_get_by_email_filters_client_side(self, paradime):
        _gql(
            {
                "listUsers": {
                    "activeUsers": [
                        ACTIVE_USER,
                        dict(ACTIVE_USER, uid="u-2", email="other@paradime.io"),
                    ]
                }
            }
        )

        user = paradime.users.get_by_email("other@paradime.io")

        assert user.uid == "u-2"

    @responses.activate
    def test_get_by_email_raises_when_absent(self, paradime):
        _gql({"listUsers": {"activeUsers": [ACTIVE_USER]}})

        with pytest.raises(ValueError, match="No active user found"):
            paradime.users.get_by_email("nobody@paradime.io")

    @responses.activate
    def test_invite_sends_enum_value_not_enum(self, paradime):
        _gql({"inviteUser": {"ok": True}})

        paradime.users.invite("new@paradime.io", UserAccountType.DEVELOPER)

        assert _sent()["variables"] == {
            "email": "new@paradime.io",
            "accountType": "DEVELOPER",
        }

    @responses.activate
    def test_update_account_type_sends_uid(self, paradime):
        _gql({"updateUserAccountType": {"ok": True}})

        paradime.users.update_account_type("u-1", UserAccountType.ADMIN)

        assert _sent()["variables"] == {"uid": "u-1", "accountType": "ADMIN"}

    @responses.activate
    def test_disable_sends_uid(self, paradime):
        _gql({"disableUser": {"ok": True}})

        paradime.users.disable("u-1")

        assert _sent()["variables"] == {"uid": "u-1"}


class TestWorkspaces:
    @responses.activate
    def test_list_all_parses_workspaces(self, paradime):
        _gql({"listWorkspaces": {"workspaces": [{"uid": "w-1", "name": "Prod"}]}})

        workspaces = paradime.workspaces.list_all()

        assert workspaces[0].uid == "w-1"
        assert workspaces[0].name == "Prod"


class TestCatalog:
    @responses.activate
    def test_refresh_issues_mutation(self, paradime):
        _gql({"refreshCatalog": {"ok": True}})

        paradime.catalog.refresh()

        assert "refreshCatalog" in _sent()["query"]


class TestAuditLog:
    @responses.activate
    def test_get_all_parses_timestamps(self, paradime):
        _gql(
            {
                "getAuditLogs": {
                    "auditLogs": [
                        {
                            "id": 1,
                            "createdDttm": "2026-07-30T09:00:00+00:00",
                            "updatedDttm": "2026-07-30T09:00:00+00:00",
                            "workspaceId": 1,
                            "workspaceName": "Prod",
                            "actorType": "USER",
                            "actorUserId": 5,
                            "actorEmail": "someone@paradime.io",
                            "eventSourceId": 2,
                            "eventSource": "BOLT",
                            "eventId": 3,
                            "eventType": "RUN_TRIGGERED",
                            "metadataJson": '{"a": 1}',
                        }
                    ]
                }
            }
        )

        logs = paradime.audit_log.get_all()

        assert logs[0].id == 1
        assert logs[0].workspace_name == "Prod"
        assert logs[0].created_dttm.year == 2026
        assert logs[0].metadata_json == '{"a": 1}'


class TestCustomIntegrationAddNodes:
    """add_nodes batches into snapshots of 10 and chains the snapshot id."""

    @staticmethod
    def _nodes(count):
        from paradime.apis.custom_integration.types import (
            Lineage,
            NodeChartLike,
            NodeChartLikeAttributes,
        )

        return [
            NodeChartLike(
                name=f"node-{i}",
                node_type="Chart",
                attributes=NodeChartLikeAttributes(),
                lineage=Lineage(),
            )
            for i in range(count)
        ]

    @responses.activate
    @pytest.mark.parametrize(
        "node_count,expected_requests",
        [(0, 1), (1, 1), (9, 1), (10, 1), (11, 2), (20, 2), (25, 3)],
    )
    def test_batches_without_trailing_empty_request(self, paradime, node_count, expected_requests):
        for i in range(expected_requests):
            _gql({"addCustomIntegrationNodes": {"snapshotId": i + 1}})

        paradime.custom_integration.add_nodes(
            integration_uid="int-1", nodes=self._nodes(node_count)
        )

        assert len(responses.calls) == expected_requests

    @responses.activate
    def test_chains_snapshot_id_and_flags_the_last_batch(self, paradime):
        _gql({"addCustomIntegrationNodes": {"snapshotId": 99}})
        _gql({"addCustomIntegrationNodes": {"snapshotId": 99}})

        paradime.custom_integration.add_nodes(integration_uid="int-1", nodes=self._nodes(15))

        first, second = _sent(0)["variables"], _sent(1)["variables"]
        assert first["snapshotId"] is None
        assert first["snapshotHasMoreNodes"] is True
        # The second request reuses the snapshot id the first one returned.
        assert second["snapshotId"] == 99
        assert second["snapshotHasMoreNodes"] is False
