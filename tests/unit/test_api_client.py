import json

import pytest
import responses
from tests.unit.constants import (
    API_ENDPOINT,
    COMPANY_TOKEN,
    LEGACY_KEY,
    LEGACY_SECRET,
    WORKSPACE_TOKEN,
)

from paradime.client.api_client import APIClient
from paradime.client.api_exception import ParadimeAPIException


def _register_gql(body=None, status=200, content_type="application/json"):
    """Register a single GraphQL response and return the responses mock."""
    if body is None:
        body = {"data": {"ok": True}}
    responses.add(
        responses.POST,
        API_ENDPOINT,
        body=json.dumps(body) if isinstance(body, (dict, list)) else body,
        status=status,
        content_type=content_type,
    )


class TestCredentialValidation:
    def test_workspace_token_needs_no_api_key(self):
        client = APIClient(api_secret=WORKSPACE_TOKEN, api_endpoint=API_ENDPOINT)
        assert client.api_key is None

    def test_company_token_requires_workspace_uid(self):
        with pytest.raises(ValueError, match="workspace_uid is required"):
            APIClient(api_secret=COMPANY_TOKEN, api_endpoint=API_ENDPOINT)

    def test_company_token_with_workspace_uid_is_accepted(self):
        client = APIClient(
            api_secret=COMPANY_TOKEN,
            workspace_uid="wsp-1",
            api_endpoint=API_ENDPOINT,
        )
        assert client.workspace_uid == "wsp-1"

    def test_legacy_secret_requires_api_key(self):
        with pytest.raises(ValueError, match="api_key is required"):
            APIClient(api_secret=LEGACY_SECRET, api_endpoint=API_ENDPOINT)

    def test_legacy_key_and_secret_is_accepted(self):
        client = APIClient(
            api_key=LEGACY_KEY,
            api_secret=LEGACY_SECRET,
            api_endpoint=API_ENDPOINT,
        )
        assert client.api_key == LEGACY_KEY


class TestAuthHeaders:
    def test_workspace_token_sends_bearer(self, api_client):
        headers = api_client._get_request_headers()
        assert headers["Authorization"] == f"Bearer {WORKSPACE_TOKEN}"
        assert "X-API-KEY" not in headers
        assert "X-API-SECRET" not in headers

    def test_company_token_sends_bearer_and_workspace_header(self):
        client = APIClient(
            api_secret=COMPANY_TOKEN,
            workspace_uid="wsp-1",
            api_endpoint=API_ENDPOINT,
        )
        headers = client._get_request_headers()
        assert headers["Authorization"] == f"Bearer {COMPANY_TOKEN}"
        assert headers["X-Paradime-Workspace"] == "wsp-1"

    def test_legacy_pair_sends_key_and_secret(self):
        client = APIClient(
            api_key=LEGACY_KEY,
            api_secret=LEGACY_SECRET,
            api_endpoint=API_ENDPOINT,
        )
        headers = client._get_request_headers()
        assert headers["X-API-KEY"] == LEGACY_KEY
        assert headers["X-API-SECRET"] == LEGACY_SECRET
        assert "Authorization" not in headers

    def test_sdk_version_header_is_always_sent(self, api_client):
        assert "X-PYTHON-SDK-VERSION" in api_client._get_request_headers()


class TestTelemetryHeaders:
    def test_telemetry_headers_present_by_default(self, api_client):
        headers = api_client._get_request_headers()
        assert "X-PYTHON-VERSION" in headers
        assert headers["X-PARADIME-RUNTIME"] == "local"

    @pytest.mark.parametrize("value", ["true", "TRUE", "1", "yes"])
    def test_opt_out_suppresses_telemetry_headers(self, api_client, monkeypatch, value):
        monkeypatch.setenv("PARADIME_DISABLE_TELEMETRY", value)
        headers = api_client._get_request_headers()
        assert "X-PYTHON-VERSION" not in headers
        assert "X-PARADIME-RUNTIME" not in headers

    def test_runtime_detection_reports_github_actions(self, api_client, monkeypatch):
        monkeypatch.setenv("GITHUB_ACTIONS", "true")
        assert api_client._get_request_headers()["X-PARADIME-RUNTIME"] == "github-actions"


class TestCallGql:
    @responses.activate
    def test_posts_query_and_variables_and_returns_data(self, api_client):
        _register_gql({"data": {"listUsers": {"activeUsers": []}}})

        result = api_client._call_gql("query Q { x }", {"a": 1})

        assert result == {"listUsers": {"activeUsers": []}}
        sent = json.loads(responses.calls[0].request.body)
        assert sent == {"query": "query Q { x }", "variables": {"a": 1}}

    @responses.activate
    def test_sends_auth_header_on_the_wire(self, api_client):
        _register_gql()
        api_client._call_gql("query Q { x }")
        assert responses.calls[0].request.headers["Authorization"] == f"Bearer {WORKSPACE_TOKEN}"

    @responses.activate
    def test_http_error_raises_paradime_exception(self, api_client):
        _register_gql({"message": "nope"}, status=401)

        with pytest.raises(ParadimeAPIException) as exc:
            api_client._call_gql("query Q { x }")

        assert "401" in str(exc.value)

    @responses.activate
    def test_graphql_body_error_raises_with_first_message(self, api_client):
        _register_gql({"errors": [{"message": "schedule not found"}]})

        with pytest.raises(ParadimeAPIException, match="schedule not found"):
            api_client._call_gql("query Q { x }")
