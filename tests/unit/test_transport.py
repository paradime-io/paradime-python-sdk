"""Tests for retry behaviour, the error taxonomy, and session reuse."""

import json

import pytest
import requests
import responses
from tests.unit.constants import API_ENDPOINT, WORKSPACE_TOKEN

from paradime.client.api_client import APIClient
from paradime.client.api_exception import (
    ParadimeAPIException,
    ParadimeAuthException,
    ParadimeConnectionError,
    ParadimeException,
    ParadimeNotFoundException,
    ParadimeRateLimitException,
    ParadimeServerException,
    ParadimeTimeoutError,
)


@pytest.fixture
def client():
    """A client with retries disabled, so status mapping is tested in isolation."""
    return APIClient(api_secret=WORKSPACE_TOKEN, api_endpoint=API_ENDPOINT, max_retries=1)


def _add(status=200, body=None, headers=None):
    responses.add(
        responses.POST,
        API_ENDPOINT,
        json=body if body is not None else {"data": {"ok": True}},
        status=status,
        headers=headers,
    )


class TestErrorTaxonomy:
    @responses.activate
    @pytest.mark.parametrize(
        "status,expected",
        [
            (401, ParadimeAuthException),
            (403, ParadimeAuthException),
            (404, ParadimeNotFoundException),
            (429, ParadimeRateLimitException),
            (500, ParadimeServerException),
            (503, ParadimeServerException),
            (418, ParadimeAPIException),
        ],
    )
    def test_status_maps_to_exception_type(self, client, status, expected):
        _add(status=status, body={"message": "boom"})

        with pytest.raises(expected) as exc:
            client.execute("query Q { x }")

        assert exc.value.status_code == status

    @responses.activate
    def test_every_subclass_is_still_a_paradime_api_exception(self, client):
        """Existing `except ParadimeAPIException` code must keep working."""
        _add(status=401, body={"message": "boom"})

        with pytest.raises(ParadimeAPIException):
            client.execute("query Q { x }")

    @responses.activate
    def test_rate_limit_exposes_retry_after(self, client):
        _add(status=429, body={"message": "slow down"}, headers={"Retry-After": "7"})

        with pytest.raises(ParadimeRateLimitException) as exc:
            client.execute("query Q { x }")

        assert exc.value.retry_after == 7.0

    @responses.activate
    def test_rate_limit_without_retry_after_header(self, client):
        _add(status=429, body={"message": "slow down"})

        with pytest.raises(ParadimeRateLimitException) as exc:
            client.execute("query Q { x }")

        assert exc.value.retry_after is None

    @responses.activate
    def test_request_id_is_captured(self, client):
        _add(status=500, body={"message": "boom"}, headers={"X-Request-Id": "req-123"})

        with pytest.raises(ParadimeServerException) as exc:
            client.execute("query Q { x }")

        assert exc.value.request_id == "req-123"

    @responses.activate
    def test_graphql_body_errors_are_exposed(self, client):
        _add(body={"errors": [{"message": "bad field", "path": ["x"]}]})

        with pytest.raises(ParadimeAPIException) as exc:
            client.execute("query Q { x }")

        assert str(exc.value) == "bad field"
        assert exc.value.errors == [{"message": "bad field", "path": ["x"]}]

    @responses.activate
    def test_non_json_success_body_raises_paradime_exception(self, client):
        """A 200 with an HTML body (a proxy error page) must not leak JSONDecodeError."""
        responses.add(
            responses.POST,
            API_ENDPOINT,
            body="<html>gateway</html>",
            status=200,
            content_type="text/html",
        )

        with pytest.raises(ParadimeAPIException, match="not valid JSON"):
            client.execute("query Q { x }")

    @responses.activate
    def test_connection_error_is_wrapped(self, client):
        responses.add(
            responses.POST,
            API_ENDPOINT,
            body=requests.ConnectionError("refused"),
        )

        with pytest.raises(ParadimeConnectionError):
            client.execute("query Q { x }")

    @responses.activate
    def test_timeout_is_wrapped(self, client):
        responses.add(responses.POST, API_ENDPOINT, body=requests.Timeout("too slow"))

        with pytest.raises(ParadimeTimeoutError):
            client.execute("query Q { x }")

    @responses.activate
    def test_transport_errors_are_paradime_exceptions(self, client):
        """`except ParadimeException` should catch network failures too."""
        responses.add(responses.POST, API_ENDPOINT, body=requests.ConnectionError("refused"))

        with pytest.raises(ParadimeException):
            client.execute("query Q { x }")


class TestRetries:
    @pytest.fixture
    def retrying_client(self, monkeypatch):
        # Collapse the exponential backoff so the tests do not actually sleep.
        monkeypatch.setattr("tenacity.nap.time.sleep", lambda _: None)
        return APIClient(api_secret=WORKSPACE_TOKEN, api_endpoint=API_ENDPOINT, max_retries=3)

    @responses.activate
    def test_retries_503_then_succeeds(self, retrying_client):
        _add(status=503, body={"message": "unavailable"})
        _add(body={"data": {"ok": True}})

        assert retrying_client.execute("query Q { x }") == {"ok": True}
        assert len(responses.calls) == 2

    @responses.activate
    @pytest.mark.parametrize("status", [429, 502, 503, 504])
    def test_retryable_statuses(self, retrying_client, status):
        _add(status=status, body={"message": "retry me"})
        _add(body={"data": {"ok": True}})

        retrying_client.execute("query Q { x }")

        assert len(responses.calls) == 2

    @responses.activate
    @pytest.mark.parametrize("status", [400, 401, 403, 404, 500])
    def test_non_retryable_statuses_fail_immediately(self, retrying_client, status):
        """500 is excluded: every operation is a POST, so blind 5xx retries could
        re-run a mutation."""
        _add(status=status, body={"message": "nope"})

        with pytest.raises(ParadimeAPIException):
            retrying_client.execute("query Q { x }")

        assert len(responses.calls) == 1

    @responses.activate
    def test_gives_up_after_max_retries(self, retrying_client):
        for _ in range(3):
            _add(status=503, body={"message": "unavailable"})

        with pytest.raises(ParadimeServerException):
            retrying_client.execute("query Q { x }")

        assert len(responses.calls) == 3

    @responses.activate
    def test_retries_connection_errors(self, retrying_client):
        responses.add(responses.POST, API_ENDPOINT, body=requests.ConnectionError("refused"))
        _add(body={"data": {"ok": True}})

        retrying_client.execute("query Q { x }")

        assert len(responses.calls) == 2

    @responses.activate
    def test_graphql_body_errors_are_not_retried(self, retrying_client):
        """A GraphQL error is a 200 — retrying it would just repeat the same failure."""
        _add(body={"errors": [{"message": "bad field"}]})

        with pytest.raises(ParadimeAPIException):
            retrying_client.execute("query Q { x }")

        assert len(responses.calls) == 1

    @responses.activate
    def test_max_retries_one_disables_retrying(self):
        client = APIClient(api_secret=WORKSPACE_TOKEN, api_endpoint=API_ENDPOINT, max_retries=1)
        _add(status=503, body={"message": "unavailable"})

        with pytest.raises(ParadimeServerException):
            client.execute("query Q { x }")

        assert len(responses.calls) == 1

    def test_rejects_max_retries_below_one(self):
        with pytest.raises(ValueError, match="max_retries must be >= 1"):
            APIClient(api_secret=WORKSPACE_TOKEN, api_endpoint=API_ENDPOINT, max_retries=0)


class TestSessionAndCompat:
    def test_client_holds_a_single_session(self, client):
        assert isinstance(client.session, requests.Session)
        assert client.session is client.session

    def test_context_manager_closes_the_session(self):
        closed = []
        client = APIClient(api_secret=WORKSPACE_TOKEN, api_endpoint=API_ENDPOINT)
        client.session.close = lambda: closed.append(True)  # type: ignore[method-assign]

        with client:
            pass

        assert closed == [True]

    def test_close_releases_the_session(self):
        closed = []
        client = APIClient(api_secret=WORKSPACE_TOKEN, api_endpoint=API_ENDPOINT)
        client.session.close = lambda: closed.append(True)  # type: ignore[method-assign]

        client.close()

        assert closed == [True]

    @responses.activate
    def test_call_gql_still_works_as_an_alias(self, client):
        """Sub-clients and any external callers still use the old private name."""
        _add(body={"data": {"ok": True}})

        assert client._call_gql("query Q { x }", {"a": 1}) == {"ok": True}
        assert json.loads(responses.calls[0].request.body)["variables"] == {"a": 1}

    @responses.activate
    def test_execute_defaults_variables_to_empty_dict(self, client):
        _add(body={"data": {"ok": True}})

        client.execute("query Q { x }")

        assert json.loads(responses.calls[0].request.body)["variables"] == {}
