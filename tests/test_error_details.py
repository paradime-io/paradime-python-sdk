import pickle
from typing import Any

import pytest
import requests

from paradime.apis.admin import transport as transport_module
from paradime.apis.admin.exception import AdminNotFoundException, AdminRateLimitedException
from paradime.apis.admin.transport import AdminTransport
from paradime.client import api_client as api_client_module
from paradime.client.api_client import APIClient
from paradime.client.api_exception import ParadimeAPIException, ParadimeRateLimitedException

FAKE_TOKEN = "FAKECOMPANYTOKEN123"
ENDPOINT = f"https://api.example.test/api/v1/{FAKE_TOKEN}/graphql"


def _response(status_code: int, text: str) -> requests.Response:
    response = requests.Response()
    response.status_code = status_code
    response.reason = "Error"
    response.url = ENDPOINT
    response._content = text.encode()
    return response


def _client() -> APIClient:
    return APIClient(api_secret="prdm_wsp_secret", api_endpoint=ENDPOINT, max_rate_limit_retries=0)


def _post(monkeypatch: pytest.MonkeyPatch, response: requests.Response) -> None:
    monkeypatch.setattr(api_client_module.requests, "post", lambda **kwargs: response)


def test_an_http_error_carries_its_status_and_redacted_body(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _post(monkeypatch, _response(503, f"Unavailable at /api/v1/{FAKE_TOKEN}/graphql"))

    with pytest.raises(ParadimeAPIException) as exc_info:
        _client()._call_gql("query q { x }")

    error = exc_info.value
    assert error.status_code == 503
    assert error.response_text == "Unavailable at /api/v1/***/graphql"
    assert str(error) == "Error: 503 - Unavailable at /api/v1/***/graphql"


def test_a_rate_limit_carries_its_status_and_body(monkeypatch: pytest.MonkeyPatch) -> None:
    _post(monkeypatch, _response(429, "slow down"))

    with pytest.raises(ParadimeRateLimitedException) as exc_info:
        _client()._call_gql("query q { x }")

    assert exc_info.value.status_code == 429
    assert exc_info.value.response_text == "slow down"


def test_a_graphql_error_has_no_http_status(monkeypatch: pytest.MonkeyPatch) -> None:
    response = _response(200, '{"errors": [{"message": "nope"}]}')
    _post(monkeypatch, response)

    with pytest.raises(ParadimeAPIException) as exc_info:
        _client()._call_gql("query q { x }")

    assert exc_info.value.status_code is None and exc_info.value.response_text is None


def test_an_unreachable_api_error_drops_the_request_and_response(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def unreachable(**kwargs: Any) -> Any:
        raise requests.exceptions.ConnectionError(
            f"Max retries exceeded with url: /api/v1/{FAKE_TOKEN}/graphql",
            request=requests.Request("POST", ENDPOINT).prepare(),
            response=_response(502, "bad gateway"),
        )

    monkeypatch.setattr(api_client_module.requests, "post", unreachable)

    with pytest.raises(requests.exceptions.ConnectionError) as exc_info:
        _client()._call_gql("query q { x }")

    assert exc_info.value.request is None and exc_info.value.response is None
    assert str(exc_info.value) == "Max retries exceeded with url: /api/v1/***/graphql"


def test_an_admin_http_error_carries_its_status(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        transport_module.requests, "post", lambda **kwargs: _response(502, "bad gateway")
    )

    with pytest.raises(ParadimeAPIException) as exc_info:
        AdminTransport(_client()).execute("query q { x }")

    assert exc_info.value.status_code == 502
    assert exc_info.value.response_text == "bad gateway"


def test_an_admin_bare_429_carries_its_status(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        transport_module.requests, "post", lambda **kwargs: _response(429, "too many")
    )

    with pytest.raises(AdminRateLimitedException) as exc_info:
        AdminTransport(_client()).execute("query q { x }")

    assert exc_info.value.status_code == 429
    assert exc_info.value.response_text == "too many"


def test_error_details_survive_pickling() -> None:
    error = ParadimeAPIException("Error: 503 - down", status_code=503, response_text="down")
    rate_limited = ParadimeRateLimitedException("slow", retry_after=2, status_code=429)
    admin = AdminNotFoundException("missing", status_code=404, response_text="missing")

    restored = pickle.loads(pickle.dumps(error))
    restored_rate_limited = pickle.loads(pickle.dumps(rate_limited))
    restored_admin = pickle.loads(pickle.dumps(admin))

    assert (restored.status_code, restored.response_text) == (503, "down")
    assert (restored_rate_limited.retry_after, restored_rate_limited.status_code) == (2, 429)
    assert (restored_admin.status_code, str(restored_admin)) == (404, "NOT_FOUND: missing")
