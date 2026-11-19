import json
import pickle
from typing import Any, Dict, List, Optional

import pytest
import requests

from paradime import Paradime
from paradime.apis.admin import transport as transport_module
from paradime.apis.admin.exception import AdminRateLimitedException
from paradime.apis.admin.transport import AdminTransport
from paradime.client import api_client as api_client_module
from paradime.client.api_client import APIClient
from paradime.client.api_exception import ParadimeAPIException, ParadimeRateLimitedException

ENDPOINT = "https://api.example.test/api/v1/company-token/graphql"


def _response(
    body: Any, *, status_code: int = 200, headers: Optional[Dict[str, str]] = None
) -> requests.Response:
    response = requests.Response()
    response.status_code = status_code
    response.url = ENDPOINT
    response.headers.update(headers or {})
    response._content = json.dumps(body).encode()
    return response


def _rate_limited(
    retry_after: Optional[int] = None, header: Optional[str] = None
) -> requests.Response:
    extensions: Dict[str, Any] = {"code": "RATE_LIMITED"}
    if retry_after is not None:
        extensions["retryAfter"] = retry_after
    return _response(
        {"errors": [{"message": "Too many requests.", "extensions": extensions}], "data": None},
        status_code=429,
        headers={"Retry-After": header} if header is not None else None,
    )


def _ok() -> requests.Response:
    return _response({"data": {"x": 1}})


class FakePost:
    def __init__(self, *responses: requests.Response) -> None:
        self.responses = list(responses)
        self.calls = 0

    def __call__(self, **kwargs: Any) -> requests.Response:
        self.calls += 1
        return self.responses.pop(0)


@pytest.fixture
def sleeps(monkeypatch: pytest.MonkeyPatch) -> List[float]:
    waits: List[float] = []
    monkeypatch.setattr(api_client_module.time, "sleep", waits.append)
    return waits


def _client(monkeypatch: pytest.MonkeyPatch, *responses: requests.Response, **kwargs: Any) -> Any:
    post = FakePost(*responses)
    monkeypatch.setattr(api_client_module.requests, "post", post)
    return APIClient(api_secret="prdm_wsp_secret", api_endpoint=ENDPOINT, **kwargs), post


def test_a_legacy_call_is_retried_after_the_wait_the_server_asks_for(
    monkeypatch: pytest.MonkeyPatch, sleeps: List[float]
) -> None:
    client, post = _client(monkeypatch, _rate_limited(retry_after=7, header="7"), _ok())

    assert client._call_gql("query q { x }") == {"x": 1}
    assert sleeps == [7] and post.calls == 2


def test_the_retry_after_header_is_used_when_the_body_has_none(
    monkeypatch: pytest.MonkeyPatch, sleeps: List[float]
) -> None:
    client, _ = _client(monkeypatch, _rate_limited(header="5"), _ok())

    client._call_gql("query q { x }")

    assert sleeps == [5]


def test_a_rate_limit_without_a_wait_retries_after_one_second(
    monkeypatch: pytest.MonkeyPatch, sleeps: List[float]
) -> None:
    client, _ = _client(monkeypatch, _rate_limited(), _ok())

    client._call_gql("query q { x }")

    assert sleeps == [1.0]


def test_a_rate_limit_in_a_200_body_is_retried_too(
    monkeypatch: pytest.MonkeyPatch, sleeps: List[float]
) -> None:
    limited = _response(
        {"errors": [{"message": "slow down", "extensions": {"code": "RATE_LIMITED"}}]}
    )
    client, _ = _client(monkeypatch, limited, _ok())

    assert client._call_gql("query q { x }") == {"x": 1}
    assert sleeps == [1.0]


def test_a_legacy_call_gives_up_after_three_retries(
    monkeypatch: pytest.MonkeyPatch, sleeps: List[float]
) -> None:
    client, post = _client(monkeypatch, *[_rate_limited(retry_after=2) for _ in range(4)])

    with pytest.raises(ParadimeRateLimitedException) as exc_info:
        client._call_gql("query q { x }")

    assert isinstance(exc_info.value, ParadimeAPIException)
    assert str(exc_info.value).startswith("Error: 429 - ")
    assert exc_info.value.retry_after == 2
    assert sleeps == [2, 2, 2] and post.calls == 4


def test_a_wait_longer_than_the_cap_raises_without_waiting(
    monkeypatch: pytest.MonkeyPatch, sleeps: List[float]
) -> None:
    client, post = _client(monkeypatch, _rate_limited(retry_after=600))

    with pytest.raises(ParadimeRateLimitedException):
        client._call_gql("query q { x }")

    assert sleeps == [] and post.calls == 1


def test_the_client_limits_are_configurable(
    monkeypatch: pytest.MonkeyPatch, sleeps: List[float]
) -> None:
    client, post = _client(
        monkeypatch,
        _rate_limited(retry_after=4),
        _rate_limited(retry_after=4),
        max_rate_limit_retries=1,
        max_retry_wait=5,
    )

    with pytest.raises(ParadimeRateLimitedException):
        client._call_gql("query q { x }")

    assert sleeps == [4] and post.calls == 2


def test_other_errors_are_not_retried(monkeypatch: pytest.MonkeyPatch, sleeps: List[float]) -> None:
    client, post = _client(monkeypatch, _response({"errors": [{"message": "nope"}]}))

    with pytest.raises(ParadimeAPIException, match="nope") as exc_info:
        client._call_gql("query q { x }")

    assert not isinstance(exc_info.value, ParadimeRateLimitedException)
    assert sleeps == [] and post.calls == 1


def test_the_admin_transport_uses_the_client_limits(monkeypatch: pytest.MonkeyPatch) -> None:
    post = FakePost(_rate_limited(retry_after=1), _rate_limited(retry_after=1))
    monkeypatch.setattr(transport_module.requests, "post", post)
    client = APIClient(
        api_secret="prdm_cmp_secret", api_endpoint=ENDPOINT, max_rate_limit_retries=1
    )
    waits: List[float] = []

    with pytest.raises(AdminRateLimitedException):
        AdminTransport(client, sleep=waits.append).execute("query q { x }")

    assert waits == [1] and post.calls == 2


def test_paradime_takes_the_retry_settings_for_legacy_and_admin_calls(
    monkeypatch: pytest.MonkeyPatch, sleeps: List[float]
) -> None:
    monkeypatch.setenv("PARADIME_DISABLE_VERSION_CHECK", "1")
    post = FakePost(_rate_limited(retry_after=1), _rate_limited(retry_after=1))
    monkeypatch.setattr(api_client_module.requests, "post", post)
    paradime = Paradime(
        api_secret="prdm_cmp_secret",
        api_endpoint=ENDPOINT,
        workspace_uid="w1",
        max_rate_limit_retries=0,
        max_retry_wait=5,
    )
    admin_waits: List[float] = []
    paradime.admin.transport.sleep = admin_waits.append

    with pytest.raises(ParadimeRateLimitedException):
        paradime.bolt.trigger_run(slug="daily")
    with pytest.raises(AdminRateLimitedException):
        paradime.admin.transport.execute("query q { x }")

    assert sleeps == [] and admin_waits == [] and post.calls == 2
    assert paradime.max_retry_wait == 5 and paradime.admin.transport.max_retry_wait == 5


def test_paradime_takes_the_request_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PARADIME_DISABLE_VERSION_CHECK", "1")
    timeouts: List[Any] = []

    def post(**kwargs: Any) -> requests.Response:
        timeouts.append(kwargs["timeout"])
        return _response({"data": {"triggerBoltRun": {"runId": 1}}})

    monkeypatch.setattr(api_client_module.requests, "post", post)
    paradime = Paradime(
        api_secret="prdm_cmp_secret", api_endpoint=ENDPOINT, workspace_uid="w1", timeout=5
    )

    paradime.bolt.trigger_run(slug="daily")
    paradime.admin.transport.execute("query q { triggerBoltRun { runId } }")

    assert timeouts == [5, 5]


def test_the_admin_transport_reads_settings_changed_after_construction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("PARADIME_DISABLE_VERSION_CHECK", "1")
    post = FakePost(_rate_limited(retry_after=1))
    monkeypatch.setattr(transport_module.requests, "post", post)
    paradime = Paradime(api_secret="prdm_cmp_secret", api_endpoint=ENDPOINT)
    waits: List[float] = []
    paradime.admin.transport.sleep = waits.append

    paradime.max_rate_limit_retries = 0

    with pytest.raises(AdminRateLimitedException):
        paradime.admin.transport.execute("query q { x }")
    assert waits == [] and post.calls == 1


def test_an_admin_transport_override_wins_over_the_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    post = FakePost(_rate_limited(retry_after=1))
    monkeypatch.setattr(transport_module.requests, "post", post)
    client = APIClient(
        api_secret="prdm_cmp_secret", api_endpoint=ENDPOINT, max_rate_limit_retries=3
    )
    waits: List[float] = []
    transport = AdminTransport(client, max_rate_limit_retries=0, sleep=waits.append)

    client.max_rate_limit_retries = 5

    with pytest.raises(AdminRateLimitedException):
        transport.execute("query q { x }")
    assert waits == [] and post.calls == 1 and transport.max_rate_limit_retries == 0


def test_the_admin_rate_limit_is_the_shared_rate_limit() -> None:
    error = AdminRateLimitedException("Slow down.", retry_after=7)

    assert isinstance(error, ParadimeRateLimitedException)
    assert error.retry_after == 7


def test_a_rate_limit_survives_pickling() -> None:
    restored = pickle.loads(pickle.dumps(ParadimeRateLimitedException("Slow.", retry_after=3)))

    assert str(restored) == "Slow." and restored.retry_after == 3
