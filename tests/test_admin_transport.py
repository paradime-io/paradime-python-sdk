from typing import Any, Dict, List, Optional

import pytest
import requests

from paradime.apis.admin import transport as transport_module
from paradime.apis.admin.exception import (
    AdminApiException,
    AdminConflictException,
    AdminConnectionTestFailedException,
    AdminInternalException,
    AdminMissingCapabilityException,
    AdminNotFoundException,
    AdminPlanUpgradeRequiredException,
    AdminRateLimitedException,
    AdminUnauthenticatedException,
    AdminValidationFailedException,
    AdminWorkspaceRequiredException,
    ErrorCode,
)
from paradime.apis.admin.transport import AdminTransport
from paradime.client.api_client import APIClient
from paradime.client.api_exception import ParadimeAPIException

ENDPOINT = "https://api.example.test/api/v1/company-token/graphql"


class FakeResponse:
    def __init__(
        self,
        body: Any = None,
        *,
        status_code: int = 200,
        headers: Optional[Dict[str, str]] = None,
        text: str = "",
    ) -> None:
        self.body = body
        self.status_code = status_code
        self.headers = headers or {}
        self.text = text

    @property
    def ok(self) -> bool:
        return self.status_code < 400

    def json(self) -> Any:
        if self.body is None:
            raise ValueError("no JSON")
        return self.body


class FakePost:
    def __init__(self, *responses: FakeResponse) -> None:
        self.responses = list(responses)
        self.requests: List[Dict[str, Any]] = []

    def __call__(self, **kwargs: Any) -> FakeResponse:
        self.requests.append(kwargs)
        return self.responses.pop(0)


def _error(code: Optional[str], message: str = "went wrong", **extensions: Any) -> Dict[str, Any]:
    if code is not None:
        extensions["code"] = code
    return {"errors": [{"message": message, "extensions": extensions}], "data": None}


@pytest.fixture
def sleeps() -> List[float]:
    return []


def _transport(
    monkeypatch: pytest.MonkeyPatch,
    sleeps: List[float],
    *responses: FakeResponse,
    **kwargs: Any,
) -> "tuple[AdminTransport, FakePost]":
    post = FakePost(*responses)
    monkeypatch.setattr(transport_module.requests, "post", post)
    client = APIClient(
        api_secret="prdm_cmp_secret", workspace_uid="w-default", api_endpoint=ENDPOINT
    )
    return AdminTransport(client, sleep=sleeps.append, **kwargs), post


def test_execute_posts_the_query_and_returns_data(
    monkeypatch: pytest.MonkeyPatch, sleeps: List[float]
) -> None:
    transport, post = _transport(monkeypatch, sleeps, FakeResponse({"data": {"workspace": {}}}))

    data = transport.execute("query q { workspace { uid } }", {"uid": "w1"})

    assert data == {"workspace": {}}
    request = post.requests[0]
    assert request["url"] == ENDPOINT
    assert request["json"] == {"query": "query q { workspace { uid } }", "variables": {"uid": "w1"}}
    assert request["headers"]["Authorization"] == "Bearer prdm_cmp_secret"
    assert request["headers"]["X-Paradime-Workspace"] == "w-default"


def test_workspace_uid_overrides_the_workspace_header_for_one_call(
    monkeypatch: pytest.MonkeyPatch, sleeps: List[float]
) -> None:
    transport, post = _transport(
        monkeypatch, sleeps, FakeResponse({"data": {}}), FakeResponse({"data": {}})
    )

    transport.execute("query q { x }", workspace_uid="w-other")
    transport.execute("query q { x }")

    assert post.requests[0]["headers"]["X-Paradime-Workspace"] == "w-other"
    assert post.requests[1]["headers"]["X-Paradime-Workspace"] == "w-default"


@pytest.mark.parametrize(
    "code, exception_class",
    [
        ("UNAUTHENTICATED", AdminUnauthenticatedException),
        ("MISSING_CAPABILITY", AdminMissingCapabilityException),
        ("WORKSPACE_REQUIRED", AdminWorkspaceRequiredException),
        ("NOT_FOUND", AdminNotFoundException),
        ("VALIDATION_FAILED", AdminValidationFailedException),
        ("CONNECTION_TEST_FAILED", AdminConnectionTestFailedException),
        ("CONFLICT", AdminConflictException),
        ("PLAN_UPGRADE_REQUIRED", AdminPlanUpgradeRequiredException),
        ("INTERNAL", AdminInternalException),
    ],
)
def test_each_error_code_raises_its_exception(
    monkeypatch: pytest.MonkeyPatch, sleeps: List[float], code: str, exception_class: type
) -> None:
    transport, _ = _transport(monkeypatch, sleeps, FakeResponse(_error(code, "no luck")))

    with pytest.raises(exception_class) as exc_info:
        transport.execute("query q { x }")

    assert exc_info.value.code == ErrorCode(code)
    assert exc_info.value.message == "no luck"
    assert isinstance(exc_info.value, ParadimeAPIException)
    assert sleeps == []


def test_every_error_code_has_an_exception() -> None:
    from paradime.apis.admin.exception import EXCEPTIONS_BY_CODE

    assert set(EXCEPTIONS_BY_CODE) == set(ErrorCode)


def test_the_input_path_is_kept_on_the_exception(
    monkeypatch: pytest.MonkeyPatch, sleeps: List[float]
) -> None:
    transport, _ = _transport(
        monkeypatch,
        sleeps,
        FakeResponse(
            _error(
                "VALIDATION_FAILED",
                "A value can't be empty.",
                field=["input", "variables", 0, "value"],
            )
        ),
    )

    with pytest.raises(AdminValidationFailedException) as exc_info:
        transport.execute("mutation m { x }")

    assert exc_info.value.field == ["input", "variables", "0", "value"]
    assert str(exc_info.value) == (
        "VALIDATION_FAILED: A value can't be empty. (field: input.variables.0.value)"
    )


def test_a_field_sent_as_a_string_becomes_a_path(
    monkeypatch: pytest.MonkeyPatch, sleeps: List[float]
) -> None:
    transport, _ = _transport(
        monkeypatch, sleeps, FakeResponse(_error("VALIDATION_FAILED", "Bad cursor.", field="after"))
    )

    with pytest.raises(AdminValidationFailedException) as exc_info:
        transport.execute("query q { x }")

    assert exc_info.value.field == ["after"]


def test_an_error_without_a_known_code_is_the_base_exception(
    monkeypatch: pytest.MonkeyPatch, sleeps: List[float]
) -> None:
    transport, _ = _transport(
        monkeypatch,
        sleeps,
        FakeResponse(
            {"errors": [{"message": "Cannot query field 'x'."}, {"message": "second"}]},
            status_code=400,
        ),
    )

    with pytest.raises(AdminApiException) as exc_info:
        transport.execute("query q { x }")

    assert type(exc_info.value) is AdminApiException
    assert exc_info.value.code is None
    assert str(exc_info.value) == "Cannot query field 'x'."
    assert len(exc_info.value.errors) == 2


def test_an_unknown_code_is_kept_in_extensions(
    monkeypatch: pytest.MonkeyPatch, sleeps: List[float]
) -> None:
    transport, _ = _transport(monkeypatch, sleeps, FakeResponse(_error("SOMETHING_NEW")))

    with pytest.raises(AdminApiException) as exc_info:
        transport.execute("query q { x }")

    assert exc_info.value.code is None
    assert exc_info.value.extensions["code"] == "SOMETHING_NEW"


def test_an_http_error_without_a_graphql_body_raises_the_api_exception(
    monkeypatch: pytest.MonkeyPatch, sleeps: List[float]
) -> None:
    transport, _ = _transport(
        monkeypatch, sleeps, FakeResponse(None, status_code=502, text="Bad Gateway")
    )

    with pytest.raises(ParadimeAPIException, match="502 - Bad Gateway"):
        transport.execute("query q { x }")


def _rate_limited(retry_after: Optional[int] = None, header: Optional[str] = None) -> FakeResponse:
    extensions: Dict[str, Any] = {"budget": "read"}
    if retry_after is not None:
        extensions["retryAfter"] = retry_after
    return FakeResponse(
        _error("RATE_LIMITED", "Too many requests.", **extensions),
        status_code=429,
        headers={"Retry-After": header} if header is not None else {},
    )


def test_a_rate_limited_request_is_retried_after_retry_after(
    monkeypatch: pytest.MonkeyPatch, sleeps: List[float]
) -> None:
    transport, post = _transport(
        monkeypatch,
        sleeps,
        _rate_limited(retry_after=7, header="7"),
        _rate_limited(retry_after=2, header="2"),
        FakeResponse({"data": {"ok": 1}}),
    )

    assert transport.execute("mutation m { x }") == {"ok": 1}
    assert sleeps == [7, 2]
    assert len(post.requests) == 3


def test_the_retry_after_header_is_used_when_the_body_has_no_retry_after(
    monkeypatch: pytest.MonkeyPatch, sleeps: List[float]
) -> None:
    transport, _ = _transport(
        monkeypatch,
        sleeps,
        FakeResponse(None, status_code=429, headers={"Retry-After": "5"}),
        _rate_limited(header="3"),
        FakeResponse({"data": {}}),
    )

    transport.execute("query q { x }")

    assert sleeps == [5, 3]


def test_a_rate_limit_in_a_200_body_is_retried_too(
    monkeypatch: pytest.MonkeyPatch, sleeps: List[float]
) -> None:
    transport, _ = _transport(
        monkeypatch,
        sleeps,
        FakeResponse(_error("RATE_LIMITED", "slow down", retryAfter=1)),
        FakeResponse({"data": {}}),
    )

    transport.execute("query q { x }")

    assert sleeps == [1]


def test_the_wait_defaults_to_one_second_when_the_server_gives_none(
    monkeypatch: pytest.MonkeyPatch, sleeps: List[float]
) -> None:
    transport, _ = _transport(monkeypatch, sleeps, _rate_limited(), FakeResponse({"data": {}}))

    transport.execute("query q { x }")

    assert sleeps == [1]


def test_a_retry_after_longer_than_the_cap_raises_without_waiting(
    monkeypatch: pytest.MonkeyPatch, sleeps: List[float]
) -> None:
    transport, post = _transport(
        monkeypatch,
        sleeps,
        _rate_limited(retry_after=600),
        FakeResponse({"data": {}}),
        max_retry_wait=30,
    )

    with pytest.raises(AdminRateLimitedException) as exc_info:
        transport.execute("query q { x }")

    assert exc_info.value.retry_after == 600
    assert sleeps == []
    assert len(post.requests) == 1


def test_retries_are_bounded(monkeypatch: pytest.MonkeyPatch, sleeps: List[float]) -> None:
    transport, post = _transport(
        monkeypatch,
        sleeps,
        *[_rate_limited(retry_after=4) for _ in range(3)],
        max_rate_limit_retries=2,
    )

    with pytest.raises(AdminRateLimitedException) as exc_info:
        transport.execute("query q { x }")

    assert exc_info.value.retry_after == 4
    assert exc_info.value.code == ErrorCode.RATE_LIMITED
    assert sleeps == [4, 4]
    assert len(post.requests) == 3


def test_other_errors_are_not_retried(monkeypatch: pytest.MonkeyPatch, sleeps: List[float]) -> None:
    transport, post = _transport(monkeypatch, sleeps, FakeResponse(_error("CONFLICT")))

    with pytest.raises(AdminConflictException):
        transport.execute("mutation m { x }")

    assert sleeps == []
    assert len(post.requests) == 1


def test_admin_exceptions_survive_pickling() -> None:
    import pickle

    from paradime.apis.admin.exception import AdminJobTimeoutException
    from paradime.apis.admin.jobs.types import AdminJob

    job = AdminJob(id="job-1", kind="checkGitRepository", status="RUNNING")
    validation = AdminValidationFailedException(
        "Bad value.",
        field=["input", "name"],
        extensions={"code": "VALIDATION_FAILED", "field": ["input", "name"]},
        errors=[{"message": "Bad value."}],
    )
    validation.job = job
    rate_limited = AdminRateLimitedException("Slow down.", retry_after=7)
    timeout = AdminJobTimeoutException("Still running.", job=job)

    restored_validation = pickle.loads(pickle.dumps(validation))
    restored_rate_limited = pickle.loads(pickle.dumps(rate_limited))
    restored_timeout = pickle.loads(pickle.dumps(timeout))

    assert type(restored_validation) is AdminValidationFailedException
    assert restored_validation.code == ErrorCode.VALIDATION_FAILED
    assert restored_validation.field == ["input", "name"]
    assert restored_validation.extensions == validation.extensions
    assert restored_validation.errors == [{"message": "Bad value."}]
    assert restored_validation.job.id == "job-1"
    assert str(restored_validation) == str(validation)
    assert restored_rate_limited.retry_after == 7
    assert restored_timeout.job.status == job.status
    assert str(restored_timeout) == "Still running."


FAKE_TOKEN = "FAKECOMPANYTOKEN123"
TOKEN_ENDPOINT = f"https://api.example.test/api/v1/{FAKE_TOKEN}/graphql"


@pytest.mark.parametrize(
    "error",
    [
        requests.exceptions.ConnectionError(
            f"HTTPSConnectionPool(host='api.example.test', port=443): Max retries exceeded "
            f"with url: /api/v1/{FAKE_TOKEN}/graphql"
        ),
        requests.exceptions.ReadTimeout(f"Read timed out. (url={TOKEN_ENDPOINT})"),
    ],
)
def test_a_connection_error_keeps_the_company_token_out(
    monkeypatch: pytest.MonkeyPatch, error: Exception
) -> None:
    import traceback

    def unreachable(**kwargs: Any) -> None:
        raise error

    monkeypatch.setattr(transport_module.requests, "post", unreachable)
    client = APIClient(api_secret="prdm_wsp_secret", api_endpoint=TOKEN_ENDPOINT)

    with pytest.raises(ParadimeAPIException) as exc_info:
        AdminTransport(client).execute("query q { x }")

    raised = exc_info.value
    shown = [
        str(raised),
        repr(raised),
        repr(raised.args),
        "".join(traceback.format_exception(type(raised), raised, raised.__traceback__)),
    ]
    assert all(FAKE_TOKEN not in text for text in shown)
    assert "/api/v1/***/graphql" in str(raised)
    assert raised.__cause__ is None and raised.__context__ is None
