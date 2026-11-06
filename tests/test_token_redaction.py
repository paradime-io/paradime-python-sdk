import socket
import traceback
from pathlib import Path
from typing import Any, Callable, List, Optional, Set, cast

import pytest
import requests
from click.testing import CliRunner
from urllib3.exceptions import MaxRetryError, NameResolutionError, ReadTimeoutError

from paradime.apis.admin.exception import AdminConnectionException
from paradime.apis.admin.transport import AdminTransport
from paradime.apis.bolt.client import BoltClient
from paradime.cli import cli as cli_module
from paradime.client.api_client import APIClient
from paradime.client.api_exception import ParadimeAPIException

FAKE_TOKEN = "FAKECOMPANYTOKEN123"
FAKE_SECRET = "prdm_cmp_FAKESECRET789"
TOKEN_ENDPOINT = f"https://api.example.test/api/v1/{FAKE_TOKEN}/graphql"
FAKE_SIGNATURE = "FAKESIGNATURE456"
PRESIGNED_URL = (
    "https://bucket.example.test/run/1/target/manifest.json"
    f"?X-Amz-Credential=FAKEKEY&X-Amz-Signature={FAKE_SIGNATURE}"
)
SECRETS = (FAKE_TOKEN, FAKE_SECRET, FAKE_SIGNATURE, "X-Amz")

Outcome = Callable[[requests.PreparedRequest], requests.Response]


def _request_texts(request: Optional[requests.PreparedRequest]) -> List[str]:
    if request is None:
        return []
    return [str(request.url), repr(dict(request.headers)), repr(request.body)]


def _texts(error: Optional[BaseException], seen: Optional[Set[int]] = None) -> List[str]:
    seen = set() if seen is None else seen
    if error is None or id(error) in seen:
        return []
    seen.add(id(error))
    texts = [
        str(error),
        repr(error),
        repr(error.args),
        repr(vars(error)),
        "".join(traceback.format_exception(type(error), error, error.__traceback__)),
    ]
    texts += _request_texts(getattr(error, "request", None))
    response = getattr(error, "response", None)
    if response is not None:
        texts += [str(response.url), response.text, repr(dict(response.headers))]
        texts += _request_texts(response.request)
    for arg in error.args:
        if isinstance(arg, BaseException):
            texts += _texts(arg, seen)
    texts += _texts(error.__cause__, seen)
    texts += _texts(error.__context__, seen)
    return texts


def _leaks(error: Optional[BaseException]) -> List[str]:
    return sorted({secret for secret in SECRETS for text in _texts(error) if secret in text})


def _patch_network(monkeypatch: pytest.MonkeyPatch, outcome: Outcome) -> None:
    def send(self: Any, request: requests.PreparedRequest, **kwargs: Any) -> requests.Response:
        return outcome(request)

    monkeypatch.setattr(requests.adapters.HTTPAdapter, "send", send)


def _unresolvable(request: requests.PreparedRequest) -> requests.Response:
    host = str(request.url).split("/")[2]
    reason = NameResolutionError(
        host, cast(Any, None), socket.gaierror(-2, "Name or service not known")
    )
    raise requests.exceptions.ConnectionError(
        MaxRetryError(cast(Any, None), request.path_url, reason), request=request
    )


def _read_timeout(request: requests.PreparedRequest) -> requests.Response:
    raise requests.exceptions.ReadTimeout(
        ReadTimeoutError(cast(Any, None), str(request.url), "Read timed out."),
        request=request,
    )


def _status(code: int, body: bytes) -> Outcome:
    def respond(request: requests.PreparedRequest) -> requests.Response:
        response = requests.Response()
        response.status_code = code
        response.reason = "Error"
        response.url = str(request.url)
        response.request = request
        response._content = body
        return response

    return respond


SERVER_ERROR = _status(500, b"Internal Server Error")


def _legacy_call() -> None:
    APIClient(api_secret=FAKE_SECRET, workspace_uid="w1", api_endpoint=TOKEN_ENDPOINT)._call_gql(
        "query q { x }"
    )


def _admin_call() -> None:
    client = APIClient(api_secret=FAKE_SECRET, workspace_uid="w1", api_endpoint=TOKEN_ENDPOINT)
    AdminTransport(client).execute("query q { x }")


@pytest.mark.parametrize(
    "call, outcome, expected_type",
    [
        (_legacy_call, _unresolvable, requests.exceptions.ConnectionError),
        (_legacy_call, _read_timeout, requests.exceptions.ReadTimeout),
        (_legacy_call, SERVER_ERROR, ParadimeAPIException),
        (_admin_call, _unresolvable, AdminConnectionException),
        (_admin_call, _read_timeout, AdminConnectionException),
        (_admin_call, SERVER_ERROR, ParadimeAPIException),
    ],
)
def test_errors_keep_the_token_and_secret_out_of_every_attribute(
    monkeypatch: pytest.MonkeyPatch,
    call: Callable[[], None],
    outcome: Outcome,
    expected_type: type,
) -> None:
    _patch_network(monkeypatch, outcome)

    with pytest.raises(expected_type) as exc_info:
        call()

    raised = exc_info.value
    assert type(raised) is expected_type
    assert _leaks(raised) == []
    assert raised.__cause__ is None and raised.__context__ is None


def test_the_walk_finds_a_token_on_a_suppressed_context() -> None:
    try:
        try:
            raise ValueError(TOKEN_ENDPOINT)
        except ValueError:
            raise RuntimeError("redacted") from None
    except RuntimeError as e:
        assert _leaks(e) == [FAKE_TOKEN]


def test_a_connection_error_keeps_the_useful_text(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_network(monkeypatch, _unresolvable)

    with pytest.raises(requests.exceptions.ConnectionError) as exc_info:
        _legacy_call()

    assert "Max retries exceeded with url: /api/v1/***/graphql" in str(exc_info.value)
    assert "Name or service not known" in str(exc_info.value)


def test_an_http_error_keeps_its_status_and_body(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_network(monkeypatch, SERVER_ERROR)

    with pytest.raises(ParadimeAPIException) as exc_info:
        _legacy_call()

    assert str(exc_info.value) == "Error: 500 - Internal Server Error"


@pytest.fixture
def cli_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(cli_module, "get_credentials_path", lambda: tmp_path / "credentials")
    monkeypatch.setattr(cli_module, "check_for_new_version", lambda: None)
    monkeypatch.setenv("PARADIME_DISABLE_VERSION_CHECK", "1")
    monkeypatch.setenv("PARADIME_API_ENDPOINT", TOKEN_ENDPOINT)
    monkeypatch.setenv("PARADIME_API_SECRET", FAKE_SECRET)
    monkeypatch.setenv("PARADIME_WORKSPACE_UID", "w1")
    monkeypatch.delenv("PARADIME_API_KEY", raising=False)


def _cli_leaks(result: Any) -> List[str]:
    texts = [result.stdout, result.stderr]
    leaks = {secret for secret in SECRETS for text in texts if secret in text}
    return sorted(leaks.union(_leaks(result.exception)))


@pytest.mark.usefixtures("cli_env")
def test_the_welcome_panel_shows_the_endpoint_without_the_company_token() -> None:
    result = CliRunner().invoke(cli_module.cli, ["version"])

    assert result.exit_code == 0, result.output
    assert _cli_leaks(result) == []
    assert "api.example.test/api/v1/***/graphql" in result.stderr


@pytest.mark.usefixtures("cli_env")
@pytest.mark.parametrize("outcome", [_unresolvable, _read_timeout, SERVER_ERROR])
def test_a_failing_cli_command_never_prints_the_company_token(
    monkeypatch: pytest.MonkeyPatch, outcome: Outcome
) -> None:
    _patch_network(monkeypatch, outcome)

    result = CliRunner().invoke(cli_module.cli, ["bolt", "run", "daily"])

    assert result.exit_code != 0
    assert _cli_leaks(result) == []


EXPIRED = _status(
    403, b"<Error><Code>AccessDenied</Code><Message>Request has expired</Message></Error>"
)


@pytest.mark.parametrize(
    "outcome, expected_type, expected_text",
    [
        (
            _unresolvable,
            requests.exceptions.ConnectionError,
            "url: /run/1/target/manifest.json (Caused by",
        ),
        (
            EXPIRED,
            requests.exceptions.HTTPError,
            "403 Client Error: Error for url: https://bucket.example.test/run/1/target/manifest.json",
        ),
    ],
)
def test_an_artifact_download_error_drops_the_presigned_query(
    monkeypatch: pytest.MonkeyPatch, outcome: Outcome, expected_type: type, expected_text: str
) -> None:
    _patch_network(monkeypatch, outcome)
    bolt = BoltClient(client=APIClient(api_secret="prdm_wsp_secret", api_endpoint="https://x"))
    monkeypatch.setattr(bolt, "get_latest_artifact_url", lambda **kwargs: PRESIGNED_URL)

    with pytest.raises(expected_type) as exc_info:
        bolt.get_latest_manifest_json(slug="daily")

    assert type(exc_info.value) is expected_type
    assert _leaks(exc_info.value) == []
    assert exc_info.value.__context__ is None
    assert expected_text in str(exc_info.value)


def test_an_artifact_download_uses_the_client_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    timeouts: List[Any] = []

    def respond(request: requests.PreparedRequest, **kwargs: Any) -> requests.Response:
        timeouts.append(kwargs.get("timeout"))
        return _status(200, b'{"nodes": {}}')(request)

    monkeypatch.setattr(
        requests.adapters.HTTPAdapter, "send", lambda self, r, **kw: respond(r, **kw)
    )
    client = APIClient(api_secret="prdm_wsp_secret", api_endpoint="https://x", timeout=7)
    bolt = BoltClient(client=client)
    monkeypatch.setattr(bolt, "get_latest_artifact_url", lambda **kwargs: PRESIGNED_URL)

    assert bolt.get_latest_manifest_json(slug="daily") == {"nodes": {}}
    assert timeouts == [7]


@pytest.mark.usefixtures("cli_env")
@pytest.mark.parametrize("outcome", [_unresolvable, EXPIRED])
def test_the_artifact_command_fails_without_the_presigned_query(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, outcome: Outcome
) -> None:
    from paradime.apis.bolt import client as bolt_client_module

    _patch_network(monkeypatch, outcome)
    monkeypatch.setattr(
        bolt_client_module.BoltClient,
        "get_latest_artifact_url",
        lambda self, **kwargs: PRESIGNED_URL,
    )
    output = tmp_path / "manifest.json"

    result = CliRunner().invoke(
        cli_module.cli, ["bolt", "artifact", "--slug", "daily", "--output-path", str(output)]
    )

    assert result.exit_code == 1
    assert isinstance(result.exception, SystemExit)
    assert "Failed to get artifact" in result.stderr
    assert not output.exists()
    assert _cli_leaks(result) == []
