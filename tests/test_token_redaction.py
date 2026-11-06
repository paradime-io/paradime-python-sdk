import traceback
from pathlib import Path
from typing import Any, Callable, List

import pytest
import requests
from click.testing import CliRunner

from paradime.apis.bolt.client import BoltClient
from paradime.cli import cli as cli_module
from paradime.client import api_client as api_client_module
from paradime.client.api_client import APIClient
from paradime.client.api_exception import ParadimeAPIException

FAKE_TOKEN = "FAKECOMPANYTOKEN123"
TOKEN_ENDPOINT = f"https://api.example.test/api/v1/{FAKE_TOKEN}/graphql"
FAKE_SIGNATURE = "FAKESIGNATURE456"
PRESIGNED_URL = (
    "https://bucket.example.test/run/1/target/manifest.json"
    f"?X-Amz-Credential=FAKEKEY&X-Amz-Signature={FAKE_SIGNATURE}"
)


def _shown(error: BaseException) -> List[str]:
    return [
        str(error),
        repr(error),
        repr(error.args),
        "".join(traceback.format_exception(type(error), error, error.__traceback__)),
    ]


def _http_error_response() -> requests.Response:
    response = requests.Response()
    response.status_code = 500
    response.url = TOKEN_ENDPOINT
    response._content = b"Internal Server Error"
    return response


def _connection_error(**kwargs: Any) -> Any:
    raise requests.exceptions.ConnectionError(
        f"HTTPSConnectionPool(host='api.example.test', port=443): Max retries exceeded "
        f"with url: /api/v1/{FAKE_TOKEN}/graphql"
    )


def _read_timeout(**kwargs: Any) -> Any:
    raise requests.exceptions.ReadTimeout(f"Read timed out. (url={TOKEN_ENDPOINT})")


def _server_error(**kwargs: Any) -> requests.Response:
    return _http_error_response()


@pytest.mark.parametrize(
    "post, expected_type",
    [
        (_connection_error, requests.exceptions.ConnectionError),
        (_read_timeout, requests.exceptions.ReadTimeout),
        (_server_error, ParadimeAPIException),
    ],
)
def test_legacy_client_errors_keep_the_company_token_out(
    monkeypatch: pytest.MonkeyPatch, post: Callable[..., Any], expected_type: type
) -> None:
    monkeypatch.setattr(api_client_module.requests, "post", post)
    client = APIClient(api_secret="prdm_wsp_secret", api_endpoint=TOKEN_ENDPOINT)

    with pytest.raises(expected_type) as exc_info:
        client._call_gql("query q { x }")

    raised = exc_info.value
    assert type(raised) is expected_type
    assert all(FAKE_TOKEN not in text for text in _shown(raised))
    assert raised.__cause__ is None and raised.__suppress_context__


def test_a_connection_error_keeps_the_useful_text(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(api_client_module.requests, "post", _connection_error)
    client = APIClient(api_secret="prdm_wsp_secret", api_endpoint=TOKEN_ENDPOINT)

    with pytest.raises(requests.exceptions.ConnectionError) as exc_info:
        client._call_gql("query q { x }")

    assert "Max retries exceeded with url: /api/v1/***/graphql" in str(exc_info.value)


def test_an_http_error_keeps_its_status_and_body(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(api_client_module.requests, "post", _server_error)
    client = APIClient(api_secret="prdm_wsp_secret", api_endpoint=TOKEN_ENDPOINT)

    with pytest.raises(ParadimeAPIException) as exc_info:
        client._call_gql("query q { x }")

    assert str(exc_info.value) == "Error: 500 - Internal Server Error"


@pytest.fixture
def cli_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(cli_module, "get_credentials_path", lambda: tmp_path / "credentials")
    monkeypatch.setattr(cli_module, "check_for_new_version", lambda: None)
    monkeypatch.setenv("PARADIME_DISABLE_VERSION_CHECK", "1")
    monkeypatch.setenv("PARADIME_API_ENDPOINT", TOKEN_ENDPOINT)
    monkeypatch.setenv("PARADIME_API_SECRET", "prdm_wsp_secret")
    monkeypatch.delenv("PARADIME_API_KEY", raising=False)
    monkeypatch.delenv("PARADIME_WORKSPACE_UID", raising=False)


def _cli_outputs(result: Any) -> List[str]:
    texts = [result.stdout, result.stderr]
    if result.exception is not None:
        texts.extend(_shown(result.exception))
    return texts


@pytest.mark.usefixtures("cli_env")
def test_the_welcome_panel_shows_the_endpoint_without_the_company_token() -> None:
    result = CliRunner().invoke(cli_module.cli, ["version"])

    assert result.exit_code == 0, result.output
    assert all(FAKE_TOKEN not in text for text in _cli_outputs(result))
    assert "api.example.test/api/v1/***/graphql" in result.stderr


@pytest.mark.usefixtures("cli_env")
@pytest.mark.parametrize("post", [_connection_error, _read_timeout, _server_error])
def test_a_failing_cli_command_never_prints_the_company_token(
    monkeypatch: pytest.MonkeyPatch, post: Callable[..., Any]
) -> None:
    monkeypatch.setattr(api_client_module.requests, "post", post)

    result = CliRunner().invoke(cli_module.cli, ["bolt", "run", "daily"])

    assert result.exit_code != 0
    assert all(FAKE_TOKEN not in text for text in _cli_outputs(result))


def _unreachable_bucket(url: str, **kwargs: Any) -> Any:
    raise requests.exceptions.ConnectionError(
        "HTTPSConnectionPool(host='bucket.example.test', port=443): Max retries exceeded with "
        f"url: /run/1/target/manifest.json?X-Amz-Credential=FAKEKEY&X-Amz-Signature="
        f"{FAKE_SIGNATURE} (Caused by NameResolutionError)"
    )


def test_an_artifact_download_error_drops_the_presigned_query(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(requests, "get", _unreachable_bucket)
    bolt = BoltClient(client=APIClient(api_secret="prdm_wsp_secret", api_endpoint="https://x"))
    monkeypatch.setattr(bolt, "get_latest_artifact_url", lambda **kwargs: PRESIGNED_URL)

    with pytest.raises(requests.exceptions.ConnectionError) as exc_info:
        bolt.get_latest_manifest_json(slug="daily")

    shown = _shown(exc_info.value)
    assert all(FAKE_SIGNATURE not in text and "X-Amz" not in text for text in shown)
    assert "url: /run/1/target/manifest.json (Caused by NameResolutionError)" in str(exc_info.value)


@pytest.mark.usefixtures("cli_env")
def test_the_artifact_command_never_prints_the_presigned_query(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from paradime.apis.bolt import client as bolt_client_module

    monkeypatch.setattr(requests, "get", _unreachable_bucket)
    monkeypatch.setattr(
        bolt_client_module.BoltClient,
        "get_latest_artifact_url",
        lambda self, **kwargs: PRESIGNED_URL,
    )

    result = CliRunner().invoke(
        cli_module.cli,
        ["bolt", "artifact", "--slug", "daily", "--output-path", str(tmp_path / "manifest.json")],
    )

    assert isinstance(result.exception, requests.exceptions.ConnectionError)
    assert all(FAKE_SIGNATURE not in text for text in _cli_outputs(result))
