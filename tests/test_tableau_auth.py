from typing import Any, Callable, Dict, List, Optional

import pytest
from click.testing import CliRunner

from paradime.cli.integrations.tableau import (
    tableau_list_datasources,
    tableau_list_workbooks,
    tableau_refresh,
)
from paradime.core.scripts import tableau

HOST = "https://tableau.example.com"
JWT = "eyJhbGciOiJIUzI1NiIsImtpZCI6ImtleS0xIn0.payload.signature"
PAT = {"personal_access_token_name": "pat-name", "personal_access_token_secret": "pat-secret"}


class FakeResponse:
    """A 200 response carrying whatever body the endpoint under test reads."""

    def __init__(self, payload: Dict[str, Any]) -> None:
        self._payload = payload
        self.status_code = 200
        self.text = ""

    def raise_for_status(self) -> None:
        pass

    def json(self) -> Dict[str, Any]:
        return self._payload


class FakeTableau:
    """Captures the sign-in request and answers every follow-up with an empty listing."""

    def __init__(self) -> None:
        self.signin_url: Optional[str] = None
        self.signin_credentials: Optional[Dict[str, Any]] = None

    def post(
        self,
        url: str,
        json: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        **kwargs: Any,
    ) -> FakeResponse:
        self.signin_url = url
        self.signin_credentials = (json or {}).get("credentials")
        return FakeResponse({"credentials": {"token": "auth-token", "site": {"id": "site-1"}}})

    def get(
        self, url: str, headers: Optional[Dict[str, str]] = None, **kwargs: Any
    ) -> FakeResponse:
        return FakeResponse({})


@pytest.fixture
def fake_tableau(monkeypatch: pytest.MonkeyPatch) -> FakeTableau:
    fake = FakeTableau()
    monkeypatch.setattr(tableau.requests, "post", fake.post)
    monkeypatch.setattr(tableau.requests, "get", fake.get)
    return fake


# Every entry point signs in the same way; each is called so that nothing but the
# sign-in round trip happens (no workbooks requested, empty listing returned).
ENTRY_POINTS: List[Any] = [
    pytest.param(
        lambda **kwargs: tableau.trigger_tableau_refresh(workbook_names=[], **kwargs),
        id="trigger_tableau_refresh",
    ),
    pytest.param(
        lambda **kwargs: tableau.trigger_tableau_datasource_refresh(datasource_names=[], **kwargs),
        id="trigger_tableau_datasource_refresh",
    ),
    pytest.param(
        lambda **kwargs: tableau.list_tableau_workbooks(json_output=True, **kwargs),
        id="list_tableau_workbooks",
    ),
    pytest.param(
        lambda **kwargs: tableau.list_tableau_datasources(json_output=True, **kwargs),
        id="list_tableau_datasources",
    ),
]


@pytest.mark.parametrize("entry_point", ENTRY_POINTS)
def test_personal_access_token_signin_is_unchanged(
    entry_point: Callable[..., Any], fake_tableau: FakeTableau
) -> None:
    entry_point(host=HOST, site_name="", api_version="3.4", **PAT)

    assert fake_tableau.signin_url == f"{HOST}/api/3.4/auth/signin"
    assert fake_tableau.signin_credentials == {
        "personalAccessTokenName": "pat-name",
        "personalAccessTokenSecret": "pat-secret",
        "site": {"contentUrl": ""},
    }


@pytest.mark.parametrize("entry_point", ENTRY_POINTS)
def test_jwt_signin_sends_a_connected_app_credential(
    entry_point: Callable[..., Any], fake_tableau: FakeTableau
) -> None:
    entry_point(host=HOST, site_name="marketing", api_version="3.4", jwt=JWT)

    assert fake_tableau.signin_credentials == {
        "jwt": JWT,
        "site": {"contentUrl": "marketing"},
    }


@pytest.mark.parametrize("entry_point", ENTRY_POINTS)
def test_jwt_signin_raises_the_api_version_to_the_connected_app_minimum(
    entry_point: Callable[..., Any], fake_tableau: FakeTableau
) -> None:
    entry_point(host=HOST, site_name="", api_version="3.4", jwt=JWT)

    assert fake_tableau.signin_url == f"{HOST}/api/3.10/auth/signin"


@pytest.mark.parametrize("api_version", ["3.9", "3.4.1"])
def test_jwt_signin_raises_any_api_version_below_the_minimum(
    api_version: str, fake_tableau: FakeTableau
) -> None:
    tableau.list_tableau_workbooks(
        host=HOST, site_name="", api_version=api_version, jwt=JWT, json_output=True
    )

    assert fake_tableau.signin_url == f"{HOST}/api/3.10/auth/signin"


@pytest.mark.parametrize("api_version", ["3.10", "3.16", "3.20"])
def test_jwt_signin_keeps_an_api_version_at_or_above_the_minimum(
    api_version: str, fake_tableau: FakeTableau
) -> None:
    tableau.list_tableau_workbooks(
        host=HOST, site_name="", api_version=api_version, jwt=JWT, json_output=True
    )

    assert fake_tableau.signin_url == f"{HOST}/api/{api_version}/auth/signin"


def test_jwt_signin_keeps_an_unparseable_api_version(fake_tableau: FakeTableau) -> None:
    tableau.list_tableau_workbooks(
        host=HOST, site_name="", api_version="exp", jwt=JWT, json_output=True
    )

    assert fake_tableau.signin_url == f"{HOST}/api/exp/auth/signin"


@pytest.mark.parametrize("entry_point", ENTRY_POINTS)
def test_missing_credentials_raise_before_any_request(
    entry_point: Callable[..., Any], fake_tableau: FakeTableau
) -> None:
    with pytest.raises(ValueError, match="Tableau credentials are missing"):
        entry_point(host=HOST, site_name="", api_version="3.4")

    assert fake_tableau.signin_url is None


@pytest.mark.parametrize("entry_point", ENTRY_POINTS)
def test_mixing_a_jwt_and_a_personal_access_token_raises(
    entry_point: Callable[..., Any], fake_tableau: FakeTableau
) -> None:
    with pytest.raises(ValueError, match="not both"):
        entry_point(host=HOST, site_name="", api_version="3.4", jwt=JWT, **PAT)

    assert fake_tableau.signin_url is None


@pytest.mark.parametrize(
    "credentials",
    [
        {"personal_access_token_name": "pat-name"},
        {"personal_access_token_secret": "pat-secret"},
    ],
    ids=["name-only", "secret-only"],
)
def test_an_incomplete_personal_access_token_pair_raises(
    credentials: Dict[str, str], fake_tableau: FakeTableau
) -> None:
    with pytest.raises(ValueError, match="Tableau credentials are missing"):
        tableau.list_tableau_workbooks(
            host=HOST, site_name="", api_version="3.4", json_output=True, **credentials
        )

    assert fake_tableau.signin_url is None


@pytest.mark.parametrize("command", [tableau_list_workbooks, tableau_list_datasources])
def test_cli_signs_in_with_a_jwt(command: Any, fake_tableau: FakeTableau) -> None:
    result = CliRunner().invoke(command, ["--host", HOST, "--jwt", JWT, "--json"])

    assert result.exit_code == 0, result.output
    assert fake_tableau.signin_credentials == {"jwt": JWT, "site": {"contentUrl": ""}}
    assert fake_tableau.signin_url == f"{HOST}/api/3.10/auth/signin"


@pytest.mark.parametrize("command", [tableau_list_workbooks, tableau_list_datasources])
def test_cli_reads_the_jwt_from_the_environment(command: Any, fake_tableau: FakeTableau) -> None:
    result = CliRunner().invoke(command, ["--host", HOST, "--json"], env={"TABLEAU_JWT": JWT})

    assert result.exit_code == 0, result.output
    assert fake_tableau.signin_credentials == {"jwt": JWT, "site": {"contentUrl": ""}}


@pytest.mark.parametrize("command", [tableau_list_workbooks, tableau_list_datasources])
def test_cli_still_signs_in_with_a_personal_access_token(
    command: Any, fake_tableau: FakeTableau
) -> None:
    result = CliRunner().invoke(
        command,
        [
            "--host",
            HOST,
            "--personal-access-token-name",
            "pat-name",
            "--personal-access-token-secret",
            "pat-secret",
            "--json",
        ],
    )

    assert result.exit_code == 0, result.output
    assert fake_tableau.signin_url == f"{HOST}/api/3.4/auth/signin"
    assert fake_tableau.signin_credentials == {
        "personalAccessTokenName": "pat-name",
        "personalAccessTokenSecret": "pat-secret",
        "site": {"contentUrl": ""},
    }


@pytest.mark.parametrize(
    "command", [tableau_refresh, tableau_list_workbooks, tableau_list_datasources]
)
def test_cli_rejects_a_missing_credential(command: Any, fake_tableau: FakeTableau) -> None:
    result = CliRunner().invoke(command, ["--host", HOST, "--json"])

    assert result.exit_code == 2
    assert "Tableau credentials are missing" in result.output
    assert fake_tableau.signin_url is None


@pytest.mark.parametrize(
    "command", [tableau_refresh, tableau_list_workbooks, tableau_list_datasources]
)
def test_cli_rejects_both_credential_kinds(command: Any, fake_tableau: FakeTableau) -> None:
    result = CliRunner().invoke(
        command,
        [
            "--host",
            HOST,
            "--jwt",
            JWT,
            "--personal-access-token-name",
            "pat-name",
            "--personal-access-token-secret",
            "pat-secret",
            "--json",
        ],
    )

    assert result.exit_code == 2
    assert "not both" in result.output
    assert fake_tableau.signin_url is None


def test_cli_refresh_accepts_a_jwt_and_still_requires_a_target() -> None:
    result = CliRunner().invoke(tableau_refresh, ["--host", HOST, "--jwt", JWT])

    assert result.exit_code == 2
    assert "Must specify either --workbook-names" in result.output
