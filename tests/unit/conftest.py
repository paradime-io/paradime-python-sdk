import pytest
from tests.unit.constants import API_ENDPOINT, RUNTIME_ENV_VARS, WORKSPACE_TOKEN

from paradime.client.api_client import APIClient


@pytest.fixture(autouse=True)
def _isolate_runtime_env(monkeypatch):
    """Keep telemetry headers deterministic regardless of the host environment.

    The runtime detector sniffs CI environment variables, so without this the
    X-PARADIME-RUNTIME header would differ between a laptop and CI.
    """
    for var in RUNTIME_ENV_VARS:
        monkeypatch.delenv(var, raising=False)


@pytest.fixture
def api_client():
    """An APIClient authenticated with a workspace-level bearer token."""
    return APIClient(api_secret=WORKSPACE_TOKEN, api_endpoint=API_ENDPOINT)
