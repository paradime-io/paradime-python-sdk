<p align="center">
  <a href="https://www.paradime.io">
        <img alt="Paradime" src="https://app.paradime.io/logo192.png" width="60" />
    </a>
</p>

<h1 align="center">
  Paradime - Python SDK
</h1>

## Installation

```sh
pip install paradime-io
```

## SDK Usage

Generate your API credentials from Paradime workspace settings.

You can authenticate with a bearer token (recommended) — either a workspace-level token
(`prdm_wsp_...`) or a company-level token (`prdm_cmp_...`; `workspace_uid` selects which
workspace requests target, and is needed for workspace-level calls). Pass it as `api_secret`; the SDK detects the
token type automatically from its prefix and `api_key` is not needed:

```python
from paradime import Paradime

paradime = Paradime(
    api_endpoint="API_ENDPOINT",
    api_secret="API_TOKEN",  # e.g. prdm_wsp_... or prdm_cmp_...
    # workspace_uid="WORKSPACE_UID",  # needed for workspace-level calls with a prdm_cmp_ token
)

# Use the paradime client to interact with the API
```

Or with the legacy API key + secret pair:

```python
from paradime import Paradime

paradime = Paradime(
    api_endpoint="API_ENDPOINT", 
    api_key="API_KEY", 
    api_secret="API_SECRET",
)

# Use the paradime client to interact with the API
```

## CLI Usage

For the full specification of the CLI, run:
```bash
paradime --help
```

Generate your API credentials from Paradime workspace settings. Then set the environment variables.

Using a bearer token (recommended):

```bash
export PARADIME_API_ENDPOINT="YOUR_API_ENDPOINT"
export PARADIME_API_SECRET="YOUR_API_TOKEN" # e.g. prdm_wsp_... or prdm_cmp_...
export PARADIME_WORKSPACE_UID="YOUR_WORKSPACE_UID" # needed for workspace-level calls with a prdm_cmp_ token
```

Or using the legacy API key + secret pair:

```bash
export PARADIME_API_ENDPOINT="YOUR_API_ENDPOINT"
export PARADIME_API_KEY="YOUR_API_KEY"
export PARADIME_API_SECRET="YOUR_API_SECRET
```

Alternatively, run `paradime login` to be prompted for either set of credentials, which will be stored locally.

## Administration API

On the Enterprise plan, `paradime.admin` manages workspaces, git, warehouse connections,
environments, environment variables, usage, users, notifications, Bolt, integrations and
DinoAI agents. Lists are iterators that read a page at a time, errors raise the
`AdminApiException` subclass of their code (for example `AdminValidationFailedException`,
with the input path in `field`), and rate-limited requests are retried after the wait the
server asks for.

```python
from paradime import Paradime
from paradime.apis.admin.environments.types import EnvironmentType
from paradime.apis.admin.git.types import DeployKeyConnectionInput, GitProvider

paradime = Paradime(api_endpoint="API_ENDPOINT", api_secret="prdm_cmp_...", workspace_uid="WORKSPACE_UID")

workspace = paradime.admin.workspaces.create("Analytics")
result = paradime.admin.git.connect(
    DeployKeyConnectionInput(provider=GitProvider.GITHUB, uri="git@github.com:acme/analytics.git"),
    workspace_uid=workspace.uid,
)
if result.job:
    paradime.admin.jobs.wait(result.job.id, workspace_uid=workspace.uid)
paradime.admin.environments.create(EnvironmentType.SCHEDULER, "Production", slug="production", workspace_uid=workspace.uid)
paradime.admin.environment_variables.set({"DBT_TARGET": "prod"}, environment_slug="production", workspace_uid=workspace.uid)
```

`workspace_uid` on a call sends `X-Paradime-Workspace` for that call only, so one company-level
key can work across workspaces. The same operations are on the CLI under `paradime admin`;
environment variable values and connection secrets are read from stdin or a file, never from
the command line:

```bash
printf '%s' "$DBT_TOKEN" | paradime admin env-vars set DBT_TOKEN --environment production
paradime admin connections set --scope SCHEDULER --environment production --config snowflake.yml
```

## Examples

Find usage examples [here](https://github.com/paradime-io/paradime-python-sdk/tree/main/examples) to get started with the Paradime Python SDK.

## Telemetry

Each API call sends `X-PYTHON-VERSION` (e.g. `3.11.5`) and `X-PARADIME-RUNTIME` (e.g. `github-actions`, `airflow`, `paradime-bolt`, `local`) to help Paradime understand how the SDK is being used. Only the *presence* of well-known CI/platform environment variables is checked — no values are ever transmitted.

To opt out:

```bash
export PARADIME_DISABLE_TELEMETRY=true
```

