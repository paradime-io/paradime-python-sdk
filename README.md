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

### Refreshing the catalog

`paradime catalog refresh` requests a catalog refresh and, by default, **waits until every refresh step ends** (docs generate, catalog build and any enabled integration syncs such as DataHub or Altimate). It logs one plain line each time a step starts, makes progress, finishes or fails, and exits non-zero if any step fails, so a Bolt command running it fails with the reason in its log:

```
[catalog refresh]    started, refresh id 8f3c...
[dbt docs generate]  started
[dbt docs generate]  done in 4m12s
[Read dbt artifacts] done in 9s
[Catalog build]      started, Write assets 18,400 / 41,200 assets
[Catalog build]      done in 1m03s
[DataHub push]       FAILED: 401 Unauthorized
[catalog refresh]    FAILED: 1 of 4 steps failed (DataHub push)
```

If a refresh is already running, the command follows that refresh instead of starting another one.

| Option | Description |
| --- | --- |
| `--no-wait` | Only request the refresh, print its id and exit 0. This is how the command behaved before it waited by default. |
| `--timeout` | How long to wait, e.g. `90s`, `45m`, `2h` (default `2h`). When it expires the command fails and names the steps that had not finished; the refresh itself continues. |
| `--allow-integration-failures` | Log a failed integration sync (e.g. DataHub, Altimate) as a warning and exit 0. A failed docs generate or catalog build always fails the command. |

The command exits 0 only when every step succeeded (or, with `--allow-integration-failures`, only integration syncs failed). It also fails when the refresh is skipped and nothing is refreshed, e.g. when the workspace has no warehouse connection for its production catalog. A single failed read of the refresh state is retried with backoff and does not fail the command.

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
from paradime.apis.admin.users.types import UserAccountType

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
paradime.admin.bolt.create_schedule(slug="nightly", name="Nightly", schedule="0 2 * * *", environment="production", commands=["dbt build"], workspace_uid=workspace.uid)
paradime.admin.users.create("data-lead@example.com", UserAccountType.ADMIN, workspace_uid=workspace.uid)
```

`admin.bolt` creates, updates, lists and deletes the schedules made in the UI or through the
API (`create_schedule`, `update_schedule`, `list_schedules`, `delete_schedule`); YAML schedules
stay in the repository and deploy with `deploy_schedules`. `update_schedule` replaces every
setting except `suspended`, `continue_on_error` and `env_vars`, so send the whole schedule.

Async operations return an `AdminJob`. `admin.jobs.wait` polls it until it finishes and returns
it with a typed `result` for its `kind`: a `DeployBoltSchedulesResult` holds the first page of
the schedules a deploy read, with why each failed when it did (`admin.jobs.deployed_schedules`
reads them all), and a `DinoAiAgentSyncResult` counts the agents a sync added, updated and
skipped.

`workspace_uid` on a call sends `X-Paradime-Workspace` for that call only, so one company API
key can work across workspaces. The same operations are on the CLI under `paradime admin`;
environment variable values and connection secrets are read from stdin or a file, never from
the command line:

```bash
printf '%s' "$DBT_TOKEN" | paradime admin env-vars set DBT_TOKEN --environment production
paradime admin connections create --scope SCHEDULER --environment production --config snowflake.yml \
  --name Snowflake --target prod --threads 8
paradime admin connections update --scope SCHEDULER --environment production --config snowflake.yml
paradime admin bolt schedules create --slug nightly --name Nightly --schedule "0 2 * * *" --environment production --command "dbt build"
paradime admin bolt schedules update nightly --config nightly.yml
paradime admin bolt schedules list --json
```

`paradime.admin.connections.create` saves a new connection: sending the same settings again
returns the stored one, and a connection that exists with other settings raises
`AdminConflictException`. `update` changes an existing connection, replacing its warehouse
settings while secrets, name, target and threads left out keep their stored values; a
connection that does not exist raises `AdminNotFoundException`.

DinoAI agent definitions follow the same rule: `paradime.admin.dinoai.create_agent` with a
`slug` returns the stored agent when sent again and raises `AdminConflictException` when an
agent with other settings holds the slug, and `update_agent` replaces an agent's definition by
slug, raising `AdminNotFoundException` when no agent holds it. A Bolt Trigger URL is paused
with `paradime.admin.bolt.pause_trigger_url` and resumed with `resume_trigger_url`; pausing a
paused one, or resuming one that runs, changes nothing.

`paradime.admin.users` invites people with a role (`create`, which sends the invitation email
and returns the invitee as `INVITED`), changes a member's role (`update`), and deactivates and
reactivates members (`deactivate`, `reactivate`); each returns the user. Inviting an email that
already belongs to a user or to a pending invitation raises `AdminConflictException`, a role the
API key can't give raises `AdminValidationFailedException` with `field` set to
``["input", "accountType"]``, and a uid the workspace does not know raises
`AdminNotFoundException`:

```bash
paradime admin users create data-lead@example.com --account-type ADMIN
paradime admin users update USER_UID --account-type BUSINESS --json
paradime admin users deactivate USER_UID --yes
```

`paradime.admin.integrations` connects the workspace's BI, ETL and observability integrations:
Looker, Tableau, Power BI, DataHub, Sigma, ThoughtSpot, Lightdash, Omni, Metabase, Fivetran,
Airbyte, Hightouch, Datadog, New Relic, PagerDuty, incident.io and Rootly. `set` takes one
integration's input, such as `MetabaseIntegrationInput`, tests the credentials and saves them;
a refused test raises `AdminConnectionTestFailedException` and saves nothing. `test` checks
credentials without saving, `disconnect` takes an `IntegrationName`, and `list` returns each
integration with its typed settings. Secrets are write-only: settings say only whether one is
saved (`has_api_key`, `has_client_secret`, ...), and a secret left out keeps the saved one
unless an endpoint setting such as the host changes.

```python
from paradime.apis.admin.integrations.types import IntegrationName, MetabaseIntegrationInput

paradime.admin.integrations.set(MetabaseIntegrationInput(host="metabase.acme.com", api_key="..."))
for integration in paradime.admin.integrations.list():
    print(integration.name, integration.status, integration.settings)
paradime.admin.integrations.disconnect(IntegrationName.METABASE)
```

The integrations the workspace's DinoAI agents connect through Nango, such as Notion, Jira and
Google Drive, are connected by a person in a browser. `list_agent_integrations` returns each one
with its status: `PENDING_USER_ACTION` waits for that person, and `include_install_urls=True`
also reads a Connect link (`install_url`) for them to open. Each read makes new links, which
work for 30 minutes and let whoever opens them connect an account, so share each only with that
person; reading them needs `integration_settings:write`, and a key without it reads None in
their place. `disconnect_agent_integration` disconnects one. `admin.setup_status.get` lists an
`AGENT_INTEGRATION` row, named in `agent_integration`, for each one the workspace connected.

```bash
paradime admin integrations set --config metabase.yml
paradime admin integrations test --config - < pagerduty.json
paradime admin integrations list --json
paradime admin integrations disconnect METABASE --yes
paradime admin agent-integrations list --install-urls
paradime admin agent-integrations disconnect NOTION --yes
```

The `--config` file holds one integration block, such as
`metabase: {host: metabase.acme.com, apiKey: ...}`.

## Examples

Find usage examples [here](https://github.com/paradime-io/paradime-python-sdk/tree/main/examples) to get started with the Paradime Python SDK.

## Telemetry

Each API call sends `X-PYTHON-VERSION` (e.g. `3.11.5`) and `X-PARADIME-RUNTIME` (e.g. `github-actions`, `airflow`, `paradime-bolt`, `local`) to help Paradime understand how the SDK is being used. Only the *presence* of well-known CI/platform environment variables is checked — no values are ever transmitted.

To opt out:

```bash
export PARADIME_DISABLE_TELEMETRY=true
```

