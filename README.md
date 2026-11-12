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
(`prdm_wsp_...`) or a company-level token (`prdm_cmp_...`, requires `workspace_uid` to
select which workspace requests target). Pass it as `api_secret`; the SDK detects the
token type automatically from its prefix and `api_key` is not needed:

```python
from paradime import Paradime

paradime = Paradime(
    api_endpoint="API_ENDPOINT",
    api_secret="API_TOKEN",  # e.g. prdm_wsp_... or prdm_cmp_...
    # workspace_uid="WORKSPACE_UID",  # required when using a company-level (prdm_cmp_) token
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
export PARADIME_WORKSPACE_UID="YOUR_WORKSPACE_UID" # required when using a company-level (prdm_cmp_) token
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

The command exits 0 only when every step succeeded (or, with `--allow-integration-failures`, only integration syncs failed). A single failed read of the refresh state is retried with backoff and does not fail the command.

## Examples

Find usage examples [here](https://github.com/paradime-io/paradime-python-sdk/tree/main/examples) to get started with the Paradime Python SDK.

## Telemetry

Each API call sends `X-PYTHON-VERSION` (e.g. `3.11.5`) and `X-PARADIME-RUNTIME` (e.g. `github-actions`, `airflow`, `paradime-bolt`, `local`) to help Paradime understand how the SDK is being used. Only the *presence* of well-known CI/platform environment variables is checked — no values are ever transmitted.

To opt out:

```bash
export PARADIME_DISABLE_TELEMETRY=true
```

