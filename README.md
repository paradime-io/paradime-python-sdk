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

Generate your API key, secret and endpoint from Paradime workspace settings.

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

Generate your API key, secret and endpoint from Paradime workspace settings. Then set the environment variables:

```bash
export PARADIME_API_ENDPOINT="YOUR_API_ENDPOINT"
export PARADIME_API_KEY="YOUR_API_KEY"
export PARADIME_API_SECRET="YOUR_API_SECRET
```

## Examples

Find usage examples [here](https://github.com/paradime-io/paradime-python-sdk/tree/main/examples) to get started with the Paradime Python SDK.
