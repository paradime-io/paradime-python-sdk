# Paradime SDK Skills

This directory contains Claude Code skills for working with the Paradime Python SDK.

## Available Skills

### `/new-integration` - Create New Integration

Autonomously creates a new integration for external services by:
1. Researching the API documentation via web search
2. Identifying authentication methods and endpoints
3. Following established patterns in the codebase
4. Generating CLI and core script files
5. Registering the integration in the CLI

**Usage:**
```
/new-integration
```

Then specify the service name (e.g., "Databricks", "Snowflake", "dbt Cloud")

**What it does:**
- Searches for official API documentation
- Determines authentication method (API key, OAuth, Bearer token, etc.)
- Finds relevant endpoints for triggering actions and listing resources
- Creates `paradime/cli/integrations/{service}.py`
- Creates `paradime/core/scripts/{service}.py`
- Updates `paradime/cli/run.py` with command registration
- Follows all established patterns:
  - ThreadPoolExecutor for parallel operations
  - Emoji-based status indicators
  - Table-formatted output
  - Progress monitoring with timestamps
  - Error handling with `handle_http_error`
  - Dashboard URL links

### `/integration-quick-ref` - Quick Reference

Shows a condensed reference of integration patterns including:
- File locations
- Command templates
- Status emoji guide
- Key implementation patterns

**Usage:**
```
/integration-quick-ref
```

## Integration Patterns

All integrations follow a two-layer architecture:

### 1. CLI Layer (`paradime/cli/integrations/`)
- Click commands for user interaction
- Environment variable configuration via `env_click_option`
- Error handling and exit codes
- User-facing messages

### 2. Core Scripts Layer (`paradime/core/scripts/`)
- Business logic and API interactions
- Concurrent execution with ThreadPoolExecutor
- Progress monitoring and status polling
- Formatted console output with emoji and tables

### Common Patterns

**Authentication:**
- API Key/Secret: Basic auth or headers
- OAuth: Token fetch + Bearer token
- Service Principal: Azure AD token flow

**Output Formatting:**
- Visual separators with `=` and `-`
- Status emoji: 🚀 ✅ ❌ 🔄 ⏳ ⚠️ 🚫 🔗 📊 🔌 🔍
- Timestamps: `HH:MM:SS` format
- Tables: Left-aligned columns with fixed width
- Dashboard URLs for all resources

**Error Handling:**
- Use `handle_http_error()` from utils
- Exit with code 1 on failure
- Network retry logic for polling
- Timeout handling

**Async Operations:**
- Poll every 5 seconds
- Log progress every 30 seconds
- Show elapsed time
- Support timeout configuration

## Examples

See existing integrations for reference:
- **Fivetran** (`fivetran.py`) - Simple API key auth, sync operations
- **Azure Data Factory** (`azure_data_factory.py`) - Service principal auth, pipeline runs
- **Airbyte** (`airbyte.py`) - Bearer token auth, job operations
- **Power BI** (`power_bi.py`) - OAuth token refresh
- **Tableau** (`tableau.py`) - Sign-in flow with tokens

## Development Guidelines

When creating or modifying integrations:

1. **Research first** - Always check the latest API documentation
2. **Match patterns** - Stay consistent with existing integrations
3. **User experience** - Clear progress indicators and helpful error messages
4. **Error handling** - Graceful failures with actionable messages
5. **Documentation** - Clear docstrings and help text
6. **Testing** - Verify with real API calls when possible

## File Structure

```
paradime/
├── cli/
│   ├── integrations/
│   │   ├── airbyte.py
│   │   ├── adf.py
│   │   ├── fivetran.py
│   │   ├── montecarlo.py
│   │   ├── power_bi.py
│   │   └── tableau.py
│   └── run.py (registration)
└── core/
    └── scripts/
        ├── airbyte.py
        ├── azure_data_factory.py
        ├── fivetran.py
        ├── montecarlo.py
        ├── power_bi.py
        ├── tableau.py
        └── utils.py
```
