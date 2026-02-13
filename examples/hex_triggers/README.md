# Hex Triggers

This directory contains examples for triggering Hex projects via CLI and Python API after dbt model completions or other events.

## Overview

This example demonstrates how to:
1. Trigger Hex project runs via the Paradime CLI
2. List Hex projects in your workspace
3. Wait for Hex project runs to complete with status polling
4. Use the Hex API directly in Python scripts

## Prerequisites

### 1. Install Dependencies

```bash
pip install paradime-io requests
```

### 2. Hex API Credentials

Set up your Hex API token as an environment variable:

```bash
export HEX_API_TOKEN="your_hex_api_token"
export HEX_BASE_URL="https://app.hex.tech"  # Optional, defaults to https://app.hex.tech
```

To get your Hex API token:
1. Log in to your Hex workspace
2. Go to Settings → API tokens
3. Create a new API token with appropriate permissions

## CLI Usage

### Trigger a Hex Project

```bash
# Basic trigger
paradime hex trigger abc-123-def-456

# Trigger with input parameters
paradime hex trigger abc-123-def-456 --input-param date=2024-01-01 --input-param region=US

# Trigger and wait for completion
paradime hex trigger abc-123-def-456 --wait

# Trigger with custom timeout
paradime hex trigger abc-123-def-456 --wait --timeout-minutes 120

# Get JSON output
paradime hex trigger abc-123-def-456 --json
```

### List Hex Projects

```bash
# List all projects
paradime hex list-projects

# List including archived projects
paradime hex list-projects --include-archived

# Get JSON output
paradime hex list-projects --json
```

## CLI Command Reference

### `paradime hex trigger`

Trigger a Hex project run.

**Arguments:**
- `PROJECT_ID` (required): UUID of the Hex project to trigger

**Options:**
- `--input-param KEY=VALUE`: Input parameters (can be used multiple times)
- `--update-published/--no-update-published`: Update cached app state (default: true)
- `--wait`: Wait for the run to complete
- `--timeout-minutes INTEGER`: Maximum wait time in minutes (default: 60)
- `--json`: Output JSON format

**Examples:**
```bash
paradime hex trigger abc-123 --input-param date=2024-01-01 --wait
```

### `paradime hex list-projects`

List all Hex projects in the workspace.

**Options:**
- `--limit INTEGER`: Number of projects to fetch (default: 100)
- `--include-archived`: Include archived projects
- `--include-trashed`: Include trashed projects
- `--json`: Output JSON format

**Examples:**
```bash
paradime hex list-projects --include-archived --json
```

## Python API Usage

For programmatic access, use the example script:

```python
from examples.hex_triggers.hex_trigger import (
    list_hex_projects,
    trigger_hex_project,
    get_run_status
)

# List all projects
projects = list_hex_projects()

# Trigger a project
result = trigger_hex_project(
    project_id="abc-123-def-456",
    input_params={"date": "2024-01-01"},
    wait_for_completion=True,
    timeout_minutes=60,
)

# Check run status
status = get_run_status(
    project_id="abc-123-def-456",
    run_id="run-uuid-here"
)
```

### Key Functions

#### `list_hex_projects()`
Retrieves all Hex projects from your workspace.

```python
projects = list_hex_projects(
    limit=100,
    include_archived=False,
    include_trashed=False,
)
```

#### `trigger_hex_project()`
Triggers a Hex project run with optional parameters.

```python
result = trigger_hex_project(
    project_id="your-project-id",
    input_params={"date": "2024-01-01"},  # Optional
    update_published_results=True,
    wait_for_completion=True,
    timeout_minutes=60,
)
```

**Parameters:**
- `project_id` (str, required): UUID of the Hex project
- `input_params` (dict, optional): Dictionary of input variable names and values
- `update_published_results` (bool): Update cached app state with run results (default: True)
- `use_cached_sql_results` (bool): Use cached SQL results (default: True)
- `wait_for_completion` (bool): Wait for the run to complete (default: False)
- `timeout_minutes` (int): Maximum wait time in minutes (default: 60)

#### `get_run_status()`
Checks the status of a Hex project run.

```python
status = get_run_status(
    project_id="your-project-id",
    run_id="run-uuid",
)
```

## Run Status Polling

When `--wait` flag is used (CLI) or `wait_for_completion=True` (Python), the command polls the Hex API every 10 seconds:

**Polling Behavior:**
- Polls every 10 seconds
- Logs progress every 60 seconds (every 6 polls)
- Checks for completion states: `COMPLETED`, `ERRORED`, `KILLED`, `UNABLE_TO_ALLOCATE_KERNEL`
- Times out after configured minutes
- Handles network errors with retry logic

**Status Values:**
- `PENDING` - Run is queued
- `RUNNING` - Run is in progress
- `COMPLETED` - Run finished successfully
- `ERRORED` - Run failed with error
- `KILLED` - Run was manually stopped
- `UNABLE_TO_ALLOCATE_KERNEL` - Unable to allocate compute resources

## Integration with Bolt Schedules

You can trigger Hex projects after dbt runs by adding a command to your Bolt schedule:

```yaml
# paradime_schedules.yml
schedules:
  - name: daily_analytics
    schedule: "0 8 * * *"
    commands:
      - dbt run --select customer_analytics
      - paradime hex trigger abc-123-def-456 --wait
```

## API Rate Limits

**Important:** The Hex API has rate limits:
- Maximum 20 requests per minute
- Maximum 60 requests per hour

When triggering multiple projects, implement rate limiting:

```python
import time

for project_id in project_ids:
    result = trigger_hex_project(project_id)
    time.sleep(3)  # Wait 3 seconds between triggers
```

## Error Handling

### CLI
The CLI exits with non-zero status codes on errors:
- Missing credentials
- API errors
- Run failures
- Timeouts

### Python
Functions return dictionaries with error information:

```python
result = trigger_hex_project(project_id)
if result.get("status") == "error":
    print(f"Error: {result['message']}")
```

## Required Hex API Permissions

The CLI and Python API require the following permissions:

- `projects:read` - List and read project information
- `projects:run` - Trigger project runs
- `runs:read` - Check run status

Ensure your API token has these permissions enabled.

## Troubleshooting

### Issue: Hex API credentials not found
**Solution**: Ensure `HEX_API_TOKEN` environment variable is set with a valid token

```bash
export HEX_API_TOKEN="your_token_here"
```

### Issue: No projects found
**Solution**: Check that your Hex workspace has projects and that the API token has access

### Issue: Rate limit exceeded
**Solution**: Implement delays between API calls and respect the 20 requests/minute limit

### Issue: Run timeout
**Solution**: Increase `--timeout-minutes` parameter or check if the Hex project has issues

### Issue: Permission denied errors
**Solution**: Verify your API token has the necessary permissions listed above

## Examples

### Trigger after dbt model completion
```bash
dbt run --select customer_analytics && \
  paradime hex trigger abc-123-def --input-param date=$(date +%Y-%m-%d) --wait
```

### List and trigger all projects
```bash
# Get all project IDs
paradime hex list-projects --json | jq -r '.[].projectId' | while read project_id; do
  echo "Triggering $project_id"
  paradime hex trigger "$project_id"
  sleep 3  # Rate limiting
done
```

### Check if run completed successfully
```bash
if paradime hex trigger abc-123-def --wait; then
  echo "Hex project completed successfully"
else
  echo "Hex project failed"
  exit 1
fi
```

## Additional Resources

- [Hex API Documentation](https://learn.hex.tech/docs/api/api-overview)
- [Hex API Reference](https://learn.hex.tech/docs/api/api-reference)
- [Paradime Documentation](https://docs.paradime.io)

## Support

For issues or questions:
- Paradime SDK: [GitHub Issues](https://github.com/paradime-io/paradime-python-sdk/issues)
- Hex Support: [Hex Community](https://community.hex.tech/)
