# Hex Triggers Integration Example

This directory contains an example for integrating Hex projects with Paradime as trigger nodes in your data lineage.

## Overview

This example demonstrates how to:
1. Create a custom integration for Hex projects in Paradime
2. Map Hex projects to trigger nodes with lineage connections to dbt models
3. Programmatically trigger Hex project runs after dbt model completions
4. Wait for Hex project runs to complete with status polling

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

### 3. Paradime API Credentials

Generate your API key, secret, and endpoint from Paradime workspace settings and update the example:

```python
paradime = Paradime(
    api_endpoint="YOUR_API_ENDPOINT",
    api_key="YOUR_API_KEY",
    api_secret="YOUR_API_SECRET",
)
```

## Example: Hex Project Triggers (`hex_trigger.py`)

Creates a custom integration for Hex projects that can be triggered based on dbt model completions.

**Features:**
- Fetches all Hex projects from your workspace
- Creates trigger nodes with metadata (project ID, owner, created/modified timestamps)
- Shows lineage connection from dbt models to Hex projects
- Provides functions to trigger project runs and monitor status
- Supports optional input parameters for parameterized Hex projects
- Implements completion polling with configurable timeout

**Usage:**

```bash
python examples/hex_triggers/hex_trigger.py
```

**Key Functions:**

### `list_hex_projects()`
Retrieves all Hex projects from your workspace.

```python
projects = list_hex_projects(
    limit=100,
    include_archived=False,
    include_trashed=False,
)
```

### `trigger_hex_project()`
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

### `get_run_status()`
Checks the status of a Hex project run.

```python
status = get_run_status(
    project_id="your-project-id",
    run_id="run-uuid",
)
```

## Integration Pattern

The example follows the standard Paradime custom integration pattern:

1. **Setup Integration**: Create or update a custom integration in Paradime
   ```python
   integration = paradime.custom_integration.upsert(
       name="HexProjects",
       logo_url="https://hex.tech/logo.svg",
       node_types=[NodeType(...)],
   )
   ```

2. **Fetch Hex Projects**: Use Hex API to fetch projects
   ```python
   hex_projects = list_hex_projects()
   ```

3. **Create Trigger Nodes**: Map Hex projects to trigger nodes
   ```python
   trigger_node = NodeTriggerLike(
       name=project_name,
       node_type="HexProject",
       attributes=NodeTriggerLikeAttributes(...),
       lineage=Lineage(
           upstream_dependencies=[
               LineageDependencyDbtObject(
                   table_name="my_dbt_model",
               ),
           ],
       ),
   )
   ```

4. **Add Nodes**: Add all trigger nodes to the integration
   ```python
   paradime.custom_integration.add_nodes(
       integration_uid=integration.uid,
       nodes=trigger_nodes,
   )
   ```

5. **Trigger Projects**: Programmatically trigger Hex projects
   ```python
   result = trigger_hex_project(project_id, input_params)
   ```

## Lineage Connections

The example demonstrates how to connect Hex projects to dbt models:

```python
lineage=Lineage(
    upstream_dependencies=[
        LineageDependencyDbtObject(
            database_name="analytics",      # Optional
            schema_name="public",           # Optional
            table_name="customer_analytics", # Required
        ),
    ],
)
```

This creates a lineage graph showing:
```
dbt_model → Hex_Project
```

## Run Status Polling

When `wait_for_completion=True`, the trigger function polls the Hex API every 10 seconds to check run status:

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

## Customization

### Modify Lineage Connections

Update the `upstream_dependencies` to match your dbt models:

```python
lineage=Lineage(
    upstream_dependencies=[
        LineageDependencyDbtObject(
            database_name="your_database",
            schema_name="your_schema",
            table_name="your_model",
        ),
    ],
)
```

### Filter Projects

Add filtering logic to only include specific projects:

```python
# Example: Only include projects owned by specific users
hex_projects = [
    project for project in list_hex_projects()
    if project.get("ownerEmail") in ["user1@company.com", "user2@company.com"]
]
```

### Add Custom Attributes

Extend the `NodeTriggerLikeAttributes` with additional metadata:

```python
attributes=NodeTriggerLikeAttributes(
    description="Custom description",
    tags=["production", "critical", "dashboard"],
    project_id=project_id,
    owner=owner_email,
    # Add any custom fields supported by your Paradime instance
)
```

### Pass Input Parameters

For parameterized Hex projects, pass input values:

```python
result = trigger_hex_project(
    project_id=project_id,
    input_params={
        "start_date": "2024-01-01",
        "end_date": "2024-01-31",
        "region": "US",
        "min_threshold": 100,
    },
)
```

## Error Handling

The example includes basic error handling. For production use, enhance error handling:

```python
import logging

logger = logging.getLogger(__name__)

try:
    result = trigger_hex_project(
        project_id=project_id,
        wait_for_completion=True,
        timeout_minutes=60,
    )
    if result["status"] == "error":
        logger.error(f"Failed to trigger Hex project: {result['message']}")
        # Send alert, retry, etc.
    elif result.get("run_status") == "ERRORED":
        logger.error(f"Hex project run failed: {result['run_id']}")
        # Handle failed run
except Exception as e:
    logger.exception(f"Unexpected error: {e}")
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
    time.sleep(3)  # Wait 3 seconds between triggers to stay under rate limit
```

## Best Practices

1. **Use Environment Variables**: Store credentials securely in environment variables
2. **Implement Retry Logic**: Add retry logic for transient API failures
3. **Monitor Executions**: Track execution status and implement alerting
4. **Respect Rate Limits**: Implement rate limiting when triggering multiple projects
5. **Document Lineage**: Clearly document lineage connections in code comments
6. **Handle Timeouts**: Set appropriate timeout values based on project complexity
7. **Validate Permissions**: Ensure API token has necessary permissions

## Required Hex API Permissions

The example requires the following Hex API permissions:

- `projects:read` - List and read project information
- `projects:run` - Trigger project runs
- `runs:read` - Check run status

Ensure your API token has these permissions enabled.

## Troubleshooting

### Issue: Hex API credentials not found
**Solution**: Ensure `HEX_API_TOKEN` environment variable is set with a valid token

### Issue: Paradime API authentication failure
**Solution**: Verify your API key, secret, and endpoint are correct

### Issue: No projects found
**Solution**: Check that your Hex workspace has projects and that the API token has access

### Issue: Rate limit exceeded
**Solution**: Implement delays between API calls and respect the 20 requests/minute limit

### Issue: Run timeout
**Solution**: Increase `timeout_minutes` parameter or check if the Hex project has issues

### Issue: Permission denied errors
**Solution**: Verify your API token has the necessary permissions listed above

## Additional Resources

- [Paradime Documentation](https://docs.paradime.io)
- [Hex API Documentation](https://learn.hex.tech/docs/api/api-overview)
- [Hex API Reference](https://learn.hex.tech/docs/api/api-reference)

## Support

For issues or questions:
- Paradime SDK: [GitHub Issues](https://github.com/paradime-io/paradime-python-sdk/issues)
- Hex Support: [Hex Community](https://community.hex.tech/)
