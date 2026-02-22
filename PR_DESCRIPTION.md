# Add Matillion Data Productivity Cloud Pipeline Integration

## Summary

This PR adds support for triggering and managing Matillion Data Productivity Cloud (DPC) pipelines through the Paradime SDK, following the same pattern as the existing Fivetran integration.

## Changes

### New Files
- **`paradime/core/scripts/matillion.py`** - Core implementation for Matillion DPC pipeline operations
  - `trigger_matillion_pipeline()` - Triggers multiple pipelines with parallel execution
  - `trigger_single_pipeline()` - Triggers a single pipeline with monitoring
  - `list_matillion_projects()` - Lists all available projects with pagination
  - `list_matillion_pipelines()` - Lists all available published pipelines
  - `_wait_for_execution_completion()` - Polls execution status until completion
  - `_get_access_token()` - Obtains OAuth access token for API authentication

- **`paradime/cli/integrations/matillion.py`** - CLI commands for Matillion DPC integration
  - `matillion_pipeline` - Command to trigger pipeline executions
  - `matillion_list_projects` - Command to list all projects
  - `matillion_list_pipelines` - Command to list all published pipelines

### Modified Files
- **`paradime/cli/run.py`** - Registered Matillion commands in the CLI

## Features

### 1. Pipeline Execution (`matillion_pipeline`)
Trigger one or more Matillion DPC pipelines with the following options:

```bash
paradime run matillion-pipeline \
  --base-url "https://us1.api.matillion.com" \
  --client-id "your-client-id" \
  --client-secret "your-client-secret" \
  --project-id "your-project-id" \
  --pipeline-name "My Pipeline" \
  --pipeline-name "Another Pipeline" \
  --environment "production" \
  --wait-for-completion \
  --timeout-minutes 1440
```

**Features:**
- ✅ OAuth 2.0 authentication with automatic token management
- ✅ Parallel execution of multiple pipelines using ThreadPoolExecutor
- ✅ Real-time progress monitoring with timestamped logs
- ✅ Wait for completion with configurable timeout
- ✅ Comprehensive error handling
- ✅ Exit code 1 on failures for CI/CD integration

**Environment Variables:**
- `MATILLION_BASE_URL` - Matillion DPC API URL (e.g., `https://us1.api.matillion.com` or `https://eu1.api.matillion.com`)
- `MATILLION_CLIENT_ID` - OAuth client ID
- `MATILLION_CLIENT_SECRET` - OAuth client secret

### 2. List Projects (`matillion_list_projects`)
List all available Matillion DPC projects:

```bash
paradime run matillion-list-projects \
  --base-url "https://us1.api.matillion.com" \
  --client-id "your-client-id" \
  --client-secret "your-client-secret"
```

**Features:**
- ✅ Displays project ID, name, and description
- ✅ Handles pagination automatically (fetches all projects)
- ✅ OAuth authentication with automatic token retrieval

### 3. List Pipelines (`matillion_list_pipelines`)
List all available Matillion DPC published pipelines:

```bash
paradime run matillion-list-pipelines \
  --base-url "https://us1.api.matillion.com" \
  --client-id "your-client-id" \
  --client-secret "your-client-secret" \
  --project-id "your-project-id" \
  --environment "production"  # Optional
```

**Features:**
- ✅ Displays pipeline name, environment, and pipeline ID
- ✅ Optional filtering by environment
- ✅ OAuth authentication with automatic token retrieval

## Implementation Details

### Matillion Data Productivity Cloud API
This implementation uses the official Matillion DPC REST API v1:

**Authentication:**
- OAuth 2.0 Client Credentials flow
- Endpoint: `POST https://id.core.matillion.com/oauth/dpc/token`
- Content-Type: `application/x-www-form-urlencoded`
- Request body: `grant_type=client_credentials&client_id=...&client_secret=...&audience=https://api.matillion.com`
- Response: `{"access_token": "...", ...}`
- Access tokens valid for 30 minutes

**Pipeline Execution:**
- Endpoint: `POST {baseUrl}/dpc/v1/projects/{projectId}/pipeline-executions`
- Request body: `{"pipelineName": "Pipeline 1", "environmentName": "Dev Environment"}`
- Optional: `"executionTag"` for preventing concurrent executions
- Optional: `"scalarVariables"` for passing variables to the pipeline
- Returns: `{"pipelineExecutionId": "1398aa31-af57-4a6a-9752-27c2e8556c3f"}`

**Status Monitoring:**
- Endpoint: `GET {baseUrl}/dpc/v1/projects/{projectId}/pipeline-executions/{executionId}`
- Status values: `RUNNING`, `SUCCESS`, `FAILED`, `CANCELLED`, `QUEUED`
- Poll interval: 5 seconds

**List Projects:**
- Endpoint: `GET {baseUrl}/dpc/v1/projects?page=0&size=25`
- Returns: Paginated response with `{"page": 0, "results": [...], "size": 25, "total": N}`
- Response includes project `id`, `name`, and `description` fields
- Automatically fetches all pages

**List Pipelines:**
- Endpoint: `GET {baseUrl}/dpc/v1/projects/{projectId}/published-pipelines`
- Returns: Paginated response with `{"page": 0, "results": [...], "size": 25, "total": N}`
- Response includes pipeline `name` and `publishedTime` fields

### Design Pattern
This implementation follows the exact same pattern as the Fivetran integration:
- Core business logic in `paradime/core/scripts/`
- CLI interface in `paradime/cli/integrations/`
- Click commands with environment variable support
- ThreadPoolExecutor for parallel execution
- Consistent logging with emojis and timestamps
- Comprehensive error handling and status reporting

### Regional Support
Matillion DPC API is available in multiple regions:
- US: `https://us1.api.matillion.com`
- EU: `https://eu1.api.matillion.com`

Users specify their region via the `MATILLION_BASE_URL` environment variable.

### Output Formatting
The commands provide rich, formatted output with:
- 📊 Visual separators and tables
- ⚡ Real-time progress updates
- ✅ Status indicators with emojis
- ⏱️ Elapsed time tracking
- 🔐 Authentication status messages

## Testing

### Manual Testing Checklist
- [ ] Test OAuth authentication and token retrieval
- [ ] Test list projects command
- [ ] Test list pipelines command
- [ ] Test environment filtering for pipelines
- [ ] Test pipeline execution with single pipeline
- [ ] Test pipeline execution with multiple pipelines
- [ ] Test wait-for-completion flag
- [ ] Test timeout handling
- [ ] Verify error handling for invalid credentials
- [ ] Verify error handling for non-existent projects/pipelines
- [ ] Test environment variable configuration
- [ ] Verify exit codes on success/failure
- [ ] Test both US and EU regional endpoints
- [ ] Test pagination for projects list

### Example Usage

```bash
# Set environment variables
export MATILLION_BASE_URL="https://us1.api.matillion.com"
export MATILLION_CLIENT_ID="your-client-id"
export MATILLION_CLIENT_SECRET="your-client-secret"

# Trigger a single pipeline
paradime run matillion-pipeline \
  --project-id "my-project-id" \
  --pipeline-name "My ETL Pipeline" \
  --environment "production"

# Trigger multiple pipelines in parallel
paradime run matillion-pipeline \
  --project-id "my-project-id" \
  --pipeline-name "Data Ingestion" \
  --pipeline-name "Data Transformation" \
  --pipeline-name "Data Quality Checks" \
  --environment "staging" \
  --wait-for-completion \
  --timeout-minutes 60

# List all projects (to get project IDs)
paradime run matillion-list-projects

# List all published pipelines
paradime run matillion-list-pipelines \
  --project-id "my-project-id"

# List pipelines for specific environment
paradime run matillion-list-pipelines \
  --project-id "my-project-id" \
  --environment "production"
```

## Authentication Setup

To use this integration, you need to generate OAuth credentials in your Matillion DPC account:

1. Log in to your Matillion Data Productivity Cloud account
2. Navigate to Settings → API Credentials
3. Create a new OAuth client
4. Copy the Client ID and Client Secret
5. Set the environment variables or pass them as CLI options

## API Documentation References

- [Matillion DPC API Authentication](https://docs.matillion.com/data-productivity-cloud/api/docs/authentication/)
- [Executing and Managing Pipelines](https://docs.matillion.com/data-productivity-cloud/api/docs/executing-and-managing-a-pipeline/)
- [Pipeline Execution and Status Retrieval Framework](https://www.matillion.com/blog/data-productivity-cloud-api-pipeline-execution-and-status-retrieval-framework)

## Related Issues

Closes ENG-2229

## Notes

- This implementation uses Matillion Data Productivity Cloud API v1
- OAuth access tokens are automatically retrieved and valid for 30 minutes
- Authentication endpoint is fixed at `https://id.core.matillion.com/oauth/dpc/token`
- **OAuth Scope Limitation**: The OAuth client credentials have a scope of "pipeline-execution" which may limit access to list all projects. If the `list-projects` command returns no results, you'll need to:
  - Find your project ID in the Matillion UI (check the URL or project settings)
  - Use the project name directly as the `project_id` parameter
  - Or assign the API client to specific projects in Matillion settings
- Pipeline names must match published pipeline names in your Matillion project
- The `project_id` can be a project name (e.g., "paradime-sdk") or a project GUID
- API rate limit is 1000 requests per minute
- Supports both US and EU regional API endpoints (e.g., `https://us1.api.matillion.com`, `https://eu1.api.matillion.com`)
- Optional `executionTag` parameter can be used to prevent concurrent executions
- Pipeline variables can be passed via `scalarVariables` in the execution request
