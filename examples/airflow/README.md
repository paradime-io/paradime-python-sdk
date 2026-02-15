# Airflow DAG Trigger Examples

This directory contains examples for triggering Airflow DAG runs using the Paradime SDK.

## Supported Platforms

- **AWS MWAA** (Managed Workflows for Apache Airflow)
- **GCP Cloud Composer** (Google Cloud's managed Airflow)
- **Astronomer** (Cloud or Self-hosted)
- **Self-hosted Airflow** instances

## Prerequisites

Set the following environment variables:

```bash
export AIRFLOW_BASE_URL="https://your-airflow-instance.com"
export AIRFLOW_USERNAME="your_username"
export AIRFLOW_PASSWORD="your_password"
```

### Platform-Specific Configuration

#### AWS MWAA

For AWS MWAA, you need to:
1. Use the MWAA webserver URL as `AIRFLOW_BASE_URL`
2. Create an Airflow user with appropriate permissions
3. Use the Airflow username and password for authentication

```bash
export AIRFLOW_BASE_URL="https://your-mwaa-id.mwaa.region.amazonaws.com"
export AIRFLOW_USERNAME="admin"
export AIRFLOW_PASSWORD="your_password"
```

#### GCP Cloud Composer

For GCP Cloud Composer, you have two authentication options:

**Option 1: Application Default Credentials (Recommended)**
1. Install google-auth: `pip install google-auth`
2. Authenticate using gcloud: `gcloud auth application-default login`
3. Use the `--use-gcp-auth` flag with CLI commands

```bash
export AIRFLOW_BASE_URL="https://your-composer-airflow-url.appspot.com"

# Use the --use-gcp-auth flag
paradime run airflow-trigger --dag-id my_dag --use-gcp-auth
```

**Option 2: Service Account Key**
1. Create a service account with Cloud Composer permissions
2. Download the JSON key file
3. Set the `GOOGLE_APPLICATION_CREDENTIALS` environment variable
4. Use the `--use-gcp-auth` flag

```bash
export AIRFLOW_BASE_URL="https://your-composer-airflow-url.appspot.com"
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account-key.json"

# Use the --use-gcp-auth flag
paradime run airflow-trigger --dag-id my_dag --use-gcp-auth
```

**Required GCP Permissions:**
- `composer.environments.get`
- `composer.operations.get`
- `iam.serviceAccounts.actAs` (if using service account)

#### Astronomer

For Astronomer:
1. Use your Astronomer Airflow deployment URL as `AIRFLOW_BASE_URL`
2. Create an API key or use your login credentials

```bash
export AIRFLOW_BASE_URL="https://your-deployment.astronomer.run"
export AIRFLOW_USERNAME="your_username"
export AIRFLOW_PASSWORD="your_password_or_api_key"
```

#### Self-hosted Airflow

For self-hosted Airflow:
1. Use your Airflow webserver URL as `AIRFLOW_BASE_URL`
2. Use your Airflow credentials for authentication

```bash
export AIRFLOW_BASE_URL="https://your-airflow.example.com"
export AIRFLOW_USERNAME="admin"
export AIRFLOW_PASSWORD="your_password"
```

## Usage

### CLI Commands

#### List Available DAGs

```bash
# List all active DAGs
paradime run airflow-list-dags

# List all DAGs (including paused)
paradime run airflow-list-dags --no-only-active
```

#### Trigger DAG Runs

```bash
# Trigger a single DAG
paradime run airflow-trigger --dag-id my_dag

# Trigger multiple DAGs
paradime run airflow-trigger --dag-id dag1 --dag-id dag2

# Trigger without waiting for completion
paradime run airflow-trigger --dag-id my_dag --no-wait-for-completion

# Trigger without showing logs
paradime run airflow-trigger --dag-id my_dag --no-show-logs

# Custom timeout (default: 1440 minutes)
paradime run airflow-trigger --dag-id my_dag --timeout-minutes 60

# GCP Cloud Composer (using Application Default Credentials)
paradime run airflow-trigger --dag-id my_dag --use-gcp-auth

# GCP Cloud Composer (multiple DAGs)
paradime run airflow-trigger --dag-id dag1 --dag-id dag2 --use-gcp-auth
```

### Python SDK

#### List DAGs

```python
from paradime.core.scripts.airflow import list_airflow_dags

list_airflow_dags(
    base_url="https://your-airflow-instance.com",
    username="your_username",
    password="your_password",
    only_active=True,
)
```

#### Trigger DAG Runs

**Standard Airflow (MWAA, Astronomer, Self-hosted):**
```python
from paradime.core.scripts.airflow import trigger_airflow_dags

results = trigger_airflow_dags(
    base_url="https://your-airflow-instance.com",
    username="your_username",
    password="your_password",
    dag_ids=["dag1", "dag2"],
    dag_run_conf={"key": "value"},  # Optional configuration
    wait_for_completion=True,
    timeout_minutes=60,
    show_logs=True,
)

# Check results
for result in results:
    print(result)
```

**GCP Cloud Composer:**
```python
from paradime.core.scripts.airflow import trigger_airflow_dags

# Using Application Default Credentials
results = trigger_airflow_dags(
    base_url="https://your-composer-airflow-url.appspot.com",
    dag_ids=["dag1", "dag2"],
    use_gcp_auth=True,  # Enable GCP authentication
    wait_for_completion=True,
    timeout_minutes=60,
    show_logs=True,
)

# Check results
for result in results:
    print(result)
```

## Features

### Real-time Progress Monitoring

When `wait_for_completion=True`, the trigger will:
- Monitor DAG run status in real-time
- Display task execution progress
- Show completion status for each task

### Task Log Display

When `show_logs=True`, the trigger will:
- Automatically fetch and display task logs
- Show logs for completed tasks (success or failure)
- Display the last 50 lines of each task's logs for readability

### Parallel Execution

The SDK supports triggering multiple DAGs in parallel using ThreadPoolExecutor for optimal performance.

### Error Handling

The SDK includes:
- Automatic retry logic for network errors
- Timeout handling for long-running DAGs
- Detailed error messages with context

## Examples

See the example scripts in this directory:
- `list_dags.py` - List all available DAGs
- `trigger_dag.py` - Trigger DAG runs with various options

Run examples:
```bash
python examples/airflow/list_dags.py
python examples/airflow/trigger_dag.py
```

## Authentication

The Airflow REST API uses basic authentication. Make sure your Airflow instance has the REST API enabled and your user has the necessary permissions to:
- View DAGs
- Trigger DAG runs
- View task logs
- View DAG run status

## Troubleshooting

### Connection Issues

If you encounter connection issues:
1. Verify the `AIRFLOW_BASE_URL` is correct and accessible
2. Check that the Airflow REST API is enabled
3. Ensure your credentials are valid

### Authentication Errors

If you get 401/403 errors:
1. Verify your username and password are correct
2. Check that your user has the necessary permissions
3. For MWAA, ensure you've created the user in the Airflow UI
4. For Cloud Composer, verify:
   - You have the necessary IAM permissions
   - Your ADC or service account is properly configured
   - Run `gcloud auth application-default login` to refresh credentials
   - Check that the service account has `composer.user` or equivalent role

### GCP Cloud Composer Issues

If you encounter issues with Cloud Composer:
1. **Missing google-auth library**: Install with `pip install google-auth`
2. **Permission denied**: Ensure your service account or user has these roles:
   - `roles/composer.user` (read and execute)
   - `roles/composer.admin` (full access)
3. **Authentication failed**:
   - Run `gcloud auth application-default login` to refresh credentials
   - Verify `GOOGLE_APPLICATION_CREDENTIALS` points to a valid service account key
4. **URL not accessible**: Get the correct Airflow webserver URL from Cloud Composer console

### API Version Compatibility

This implementation uses Airflow REST API v2, which is available in:
- Airflow 2.0+ (v2 API introduced in Airflow 2.0)
- Airflow 3.0+ (v1 API removed, v2 is required)
- AWS MWAA (all versions with Airflow 2.0+)
- Astronomer Cloud and Enterprise

For older Airflow versions (1.x), you may need to use the experimental API or upgrade your instance.
