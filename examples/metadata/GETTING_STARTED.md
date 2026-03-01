# Getting Started with Paradime Metadata SDK

This guide will help you get up and running with the Paradime Metadata SDK for dbt project analysis. The SDK provides powerful capabilities for analyzing your dbt projects' metadata, performance, quality, and governance.

## Prerequisites

### 1. Installation

Install the Paradime Python SDK with metadata support:

```bash
# Using pip
pip install "paradime-io"

# Using poetry
poetry add "paradime-io"
```

### 2. API Credentials

Set up your Paradime API credentials. You can find these in your Paradime workspace settings:

```bash
export PARADIME_API_KEY="your-api-key"
export PARADIME_API_SECRET="your-api-secret" 
export PARADIME_API_ENDPOINT="your-api-endpoint"
```

Or create a `.env` file:
```bash
PARADIME_API_KEY=your-api-key
PARADIME_API_SECRET=your-api-secret
PARADIME_API_ENDPOINT=your-api-endpoint
```

### 3. Schedule Setup

Ensure you have a dbt schedule in Paradime that has run recently. The SDK analyzes metadata from your schedule runs.

## Your First Analysis

### Step 1: Basic SDK Usage

Start with the basic usage example to understand core concepts:

```python
from paradime.client.paradime_client import Paradime

# Initialize the client
client = Paradime(
    api_key="your-api-key",
    api_secret="your-api-secret",
    api_endpoint="your-api-endpoint"
)

# Set your schedule name
schedule_name = "your-schedule-name"

# Get basic model information
models = client.metadata.get_model_health(schedule_name)
print(f"Found {len(models)} models")

# Get test results
tests = client.metadata.get_test_results(schedule_name)
print(f"Found {len(tests)} tests")

# Get source freshness
sources = client.metadata.get_source_freshness(schedule_name)
print(f"Found {len(sources)} sources")

# Export data as JSON
import json
models_data = [{'name': m.name, 'status': m.status} for m in models[:3]]
print(json.dumps(models_data, indent=2))
```

### Step 2: Run the Basic Usage Example

```bash
# Navigate to the examples directory
cd examples/metadata

# Update the schedule name in basic_usage.py to match your project
# Then run the example
python basic_usage.py
```

## Understanding the SDK

### Core Data Types

The SDK provides access to all dbt resource types:

| Resource Type | SDK Method | Description |
|---------------|------------|-------------|
| **Models** | `get_model_health()` | Model execution status, performance, and metadata |
| **Tests** | `get_test_results()` | Test execution results and failure details |
| **Sources** | `get_source_freshness()` | Source freshness status and timing |
| **Seeds** | `get_seed_data()` | Seed file information and status |
| **Snapshots** | `get_snapshot_data()` | Snapshot execution and metadata |
| **Exposures** | `get_exposure_data()` | Downstream usage and BI tool integration |

### Custom SQL Queries

Use the built-in DuckDB database for custom analysis:

```python
# Custom query example
results = client.metadata.query_sql("""
    SELECT name, execution_time, status
    FROM models_with_tests 
    WHERE execution_time > 10
    ORDER BY execution_time DESC
""", [schedule_name])
```

### Available Database Tables

The SDK creates these tables for analysis:

- `models_with_tests` - Models with aggregated test counts
- `dbt_run_results` - Raw dbt run results
- `dbt_source_freshness_results` - Source freshness data
- `dbt_test_data` - Test execution details
- `dbt_seed_data` - Seed information
- `dbt_snapshot_data` - Snapshot details
- `dbt_exposure_data` - Exposure information

## Common Analysis Patterns

### 1. Model Performance Analysis

```python
# Get models with performance issues
models = client.metadata.get_model_health(schedule_name)
slow_models = [m for m in models if m.execution_time and m.execution_time > 60]

print("Models taking > 60 seconds:")
for model in slow_models:
    print(f"  {model.name}: {model.execution_time:.1f}s")
```

### 2. Data Quality Monitoring

```python
# Check for failed tests
tests = client.metadata.get_test_results(schedule_name, failed_only=True)
if tests:
    print("Failed tests found:")
    for test in tests:
        print(f"  {test.test_name}: {test.error_message}")
```

### 3. Source Freshness Checking

```python
# Check source freshness
sources = client.metadata.get_source_freshness(schedule_name)
stale_sources = [s for s in sources if s.freshness_status == 'error']

if stale_sources:
    print("Stale sources:")
    for source in stale_sources:
        print(f"  {source.source_name}.{source.name}: {source.hours_since_load:.1f}h old")
```

## Advanced Usage

### JSON Data Export

Export metadata for API integrations and automated workflows:

```python
import json

# Export models data
models = client.metadata.get_model_health(schedule_name)
models_data = []
for model in models:
    models_data.append({
        'name': model.name,
        'status': model.status,
        'execution_time': model.execution_time,
        'health_status': model.health_status,
        'total_tests': model.total_tests,
        'materialized_type': model.materialized_type
    })

# Convert to JSON
models_json = json.dumps(models_data, indent=2, default=str)
print(models_json)

# Save to file
with open('models_export.json', 'w') as f:
    json.dump(models_data, f, indent=2, default=str)

# Export custom query results
df = client.metadata.query_sql("SELECT * FROM models_with_tests LIMIT 10")
query_json = df.to_json(orient='records', indent=2)
```

### Custom Analysis Classes

Build reusable analysis classes:

```python
class ProjectAnalyzer:
    def __init__(self, metadata_client):
        self.metadata = metadata_client
    
    def get_project_health(self, schedule_name):
        models = self.metadata.get_model_health(schedule_name)
        tests = self.metadata.get_test_results(schedule_name)
        
        return {
            'total_models': len(models),
            'failed_models': len([m for m in models if m.status == 'fail']),
            'total_tests': len(tests),
            'failed_tests': len([t for t in tests if t.status == 'fail'])
        }

# Usage
analyzer = ProjectAnalyzer(client.metadata)
health = analyzer.get_project_health(schedule_name)
print(f"Project health: {health}")
```

### Batch Analysis

Analyze multiple schedules:

```python
schedules = ['prod-daily', 'staging-hourly', 'dev-manual']

for schedule in schedules:
    models = client.metadata.get_model_health(schedule)
    print(f"{schedule}: {len(models)} models")
```

## Example Progression

Follow this learning path:

### 1. Start Here
- **`basic_usage.py`** - Learn SDK fundamentals

### 2. List and Browse Resources
- **`list_models.py`** - Browse and discover models in your project
- **`list_sources.py`** - Browse and discover data sources

### 3. Detailed Analysis
- **`single_model_status.py`** - Deep dive into individual model status and tests
- **`single_source_status.py`** - Detailed source freshness and table information

### 4. Advanced Analysis
- **`performance_analysis.py`** - Model execution and optimization
- **`quality_monitoring.py`** - Data quality and testing
- **`discovery_insights.py`** - Lineage and documentation analysis

## Troubleshooting

### Common Issues

**No data found:**
- Verify your schedule name is correct
- Ensure your schedule has run recently
- Check that your dbt project generates metadata files

**Authentication errors:**
- Verify API credentials are set correctly
- Check that credentials have access to your workspace

**Empty results:**
- Confirm your schedule has models, tests, or sources
- Check that run_results.json and manifest.json are generated

**SQL query errors:**
- Verify table names (use `models_with_tests` instead of raw table names)
- Check column names in the database schema
- Ensure parameters are passed correctly to `query_sql()`

### Getting Help

1. **Check Available Data:**
   ```python
   # See what's available for your schedule
   models = client.metadata.get_model_health(schedule_name)
   tests = client.metadata.get_test_results(schedule_name)
   print(f"Models: {len(models)}, Tests: {len(tests)}")
   ```

2. **Debug Queries:**
   ```python
   # Test with a simple query first
   result = client.metadata.query_sql("SELECT COUNT(*) as count FROM models_with_tests")
   print(result)
   ```

3. **Inspect Data Structure:**
   ```python
   models = client.metadata.get_model_health(schedule_name)
   if models:
       model = models[0]
       print(f"Model attributes: {dir(model)}")
   ```

## Next Steps

Once you're comfortable with the basics:

1. **Create Custom Dashboards** - Use the SDK data to build monitoring dashboards
2. **Automate Quality Checks** - Set up scheduled analysis for continuous monitoring  
3. **Build CI/CD Integration** - Add quality gates to your deployment pipeline
4. **Extend Analysis** - Create domain-specific analysis for your use cases

The Paradime Metadata SDK provides the foundation for comprehensive dbt project analysis - explore the advanced examples to see what's possible!