# Paradime Metadata SDK

The Paradime Metadata SDK provides dbt Discovery API feature parity for comprehensive metadata analysis and health monitoring.

## Features

- **Model Health Monitoring**: Track execution status, test results, and performance metrics
- **Multi-Command Artifact Collection**: Intelligently gather artifacts from all commands in a schedule run
- **Upstream/Downstream Analysis**: Analyze model dependencies and impact assessment  
- **Test Result Tracking**: Monitor data quality tests and failures
- **Source Freshness**: Check data source staleness and timeliness
- **Flexible Querying**: Raw SQL interface plus predefined analytics methods
- **DuckDB Backend**: Fast in-memory analytics with optional persistence

## Quick Start

```python
from paradime import Paradime

# Initialize client
client = Paradime(
    api_key="your_api_key",
    api_secret="your_api_secret", 
    api_endpoint="your_api_endpoint"
)

# Get health dashboard
dashboard = client.metadata.get_health_dashboard("prod_daily")
print(f"Healthy models: {dashboard.healthy_models}/{dashboard.total_models}")

# Get model health details
models = client.metadata.get_model_health("prod_daily")
for model in models:
    print(f"{model.name}: {model.health_status}")

# Check upstream dependencies
upstream = client.metadata.get_upstream_model_health("customer_ltv", "prod_daily")
for dep in upstream:
    print(f"L{dep.level}: {dep.name} ({dep.health_status})")
```

## Core Methods

### Health Monitoring
- `get_model_health(schedule_name)` - Model execution status and test results
- `get_health_dashboard(schedule_name)` - Comprehensive health metrics
- `get_models_with_failing_tests(schedule_name)` - Models with test failures

### Test Analysis  
- `get_test_results(schedule_name, failed_only=False)` - Test execution results
- Data quality monitoring and failure tracking

### Source Freshness
- `get_source_freshness(schedule_name)` - Source data staleness check
- Freshness criteria validation

### Dependency Analysis
- `get_upstream_model_health(model_name, schedule_name)` - Upstream dependency health
- `get_downstream_impact(model_name, schedule_name)` - Downstream impact analysis
- Dependency graph traversal with configurable depth

### Performance Analytics
- `get_slowest_models(schedule_name, limit=10)` - Performance bottleneck identification
- `get_performance_metrics(schedule_name, days=7)` - Execution time trends

### Custom Querying
- `query_sql(sql, schedule_name, parameters)` - Raw SQL against DuckDB metadata
- Full SQL analytics capabilities with pandas DataFrame results

## Architecture

### Multi-Command Artifact Collection
```
Schedule Run → Multiple Commands → Smart Artifact Collection
    ├── dbt deps (no artifacts)
    ├── dbt run (manifest.json, run_results.json) 
    ├── dbt test (run_results.json)
    └── dbt source freshness (sources.json)
                ↓
        Merge & Parse → DuckDB → Query Interface
```

### Data Flow
1. **Fetch**: Use enhanced Bolt SDK to collect artifacts from all relevant commands
2. **Parse**: dbt-artifacts-parser + backend analytics logic for type-safe processing  
3. **Load**: Transform data into DuckDB tables matching backend BigQuery schema
4. **Query**: Multiple interfaces from SQL to predefined dashboard methods

### Database Schema
- `dbt_run_results` - Model/test execution results with metadata
- `dbt_source_freshness_results` - Source freshness validation
- `model_metadata` - Model definitions, dependencies, and configuration
- `models_with_tests` (view) - Aggregated model health with test results

## Dependencies
- `dbt-artifacts-parser` - Type-safe dbt artifact parsing
- `duckdb` - In-memory analytics database  
- `pandas` - Data manipulation and analysis

## Examples

See `examples/metadata_usage.py` for comprehensive usage examples including:
- Health monitoring dashboards
- Dependency impact analysis
- Custom SQL analytics
- Performance optimization
- Alert-ready health checks

## Integration with Existing Backend

The metadata SDK reuses proven parsing logic from `backend/core/schedule/analytics.py` while providing:
- Client-side flexibility for custom analytics
- Real-time artifact processing
- SQL querying capabilities
- dbt Discovery API compatibility

This enables applications like ZScaler's health monitoring Streamlit app to have comprehensive visibility into dbt project health and dependencies.