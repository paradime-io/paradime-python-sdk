# Paradime Metadata SDK Examples

This folder contains comprehensive examples demonstrating how to use the Paradime Metadata SDK for dbt project analysis. These examples showcase text-based analytics and reporting capabilities across five key areas of dbt project monitoring and optimization.

## Overview

The Paradime Metadata SDK provides powerful capabilities for analyzing your dbt projects, enabling comprehensive metadata analysis, performance monitoring, and quality assessment with flexible local analysis and custom reporting.

## Getting Started

If you're new to the Paradime Metadata SDK, we recommend this progression:

1. **Read** [`GETTING_STARTED.md`](GETTING_STARTED.md) for detailed setup instructions
2. **Run** `basic_usage.py` for hands-on SDK introduction  
3. **Explore** the advanced analysis examples

### Quick Start

### Basic Usage (`basic_usage.py`)

A simple introduction showing fundamental SDK concepts and basic analysis patterns. Perfect for getting started before exploring the advanced examples.

**Key Features**:
- SDK initialization and authentication
- Basic model, test, and source queries
- Simple analysis patterns
- Custom SQL query examples
- JSON data export and file saving
- Data availability checking

**Sample Output**:
```
🚀 Paradime Metadata SDK - Basic Usage
📊 BASIC MODEL ANALYSIS
   📈 Total Models: 43
   ✅ Successful: 43 (100.0% success rate)
   
🧪 BASIC TEST ANALYSIS  
   📋 Total Tests: 32
   ✅ Passed: 32 (100.0% pass rate)

📄 JSON DATA EXPORT
   📊 Models exported to JSON format
   💾 Ready for API integration and file export
```

**Run the example**:
```bash
python examples/metadata/basic_usage.py
```

## Analysis Examples

### 1. List and Browse Examples

#### List Models (`list_models.py`)

Browse and explore all models in your dbt project with filtering and search capabilities.

**Key Features**:
- Complete model listing with status and metadata
- Search and filter capabilities
- Model type and materialization overview
- Quick project overview

#### List Sources (`list_sources.py`)

Discover and browse all data sources in your dbt project.

**Key Features**:
- Complete source listing with freshness status
- Source table enumeration
- Freshness status overview
- Source health summary

### 2. Detailed Status Examples

#### Single Model Status (`single_model_status.py`)

Deep dive analysis of a specific model including tests, performance, and dependencies.

**Key Features**:
- Model execution details and performance
- Associated test results and failures
- Dependency mapping and lineage
- Detailed model metadata

#### Single Source Status (`single_source_status.py`)

Comprehensive analysis of a specific data source including all tables and freshness status.

**Key Features**:
- Source table enumeration with status
- Freshness thresholds and timing
- Source health overview
- Table-level metadata

### 3. Advanced Analysis Examples

#### Performance Analysis (`performance_analysis.py`)

Analyzes model execution performance, identifying bottlenecks and optimization opportunities.

**Key Features**:
- Model execution time analysis
- Performance by materialization type
- Slowest models identification
- Schema-level performance metrics
- Custom SQL performance queries

**Sample Output**:
```
📊 MODEL EXECUTION TIME ANALYSIS
   Total Models: 43
   Total Execution Time: 390.2s (6.5m)
   Average Execution Time: 9.1s
   
   🐌 TOP 10 SLOWEST MODELS:
    1. stg_query_history_enriched: 64.0s | incremental
    2. stg_cost_per_query: 49.1s | incremental
```

#### Quality Monitoring (`quality_monitoring.py`)

Comprehensive data quality monitoring and test analysis.

**Key Features**:
- Failed models and tests analysis
- Source freshness monitoring
- Test coverage analysis
- Data quality scoring
- Quality improvement recommendations

**Sample Output**:
```
🔍 QUALITY MONITORING
   Models: 43 total, 0 failed (0.0% failure rate)
   Tests: 32 total, 0 failed (0.0% failure rate)
   
🎯 DATA QUALITY SCORE: 83.1%
   Model Health: 100.0% | Test Success: 100.0%
   Source Freshness: 52.9% | Test Coverage: 25.6%
```

#### Discovery Insights (`discovery_insights.py`)

Dataset discovery, documentation analysis, and lineage exploration.

**Key Features**:
- Dataset documentation analysis
- Model-level lineage analysis
- Ancestry and dependency mapping
- Data catalog functionality
- Project lineage insights

**Sample Output**:
```
📚 DATASET DOCUMENTATION
   Found 43 models with documentation
   
🕸️ MODEL-LEVEL LINEAGE ANALYSIS
   Total Nodes: 69 (Models: 43, Sources: 17, Seeds: 3)
   
🌟 MOST CONNECTED MODELS:
   • warehouse_utilization_reccs: 3 deps, 11 children
```

## Setup and Usage

### Prerequisites

1. Install the Paradime Python SDK with metadata dependencies:
```bash
poetry add "paradime-io[metadata]"
# or
pip install "paradime-io[metadata]"
```

2. Set up your Paradime API credentials:
```bash
export PARADIME_API_KEY="your-api-key"
export PARADIME_API_SECRET="your-api-secret" 
export PARADIME_API_ENDPOINT="your-api-endpoint"
```

### Running Examples

Each example can be run independently:

```bash
# Start with basic usage
python examples/metadata/basic_usage.py

# List and browse examples
python examples/metadata/list_models.py
python examples/metadata/list_sources.py

# Detailed status examples  
python examples/metadata/single_model_status.py
python examples/metadata/single_source_status.py

# Advanced analysis examples
python examples/metadata/performance_analysis.py
python examples/metadata/quality_monitoring.py
python examples/metadata/discovery_insights.py
```

Or with poetry:
```bash
poetry run python examples/metadata/performance_analysis.py
```

### Configuration

All examples use the schedule name `'daily prod run'`. To analyze your own dbt project, update the schedule name in each example:

```python
# Change this line in each example
schedule_name = 'your-schedule-name'
```

## Analysis Capabilities

Each example provides comprehensive analysis across different aspects of your dbt project:

| Example | Key Analysis Areas |
|---------|-------------------|
| List Models | Model browsing, filtering, status overview, project discovery |
| List Sources | Source discovery, freshness overview, table enumeration |
| Single Model Status | Individual model deep dive, test details, performance metrics |
| Single Source Status | Individual source analysis, table status, freshness details |
| Performance Analysis | Model execution times, bottlenecks, materialization performance |
| Quality Monitoring | Test failures, source freshness, coverage analysis |
| Discovery Insights | Lineage mapping, documentation coverage, dependency analysis |

## Key Features

### Text-Based Analysis
All examples focus on text-based reporting and analysis, avoiding complex visualizations while providing comprehensive insights through clear, actionable reports.

### Complete Resource Support
The SDK supports all dbt resource types:
- **Models**: Full execution and metadata analysis
- **Tests**: Complete test result and coverage analysis
- **Sources**: Freshness monitoring and dependency tracking
- **Seeds**: Data profiling and lineage analysis
- **Snapshots**: Evolution tracking and performance analysis
- **Exposures**: Downstream usage and impact analysis

### Custom SQL Queries
Each example includes custom SQL queries demonstrating how to extend the analysis using the SDK's DuckDB backend for advanced reporting and custom metrics.

### Performance Optimized
Uses optimized views and indexes for fast analysis of large dbt projects, ensuring efficient processing of extensive metadata.

## Understanding the Output

### Health Status Indicators
- 🟢 **Healthy**: Models running successfully with no test failures
- 🟡 **Warning**: Models with test failures but successful execution
- 🔴 **Critical**: Models with execution failures

### Quality Grades
- **A (90-100%)**: Excellent governance and quality
- **B (80-89%)**: Good quality with minor improvements needed
- **C (70-79%)**: Adequate quality, some attention required
- **D (60-69%)**: Poor quality, significant improvements needed
- **F (<60%)**: Critical issues requiring immediate attention

## Advanced Usage

### Custom Analysis
Extend any example with custom analysis:

```python
# Access the underlying database for custom queries
custom_query = """
    SELECT name, execution_time, status
    FROM models_with_tests 
    WHERE execution_time > 30
    ORDER BY execution_time DESC
"""
results = client.metadata.query_sql(custom_query)
```

### Batch Analysis
Process multiple schedules:

```python
schedules = ['project-a', 'project-b', 'project-c']
for schedule in schedules:
    analyzer = PerformanceAnalyzer(client.metadata)
    analysis = analyzer.get_execution_time_analysis(schedule)
    # Process results...
```

## Troubleshooting

### Common Issues

1. **Schedule Not Found**: Verify your schedule name exists in your Paradime workspace
2. **Empty Results**: Ensure your dbt project has run recently and generated metadata
3. **Missing Dependencies**: Install with `poetry add "paradime-io[metadata]"`

### Getting Help

- Check the main SDK documentation
- Verify your API credentials are correctly set
- Ensure your schedule has recent run data
- Review the error messages for specific guidance

## Next Steps

1. **Customize Analysis**: Modify the examples for your specific use cases
2. **Automate Reporting**: Set up scheduled runs for regular project monitoring
3. **Integrate with CI/CD**: Add quality gates based on the analysis results
4. **Build Dashboards**: Use the SDK data to build custom monitoring dashboards
5. **Extend Functionality**: Add new analysis patterns based on your team's needs

These examples demonstrate the full power of the Paradime Metadata SDK for comprehensive dbt project analysis, providing Discovery API equivalent functionality with the flexibility of local analysis and custom reporting.