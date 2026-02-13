"""
Example usage of the Paradime Metadata SDK for dbt health monitoring and analysis.

This example demonstrates how to use the metadata API to:
- Get model health status
- Monitor test results
- Check source freshness
- Analyze upstream dependencies
- Get health dashboard metrics
- Execute custom SQL queries
"""

from paradime import Paradime


def main():
    # Initialize Paradime client
    client = Paradime(
        api_key="your_api_key",
        api_secret="your_api_secret", 
        api_endpoint="your_api_endpoint"
    )
    
    # Schedule name to analyze
    schedule_name = "prod_daily"
    
    print(f"🔍 Analyzing metadata for schedule: {schedule_name}")
    
    # 1. Get overall health dashboard
    print("\n📊 Health Dashboard:")
    dashboard = client.metadata.get_health_dashboard(schedule_name)
    print(f"Total Models: {dashboard.total_models}")
    print(f"✅ Healthy: {dashboard.healthy_models}")
    print(f"⚠️ Warning: {dashboard.warning_models}")
    print(f"🔴 Critical: {dashboard.critical_models}")
    print(f"📈 Avg Execution Time: {dashboard.avg_execution_time:.2f}s")
    print(f"🧪 Test Success Rate: {dashboard.test_success_rate:.1f}%")
    
    # 2. Get model health details
    print("\n🏗️ Model Health Status:")
    model_health = client.metadata.get_model_health(schedule_name)
    for model in model_health[:5]:  # Show first 5 models
        status_emoji = "🔴" if model.health_status == "Critical" else "⚠️" if model.health_status == "Warning" else "✅"
        print(f"{status_emoji} {model.name}: {model.status} ({model.execution_time}s) - {model.failed_tests}/{model.total_tests} tests failed")
    
    # 3. Check for models with failing tests
    print("\n🚨 Models with Failing Tests:")
    failing_models = client.metadata.get_models_with_failing_tests(schedule_name)
    for model in failing_models:
        print(f"⚠️ {model.name}: {model.failed_tests} failed tests")
    
    # 4. Get test results
    print("\n🧪 Failed Tests:")
    failed_tests = client.metadata.get_test_results(schedule_name, failed_only=True)
    for test in failed_tests[:3]:  # Show first 3 failed tests
        print(f"❌ {test.test_name}: {test.status}")
        if test.error_message:
            print(f"   Error: {test.error_message[:100]}...")
    
    # 5. Check source freshness
    print("\n📊 Source Freshness:")
    source_freshness = client.metadata.get_source_freshness(schedule_name)
    for source in source_freshness:
        status_emoji = "🔴" if source.freshness_status == "error" else "⚠️" if source.freshness_status == "warn" else "✅"
        hours_since = source.hours_since_load or 0
        print(f"{status_emoji} {source.source_name}.{source.table_name}: {source.freshness_status} ({hours_since:.1f}h ago)")
    
    # 6. Analyze upstream dependencies for a critical model
    print("\n🔗 Upstream Dependency Analysis:")
    critical_model = "customer_churn_prediction"  # Example model name
    try:
        upstream_deps = client.metadata.get_upstream_model_health(critical_model, schedule_name)
        print(f"Upstream dependencies for {critical_model}:")
        for dep in upstream_deps:
            status_emoji = "🔴" if dep.health_status == "Critical" else "⚠️" if dep.health_status == "Warning" else "✅"
            indent = "  " * dep.level
            print(f"{indent}{status_emoji} L{dep.level}: {dep.name} ({dep.status})")
    except Exception as e:
        print(f"Could not analyze upstream dependencies: {e}")
    
    # 7. Get performance metrics
    print("\n⚡ Performance Metrics:")
    slowest_models = client.metadata.get_slowest_models(schedule_name, limit=5)
    for i, model in enumerate(slowest_models, 1):
        print(f"{i}. {model.name}: {model.execution_time:.2f}s")
    
    # 8. Custom SQL query example
    print("\n🔍 Custom SQL Analysis - Models by Schema:")
    try:
        schema_analysis = client.metadata.query_sql("""
            SELECT 
                schema_name,
                COUNT(*) as model_count,
                AVG(execution_time) as avg_execution_time,
                COUNT(CASE WHEN health_status = 'Critical' THEN 1 END) as critical_models
            FROM models_with_tests
            WHERE schedule_name = ?
            GROUP BY schema_name
            ORDER BY critical_models DESC, avg_execution_time DESC
        """, schedule_name, parameters=[schedule_name])
        
        for row in schema_analysis.itertuples(index=False):
            print(f"📁 {row.schema_name}: {row.model_count} models, {row.avg_execution_time:.2f}s avg, {row.critical_models} critical")
    
    except Exception as e:
        print(f"Custom query failed: {e}")
    
    # 9. Downstream impact analysis
    print("\n📈 Downstream Impact Analysis:")
    failed_model = "raw_customer_events"  # Example failed model
    try:
        impact = client.metadata.get_downstream_impact(failed_model, schedule_name)
        print(f"Impact of failed model '{failed_model}':")
        print(f"🚨 {len(impact.critical_models)} critically affected")
        print(f"⚠️ {len(impact.warning_models)} potentially affected") 
        print(f"📊 {impact.total_affected} total downstream models")
    except Exception as e:
        print(f"Could not analyze downstream impact: {e}")


if __name__ == "__main__":
    main()