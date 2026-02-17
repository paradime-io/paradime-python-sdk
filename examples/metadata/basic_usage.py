#!/usr/bin/env python3
"""
Simple Metadata SDK Example

This example shows basic usage of the Paradime Metadata SDK:
- Get model status and health
- Check test results
- Query source freshness
- Run custom SQL queries
"""
import os

from paradime.client.paradime_client import Paradime


def main() -> None:
    print("🚀 Simple Metadata SDK Example")
    print("=" * 32)

    # Initialize client
    client = Paradime(
        api_key=os.getenv("PARADIME_API_KEY") or "",
        api_secret=os.getenv("PARADIME_API_SECRET") or "",
        api_endpoint=os.getenv("PARADIME_API_ENDPOINT") or "",
    )

    try:
        # Change this to your schedule name
        schedule_name = "daily prod run"
        print(f"\n📊 Analyzing: {schedule_name}")

        # 1. Get model health
        print("\n🏥 Model Health:")
        models = client.metadata.get_model_health(schedule_name)
        success_count = sum(1 for m in models if m.status == "success")
        print(f"  Total models: {len(models)}")
        print(f"  Successful: {success_count}")
        print(f"  Success rate: {(success_count/len(models)*100):.1f}%")

        # Show slowest models
        models_with_time = [m for m in models if m.execution_time is not None]
        if models_with_time:
            slowest = sorted(models_with_time, key=lambda x: x.execution_time or 0, reverse=True)[
                :3
            ]
            print("\n  ⏱️ Slowest models:")
            for model in slowest:
                print(f"    {model.name}: {model.execution_time:.1f}s")

        # 2. Get test results
        print("\n🧪 Test Results:")
        tests = client.metadata.get_test_results(schedule_name)
        passed_tests = sum(1 for t in tests if t.status == "pass")
        print(f"  Total tests: {len(tests)}")
        print(f"  Passed: {passed_tests}")
        if tests:
            print(f"  Pass rate: {(passed_tests/len(tests)*100):.1f}%")

        # 3. Check source freshness
        print("\n🕒 Source Freshness:")
        sources = client.metadata.get_source_freshness(schedule_name)
        fresh_sources = sum(1 for s in sources if s.freshness_status == "pass")
        print(f"  Total sources: {len(sources)}")
        print(f"  Fresh: {fresh_sources}")

        # 4. Run custom SQL query
        print("\n🗃️ Custom Query (Models by schema):")
        query = """
            SELECT schema_name, COUNT(*) as model_count
            FROM models_with_tests 
            WHERE resource_type = 'model'
            GROUP BY schema_name 
            ORDER BY model_count DESC
            LIMIT 5
        """

        results = client.metadata.query_sql(query, schedule_name)
        for _, row in results.iterrows():
            print(f"  {row['schema_name']}: {row['model_count']} models")

        print("\n✅ Analysis complete!")

    except Exception as e:
        print(f"❌ Error: {e}")
        print("\nMake sure to set your API credentials:")
        print("export PARADIME_API_KEY='your-key'")
        print("export PARADIME_API_SECRET='your-secret'")
        print("export PARADIME_API_ENDPOINT='your-endpoint'")

    finally:
        client.metadata.close()


if __name__ == "__main__":
    main()
