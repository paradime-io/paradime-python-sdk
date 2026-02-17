#!/usr/bin/env python3
"""
Simple Quality Monitoring Example

This example shows how to monitor data quality:
- Test results and failures
- Model health status
- Source freshness issues
"""
import os

from paradime.client.paradime_client import Paradime


def main() -> None:
    print("🏥 Simple Quality Monitoring")
    print("=" * 29)

    # Initialize client
    client = Paradime(
        api_key=os.getenv("PARADIME_API_KEY") or "",
        api_secret=os.getenv("PARADIME_API_SECRET") or "",
        api_endpoint=os.getenv("PARADIME_API_ENDPOINT") or "",
    )

    try:
        # Change this to your schedule name
        schedule_name = "daily prod run"
        print(f"\n🏥 Monitoring: {schedule_name}")

        # Get data
        models = client.metadata.get_model_health(schedule_name)
        tests = client.metadata.get_test_results(schedule_name)
        sources = client.metadata.get_source_freshness(schedule_name)

        print("\n📊 Overview:")
        print(f"  Models: {len(models)}")
        print(f"  Tests: {len(tests)}")
        print(f"  Sources: {len(sources)}")

        # 1. Model health check
        print("\n🏥 Model Health:")
        failed_models = [m for m in models if m.status in ["error", "fail"]]
        success_models = [m for m in models if m.status == "success"]

        print(f"  Successful: {len(success_models)}")
        print(f"  Failed: {len(failed_models)}")

        if failed_models:
            print("\n  Failed models:")
            for model in failed_models[:3]:
                error = model.error_message or "No error message"
                print(f"    • {model.name}: {error[:50]}...")

        # 2. Test results
        print("\n🧪 Test Results:")
        failed_tests = [t for t in tests if t.status in ["error", "fail"]]
        passed_tests = [t for t in tests if t.status == "pass"]

        print(f"  Passed: {len(passed_tests)}")
        print(f"  Failed: {len(failed_tests)}")

        if tests:
            pass_rate = (len(passed_tests) / len(tests)) * 100
            print(f"  Pass rate: {pass_rate:.1f}%")

        if failed_tests:
            print("\n  Failed tests:")
            for test in failed_tests[:3]:
                error = test.error_message or "No error message"
                print(f"    • {test.test_name}: {error[:50]}...")

        # 3. Source freshness
        print("\n🕒 Source Freshness:")
        fresh_sources = [s for s in sources if s.freshness_status == "pass"]
        stale_sources = [s for s in sources if s.freshness_status == "error"]
        warning_sources = [s for s in sources if s.freshness_status == "warn"]

        print(f"  Fresh: {len(fresh_sources)}")
        print(f"  Stale: {len(stale_sources)}")
        print(f"  Warning: {len(warning_sources)}")

        if stale_sources:
            print("\n  Stale sources:")
            for source in stale_sources[:3]:
                hours = source.hours_since_load or 0
                print(f"    • {source.source_name}.{source.name}: {hours:.1f}h old")

        # 4. Overall health score
        print("\n📊 Quality Health Score:")
        total_issues = len(failed_models) + len(failed_tests) + len(stale_sources)
        total_items = len(models) + len(tests) + len(sources)

        if total_items > 0:
            health_score = ((total_items - total_issues) / total_items) * 100
            print(f"  Overall health: {health_score:.1f}%")

            if health_score >= 95:
                print("  Status: ✅ Excellent")
            elif health_score >= 85:
                print("  Status: 🟢 Good")
            elif health_score >= 75:
                print("  Status: 🟡 Fair")
            else:
                print("  Status: ❌ Poor")

        # 5. Test coverage analysis
        print("\n🧪 Test Coverage:")
        models_with_tests = [m for m in models if m.total_tests > 0]
        coverage_rate = (len(models_with_tests) / len(models) * 100) if models else 0
        print(f"  Models with tests: {len(models_with_tests)} ({coverage_rate:.1f}%)")
        print(f"  Models without tests: {len(models) - len(models_with_tests)}")

        # 6. Simple recommendations
        print("\n💡 Recommendations:")
        if failed_models:
            print("  • Fix failed models immediately")
        if failed_tests:
            print("  • Address failing tests")
        if stale_sources:
            print("  • Check stale data sources")
        if coverage_rate < 80:
            print("  • Add tests to untested models")

        print("\n✅ Monitoring complete!")

    except Exception as e:
        print(f"❌ Error: {e}")
        print("\nMake sure to set your API credentials and check the schedule name.")

    finally:
        client.metadata.close()


if __name__ == "__main__":
    main()
