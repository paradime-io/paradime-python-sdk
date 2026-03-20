#!/usr/bin/env python3
"""
Single Model Status Example

This example shows how to fetch the status of a specific model and its tests:
- Get model execution status
- Check associated test results
- View model details and performance
"""
import os

from paradime.client.paradime_client import Paradime


def main() -> None:
    print("🏥 Single Model Status Check")
    print("=" * 30)

    # Initialize client
    client = Paradime(
        api_key=os.getenv("PARADIME_API_KEY") or "",
        api_secret=os.getenv("PARADIME_API_SECRET") or "",
        api_endpoint=os.getenv("PARADIME_API_ENDPOINT") or "",
    )

    try:
        # Change these to your schedule and model names
        schedule_name = "daily prod run"
        model_name = "customer_orders"  # Change to your model name

        print(f"\n🔍 Checking model: {model_name}")
        print(f"   Schedule: {schedule_name}")

        # Get all models and tests
        models = client.metadata.get_model_health(schedule_name)
        tests = client.metadata.get_test_results(schedule_name)

        # Find the specific model
        target_model = None
        for model in models:
            if model.name == model_name:
                target_model = model
                break

        if not target_model:
            print(f"\n❌ Model '{model_name}' not found")
            print("\nAvailable models:")
            for model in models[:5]:  # Show first 5
                print(f"  • {model.name}")
            return

        # Display model status
        print("\n📊 Model Details:")
        print(f"  Name: {target_model.name}")
        print(f"  Database: {target_model.database_name}")
        print(f"  Schema: {target_model.schema_name}")
        print(f"  Materialization: {target_model.materialized_type}")

        # Model execution status
        print("\n🏥 Model Status:")
        if target_model.status == "success":
            print("  Status: ✅ Success")
        elif target_model.status == "error":
            print("  Status: ❌ Error")
        elif target_model.status == "fail":
            print("  Status: ❌ Failed")
        else:
            print(f"  Status: ❓ {target_model.status}")

        # Error message if any
        if target_model.error_message:
            print(f"  Error: {target_model.error_message}")

        # Performance information
        print("\n⏱️ Performance:")
        if target_model.execution_time:
            print(f"  Execution time: {target_model.execution_time:.1f}s")
        else:
            print("  Execution time: Unknown")

        if target_model.executed_at:
            print(f"  Last run: {target_model.executed_at}")

        # Test information
        print("\n🧪 Tests Overview:")
        print(f"  Total tests: {target_model.total_tests}")
        print(f"  Failed tests: {target_model.failed_tests}")
        if target_model.total_tests > 0:
            pass_rate = (
                (target_model.total_tests - target_model.failed_tests) / target_model.total_tests
            ) * 100
            print(f"  Pass rate: {pass_rate:.1f}%")

        # Find tests for this model
        model_tests = []
        for test in tests:
            # Check if this test depends on our model
            if target_model.unique_id in (test.depends_on_nodes or []):
                model_tests.append(test)

        if model_tests:
            print(f"\n🧪 Individual Test Results ({len(model_tests)} tests):")
            for test in model_tests:
                if test.status == "pass":
                    status_icon = "✅"
                elif test.status in ["fail", "error"]:
                    status_icon = "❌"
                else:
                    status_icon = "❓"

                print(f"  {status_icon} {test.test_name}")
                if test.status in ["fail", "error"] and test.error_message:
                    error_preview = (
                        test.error_message[:80] + "..."
                        if len(test.error_message) > 80
                        else test.error_message
                    )
                    print(f"      Error: {error_preview}")
        else:
            print("\n🧪 No tests found for this model")

        # Dependencies and children
        print("\n🕸️ Lineage:")
        print(f"  Dependencies: {len(target_model.depends_on or [])}")
        print(f"  Children: {len(target_model.children_l1 or [])}")

        if target_model.depends_on:
            print("  Depends on:")
            for dep in target_model.depends_on[:3]:  # Show first 3
                dep_name = dep.split(".")[-1] if "." in dep else dep
                print(f"    • {dep_name}")
            if len(target_model.depends_on) > 3:
                print(f"    ... and {len(target_model.depends_on) - 3} more")

        # Additional metadata
        if target_model.description:
            print("\n📝 Description:")
            print(
                f"  {target_model.description[:100]}{'...' if len(target_model.description) > 100 else ''}"
            )

        if target_model.owner:
            print(f"\n👤 Owner: {target_model.owner}")

        if target_model.tags:
            print(f"\n🏷️ Tags: {', '.join(target_model.tags)}")

        print("\n✅ Model check complete!")

    except Exception as e:
        print(f"❌ Error: {e}")
        print("\nMake sure to:")
        print("• Set your API credentials")
        print("• Update the schedule_name and model_name variables")

    finally:
        client.metadata.close()


if __name__ == "__main__":
    main()
