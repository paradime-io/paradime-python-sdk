#!/usr/bin/env python3
"""
List Available Models

This helper script lists all available models in your schedule
to help you find the correct model_name for other examples.
"""
import os

from paradime.client.paradime_client import Paradime


def main() -> None:
    print("📋 List Available Models")
    print("=" * 26)

    # Initialize client
    client = Paradime(
        api_key=os.getenv("PARADIME_API_KEY") or "",
        api_secret=os.getenv("PARADIME_API_SECRET") or "",
        api_endpoint=os.getenv("PARADIME_API_ENDPOINT") or "",
    )

    try:
        # Change this to your schedule name
        schedule_name = "daily prod run"

        print(f"\n🔍 Listing models for: {schedule_name}")

        # Get all models
        models = client.metadata.get_model_health(schedule_name)

        if not models:
            print("\n❌ No models found")
            return

        print(f"\n📊 Found {len(models)} models:")
        print()

        # Group models by schema
        models_by_schema: dict[str, list] = {}
        for model in models:
            schema = model.schema_name or "unknown"
            if schema not in models_by_schema:
                models_by_schema[schema] = []
            models_by_schema[schema].append(model)

        # Display grouped models
        for schema_name, schema_models in models_by_schema.items():
            print(f"📁 Schema: {schema_name}")
            print(f"   Models ({len(schema_models)}):")

            for model in sorted(schema_models, key=lambda x: x.name):
                status_icon = (
                    "✅"
                    if model.status == "success"
                    else "❌" if model.status in ["error", "fail"] else "❓"
                )
                timing = f" ({model.execution_time:.1f}s)" if model.execution_time else ""
                print(f"     {status_icon} {model.name}{timing}")
                print(f"        Database: {model.database_name}")
                print(f"        Materialization: {model.materialized_type}")
                print(f"        Status: {model.status}")
                print(f"        Tests: {model.total_tests} total, {model.failed_tests} failed")
                if model.owner:
                    print(f"        Owner: {model.owner}")
                if model.description:
                    desc = (
                        model.description[:60] + "..."
                        if len(model.description) > 60
                        else model.description
                    )
                    print(f"        Description: {desc}")
                print()
            print()

        # Show example usage
        if models:
            example_model = models[0]
            print("💡 Example usage in single_model_status.py:")
            print(f'   schedule_name = "{schedule_name}"')
            print(f'   model_name = "{example_model.name}"')

        print("\n✅ Model listing complete!")

    except Exception as e:
        print(f"❌ Error: {e}")
        print("\nMake sure to:")
        print("• Set your API credentials")
        print("• Update the schedule_name variable")

    finally:
        client.metadata.close()


if __name__ == "__main__":
    main()
